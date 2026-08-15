/*********************************************************************
 *
 * Copyright © 2025–2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC – https://dankest.llc
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This file is part of XChain Platform. Licensed under the GNU Affero
 * General Public License v3.0 or later; see LICENSE.md. A commercial
 * license (without AGPL source-disclosure terms) is available -
 * contact legal@dankest.llc.
 *
 **********************************************************************
 *
 * Shared SQL helpers for rebuilding derived aggregate tables after a block
 * apply or rollback.  ClientApplier and ClientRollback both call these so
 * the two code paths cannot silently diverge when balance semantics change.
 *
 * Both helpers accept an optional scope: when the caller knows exactly which
 * (key, tick_id) ids the just-applied rows touched, the DELETE and the
 * re-aggregation are limited to those ids instead of the whole table; the
 * unscoped recompute is O(full history) per call, which on a large mainnet
 * replica means re-summing millions of credit/debit rows for every live
 * block.  The scope is a superset rectangle (key IN (...) AND tick_id IN
 * (...)) rather than exact pairs: recomputing an untouched pair inside the
 * rectangle is harmless (it re-derives the same value) and the single-column
 * IN predicates stay sargable on the per-column indexes, where a row-
 * constructor pair list may not.  No scope = full rebuild (rollback path).
 *
 * Callers are responsible for error handling (e.g. swallowing errno 1146
 * when the tables do not exist on a decoder replica).
 *
 ********************************************************************/

function inList(ids) {
    return '(' + ids.map(() => '?').join(', ') + ')';
}

// All amount arithmetic must run at DECIMAL(65,18): a bare VARCHAR amount in
// numeric context is promoted to DOUBLE, which silently corrupts anything
// past ~16 significant digits (a 20-digit amount came back as
// '1.2345678901234567e19'; the source indexer holds the exact string).
const SUM_LEDGER = "SUM(CASE WHEN t.type = 'credit' THEN CAST(t.amount AS DECIMAL(65,18)) ELSE -CAST(t.amount AS DECIMAL(65,18)) END)";

// Render a DECIMAL(65,18) aggregate the way the source indexer writes amounts
// (mathjs bignumber String(): minimal decimal, e.g. '600', '10.5'), so a rebuilt
// row is byte-identical to the source row, not just numerically equal.
// CAST(decimal AS CHAR) always emits all 18 decimals, so the '.' is always
// present to stop the zero-trim eating integer digits.
function minimalDecimal(sumExpr) {
    return "TRIM(TRAILING '.' FROM TRIM(TRAILING '0' FROM CAST(" + sumExpr + ' AS CHAR)))';
}

// Render a DECIMAL(60,decimals) supply aggregate the way the source indexer writes
// it (mathjs bignumber String(): minimal decimal, e.g. '600', '10.5'), so a
// recomputed row is byte-identical to the source row. For decimals=0 the value has
// no fractional part and CAST(... AS CHAR) emits no '.', so the trailing-zero trim
// would eat integer zeros ('100' -> '1'); emit it verbatim in that case. For
// decimals>0 the CAST always carries the '.' + all scale digits, so trimming
// trailing zeros then the bare '.' is safe.
function minimalSupply(sumExpr, decimals) {
    if (decimals === 0) return 'CAST(' + sumExpr + ' AS CHAR)';
    return "TRIM(TRAILING '.' FROM TRIM(TRAILING '0' FROM CAST(" + sumExpr + ' AS CHAR)))';
}

// Recompute tokens.supply from the surviving credits/debits/escrows after a reorg
// rollback. Logical mirror of xchain-indexer getTokenSupply, so keep the two in step:
// supply = (credits - debits) + escrows.
//
// Each ledger row is summed at the EXACT ledger scale (DECIMAL(60,18)) and the TOTAL
// is rounded ONCE to the token's own decimals, mirroring the indexer's exact-ledger
// rule (XC-1459, xchain-indexer/src/ledger_amount_precision_activation.js). The
// former shape cast each ROW to DECIMAL(60,d) first, which agreed with the indexer
// only while every stored amount already sat on the token's grid; once the indexer
// stores fee amounts finer than the tick (0.5 XCHAIN against a decimals=0 gas tick),
// per-row rounding here inflates a rebuilt supply by up to one unit per row and the
// follower silently diverges from the source it replicates. Rows written before the
// flag-day are on the tick's grid, so this is value-identical for them.
//
// The outer CAST stays at the token's OWN DECIMAL(60, decimals) precision, NOT the
// fixed DECIMAL(65,18) used for balances, because that is the scale the source writes
// its supply at, and byte-identity with the source row is the point. Without this
// whole function a surviving token whose supply was mutated in place by an orphaned
// MINT kept the inflated value until the next full snapshot, while the source's own
// rollback had already corrected it.
//
// A DECIMAL scale cannot be a bind parameter, hence one UPDATE per distinct precision
// clamped to [0,18] (typically 1 or 2). A token with no ledger rows resolves to '0'
// through the LEFT JOIN, matching getTokenSupply's all-zero result.
async function recomputeTokenSupplies(db) {
    let precisions = await db.doQuery('SELECT DISTINCT decimals FROM tokens');
    for (let row of (precisions || [])) {
        let d = Math.max(0, Math.min(18, parseInt(row.decimals) || 0));
        let castAmt = 'CAST(amount AS DECIMAL(60,18))';
        let roundOnce = 'CAST(SUM(amt) AS DECIMAL(60,' + d + '))';
        await db.doQuery(
            'UPDATE tokens tok LEFT JOIN ('
            +   'SELECT tick_id, ' + minimalSupply(roundOnce, d) + ' AS supply FROM ('
            +     'SELECT tick_id,  ' + castAmt + ' AS amt FROM credits '
            +     'UNION ALL '
            +     'SELECT tick_id, -' + castAmt + ' AS amt FROM debits '
            +     'UNION ALL '
            +     'SELECT tick_id,  ' + castAmt + ' AS amt FROM escrows'
            +   ') u GROUP BY tick_id'
            + ') agg ON agg.tick_id = tok.tick_id '
            + "SET tok.supply = COALESCE(agg.supply, '0') "
            + 'WHERE tok.decimals = ?',
            [d]
        );
    }
}

// Recompute the balances table from the surviving credits/debits rows.
// scope (optional): { addressIds: [...], tickIds: [...] }; limits the
// rebuild to those ids; an empty list means nothing was touched (no-op).
async function rebuildBalances(db, scope) {
    let pred = '';
    let args = [];
    if (scope) {
        if (!scope.addressIds.length || !scope.tickIds.length) return;
        pred = ' WHERE address_id IN ' + inList(scope.addressIds)
             + ' AND tick_id IN ' + inList(scope.tickIds);
        args = scope.addressIds.concat(scope.tickIds);
    }
    await db.doQuery('DELETE FROM balances' + pred, args);
    await db.doQuery(`INSERT INTO balances (address_id, tick_id, amount)
        SELECT address_id, tick_id,
            ${minimalDecimal('COALESCE(' + SUM_LEDGER + ', 0)')}
        FROM (
            SELECT address_id, tick_id, amount, 'credit' as type FROM credits${pred}
            UNION ALL
            SELECT address_id, tick_id, amount, 'debit' as type FROM debits${pred}
        ) t
        GROUP BY address_id, tick_id
        HAVING ${SUM_LEDGER} != 0`,
        args.concat(args));
}

module.exports = { rebuildBalances, minimalDecimal, recomputeTokenSupplies };
