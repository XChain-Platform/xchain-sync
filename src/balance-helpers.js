/*********************************************************************
 *
 * Copyright © 2025–2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC – https://dankest.llc
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This file is part of XChain Platform. Licensed under the GNU Affero
 * General Public License v3.0 or later; see LICENSE.md. A commercial
 * license (without AGPL source-disclosure terms) is available —
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
 * re-aggregation are limited to those ids instead of the whole table — the
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
// '1.2345678901234567e19' — the source indexer holds the exact string).
const SUM_LEDGER = "SUM(CASE WHEN t.type = 'credit' THEN CAST(t.amount AS DECIMAL(65,18)) ELSE -CAST(t.amount AS DECIMAL(65,18)) END)";

// Render a DECIMAL(65,18) aggregate the way the source indexer writes amounts
// (mathjs bignumber String(): minimal decimal — '600', '10.5'), so a rebuilt
// row is byte-identical to the source row, not just numerically equal.
// CAST(decimal AS CHAR) always emits all 18 decimals, so the '.' is always
// present to stop the zero-trim eating integer digits.
function minimalDecimal(sumExpr) {
    return "TRIM(TRAILING '.' FROM TRIM(TRAILING '0' FROM CAST(" + sumExpr + ' AS CHAR)))';
}

// Recompute the balances table from the surviving credits/debits rows.
// scope (optional): { addressIds: [...], tickIds: [...] } — limits the
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

module.exports = { rebuildBalances };
