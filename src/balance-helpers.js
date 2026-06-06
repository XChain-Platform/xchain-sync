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
 * Callers are responsible for error handling (e.g. swallowing errno 1146
 * when the tables do not exist on a decoder replica).
 *
 ********************************************************************/

// Recompute the balances table from the surviving credits/debits rows.
// The HAVING clause uses no explicit CAST so that non-integer amounts are
// compared correctly; MariaDB implicitly promotes the VARCHAR amounts to
// DECIMAL for the SUM arithmetic.
async function rebuildBalances(db) {
    await db.doQuery("DELETE FROM balances");
    await db.doQuery(`INSERT INTO balances (address_id, tick_id, amount)
        SELECT address_id, tick_id,
            CAST(COALESCE(SUM(CASE WHEN t.type = 'credit' THEN t.amount ELSE -t.amount END), 0) AS CHAR)
        FROM (
            SELECT address_id, tick_id, amount, 'credit' as type FROM credits
            UNION ALL
            SELECT address_id, tick_id, amount, 'debit' as type FROM debits
        ) t
        GROUP BY address_id, tick_id
        HAVING SUM(CASE WHEN t.type = 'credit' THEN t.amount ELSE -t.amount END) != 0`);
}

// Recompute the contract_balances table from the surviving valid
// deposits/withdrawals rows.  Uses DECIMAL(65,18) to preserve fractional
// precision in the HAVING filter; only rows where custody is still positive
// are retained (matches source indexer updateContractBalances behaviour).
async function rebuildContractBalances(db) {
    await db.doQuery("DELETE FROM contract_balances");
    await db.doQuery(`INSERT INTO contract_balances (contract_index, tick_id, amount)
        SELECT contract_index, tick_id,
            CAST(SUM(CASE WHEN t.type = 'deposit' THEN CAST(t.amount AS DECIMAL(65,18)) ELSE -CAST(t.amount AS DECIMAL(65,18)) END) AS CHAR)
        FROM (
            SELECT contract_index, tick_id, amount, 'deposit' as type FROM deposits
                WHERE status_id = (SELECT id FROM index_statuses WHERE status = 'valid')
            UNION ALL
            SELECT contract_index, tick_id, amount, 'withdrawal' as type FROM withdrawals
                WHERE status_id = (SELECT id FROM index_statuses WHERE status = 'valid')
        ) t
        GROUP BY contract_index, tick_id
        HAVING SUM(CASE WHEN t.type = 'deposit' THEN CAST(t.amount AS DECIMAL(65,18)) ELSE -CAST(t.amount AS DECIMAL(65,18)) END) > 0`);
}

module.exports = { rebuildBalances, rebuildContractBalances };
