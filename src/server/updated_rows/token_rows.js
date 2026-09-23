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
 * XChain Sync - Updated rows: the two tokens-table classes
 *
 ********************************************************************/

const { add } = require('./accumulator.js');

// The two classes that refresh a surviving `tokens` row, split out of the entry because
// they share one shape: the row's action_index is pinned at the tick's first issuance, so
// nothing the tick does later moves it back into the action-scoped stream's window. They
// differ only in what pins the mutation to a block - a ledger row for supply, an `issues`
// row for the metadata. Each adds to `acc` in place, like every other class here.

async function collectTokenSupplyRows(db, from, to, conn, acc){
    // 6. tokens.supply refresh on surviving token rows. The indexer materialises
    //    tokens.supply as an in-place UPDATE (db.createToken on DEPLOY/ISSUE/MINT and
    //    db.updateTokens after order/swap/dispense settlement and STAKE rebalances). The
    //    row's action_index stays at the DEPLOY action, and last_action_index is also
    //    written back to that same DEPLOY index (createToken sets both from the first
    //    valid issuance), so BOTH columns sit below the catch-up cursor: the
    //    action-scoped stream keyed on action_index never carries the later supply bump.
    //    Followers therefore served a stale supply (invisible to /status counts and not
    //    covered by any hash). Supply changes exactly when a credit / debit / escrow row
    //    is written for the tick, and those ledger tables ARE action-scoped (they ride the
    //    per-block / catch-up stream). So the set of ticks whose supply moved in this
    //    window is exactly the set of tick_ids touched by a credit / debit / escrow row
    //    whose action falls in [from, to]. We carry the CURRENT full tokens row for those
    //    ticks (SELECT t.* -> the source `id`, which followers replicate verbatim, so the
    //    follower's INSERT ... ON DUPLICATE KEY UPDATE lands on the matching PRIMARY KEY
    //    row and overwrites supply to the source's current value). Idempotent: re-sending
    //    an already-current row is a no-op. Reorg-safe: on rollback the source
    //    re-materialises supply (rollback.js -> updateTokens) and the next forward window's
    //    ledger changes re-emit the refreshed row; in-order block apply means a later
    //    window's row never lands before an earlier one. tokens.supply stays out of the
    //    consensus block hashes, but since 2026-07-07 this class HAS a state_hash twin:
    //    buildStateHashData's token_supply class hashes (tick, supply) for the same
    //    ledger-touched tick set (flag-day gated per chain via
    //    TOKEN_SUPPLY_STATE_HASH_ACTIVATION), so once armed, a follower that drops this
    //    upsert halts at the block instead of serving a stale supply.
    try {
        // Join each ledger table to `actions` independently and UNION the tick_ids,
        // rather than UNION ALL-ing the three full tables into a derived table and
        // joining once. The derived-table form forces MariaDB to materialise every
        // credits/debits/escrows row before the block-range predicate can apply (it
        // cannot push `a.block_index BETWEEN ? AND ?` down into the UNION ALL), an
        // O(total ledger size) scan on every block/catch-up window. Per-branch joins
        // let the optimiser drive from `actions` (block_index range) into each table
        // via its action_index index. UNION (not UNION ALL) preserves the original
        // SELECT DISTINCT semantics, so the emitted tick set is byte-identical.
        let tokenRows = await db.findLedgerTouchedTokens(from, to, conn);
        add(acc, 'tokens', tokenRows);
    } catch(e){ if(e && typeof e.errno === 'number' && e.errno !== 1146 && e.errno !== 1054) throw e; }
}

async function collectTokenEditRows(db, from, to, conn, acc){
    // 7. tokens metadata refresh on surviving token rows, class 6's sibling one column
    //    family over. Every valid ISSUE re-derives the WHOLE tokens row from the tick's
    //    `issues` history (issue/settle.js -> createToken, then updateTokens ->
    //    getTokenInfo's replay), so an ISSUE that EDITS an existing tick is an in-place
    //    UPDATE of owner_id, description, the seven locks, the callback and list fields,
    //    the mint window and the bridge opt-in. action_index and last_action_index both
    //    stay pinned at the FIRST issuance, so - exactly as with supply - the
    //    action-scoped stream carries the new `issues` row but never the edited `tokens`
    //    row it produced.
    //
    //    Class 6 hid this for every edit that also moves a balance (its ledger-touched
    //    tick set catches those), which is why it surfaced as an intermittent bug rather
    //    than a permanent one: a fee-free edit that writes NO credit / debit / escrow row
    //    in its own tick left the replica on the pre-edit row until some unrelated later
    //    action on the tick moved a balance and re-emitted it. An ownership TRANSFER
    //    (`ISSUE|0|<TICK>||||||<new owner>`) is exactly that shape, so explorer read APIs
    //    served the OLD owner indefinitely while consensus had the new one
    //    until this class ships (ownership-gated client UI reads this field).
    //
    //    Keyed on the tick's valid `issues` rows in the window rather than on the ISSUE
    //    action alone: `issues` stores every ISSUE, valid or not, and only a valid one
    //    reaches createToken, so an invalid edit must not re-emit (harmless, but it would
    //    make the class claim a mutation that never happened). SELECT t.* and the
    //    add()-by-action_index dedup are class 6's, so a tick reached by both classes in
    //    one window emits once.
    //
    //    FORWARD ONLY, deliberately. A reorg that orphans an edit-ISSUE leaves no valid
    //    `issues` row behind for the tick, so nothing re-emits the row and the follower
    //    keeps the orphaned edit's values while the source re-folds back (rollback.js ->
    //    updateTokens). That reverse leg needs a replica-side re-derive beside the escrow
    //    one in ClientRollback and is not built here.
    //
    //    UN-GATED, like classes 5 and 5b: shipping a row is not a hash preimage, and no
    //    state_hash class covers these columns (the token_supply twin hashes (tick, supply)
    //    only), so a follower that never receives the edit diverges silently instead of
    //    halting. That is the gap this closes, and it must be live before any future
    //    state-hash twin arms or a follower would halt on a row it was never sent.
    try {
        let tokenRows = await db.doQuery(
            "SELECT t.* FROM `tokens` t WHERE t.tick_id IN (" +
                "SELECT i.tick_id FROM issues i " +
                    "JOIN actions a ON a.action_index = i.action_index " +
                    "JOIN index_statuses s ON s.id = i.status_id AND s.status = 'valid' " +
                    "WHERE a.block_index BETWEEN ? AND ? AND i.tick_id IS NOT NULL)",
            [from, to], conn);
        add(acc, 'tokens', tokenRows);
    } catch(e){ if(e && typeof e.errno === 'number' && e.errno !== 1146 && e.errno !== 1054) throw e; }
}
module.exports = { collectTokenSupplyRows, collectTokenEditRows };
