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
 * XChain Sync - In-place "updated rows" channel (source side)
 *
 * The per-block payload and the incremental snapshot both scope rows by
 * action_index (rows whose creating action falls in the block/catch-up window).
 * That captures INSERTs and mutations to rows created inside the window, but it
 * cannot carry an in-place mutation the indexer makes to a SURVIVING row (one
 * created by an earlier, below-window action and later mutated in place).
 * Those mutations were silently dropped on every follower from bootstrap onward
 * (no UPDATE path on the apply side, no hash coverage to detect the gap).
 *
 * This module collects, for a block window [fromBlock, toBlock], the CURRENT
 * full state of every surviving row mutated in place during that window, keyed
 * by the table's natural row identity (action_index, which is UNIQUE on every
 * affected table). The follower applies them with INSERT ... ON DUPLICATE KEY
 * UPDATE (see ClientApplier._upsertRows), so re-sending a row already current is
 * a harmless no-op. The detection mirrors (in the forward direction) the exact
 * reorg-reset predicates ClientRollback already runs (which themselves mirror
 * xchain-indexer/src/rollback.js), so source and follower converge byte-for-byte.
 *
 * Covered in-place mutation classes (all indexer-only):
 *   - deactivation_block stamp on stakes / delegations / contract_stakes /
 *     contract_delegations (set on UNSTAKE / DELEGATE-revoke to
 *     actionBlock + ACTIVATION_DELAY_BLOCKS; a stamp lands in this window
 *     iff deactivation_block in [fromBlock+delay, toBlock+delay], an indexed
 *     range scan).
 *   - amount reduction on contract_stakes / contract_unstakes (contract SLASH)
 *     and stakes / unstakes (capability SLASH), found via the per-row slash
 *     debit logs (contract_slash_debits / capability_slash_debits), which are
 *     themselves block-streamed and carry block_index.
 *   - request_status flip on a surviving v0 attests / xcalls request row, stamped
 *     with resolved_block = the resolving block.
 *   - cooldown-maturity status_id flip on surviving unstakes / contract_unstakes
 *     (markCooldownsCompleted sets status_id = 'completed' in place at the maturity
 *     block), keyed by cooldown_end_block in [fromBlock, toBlock]. Forward twin
 *     of ClientRollback's reverse status reset and cooldownCredits.js's forward
 *     refund-credit selection (same maturity-block key).
 *   - invalid_archive stamp on a surviving anchor_actions v1 parent row when the
 *     completing v2 chunk of its batch lands in this window and CRC fails. The
 *     parent's action_index is below the window, so the action-scoped stream misses
 *     it. Keyed by the completing v2 chunk's block_index (in [fromBlock, toBlock]).
 *     Forward twin of ClientRollback's reverse 'unverified' reset (which joins on
 *     exactly this predicate). status_id is not hashed raw; it is resolved via
 *     status name by the follower's upsert using the replicated index_statuses table.
 *   - supply refresh on a surviving tokens row (db.createToken / db.updateTokens UPDATE
 *     tokens.supply in place on DEPLOY / ISSUE / MINT / settlement / STAKE-rebalance). The
 *     row's action_index AND last_action_index both stay at the DEPLOY action, below the
 *     cursor, so the action-scoped stream misses every later supply bump. Found via the
 *     ticks touched by a credit / debit / escrow row in this window (those ledger tables
 *     are action-scoped, so they pin the supply-change to a block). Carries the full
 *     current tokens row; the follower's upsert overwrites supply on the PRIMARY-KEY match.
 *     NOT mirrored in stateHash.js: supply is deliberately not in any hash preimage.
 *
 * tokens.escrow_action_index is intentionally NOT carried here: it is re-derived
 * on the follower from the already-replicated offer/status tables (the same
 * re-derive ClientRollback runs on reorg), so it needs no wire field. See
 * ClientApplier._maybeRederiveEscrow.
 *
 ********************************************************************/

// Tables carrying the deactivation_block stamp (value-threshold detection).
const DEACTIVATION_TABLES = ['stakes', 'delegations', 'contract_stakes', 'contract_delegations'];

// SLASH amount reductions: each surviving stake/unstake row whose amount was cut
// is reachable by joining its action_index to the slash debit log for this window.
// target_table is the literal the indexer writes (createContractSlashDebit /
// createCapabilitySlashDebit), matching ClientRollback's restore JOIN.
const SLASH_SPECS = [
    { table: 'contract_stakes',   debits: 'contract_slash_debits',  target: 'contract_stakes'   },
    { table: 'contract_unstakes', debits: 'contract_slash_debits',  target: 'contract_unstakes' },
    { table: 'stakes',            debits: 'capability_slash_debits', target: 'stakes'            },
    { table: 'unstakes',          debits: 'capability_slash_debits', target: 'unstakes'          }
];

// v0 request rows whose request_status went terminal in this window (resolved_block stamp).
const REQUEST_STATUS_TABLES = ['attests', 'xcalls'];

// VOTE polls whose finalization went terminal in this window. The per-block sweep
// flips a surviving polls row (created at the v0 block, below the window) from
// 'open' to 'finalized'/'failed_quorum' IN PLACE, stamping resolved_block; the
// action-scoped stream carries the v2's poll_results rows but not this summary
// flip. Forward twin of ClientRollback's polls re-open reset (same key).
const POLL_FINALIZE_TABLES = ['polls'];

// Surviving unstake rows whose status_id was flipped to 'completed' in place when their
// cooldown matured (markCooldownsCompleted). Keyed by cooldown_end_block (the maturity
// block), exactly as ClientRollback's reverse reset and cooldownCredits.js's forward
// credit select. action_index is UNIQUE on both, so the follower's upsert lands cleanly.
const COOLDOWN_STATUS_TABLES = ['unstakes', 'contract_unstakes'];

// Collect the in-place-mutated surviving rows for the block window [fromBlock, toBlock].
// Returns a { tableName: [rows] } map (only non-empty tables). Rows are raw DB rows;
// the caller is responsible for wire-encoding binary columns (encodeRow / encodeTables).
//
//   db              the source Database (indexer dbType only; callers must gate)
//   fromBlock       inclusive lower block bound of the window
//   toBlock         inclusive upper block bound of the window
//   activationDelay frozen per-chain ACTIVATION_DELAY_BLOCKS; null skips the
//                   deactivation_block class (matching ClientRollback's caution
//                   when no coin is known) rather than scanning with a wrong delay
//   conn            optional connection (so a snapshot's REPEATABLE READ view reads
//                   the updated rows at the same height as the rest of the payload)
async function collectUpdatedRows(db, fromBlock, toBlock, activationDelay, conn){
    let from = Number(fromBlock);
    let to   = Number(toBlock);

    // table -> Map(action_index -> row). The Map dedups rows reached by more than
    // one class (e.g. a stake both deactivated and slashed in the same window) by
    // their UNIQUE action_index, so each table emits each surviving row once.
    let acc = {};
    function add(table, rows){
        if(!rows || rows.length === 0) return;
        let m = acc[table] || (acc[table] = new Map());
        for(let r of rows){
            if(r && r.action_index !== undefined && r.action_index !== null)
                m.set(String(r.action_index), r);
        }
    }

    // 1. deactivation_block stamps: indexed range scan on each table. A stamp of
    //    value V was written by an action in block V - delay, so a stamp landed in
    //    [from, to] iff V in [from+delay, to+delay]. Skipped when delay is unknown.
    if(activationDelay != null){
        for(let table of DEACTIVATION_TABLES){
            try {
                let rows = await db.doQuery(
                    "SELECT * FROM `" + table + "` WHERE deactivation_block IS NOT NULL AND deactivation_block BETWEEN ? AND ?",
                    [from + activationDelay, to + activationDelay], conn);
                add(table, rows);
            } catch(e){
                if(e && typeof e.errno === 'number' && e.errno !== 1146 && e.errno !== 1054) throw e;
                // Table/column may not exist on older source schemas; skip.
            }
        }
    }

    // 2. SLASH amount reductions: join the slashed stake/unstake row to its debit
    //    log entry for this window. No DISTINCT; the add() Map dedups by action_index
    //    (avoids DISTINCT over wide/blob columns).
    for(let spec of SLASH_SPECS){
        try {
            let rows = await db.doQuery(
                "SELECT t.* FROM `" + spec.table + "` t " +
                "JOIN `" + spec.debits + "` d ON d.stake_action_index = t.action_index " +
                "WHERE d.target_table = ? AND d.block_index BETWEEN ? AND ?",
                [spec.target, from, to], conn);
            add(spec.table, rows);
        } catch(e){
            if(e && typeof e.errno === 'number' && e.errno !== 1146 && e.errno !== 1054) throw e;
            // Table may not exist on older source schemas; skip.
        }
    }

    // 3. request_status flips on surviving v0 attest/xcall request rows. Keyed on
    //    resolved_block (the resolving block stamp), which captures both the response
    //    and the deadline-expiry flip paths, mirroring ClientRollback's reset key.
    for(let table of REQUEST_STATUS_TABLES){
        try {
            let rows = await db.doQuery(
                "SELECT * FROM `" + table + "` WHERE version = 0 AND resolved_block BETWEEN ? AND ?",
                [from, to], conn);
            add(table, rows);
        } catch(e){
            if(e && typeof e.errno === 'number' && e.errno !== 1146 && e.errno !== 1054) throw e;
            // Table/column may not exist on older source schemas; skip.
        }
    }

    // 3b. VOTE poll finalization flip on surviving polls rows. Keyed on
    //     resolved_block (stamped by the finalize sweep), which captures both the
    //     end_block close and the early-decide path. No version predicate: polls
    //     has one row shape (the v0 create), unlike attests/xcalls.
    for(let table of POLL_FINALIZE_TABLES){
        try {
            let rows = await db.doQuery(
                "SELECT * FROM `" + table + "` WHERE resolved_block BETWEEN ? AND ?",
                [from, to], conn);
            add(table, rows);
        } catch(e){
            if(e && typeof e.errno === 'number' && e.errno !== 1146 && e.errno !== 1054) throw e;
            // Table/column may not exist on older source schemas; skip.
        }
    }

    // 4. cooldown-maturity status_id flip on surviving unstakes / contract_unstakes.
    //    markCooldownsCompleted flips status_id to 'completed' in place on a row whose
    //    creating action is in an earlier block, so the action-scoped stream misses it.
    //    The flip lands at the maturity block, falling in [from, to] iff
    //    cooldown_end_block in [from, to] (no activation-delay offset, unlike the
    //    deactivation_block stamp). Carries the current row state, mirroring how the
    //    deactivation_block class carries the surviving stamped row. add() dedups by
    //    the UNIQUE action_index against any SLASH row for the same unstake.
    for(let table of COOLDOWN_STATUS_TABLES){
        try {
            let rows = await db.doQuery(
                "SELECT * FROM `" + table + "` WHERE cooldown_end_block BETWEEN ? AND ?",
                [from, to], conn);
            add(table, rows);
        } catch(e){
            if(e && typeof e.errno === 'number' && e.errno !== 1146 && e.errno !== 1054) throw e;
            // Table/column may not exist on older source schemas; skip.
        }
    }

    // 5. invalid_archive stamp on surviving anchor_actions v1 parent rows. When the
    //    final v2 chunk of a chunked archive batch lands and the reassembled blob fails
    //    its CRC check, anchor.js stamps the v1 parent 'invalid_archive' in place. The
    //    parent's action_index is in an earlier block (chunking spans blocks by design),
    //    so the action-scoped stream for the completing chunk's block carries the chunk
    //    row but NOT the parent's flipped status. This class finds those parents via a
    //    self-join to their completing v2 chunk, keyed by the chunk's block_index in
    //    [from, to]. Mirrors ClientRollback's reverse 'unverified' reset predicate.
    //    Carries the full parent row so the follower's upsert refreshes status_id in
    //    place (INSERT ... ON DUPLICATE KEY UPDATE). Table may not exist on schemas
    //    without ANCHOR support.
    try {
        let anchorRows = await db.doQuery(
            "SELECT DISTINCT p.* FROM anchor_actions p " +
            "JOIN anchor_actions c ON c.version = 2 AND c.match_batch_seq = p.match_batch_seq " +
            "JOIN index_statuses ps ON ps.id = p.status_id AND ps.status = 'invalid_archive' " +
            "JOIN index_statuses cs ON cs.id = c.status_id AND cs.status = 'valid' " +
            "WHERE p.version = 1 AND c.block_index BETWEEN ? AND ?",
            [from, to], conn);
        add('anchor_actions', anchorRows);
    } catch(e){
        if(e && typeof e.errno === 'number' && e.errno !== 1146 && e.errno !== 1054) throw e;
        // Table/columns may not exist on older source schemas; skip.
    }

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
        let tokenRows = await db.doQuery(
            "SELECT t.* FROM `tokens` t WHERE t.tick_id IN (" +
                "SELECT c.tick_id FROM credits c JOIN actions a ON a.action_index = c.action_index " +
                    "WHERE a.block_index BETWEEN ? AND ? AND c.tick_id IS NOT NULL " +
                "UNION " +
                "SELECT d.tick_id FROM debits d JOIN actions a ON a.action_index = d.action_index " +
                    "WHERE a.block_index BETWEEN ? AND ? AND d.tick_id IS NOT NULL " +
                "UNION " +
                "SELECT e.tick_id FROM escrows e JOIN actions a ON a.action_index = e.action_index " +
                    "WHERE a.block_index BETWEEN ? AND ? AND e.tick_id IS NOT NULL)",
            [from, to, from, to, from, to], conn);
        add('tokens', tokenRows);
    } catch(e){
        if(e && typeof e.errno === 'number' && e.errno !== 1146 && e.errno !== 1054) throw e;
        // Table/columns may not exist on older source schemas; skip.
    }

    let out = {};
    for(let table in acc){
        let arr = Array.from(acc[table].values());
        if(arr.length > 0) out[table] = arr;
    }
    return out;
}

module.exports = { collectUpdatedRows, DEACTIVATION_TABLES, SLASH_SPECS, REQUEST_STATUS_TABLES, COOLDOWN_STATUS_TABLES, POLL_FINALIZE_TABLES };
