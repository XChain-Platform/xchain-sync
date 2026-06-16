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
 * XChain Sync - In-place "updated rows" channel (source side)
 *
 * The per-block payload and the incremental snapshot both scope rows by
 * action_index (rows whose creating action falls in the block/catch-up window).
 * That captures INSERTs and mutations to rows created inside the window, but it
 * can never carry an in-place mutation the indexer makes to a SURVIVING row —
 * one created by an earlier (below-window) action and later mutated in place.
 * Those mutations were silently dropped on every follower from bootstrap onward
 * (no UPDATE path on the apply side, no hash coverage to detect the gap).
 *
 * This module collects, for a block window [fromBlock, toBlock], the CURRENT
 * full state of every surviving row mutated in place during that window, keyed
 * by the table's natural row identity (action_index, which is UNIQUE on every
 * affected table). The follower applies them with INSERT … ON DUPLICATE KEY
 * UPDATE (see ClientApplier._upsertRows), so re-sending a row already current is
 * a harmless no-op. The detection mirrors — in the forward direction — the exact
 * reorg-reset predicates ClientRollback already runs (which themselves mirror
 * xchain-indexer/src/rollback.js), so source and follower converge byte-for-byte.
 *
 * Covered in-place mutation classes (all indexer-only):
 *   - deactivation_block stamp on stakes / delegations / contract_stakes /
 *     contract_delegations (set on UNSTAKE / DELEGATE-revoke to
 *     actionBlock + ACTIVATION_DELAY_BLOCKS — so a stamp landed in this window
 *     iff deactivation_block ∈ [fromBlock+delay, toBlock+delay], an indexed
 *     range scan).
 *   - amount reduction on contract_stakes / contract_unstakes (contract SLASH)
 *     and stakes / unstakes (capability SLASH) — found via the per-row slash
 *     debit logs (contract_slash_debits / capability_slash_debits), which are
 *     themselves block-streamed and carry block_index.
 *   - request_status flip on a surviving v0 attests / xcalls request row, stamped
 *     with resolved_block = the resolving block.
 *   - cooldown-maturity status_id flip on surviving unstakes / contract_unstakes
 *     (markCooldownsCompleted sets status_id = 'completed' in place at the maturity
 *     block) — keyed by cooldown_end_block ∈ [fromBlock, toBlock]. The forward twin
 *     of ClientRollback's reverse status reset and of cooldownCredits.js's forward
 *     refund-credit selection (same maturity-block key).
 *
 * tokens.escrow_action_index is intentionally NOT carried here: it is re-derived
 * on the follower from the already-replicated offer/status tables (the same
 * re-derive ClientRollback runs on reorg), so it needs no wire field — see
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

// Surviving unstake rows whose status_id was flipped to 'completed' in place when their
// cooldown matured (markCooldownsCompleted). Keyed by cooldown_end_block — the maturity
// block — exactly as ClientRollback's reverse reset and cooldownCredits.js's forward
// credit select. action_index is UNIQUE on both, so the follower's upsert lands cleanly.
const COOLDOWN_STATUS_TABLES = ['unstakes', 'contract_unstakes'];

// Collect the in-place-mutated surviving rows for the block window [fromBlock, toBlock].
// Returns a { tableName: [rows] } map (only non-empty tables). Rows are raw DB rows —
// the caller is responsible for wire-encoding binary columns (encodeRow / encodeTables).
//
//   db              the source Database (indexer dbType only — callers must gate)
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

    // 1. deactivation_block stamps — indexed range scan on each table. A stamp of
    //    value V was written by an action in block V - delay, so a stamp landed in
    //    [from, to] iff V ∈ [from+delay, to+delay]. Skipped when delay is unknown.
    if(activationDelay != null){
        for(let table of DEACTIVATION_TABLES){
            try {
                let rows = await db.doQuery(
                    "SELECT * FROM `" + table + "` WHERE deactivation_block IS NOT NULL AND deactivation_block BETWEEN ? AND ?",
                    [from + activationDelay, to + activationDelay], conn);
                add(table, rows);
            } catch(e){
                // Table/column may not exist on older source schemas — skip.
            }
        }
    }

    // 2. SLASH amount reductions — join the slashed stake/unstake row to its debit
    //    log entry for this window. No DISTINCT: the add() Map dedups by action_index
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
            // Table may not exist on older source schemas — skip.
        }
    }

    // 3. request_status flips on surviving v0 attest/xcall request rows. Keyed on
    //    resolved_block (the resolving block stamp) — captures both the response and
    //    the deadline-expiry flip paths, mirroring ClientRollback's reset key.
    for(let table of REQUEST_STATUS_TABLES){
        try {
            let rows = await db.doQuery(
                "SELECT * FROM `" + table + "` WHERE version = 0 AND resolved_block BETWEEN ? AND ?",
                [from, to], conn);
            add(table, rows);
        } catch(e){
            // Table/column may not exist on older source schemas — skip.
        }
    }

    // 4. cooldown-maturity status_id flip on surviving unstakes / contract_unstakes.
    //    markCooldownsCompleted flips status_id to 'completed' in place on a row whose
    //    creating action is in an earlier block, so the action-scoped stream misses it.
    //    The flip lands at the maturity block, so it falls in [from, to] iff
    //    cooldown_end_block ∈ [from, to] (no activation-delay offset — unlike the
    //    deactivation_block stamp). Carry the current row state, mirroring how the
    //    deactivation_block class carries the surviving stamped row. add() dedups by
    //    the UNIQUE action_index against any SLASH row for the same unstake.
    for(let table of COOLDOWN_STATUS_TABLES){
        try {
            let rows = await db.doQuery(
                "SELECT * FROM `" + table + "` WHERE cooldown_end_block BETWEEN ? AND ?",
                [from, to], conn);
            add(table, rows);
        } catch(e){
            // Table/column may not exist on older source schemas — skip.
        }
    }

    let out = {};
    for(let table in acc){
        let arr = Array.from(acc[table].values());
        if(arr.length > 0) out[table] = arr;
    }
    return out;
}

module.exports = { collectUpdatedRows, DEACTIVATION_TABLES, SLASH_SPECS, REQUEST_STATUS_TABLES, COOLDOWN_STATUS_TABLES };
