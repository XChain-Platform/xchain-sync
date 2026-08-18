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
 *   - signing_pubkey_id rotation on a surviving contract_stakes row, applied when a
 *     DELEGATE v1 delegation matures (CONTRACT_DELEGATION_MATERIALIZE) and found via
 *     the contract_delegation_rotations journal, which is block-streamed and carries
 *     block_index. Forward twin of ClientRollback's reverse key restore.
 *   - request_status flip on a surviving v0 attests / xcalls request row, stamped
 *     with resolved_block = the resolving block.
 *   - cooldown-maturity status_id flip on surviving unstakes / contract_unstakes
 *     (markCooldownsCompleted sets status_id = 'completed' in place at the maturity
 *     block), keyed by cooldown_end_block in [fromBlock, toBlock]. Forward twin
 *     of ClientRollback's reverse status reset and cooldownCredits.js's forward
 *     refund-credit selection (same maturity-block key).
 *   - invalid_archive stamp on a surviving anchor_actions archive-head (v1/v6) parent
 *     row when the completing v2 chunk of its batch lands in this window and CRC
 *     fails. The parent's action_index is below the window, so the action-scoped
 *     stream misses it; this is keyed by the completing chunk's block_index instead.
 *     Forward twin of ClientRollback's reverse 'unverified' reset. status_id is not
 *     hashed raw; the follower's upsert resolves it by name through index_statuses.
 *   - supply refresh on a surviving tokens row (the indexer UPDATEs tokens.supply in
 *     place on DEPLOY / ISSUE / MINT / settlement / STAKE-rebalance). Both
 *     action_index and last_action_index stay pinned at the DEPLOY action, below the
 *     cursor, so the action-scoped stream misses every later supply bump. Found via
 *     the ticks touched by a credit / debit / escrow row in this window, since those
 *     ledger tables are action-scoped and pin the supply change to a block.
 *
 * tokens.escrow_action_index rides along here (the tokens class selects `t.*`), so the
 * source's own authoritative gate value lands on the replica. The follower ALSO
 * re-derives it from the already-replicated offer/status tables whenever a payload
 * touches an escrow-relevant table (ClientApplier._maybeRederiveEscrow), so the wire
 * value is a convergent carry and the local derive is the corrective pass rather than
 * the sole writer. That derive is triggered only by payload.data tables, never by
 * updated_rows itself.
 *
 ********************************************************************/

const { ARCHIVE_HEAD_VERSIONS_SQL, ARCHIVE_CHUNK_HEIGHT_COL } = require('./stateHash');

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

// Stake-ledger tables the DELEGATE v1 materialization sweep rewrites signing_pubkey_id on
// (CONTRACT_DELEGATION_MATERIALIZE). Each rewrite is journaled in contract_delegation_rotations
// with the table it landed on, exactly as the slash debits are, so both directions (this
// forward carry and ClientRollback's reverse restore) key the same way.
const ROTATION_TABLES = ['contract_stakes', 'contract_unstakes'];

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

// BET in-place flips: a surviving bet_feeds row is mutated in place by
// the closed latch (closed_block stamp, end-of-block pass) and by the terminal flip
// (terminal_block stamp: resolve tx / cancel tx / BET_EXPIRE pass); a surviving bets
// row is mutated by settlement (settled_block stamp: won/lost/refunded). Each class
// is keyed by its stamp landing in the window, mirroring (forward) the exact
// ClientRollback resets and the state_hash bet_feed_status/bet_status classes.
// status_id is not hashed raw; the follower's upsert resolves it via the replicated
// index_statuses table.
const BET_STATUS_SPECS = [
    { table: 'bet_feeds', stamps: ['closed_block', 'terminal_block'] },
    { table: 'bets',      stamps: ['settled_block'] }
];

// Returns a { tableName: [rows] } map of non-empty tables. Rows are raw DB rows; the
// caller wire-encodes binary columns. `db` must be an indexer-dbType Database (callers
// gate that). A null `activationDelay` SKIPS the deactivation_block class rather than
// scanning with a wrong delay, matching ClientRollback's caution when no coin is known.
// Passing `conn` lets a snapshot's REPEATABLE READ view read these at the same height
// as the rest of its payload.
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

    // 2b. DELEGATE v1 signing-key rotations materialized onto surviving contract_stakes rows.
    //     Same shape as the slash class: the mutated row's action_index is below the window
    //     (the STAKE happened earlier), so only its journal entry pins the change to a block.
    //     Without this the follower keeps the pre-rotation key and hands contracts a different
    //     staker set than the source (and slashes a key the source no longer carries).
    for(let rotTbl of ROTATION_TABLES){
        try {
            let rows = await db.doQuery(
                "SELECT t.* FROM `" + rotTbl + "` t " +
                "JOIN `contract_delegation_rotations` r ON r.stake_action_index = t.action_index " +
                "WHERE r.target_table = ? AND r.block_index BETWEEN ? AND ?",
                [rotTbl, from, to], conn);
            add(rotTbl, rows);
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
    //    The flip lands at the maturity block, so cooldown_end_block in [from, to] is
    //    the key, with no activation-delay offset unlike the deactivation_block stamp.
    //    add() dedups by the UNIQUE action_index against any SLASH row for the same row.
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

    // 4b. BET status flips on surviving bet_feeds / bets rows, keyed on the stamp
    //     columns landing in [from, to] (a feed can latch AND go terminal in the
    //     same window; the OR plus the Map dedup emits its row once). Carries the
    //     full current row so the follower's upsert refreshes feed_status_id /
    //     bet_status_id and the stamps in place. Tables may not exist on older
    //     source schemas (pre-BET builds); skip like the classes above.
    for(let spec of BET_STATUS_SPECS){
        try {
            let where = spec.stamps.map(col => "`" + col + "` BETWEEN ? AND ?").join(' OR ');
            let args  = [];
            for(let i = 0; i < spec.stamps.length; i++){ args.push(from); args.push(to); }
            let rows = await db.doQuery(
                "SELECT * FROM `" + spec.table + "` WHERE " + where, args, conn);
            add(spec.table, rows);
        } catch(e){
            if(e && typeof e.errno === 'number' && e.errno !== 1146 && e.errno !== 1054) throw e;
            // Table/columns may not exist on older source schemas; skip.
        }
    }

    // 5. invalid_archive stamp on surviving anchor_actions archive-head parent rows
    //    (v1 legacy, v6 publisher-bearing; ARCHIVE_HEAD_VERSIONS in stateHash.js). When
    //    the final v2 chunk of a chunked archive batch lands and the reassembled blob
    //    fails CRC, the parent is stamped 'invalid_archive' in place. Chunking spans
    //    blocks by design, so the parent's action_index is in an earlier block and the
    //    action-scoped stream carries the chunk row but not the parent's flipped status.
    //    The self-join keyed on the completing chunk's height mirrors ClientRollback's
    //    reverse 'unverified' reset predicate. Table may not exist on schemas without
    //    ANCHOR support.
    //    HEIGHT KEY: `block_index_doge` (shared ARCHIVE_CHUNK_HEIGHT_COL), the DOGE
    //    block the completing chunk landed in. This class used to key on
    //    `c.block_index`, which a v2 continuation row NEVER populates (it carries the
    //    CHECKPOINTED height and only anchor.js `_parseCheckpoint` assigns it), so
    //    `NULL BETWEEN from AND to` was never true and the class shipped ZERO rows: a
    //    follower never received the stamped parent at all. UN-GATED, unlike the
    //    state-hash twin of this class: shipping the row is not a hash preimage, and
    //    this fix must be live BEFORE the state-hash flag day or the follower halts on
    //    a parent row it was never sent.
    try {
        let anchorRows = await db.doQuery(
            "SELECT DISTINCT p.* FROM anchor_actions p " +
            "JOIN anchor_actions c ON c.version = 2 AND c.match_batch_seq = p.match_batch_seq " +
            "JOIN index_statuses ps ON ps.id = p.status_id AND ps.status = 'invalid_archive' " +
            "JOIN index_statuses cs ON cs.id = c.status_id AND cs.status = 'valid' " +
            "WHERE p.version " + ARCHIVE_HEAD_VERSIONS_SQL + " AND " + ARCHIVE_CHUNK_HEIGHT_COL + " BETWEEN ? AND ?",
            [from, to], conn);
        add('anchor_actions', anchorRows);
    } catch(e){
        if(e && typeof e.errno === 'number' && e.errno !== 1146 && e.errno !== 1054) throw e;
        // Table/columns may not exist on older source schemas; skip.
    }

    // 6. tokens.supply refresh on surviving token rows. The indexer materialises supply
    //    as an in-place UPDATE on DEPLOY/ISSUE/MINT, on settlement and on STAKE
    //    rebalances, while action_index and last_action_index both stay pinned at the
    //    DEPLOY action, below the catch-up cursor, so the action-scoped stream never
    //    carries a later supply bump and followers served a stale supply. Supply moves
    //    exactly when a credit / debit / escrow row is written for the tick, and those
    //    ledger tables ARE action-scoped, so the ticks touched in [from, to] are exactly
    //    the set to refresh. Carrying the full current row makes the follower's upsert
    //    land on the matching PRIMARY KEY and overwrite supply, idempotently. Reorg-safe
    //    because the source re-materialises supply on rollback and the next forward
    //    window's ledger changes re-emit the row, and block apply is in order.
    //    tokens.supply is in no consensus block hash, but it does have a state_hash twin
    //    (buildStateHashData's token_supply class, flag-day gated per chain), so once
    //    armed a follower that drops this upsert halts instead of serving stale supply.
    try {
        // Join each ledger table to `actions` independently and UNION the tick_ids rather
        // than UNION ALL-ing the three full tables into a derived table. MariaDB cannot
        // push `a.block_index BETWEEN ? AND ?` down into a UNION ALL, so the derived-table
        // form materialises every credits/debits/escrows row first, an O(total ledger)
        // scan on every window. Per-branch joins let the optimiser drive from `actions`
        // into each table via its action_index index. UNION, not UNION ALL, preserves the
        // original SELECT DISTINCT semantics, so the emitted tick set is unchanged.
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

module.exports = { collectUpdatedRows, DEACTIVATION_TABLES, SLASH_SPECS, ROTATION_TABLES, REQUEST_STATUS_TABLES, COOLDOWN_STATUS_TABLES, POLL_FINALIZE_TABLES, BET_STATUS_SPECS };
