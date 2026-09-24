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
 * UPDATE (see ClientApplier.upsertRows), so re-sending a row already current is
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
 *   - batch-completion verdict stamp on a surviving ATTEST v5 batch head when the v6
 *     continuation that completes its slot coverage lands in this window and the
 *     reassembly or batch quorum fails. The head's action_index is below the window,
 *     so the action-scoped stream carries the chunk but not the head's flipped status;
 *     keyed by the completing continuation's block_index and scoped to one author.
 *   - supply refresh on a surviving tokens row (the indexer UPDATEs tokens.supply in
 *     place on DEPLOY / ISSUE / MINT / settlement / STAKE-rebalance). Both
 *     action_index and last_action_index stay pinned at the DEPLOY action, below the
 *     cursor, so the action-scoped stream misses every later supply bump. Found via
 *     the ticks touched by a credit / debit / escrow row in this window, since those
 *     ledger tables are action-scoped and pin the supply change to a block.
 *   - metadata refresh on a surviving tokens row (the indexer re-derives every
 *     derived token column from the `issues` history on each valid ISSUE, so an
 *     EDIT of an existing tick - ownership TRANSFER, description, the locks, the
 *     callback and list fields, the bridge opt-in - is an in-place UPDATE). Both
 *     action_index and last_action_index stay pinned at the first issuance, below
 *     the cursor, so the action-scoped stream misses the edit. Found via the
 *     ticks carrying a valid `issues` row in this window, since `issues` is
 *     action-scoped and pins the edit to a block.
 *
 * tokens.escrow_action_index rides along here (the tokens class selects `t.*`), so the
 * source's own authoritative gate value lands on the replica. The follower ALSO
 * re-derives it from the already-replicated offer/status tables whenever a payload
 * touches an escrow-relevant table (ClientApplier.maybeRederiveEscrow), so the wire
 * value is a convergent carry and the local derive is the corrective pass rather than
 * the sole writer. That derive is triggered only by payload.data tables, never by
 * updated_rows itself.
 *
 ********************************************************************/

const { ARCHIVE_HEAD_VERSIONS_SQL, ARCHIVE_CHUNK_HEIGHT_COL } = require('../consensus/state_hash');
const {
    DEACTIVATION_TABLES, SLASH_SPECS, ROTATION_TABLES, REQUEST_STATUS_TABLES,
    POLL_FINALIZE_TABLES, COOLDOWN_STATUS_TABLES, ATTEST_BATCH_HEAD_VERSION,
    ATTEST_BATCH_CONTINUATION_VERSION, ATTEST_BATCH_COMPLETION_STAMP, BET_STATUS_SPECS
} = require('./updated_rows/table_specs.js');
const { add } = require('./updated_rows/accumulator.js');
const { collectTokenSupplyRows, collectTokenEditRows } = require('./updated_rows/token_rows.js');

// Query classes skip missing tables or columns on older source schemas. Other
// numeric database errors propagate to the caller.
async function collectDeactivationAndSlashRows(db, from, to, activationDelay, conn, acc){
    // 1. deactivation_block stamps: indexed range scan on each table. A stamp of
    //    value V was written by an action in block V - delay, so a stamp landed in
    //    [from, to] iff V in [from+delay, to+delay]. Skipped when delay is unknown.
    if(activationDelay != null){
        for(let table of DEACTIVATION_TABLES){
            try {
                let rows = await db.findDeactivationStampedRows(table, from + activationDelay, to + activationDelay, conn);
                add(acc, table, rows);
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
            let rows = await db.findSlashDebitedRows(spec, from, to, conn);
            add(acc, spec.table, rows);
        } catch(e){
            if(e && typeof e.errno === 'number' && e.errno !== 1146 && e.errno !== 1054) throw e;
            // Table may not exist on older source schemas; skip.
        }
    }
}

async function collectRotationAndRequestRows(db, from, to, conn, acc){
    // 2b. DELEGATE v1 signing-key rotations materialized onto surviving contract_stakes rows.
    //     Same shape as the slash class: the mutated row's action_index is below the window
    //     (the STAKE happened earlier), so only its journal entry pins the change to a block.
    //     Without this the follower keeps the pre-rotation key and hands contracts a different
    //     staker set than the source (and slashes a key the source no longer carries).
    for(let rotTbl of ROTATION_TABLES){
        try {
            let rows = await db.findRotatedStakeRows(rotTbl, from, to, conn);
            add(acc, rotTbl, rows);
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
            let rows = await db.findResolvedRequestRows(table, from, to, conn);
            add(acc, table, rows);
        } catch(e){
            if(e && typeof e.errno === 'number' && e.errno !== 1146 && e.errno !== 1054) throw e;
            // Table/column may not exist on older source schemas; skip.
        }
    }
}

async function collectPollAndCooldownRows(db, from, to, conn, acc){
    // 3b. VOTE poll finalization flip on surviving polls rows. Keyed on
    //     resolved_block (stamped by the finalize sweep), which captures both the
    //     end_block close and the early-decide path, OR on callback_due_block with a
    //     fired stamp: the deferred binding-callback sweep UPDATEs the surviving row's
    //     callback_execute_action_index at the due block (resolved_block + delay),
    //     above the finalize window, so the first key alone never carried it. One
    //     scan, SELECT * so the follower's upsert refreshes the whole row. No version
    //     predicate: polls has one row shape (the v0 create), unlike attests/xcalls.
    for(let table of POLL_FINALIZE_TABLES){
        try {
            let rows = await db.findFinalizedPollRows(table, from, to, conn);
            add(acc, table, rows);
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
            let rows = await db.findMaturedCooldownRows(table, from, to, conn);
            add(acc, table, rows);
        } catch(e){
            if(e && typeof e.errno === 'number' && e.errno !== 1146 && e.errno !== 1054) throw e;
            // Table/column may not exist on older source schemas; skip.
        }
    }
}

async function collectBetStatusRows(db, from, to, conn, acc){
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
            let rows = await db.findBetStampedRows(spec, where, args, conn);
            add(acc, spec.table, rows);
        } catch(e){
            if(e && typeof e.errno === 'number' && e.errno !== 1146 && e.errno !== 1054) throw e;
            // Table/columns may not exist on older source schemas; skip.
        }
    }
}

async function collectInvalidArchiveRows(db, from, to, conn, acc){
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
    //    block the completing chunk landed in. Keying this class on
    //    `c.block_index` fails: a v2 continuation row NEVER populates it (it carries the
    //    CHECKPOINTED height and only anchor/index.js `parseCheckpoint` assigns it), so
    //    `NULL BETWEEN from AND to` is never true and the class ships ZERO rows: a
    //    follower never receives the stamped parent at all. UN-GATED, unlike the
    //    state-hash twin of this class: shipping the row is not a hash preimage, and
    //    this fix must be live BEFORE the state-hash flag day or the follower halts on
    //    a parent row it was never sent.
    try {
        let anchorRows = await db.findInvalidArchiveHeadRows(from, to, conn);
        add(acc, 'anchor_actions', anchorRows);
    } catch(e){
        if(e && typeof e.errno === 'number' && e.errno !== 1146 && e.errno !== 1054) throw e;
        // Table/columns may not exist on older source schemas; skip.
    }
}

async function collectAttestBatchHeadRows(db, from, to, conn, acc){
    // 5b. ATTEST batch-head verdict flip, the same shape as class 5 one rail over. When the
    //     v6 continuation that completes a batch's slot coverage lands in this window and the
    //     reassembly or the batch quorum fails, the indexer stamps the FAILURE on the v5 HEAD
    //     in place (db.setAttestBatchStatus, called from actions/attest/index.js absorbCompletedBatch).
    //     Chunking spans blocks by design, so the head's action_index is below the window and
    //     the action-scoped stream carries the completing chunk row but not the head's flipped
    //     status: every replica kept the head's PRE-FLIP verdict (still 'valid'), served a batch
    //     the source had condemned, and nothing detected it because the class is unhashed
    //     (stateHash.js reads attests at version = 0 only, so this divergence never halts).
    //
    //     Keyed on the COMPLETING CHUNK's block_index, which attests rows always carry
    //     (NOT NULL, indexed, written by createAttestationBatchAction) - unlike the anchor
    //     rail's v2 continuation, whose block_index is never populated and which therefore
    //     needed the block_index_doge key.
    //
    //     Predicate mirrors, in the forward direction, the indexer's reverse restore
    //     (xchain-indexer/src/rollback.js, the head -> 'valid' reset): head at
    //     batch_chunk_index = 0, a status carrying the completion marker (which is what
    //     separates an after-the-fact stamp from a head that was terminal when written), a
    //     VALID continuation of the same batch key, and the SAME AUTHOR on both rows via
    //     actions.source_id. Author scoping is not an optimisation: a batch key is sha256
    //     over the window it names, so anyone can mint wires under another publisher's key,
    //     and (key, author) has been the batch's identity since the rail shipped. An
    //     unresolvable author on either side is a NULL that no equality matches, so it
    //     authenticates nothing rather than everything (fail closed), matching authoredBy.
    //
    //     Aliases are ah/ac (head, chunk) rather than class 5's p/c: the anchor class's
    //     regression guard is a file-wide regex over this source that forbids the literal
    //     `c.block_index BETWEEN` (never populated on a v2 archive chunk). Distinct aliases
    //     keep that guard sharp on the rail it was written for while letting this rail use
    //     the column that IS populated on its own continuations.
    //
    //     No DISTINCT: attests is a wide table (MEDIUMTEXT payload / response_payload /
    //     batch_chunk_b64), and a head with several completing-shaped continuations would
    //     make MySQL de-duplicate over those blobs. add() already dedups by the UNIQUE
    //     action_index, exactly as the SLASH class does.
    //
    //     UN-GATED for class 5's reason: shipping the row is not a hash preimage, and the
    //     carry must be live before any future state-hash twin of this class arms, or a
    //     follower halts on a head row it was never sent.
    try {
        let attestHeadRows = await db.findFailedAttestBatchHeads(
            ATTEST_BATCH_HEAD_VERSION, ATTEST_BATCH_CONTINUATION_VERSION, ATTEST_BATCH_COMPLETION_STAMP, from, to, conn);
        add(acc, 'attests', attestHeadRows);
    } catch(e){
        if(e && typeof e.errno === 'number' && e.errno !== 1146 && e.errno !== 1054) throw e;
        // Table/columns may not exist on older source schemas (pre-batch-rail builds); skip.
    }
}

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
    let acc  = {};
    await collectDeactivationAndSlashRows(db, from, to, activationDelay, conn, acc);
    await collectRotationAndRequestRows(db, from, to, conn, acc);
    await collectPollAndCooldownRows(db, from, to, conn, acc);
    await collectBetStatusRows(db, from, to, conn, acc);
    await collectInvalidArchiveRows(db, from, to, conn, acc);
    await collectAttestBatchHeadRows(db, from, to, conn, acc);
    await collectTokenSupplyRows(db, from, to, conn, acc);
    await collectTokenEditRows(db, from, to, conn, acc);
    let out = {};
    for(let table in acc){
        let arr = Array.from(acc[table].values());
        if(arr.length > 0) out[table] = arr;
    }
    return out;
}

module.exports = { collectUpdatedRows, DEACTIVATION_TABLES, SLASH_SPECS, ROTATION_TABLES, REQUEST_STATUS_TABLES, COOLDOWN_STATUS_TABLES, POLL_FINALIZE_TABLES, BET_STATUS_SPECS,
                   ATTEST_BATCH_HEAD_VERSION, ATTEST_BATCH_CONTINUATION_VERSION, ATTEST_BATCH_COMPLETION_STAMP };
