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
 * XChain Indexer Sync - Client Rollback
 *
 * Handles rolling back the local replica database to a given block.
 * Table lists are copied from xchain-indexer/src/rollback.js and
 * MUST be kept in sync when new tables are added to the indexer.
 * test/unit/rollback-coverage.test.js enforces that the sync set
 * covers every table ServerPoller replicates.
 *
 ********************************************************************/

const balanceHelpers = require('./balance-helpers');
const lifecycle      = require('./tableLifecycle');
const replicatedTables = require('./replicatedTables');
const { activationDelayBlocks, gasTickSymbol } = require('./consensus-constants');
const { ARCHIVE_HEAD_VERSIONS_SQL } = require('./stateHash');

class ClientRollback {

    constructor(db, util, coin) {
        this.db   = db;
        this.util = util;

        // Frozen per-chain STAKING.ACTIVATION_DELAY_BLOCKS, needed to mirror the source
        // indexer's reorg deactivation_block re-NULL resets (see _rollbackIndexer). A wrong
        // or zero delay would wrongly clear legitimately-earned deactivations, so a coin that
        // is supplied but unrecognized is a hard error (real misconfiguration). An omitted
        // coin is legacy/no-op: activationDelay stays null and the deactivation mirror is
        // skipped (with a warning) rather than run with a wrong value. The production wiring
        // (SyncService._startClientSyncForChain) always passes cfg.coin.
        this.coin = coin;
        let delay = activationDelayBlocks(coin); // null if omitted, undefined if unrecognized
        if(delay === undefined){
            throw new Error('ClientRollback: unrecognized coin "' + coin + '" - no frozen ACTIVATION_DELAY_BLOCKS (see src/consensus-constants.js)');
        }
        this.activationDelay = delay;

        // Generic rollback table lists, generated from the table-lifecycle
        // registry (src/tableLifecycle.js, the byte-identical twin of the
        // xchain-indexer copy). replicaRollbackTables() yields exactly the
        // source indexer's generic lists minus indexer-local tables that never
        // exist on a replica (e.g. pending_hub_pushes), so the two rollbacks
        // can no longer drift apart table-by-table: a table added to the
        // registry joins both sides at once. Per-table rationale lives with
        // the registry entries; the bespoke in-place resets/restores below
        // stay hand-written (and remain drift-guarded by the parity tests in
        // test/unit/rollback-coverage.test.js).
        let rollbackLists = lifecycle.replicaRollbackTables();
        this.blockTables  = rollbackLists.blockTables;
        this.indexTables  = rollbackLists.indexTables;
        this.dataTables   = rollbackLists.dataTables;

        // ── Decoder-DB rollback (used by _rollbackDecoder) ──
        // Decoder schema has no actions / balances / sync_meta. Tx-scoped tables
        // are deleted before the block-scoped transactions row that gave them their
        // tx_index scope. index_*/pubkeys/events are append-only and left untouched
        // (the sync stream re-introduces them with INSERT IGNORE).
        //
        // Leaving orphan index_* rows is safe ONLY because their AUTO_INCREMENT ids are
        // purely local artifacts that no longer feed any consensus value: under the
        // current BLOCK_HASH_VERSION the block hashes are computed from the RESOLVED strings
        // (address/tick/action/status), not from address_id/tick_id/etc. (see
        // xchain-indexer/src/db.js getBlockHashes + xchain-sync/src/BlockHasher.js). If a
        // lookup id is ever reintroduced into a consensus-visible projection, these orphan
        // rows would silently fork hashes after a reorg and this skip would become a bug.

        // Block-scoped tables, deleted by block_index. Derived from the decoder
        // replication topology (the same source ServerPoller streams from) so the
        // stream and rollback sides can no longer drift apart table-by-table,
        // mirroring the lifecycle-derived indexer lists above. Order matters:
        // rollback deletes transactions before blocks (tx rows scope the tx-scoped
        // tables above them), while the topology lists blocks first - hence the
        // copy-and-reverse.
        this.decoderBlockTables = [...replicatedTables.getTopology('decoder').blockScoped].reverse();

        // Tx-scoped tables, deleted by tx_index for the rolled-back blocks' transactions.
        // Also topology-derived. dispensers is absent from the topology's txScoped by
        // design: it is not per-block replicated (the decoder live-prunes it, which
        // the block stream can't model (see replicatedTables.js)); it converges via
        // the full snapshot only. Deleting its rows on a reorg would corrupt that
        // full-snapshot state with no live stream to restore them, so a reorg leaves
        // dispensers untouched.
        this.decoderTxScopedTables = [...replicatedTables.getTopology('decoder').txScoped];
    }

    // Roll back all data at or after the given block_index.
    // Branches on db.dbType: decoder has a different table layout (no actions,
    // no balances, tx-scoped tables instead of action-scoped) and no
    // contract_emissions / sync_meta.
    async rollback(block_index){
        let dbType = (this.db && this.db.dbType) || 'indexer';
        if(dbType === 'decoder'){
            return this._rollbackDecoder(block_index);
        }
        return this._rollbackIndexer(block_index);
    }

    // Indexer rollback (original behaviour)
    async _rollbackIndexer(block_index){
        let timer = this.util.startTimer();
        console.log('Starting indexer rollback to block ' + block_index + '...');

        let firstActionIndex = await this.db.getFirstActionIndex(block_index);

        // Truncation floor, read BEFORE the transaction opens (getSyncState may run its
        // one-time CREATE TABLE, and DDL inside a transaction implicitly commits in
        // MariaDB, which would break this rollback's atomicity). A truncated replica
        // holds only [base..tip] of `orders`/`order_matches`, so the pair-scoped market
        // sweep below cannot tell "no order ever existed" from "the order predates my
        // floor"; it is skipped there rather than deleting a market the source keeps.
        // Guarded on the method existing, mirroring ClientSync._persistBootstrapBase,
        // so db instances without the durable store degrade to full-history behaviour.
        let truncatedReplica = false;
        if(this.db && typeof this.db.getSyncState === 'function'){
            let base = await this.db.getSyncState('bootstrap_base:' + ((this.db && this.db.dbType) || 'indexer'));
            truncatedReplica = (base !== null && base !== undefined && parseInt(base, 10) > 0);
        }

        // Pairs whose orders/matches this rollback is about to orphan, collected BEFORE
        // the dataTables delete removes those rows (mirror of the `markets` array the
        // source builds in xchain-indexer/src/rollback.js). Consumed by the IDX-2 sweep
        // further down. NULL tick ids (the native-coin side of a COINPay match) are
        // dropped: markets is keyed on a real tick pair.
        let affectedMarketPairs = [];
        if(firstActionIndex !== null && !truncatedReplica){
            try {
                let rows = await this.db.doQuery(
                    "SELECT DISTINCT give_tick_id AS tick1_id, get_tick_id AS tick2_id FROM orders " +
                    "WHERE action_index >= ? AND give_tick_id IS NOT NULL AND get_tick_id IS NOT NULL " +
                    "UNION " +
                    "SELECT DISTINCT give_tick_id AS tick1_id, get_tick_id AS tick2_id FROM order_matches " +
                    "WHERE action_index >= ? AND give_tick_id IS NOT NULL AND get_tick_id IS NOT NULL",
                    [firstActionIndex, firstActionIndex]);
                for(let row of (rows || [])){
                    let t1 = Number(row.tick1_id), t2 = Number(row.tick2_id);
                    if(!affectedMarketPairs.some(p => (p.tick1_id === t1 && p.tick2_id === t2) ||
                                                     (p.tick1_id === t2 && p.tick2_id === t1)))
                        affectedMarketPairs.push({ tick1_id: t1, tick2_id: t2 });
                }
            } catch(e){
                // Schema-gap errors (missing table/column on older replicas) are safe to skip:
                // the sweep below then has nothing to do. Everything else (deadlock, lock-wait,
                // connection drop) aborts here, before the transaction opens.
                if(e.errno !== 1146 && e.errno !== 1054) throw e;
            }
        }

        await this.db.beginTransaction();
        try {
            // Delete from contract_emissions first (references contract_executions)
            if(firstActionIndex !== null){
                try {
                    await this.db.doQuery(
                        `DELETE FROM contract_emissions WHERE execution_index IN
                            (SELECT action_index FROM contract_executions WHERE action_index >= ?)`,
                        [firstActionIndex]
                    );
                } catch(e){
                    // Only errno 1146 (table missing on older replica schemas) is a
                    // benign skip. Transient/operational faults (deadlock 1213,
                    // lock-wait 1205, connection drop) must abort the reorg-reset so
                    // the outer catch rolls back and retries cleanly, matching every
                    // sibling statement below. Swallowing them here would commit a
                    // partial rollback (orphaned contract_emissions on a consensus table).
                    if(e.errno !== 1146) throw e;
                }
            }

            // ── In-place column resets (mirror xchain-indexer/src/rollback.js) ──
            // The source indexer's rollback runs in-place UPDATEs on SURVIVING rows
            // (action_index < firstActionIndex) to undo stamps that orphaned actions
            // wrote on them. The DELETE loops below only drop orphaned-RANGE rows, so
            // without replaying these resets the replica keeps the stale stamp and
            // diverges from the source (and from a from-genesis replay) after a reorg.
            // a consensus-affecting split. These three resets key purely on
            // firstActionIndex / block_index, so they port exactly; run them BEFORE the
            // deletes, matching the source order.
            //
            // NOTE: the source ALSO re-NULLs deactivation_block on
            // stakes/delegations/contract_stakes/contract_delegations. Those resets key on
            // ACTIVATION_DELAY_BLOCKS, which is a frozen per-chain node-local consensus
            // constant (never hub-overlaid; the indexer's _mergeHubParams overlay is empty
            // for consensus params), so the replica now holds it via consensus-constants.js
            // and mirrors all four resets below.
            if(firstActionIndex !== null){
                // tokens.escrow_action_index (the ownership-escrow gate) is RE-DERIVED below,
                // AFTER the dataTables delete (mirror of xchain-indexer rollback.js), a range
                // reset here would only handle the SET direction (offer orphaned), not the CLEAR
                // direction (a surviving offer whose release was orphaned).

                // attests (ATTEST v0 request): an orphaned v1 response / v2 expiry
                // flipped a surviving request out of 'pending'. Reset it (keyed on
                // resolved_block so BOTH flip paths reset) so a re-applied response is not
                // rejected as already-resolved and the pending-only deadline sweep can
                // re-synthesize the v2 row.
                try {
                    await this.db.doQuery(
                        "UPDATE attests SET request_status = 'pending', resolved_block = NULL " +
                        "WHERE version = 0 AND request_status IN ('fulfilled', 'errored', 'expired') AND resolved_block >= ?",
                        [block_index]
                    );
                } catch(e){
                    // Schema-gap errors (missing table/column on older replicas) are safe to skip.
                    // All other errors (deadlock, lock-wait, connection drop) must abort the reorg-reset.
                    if(e.errno !== 1146 && e.errno !== 1054) throw e;
                }

                // xcalls (XCALL v0 request): an orphaned result callback / deadline
                // expiry flipped a surviving request terminal. Reset so a re-applied
                // result is not silently lost to the already-resolved interlock (and an
                // expiry re-arms).
                try {
                    await this.db.doQuery(
                        "UPDATE xcalls SET request_status = 'pending', result_status = NULL, " +
                        "result_payload = NULL, resolved_block = NULL, callback_action_index = NULL " +
                        "WHERE version = 0 AND request_status IN ('completed', 'expired') AND resolved_block >= ?",
                        [block_index]
                    );
                } catch(e){
                    // Schema-gap errors (missing table/column on older replicas) are safe to skip.
                    // All other errors (deadlock, lock-wait, connection drop) must abort the reorg-reset.
                    if(e.errno !== 1146 && e.errno !== 1054) throw e;
                }

                // polls (VOTE v0): an orphaned VOTE v2 finalization flipped a surviving
                // poll (created in an earlier block) terminal IN PLACE. The generic
                // action_index delete below drops the v2's poll_results / escrow-release
                // rows, but cannot re-open the surviving polls row. Reset it (keyed on
                // resolved_block, which the finalize sweep stamps) so the replica matches
                // the source's re-opened poll and a re-streamed finalization upserts
                // cleanly. Mirrors xchain-indexer/src/rollback.js's polls re-open block.
                try {
                    await this.db.doQuery(
                        "UPDATE polls SET poll_status = 'open', winning_option = NULL, total_weight = NULL, " +
                        "total_voters = NULL, quorum_met = NULL, min_voters_met = NULL, " +
                        "fail_reason = NULL, decided_early = NULL, effective_close_block = NULL, " +
                        "finalized_action_index = NULL, resolved_block = NULL, " +
                        "deposit_resolved = NULL, callback_execute_action_index = NULL " +
                        "WHERE poll_status IN ('finalized', 'failed_quorum') AND resolved_block >= ?",
                        [block_index]
                    );
                } catch(e){
                    // Schema-gap errors (missing table/column on older replicas) are safe to skip.
                    // All other errors (deadlock, lock-wait, connection drop) must abort the reorg-reset.
                    if(e.errno !== 1146 && e.errno !== 1054) throw e;
                }

                // BET in-place flip resets: the updated_rows BET classes
                // carried surviving bet_feeds / bets rows latched, terminal-flipped or
                // settled in the now-orphaned range; the action-scoped delete below
                // cannot un-flip them. Mirrors xchain-indexer/src/rollback.js's BET
                // reset block byte-for-byte in predicate order: (a) terminal feeds
                // whose latch survives go back to 'closed'; (b) terminal feeds with
                // no surviving latch go back to 'open'; (c) orphaned latches clear
                // last; then settled bets re-open. Statuses resolve through the
                // replicated index_statuses rows (JOIN misses are no-ops pre-BET).
                try {
                    await this.db.doQuery(
                        "UPDATE bet_feeds f JOIN index_statuses cs ON (cs.status = 'closed') " +
                        "SET f.feed_status_id = cs.id, f.terminal_block = NULL " +
                        "WHERE f.terminal_block >= ? AND f.closed_block IS NOT NULL AND f.closed_block < ?",
                        [block_index, block_index]
                    );
                    await this.db.doQuery(
                        "UPDATE bet_feeds f JOIN index_statuses os ON (os.status = 'open') " +
                        "SET f.feed_status_id = os.id, f.terminal_block = NULL " +
                        "WHERE f.terminal_block >= ? AND (f.closed_block IS NULL OR f.closed_block >= ?)",
                        [block_index, block_index]
                    );
                    await this.db.doQuery(
                        "UPDATE bet_feeds f JOIN index_statuses os ON (os.status = 'open') " +
                        "SET f.feed_status_id = os.id, f.closed_block = NULL " +
                        "WHERE f.closed_block >= ?",
                        [block_index]
                    );
                    await this.db.doQuery(
                        "UPDATE bets b JOIN index_statuses os ON (os.status = 'open') " +
                        "SET b.bet_status_id = os.id, b.settled_block = NULL " +
                        "WHERE b.settled_block >= ?",
                        [block_index]
                    );
                } catch(e){
                    // Schema-gap errors (missing table/column on older replicas) are safe to skip.
                    if(e.errno !== 1146 && e.errno !== 1054) throw e;
                }

                // contract_slash_debits restore: an orphaned SLASH reduced
                // contract_stakes/contract_unstakes.amount IN PLACE on surviving rows (the
                // source records each debit's pre-slash `prev_amount`). The action-scoped
                // delete below drops orphaned-range rows but never re-streams the surviving
                // mutated row, so the replica keeps the slashed amount and diverges from the
                // source after a reorg. Mirror the source restore (xchain-indexer
                // rollback.js): copy back the EARLIEST orphaned debit's `prev_amount` per row
                // This is a pure string copy, byte-identical to the source (no arithmetic). Keys
                // only on block_index/stake_action_index, so it ports cleanly (no
                // ACTIVATION_DELAY_BLOCKS dependency).
                //
                // Same-block tiebreak is (execution_index, slash_position), the EXECUTE's
                // on-chain action_index plus the emission-loop index, the deterministic total
                // order the source uses for contract_emissions, NOT the AUTO_INCREMENT `id`.
                // This MUST byte-match the source indexer or a reorg retracting a block with
                // ≥2 contract slashes on one stake row restores a divergent amount on the
                // replica vs the source (stake-weight fork).
                for(let slashTbl of ['contract_stakes', 'contract_unstakes']){
                    try {
                        await this.db.doQuery(
                            "UPDATE " + slashTbl + " t " +
                            "JOIN contract_slash_debits d ON d.stake_action_index = t.action_index " +
                            "SET t.amount = d.prev_amount " +
                            "WHERE d.target_table = ? AND d.block_index >= ? " +
                            "AND NOT EXISTS (" +
                            "  SELECT 1 FROM contract_slash_debits e " +
                            "  WHERE e.target_table = d.target_table " +
                            "    AND e.stake_action_index = d.stake_action_index " +
                            "    AND e.block_index >= ? " +
                            "    AND (e.block_index < d.block_index " +
                            "         OR (e.block_index = d.block_index " +
                            "             AND (e.execution_index < d.execution_index " +
                            "                  OR (e.execution_index = d.execution_index " +
                            "                      AND e.slash_position < d.slash_position)))))",
                            [slashTbl, block_index, block_index]
                        );
                    } catch(e){
                        // Schema-gap errors (missing table/column on older replicas) are safe to skip.
                        // All other errors (deadlock, lock-wait, connection drop) must abort the reorg-reset.
                        if(e.errno !== 1146 && e.errno !== 1054) throw e;
                    }
                }

                // capability_slash_debits restore (WI-2 bump 2): the capability-stake twin
                // of the contract restore above. An orphaned SLASH burned stakes/unstakes.amount
                // IN PLACE on surviving rows; copy back the EARLIEST orphaned debit's verbatim
                // prev_amount per row. Same shape/keys as the contract path; mirrors the source
                // indexer (xchain-indexer rollback.js).
                //
                // Same-block tiebreak is slash_action_index (the deterministic, replay-stable
                // wire-SLASH action_index), NOT AUTO_INCREMENT `id`; must byte-match the source
                // indexer or a reorg retracting a block with ≥2 capability slashes on one stake
                // row restores a divergent amount on the replica vs the source (stake-weight fork).
                for(let slashTbl of ['stakes', 'unstakes']){
                    try {
                        await this.db.doQuery(
                            "UPDATE " + slashTbl + " t " +
                            "JOIN capability_slash_debits d ON d.stake_action_index = t.action_index " +
                            "SET t.amount = d.prev_amount " +
                            "WHERE d.target_table = ? AND d.block_index >= ? " +
                            "AND NOT EXISTS (" +
                            "  SELECT 1 FROM capability_slash_debits e " +
                            "  WHERE e.target_table = d.target_table " +
                            "    AND e.stake_action_index = d.stake_action_index " +
                            "    AND e.block_index >= ? " +
                            "    AND (e.block_index < d.block_index " +
                            "         OR (e.block_index = d.block_index AND e.slash_action_index < d.slash_action_index)))",
                            [slashTbl, block_index, block_index]
                        );
                    } catch(e){
                        // Schema-gap errors (missing table/column on older replicas) are safe to skip.
                        // All other errors (deadlock, lock-wait, connection drop) must abort the reorg-reset.
                        if(e.errno !== 1146 && e.errno !== 1054) throw e;
                    }
                }

                // anchor_reward_reconcile_log restore (RB-ANCHOR): mirror of
                // xchain-indexer/src/rollback.js. An orphaned anchor reconcile DELETEd loser
                // validator_rewards rows from earlier SURVIVING blocks (block_index =
                // SNAPSHOT_BLOCK) and pre-imaged them in the replicated anchor_reward_reconcile_log
                // keyed to the reconcile's (ANCHOR) block. The generic block delete below drops the
                // log rows but never re-creates the losers, so re-INSERT those whose original
                // earn-block (reward_block_index) survives the reorg (< block_index). amount is the
                // frozen consensus reward constant per round, so INSERT IGNORE is value-stable and
                // idempotent whether or not the source's forward DELETE reached this replica. Runs
                // BEFORE the generic delete so the log rows still exist. A loser whose
                // MATERIALIZATION block (reward_derive_block_index) is itself inside the
                // orphaned range is NOT restored: its earn-block survives, but a replay to
                // reorg_block-1 never derived it, so restoring it would mint an orphan.
                try {
                    await this.db.doQuery(
                        "INSERT IGNORE INTO validator_rewards " +
                        "(source_id, signing_pubkey_id, reward_type, round_reference, amount, block_index, derive_block_index) " +
                        "SELECT d.source_id, d.signing_pubkey_id, d.reward_type, d.round_reference, " +
                        "       d.amount, d.reward_block_index, d.reward_derive_block_index " +
                        "  FROM anchor_reward_reconcile_log d " +
                        " WHERE d.block_index >= ? AND d.reward_block_index < ? " +
                        "   AND (d.reward_derive_block_index IS NULL OR d.reward_derive_block_index < ?)",
                        [block_index, block_index, block_index]
                    );
                } catch(e){
                    // Schema-gap errors (missing table/column on older replicas) are safe to skip.
                    // All other errors (deadlock, lock-wait, connection drop) must abort the reorg-reset.
                    if(e.errno !== 1146 && e.errno !== 1054) throw e;
                }

                // deactivation_block re-NULL, mirror of xchain-indexer/src/rollback.js.
                // Orphaned UNSTAKE / DELEGATE-revoke actions stamped deactivation_block =
                // actionBlock + activationDelay IN PLACE on surviving parent stake/delegation
                // rows (created by a much earlier STAKE/DELEGATE in a surviving block). The
                // action-scoped delete below drops the orphaned action row but cannot undo
                // that in-place UPDATE, so without this reset the surviving parent keeps a
                // non-NULL deactivation_block; every active-set read gates on
                // (deactivation_block IS NULL OR deactivation_block > currentBlock), so once
                // the new chain passes the stale value the staker silently drops out on the
                // replica while the source (and a from-genesis replay) keeps it active, a
                // consensus-affecting divergence. The reset is PRECISE: we match the EXACT
                // value an orphaned action wrote (orphanBlock + activationDelay), never a
                // blanket >= block_index, so legitimately-earned earlier deactivations are
                // preserved. Keyed identically to the source (this.activationDelay holds the
                // frozen per-chain ACTIVATION_DELAY_BLOCKS).
                let activationDelay = this.activationDelay;
                if(activationDelay == null){
                    // No coin supplied at construction (legacy/test path). Skip rather than
                    // run with a wrong value, but warn since on a real replica this would
                    // silently reintroduce the deactivation-block sync divergence.
                    console.warn('ClientRollback: deactivation_block re-NULL mirror skipped (no coin supplied)');
                } else {

                // stakes ← orphaned unstakes (capability staking)
                try {
                    await this.db.doQuery(
                        "UPDATE stakes s " +
                        "JOIN unstakes u ON u.signing_pubkey_id = s.signing_pubkey_id " +
                        "SET s.deactivation_block = NULL " +
                        "WHERE u.block_index >= ? " +
                        "  AND s.deactivation_block IS NOT NULL " +
                        "  AND s.deactivation_block = u.block_index + ?",
                        [block_index, activationDelay]
                    );
                } catch(e){
                    // Schema-gap errors (missing table/column on older replicas) are safe to skip.
                    // All other errors (deadlock, lock-wait, connection drop) must abort the reorg-reset.
                    if(e.errno !== 1146 && e.errno !== 1054) throw e;
                }

                // delegations ← orphaned DELEGATE-revoke rows (a revoke is itself a
                // delegations row; the parent it stamped is an earlier delegations row for
                // the same source + signing pubkey (self-join).
                try {
                    await this.db.doQuery(
                        "UPDATE delegations p " +
                        "JOIN delegations r ON r.source_id = p.source_id " +
                        "  AND r.signing_pubkey_id = p.signing_pubkey_id " +
                        "SET p.deactivation_block = NULL " +
                        "WHERE r.block_index >= ? " +
                        "  AND p.deactivation_block IS NOT NULL " +
                        "  AND p.deactivation_block = r.block_index + ?",
                        [block_index, activationDelay]
                    );
                } catch(e){
                    // Schema-gap errors (missing table/column on older replicas) are safe to skip.
                    // All other errors (deadlock, lock-wait, connection drop) must abort the reorg-reset.
                    if(e.errno !== 1146 && e.errno !== 1054) throw e;
                }

                // contract_stakes ← orphaned contract_unstakes (contract staking, all chains)
                try {
                    await this.db.doQuery(
                        "UPDATE contract_stakes cs " +
                        "JOIN contract_unstakes cu ON cu.signing_pubkey_id = cs.signing_pubkey_id " +
                        "  AND cu.target_contract_index = cs.target_contract_index " +
                        "  AND cu.tick_id = cs.tick_id " +
                        "SET cs.deactivation_block = NULL " +
                        "WHERE cu.block_index >= ? " +
                        "  AND cs.deactivation_block IS NOT NULL " +
                        "  AND cs.deactivation_block = cu.block_index + ?",
                        [block_index, activationDelay]
                    );
                } catch(e){
                    // Schema-gap errors (missing table/column on older replicas) are safe to skip.
                    // All other errors (deadlock, lock-wait, connection drop) must abort the reorg-reset.
                    if(e.errno !== 1146 && e.errno !== 1054) throw e;
                }

                // contract_delegations ← orphaned DELEGATE v3 contract-revokes. No child row
                // exists (pure in-place UPDATE), so key on the value threshold: anything at or
                // above block_index + activationDelay was stamped by an orphaned revoke (any
                // surviving revoke stamps a strictly smaller value).
                try {
                    await this.db.doQuery(
                        "UPDATE contract_delegations " +
                        "SET deactivation_block = NULL " +
                        "WHERE deactivation_block IS NOT NULL " +
                        "  AND deactivation_block >= ?",
                        [Number(block_index) + activationDelay]
                    );
                } catch(e){
                    // Schema-gap errors (missing table/column on older replicas) are safe to skip.
                    // All other errors (deadlock, lock-wait, connection drop) must abort the reorg-reset.
                    if(e.errno !== 1146 && e.errno !== 1054) throw e;
                }
                } // end deactivation_block mirror (activationDelay present)

                // Cooldown-maturity reversal moved OUT of this firstActionIndex guard (see below,
                // after the guard closes) - the legacy maturity path mutates a surviving row with
                // no actions row in the maturity block, so an orphaned range with no other actions
                // leaves firstActionIndex null and would skip it, diverging the replica from the
                // (now-fixed) source.
            }

            // Reverse orphaned cooldown-maturity completions, mirror of
            // xchain-indexer/src/rollback.js _reverseCooldownMaturities. When a capability/contract
            // UNSTAKE cooldown matures, processCooldownCompletions writes a refund credit carrying
            // the unstake's OWN (earlier-block) action_index and flips the surviving unstake row's
            // status_id to 'completed' IN PLACE. Both effects live on rows whose action_index <
            // firstActionIndex, so the dataTables delete below can't touch them; the credit has no
            // block_index and can't be range-deleted at all. In the LEGACY attribution era the
            // maturity block mints NO actions row, so an orphaned range containing only such a
            // maturity leaves firstActionIndex null - this MUST run unconditionally (outside the
            // guard) or the replica keeps a phantom refund + stuck 'completed' unstake that the
            // re-maturity sweep skips forever, diverging from the source (which now reverses it on
            // every rollback). Predicates key only on cooldown_end_block/block_index; the GAS tick
            // (capability refund) is the frozen consensus constant, never a hub poll. rebuildBalances
            // below recomputes wholesale from the surviving credits, so no per-row seeding is needed.
            // Runs BEFORE the dataTables delete.
            try {
                let completedStatusId = await this.db.getStatusId('completed');
                let validStatusId     = await this.db.getStatusId('valid');
                if(completedStatusId !== null && validStatusId !== null){
                    let gasTick = gasTickSymbol();
                    if(gasTick){
                        // Capability maturity refund is paid in GAS, keyed by the unstake's action_index.
                        await this.db.doQuery(
                            "DELETE c FROM credits c " +
                            "JOIN unstakes u ON u.action_index = c.action_index AND u.source_id = c.address_id " +
                            "JOIN index_tickers g ON g.id = c.tick_id AND g.tick = ? " +
                            "WHERE u.status_id = ? AND u.cooldown_end_block >= ? AND u.block_index < ?",
                            [gasTick, completedStatusId, block_index, block_index]);
                    }
                    // Contract maturity refund is paid in the unstake's own tick.
                    await this.db.doQuery(
                        "DELETE c FROM credits c " +
                        "JOIN contract_unstakes cu ON cu.action_index = c.action_index AND cu.source_id = c.address_id AND cu.tick_id = c.tick_id " +
                        "WHERE cu.status_id = ? AND cu.cooldown_end_block >= ? AND cu.block_index < ?",
                        [completedStatusId, block_index, block_index]);
                    // Reset the in-place 'completed' flip to 'valid' so the sweep re-matures the cooldown.
                    await this.db.doQuery(
                        "UPDATE unstakes SET status_id = ? WHERE status_id = ? AND cooldown_end_block >= ? AND block_index < ?",
                        [validStatusId, completedStatusId, block_index, block_index]);
                    await this.db.doQuery(
                        "UPDATE contract_unstakes SET status_id = ? WHERE status_id = ? AND cooldown_end_block >= ? AND block_index < ?",
                        [validStatusId, completedStatusId, block_index, block_index]);
                }
            } catch(e){
                // Schema-gap errors (missing table/column on older replicas) are safe to skip.
                // All other errors (deadlock, lock-wait, connection drop) must abort the reorg-reset.
                if(e.errno !== 1146 && e.errno !== 1054) throw e;
            }

            // Reset an anchor batch's surviving archive-head parent (v1/v6,
            // ARCHIVE_HEAD_VERSIONS in stateHash.js) stamped 'invalid_archive' by an
            // orphaned final chunk, mirror of xchain-indexer rollback.js. When the last v2
            // chunk of a chunked archive batch lands and the reassembled blob fails its CRC
            // check, the source stamps the parent (in an earlier, surviving block)
            // 'invalid_archive' IN PLACE. If that completing chunk is in the orphaned range,
            // the dataTables delete below removes the chunk but cannot undo the parent stamp,
            // leaving the replica's parent stuck 'invalid_archive' while a from-genesis replay
            // re-derives its pre-flip status. Self-join the parent to an orphaned v2 chunk of
            // the same match_batch_seq whose status is 'valid' (a completing chunk is always
            // 'valid'; the valid-filter excludes a late rejected duplicate) and reset to
            // 'unverified', the conservative re-verification state. Runs BEFORE the delete.
            if(firstActionIndex !== null){
                try {
                    await this.db.doQuery(
                        "UPDATE anchor_actions p " +
                        "JOIN index_statuses ps ON ps.id = p.status_id AND ps.status = 'invalid_archive' " +
                        "JOIN anchor_actions c ON c.version = 2 AND c.match_batch_seq = p.match_batch_seq AND c.action_index >= ? " +
                        "JOIN index_statuses cs ON cs.id = c.status_id AND cs.status = 'valid' " +
                        "JOIN index_statuses us ON us.status = 'unverified' " +
                        "SET p.status_id = us.id " +
                        "WHERE p.version " + ARCHIVE_HEAD_VERSIONS_SQL + " AND p.action_index < ?",
                        [firstActionIndex, firstActionIndex]);
                } catch(e){
                    // Schema-gap errors (missing table/column on older replicas) are safe to skip.
                    // All other errors (deadlock, lock-wait, connection drop) must abort the reorg-reset.
                    if(e.errno !== 1146 && e.errno !== 1054) throw e;
                }
            }

            if(firstActionIndex !== null){
                for(let table of this.dataTables){
                    try {
                        await this.db.doQuery("DELETE FROM `" + table + "` WHERE action_index >= ?", [firstActionIndex]);
                    } catch(e){
                        // Discriminate like the bespoke resets above: swallow ONLY genuine
                        // schema gaps (1146 missing table / 1054 missing column on older or
                        // decoder-shaped replicas). A transient/operational error (deadlock
                        // 1213, lock-wait 1205, connection drop) must ABORT the whole
                        // rollback so the txn rolls back and the reorg retries, rather than
                        // committing a PARTIAL ledger rollback. These are consensus tables;
                        // the only prior backstop was the next block's VERIFY_STATE_COMMITMENT
                        // recompute, which is absent when that flag is off (truncated replicas).
                        if(e.errno !== 1146 && e.errno !== 1054) throw e;
                    }
                }

                // Re-derive tokens.escrow_action_index AFTER the dataTables delete (mirror of
                // xchain-indexer rollback.js). Orphaned offers + their append-only status rows
                // (order_statuses/swap_statuses/dispenser_statuses) are now gone, so a surviving
                // GIVE_OWNERSHIP offer whose release was orphaned has reverted to its latest
                // surviving status. Set the gate to that offer's action_index (or NULL if none
                // survives). The SQL between the ESCROW-REDERIVE-SQL markers is kept logically
                // identical with xchain-indexer/src/rollback.js (cross-repo drift guard in
                // rollback-coverage.test.js) so source + replica derive byte-identical values.
                // Affected set = currently-escrowed tokens (Class A) UNION tokens with a
                // surviving still-escrowed GIVE_OWNERSHIP offer (Class B).
                try {
                    // Shared with the forward-apply path (ClientApplier) so source +
                    // replica derive byte-identical escrow gates. The marker SQL lives
                    // in rederiveEscrowGate() below (cross-repo drift guard reads it there).
                    await rederiveEscrowGate(this.db);
                } catch(e){
                    // Schema-gap errors (missing table/column on older replicas) are safe to skip.
                    // All other errors (deadlock, lock-wait, connection drop) must abort the reorg-reset.
                    if(e.errno !== 1146 && e.errno !== 1054) throw e;
                }
            }

            // Sweep orphan icon rows after the dataTables delete has removed any
            // reorged token rows. icons is keyed by token_id (FK to tokens), so
            // icons for reorged tokens become orphans; the next full snapshot would
            // eventually overwrite them, but serving stale icon state for a token
            // that no longer exists is misleading. Mirrors xchain-indexer rollback.js.
            try {
                await this.db.doQuery('DELETE FROM icons WHERE token_id NOT IN (SELECT id FROM tokens)', []);
            } catch(e){
                // Only errno 1146 (table missing on older schemas) is a benign skip;
                // a transient/operational fault must abort so the outer catch rolls
                // back and retries rather than committing a partial rollback.
                if(e.errno !== 1146) throw e;
            }

            for(let table of this.blockTables){
                try {
                    await this.db.doQuery("DELETE FROM `" + table + "` WHERE block_index >= ?", [block_index]);
                } catch(e){
                    // Swallow only schema gaps (see the dataTables loop); a transient error
                    // must abort the rollback rather than leave block-scoped consensus rows
                    // (blocks/transactions/slash_events/state_tree_roots) half-deleted.
                    if(e.errno !== 1146 && e.errno !== 1054) throw e;
                }
            }

            // validator_rewards MATERIALIZATION-block delete, mirror of
            // xchain-indexer/src/rollback.js. The loop above scopes on block_index,
            // which for a reward is its EARN block. The BTC-side anchor/archive
            // derivation earns at the checkpoint's SNAPSHOT_BLOCK but writes the row while
            // processing a later BTC block, stamped derive_block_index, so a reorg into that
            // gap orphans the block that minted the reward while leaving its earn-block below
            // the delete's scope. The replica must drop exactly the rows the source drops or
            // its COLLECT rail reads a different SUM(validator_rewards). NULL (every same-block
            // writer) is never matched. Runs BEFORE the index-lookup deletes below, which
            // require no surviving row to reference an id they remove.
            try {
                await this.db.doQuery('DELETE FROM validator_rewards WHERE derive_block_index >= ?', [block_index]);
            } catch(e){
                // Schema-gap errors (missing table/column on older replicas) are safe to skip:
                // such a replica holds no derived reward either. All other errors (deadlock,
                // lock-wait, connection drop) must abort the reorg-reset.
                if(e.errno !== 1146 && e.errno !== 1054) throw e;
            }

            // Roll back the index id lookups (index_addresses / index_tickers), mirroring
            // the source indexer's rollback (xchain-indexer/src/rollback.js). These tables
            // are replicated VERBATIM by id (ClientApplier copies the server's ids), so the
            // replica must delete the same orphaned-block id rows the source deletes; the
            // forward stream then re-introduces them under their reproduced ids. Once an
            // address/ticker can be referenced on the wire as ^<id>, its id is
            // consensus-relevant (resolved to a canonical string at block-hash time), so a
            // surviving orphaned id would fork the replica's checkpoint after a reorg. Rows
            // with block_index NULL (pre-migration) are never matched and left untouched.
            // MUST run after the action_index and block_index data deletes above so no
            // surviving row references a deleted id.
            for(let table of this.indexTables){
                try {
                    await this.db.doQuery("DELETE FROM `" + table + "` WHERE block_index >= ?", [block_index]);
                } catch(e){
                    // Swallow only schema gaps (missing table 1146 / lacks block_index 1054
                    // on older replicas); a transient error must abort. A surviving orphaned
                    // ^<id> row is consensus-relevant (forks the checkpoint after a reorg, per
                    // the note above), so a half-done index rollback must never commit.
                    if(e.errno !== 1146 && e.errno !== 1054) throw e;
                }
            }

            // Mirror the source indexer's orphan sweep of the two derived tables that
            // reference a rolled-back index id but are neither action_index/block_index
            // deleted above nor recomputed: markets (tick1_id/tick2_id) and pubkeys
            // (address_id). The source (xchain-indexer/src/rollback.js) deletes these when
            // their index id no longer resolves; the replica's replication NEVER propagates
            // a deletion for them (markets upserts on the full-dump, pubkeys is INSERT
            // IGNORE), so without mirroring the sweep the source row is gone but the replica
            // keeps the orphan, and on id reclaim serves the OLD market/pubkey (a
            // non-consensus source<->replica divergence, not a fork: neither table is in the
            // block hash, the stateHash preimage, or the index-map parity checksum). After
            // the local delete the source's reproduced row pages back in cleanly (the
            // address_id/pair PK is free again). balances needs no mirror here: the replica
            // recomputes it wholesale (rebuildBalances) so the orphan never re-derives.
            try {
                await this.db.doQuery(
                    "DELETE FROM markets WHERE tick1_id NOT IN (SELECT id FROM index_tickers) " +
                    "OR tick2_id NOT IN (SELECT id FROM index_tickers)", []);
            } catch(e){
                // Schema-gap errors (missing table/column on older replicas) are safe to skip.
                // All other errors (deadlock, lock-wait, connection drop) must abort the reorg-reset.
                if(e.errno !== 1146 && e.errno !== 1054) throw e;
            }

            // IDX-2 (mirror of xchain-indexer/src/rollback.js): the dangling-tick sweep
            // above misses a market whose pair was FIRST traded only in the orphaned range
            // while both its ticks survive (issued in earlier surviving blocks). The source
            // deletes that markets row; the replica used to keep it, and no replication
            // channel could remove it - markets rides the snapshot as an UPSERT full-dump
            // (SnapshotBuilder indexerFullDump), which refreshes present rows and never
            // deletes absent ones. The result was a stale, zeroed OHLCV row that
            // xchain-explorer served forever, invisible to every guard (markets is unhashed,
            // outside /status table counts, and _verifyTableCounts only reports remote >
            // local). Scoped to the pairs this rollback orphaned, each dropped only when no
            // surviving orders/order_matches row references either orientation - the same
            // predicate the source applies.
            try {
                for(let pair of affectedMarketPairs){
                    let survives = await this.db.doQuery(
                        "SELECT 1 FROM orders WHERE (give_tick_id=? AND get_tick_id=?) " +
                        "OR (give_tick_id=? AND get_tick_id=?) LIMIT 1",
                        [pair.tick1_id, pair.tick2_id, pair.tick2_id, pair.tick1_id]);
                    if(!survives || survives.length === 0){
                        survives = await this.db.doQuery(
                            "SELECT 1 FROM order_matches WHERE (give_tick_id=? AND get_tick_id=?) " +
                            "OR (give_tick_id=? AND get_tick_id=?) LIMIT 1",
                            [pair.tick1_id, pair.tick2_id, pair.tick2_id, pair.tick1_id]);
                    }
                    if(!survives || survives.length === 0){
                        await this.db.doQuery(
                            "DELETE FROM markets WHERE (tick1_id=? AND tick2_id=?) OR (tick1_id=? AND tick2_id=?)",
                            [pair.tick1_id, pair.tick2_id, pair.tick2_id, pair.tick1_id]);
                    }
                }
            } catch(e){
                // Schema-gap errors (missing table/column on older replicas) are safe to skip.
                // All other errors (deadlock, lock-wait, connection drop) must abort the reorg-reset.
                if(e.errno !== 1146 && e.errno !== 1054) throw e;
            }
            try {
                await this.db.doQuery(
                    "DELETE FROM pubkeys WHERE address_id NOT IN (SELECT id FROM index_addresses)", []);
            } catch(e){
                // Schema-gap errors (missing table/column on older replicas) are safe to skip.
                // All other errors (deadlock, lock-wait, connection drop) must abort the reorg-reset.
                if(e.errno !== 1146 && e.errno !== 1054) throw e;
            }

            // price_snapshots anchors each round to a block via reference_block
            // (its equivalent of block_index) rather than block_index itself, so
            // it falls outside the generic blockTables loop above and needs its
            // own delete, mirroring the source indexer's rollback. Live
            // convergence on this table is hub-driven (the hub DB sync mirror
            // delivers row:deleted events), but that propagation lags the local
            // reorg, leaving the replica serving finalized rows for orphaned
            // rounds until the hub catches up. Deleting them here closes that
            // staleness window; any later hub-driven delete of an already-gone
            // row is a harmless SQL no-op.
            //
            // Note: other hub-mirrored block-anchored tables (state_checkpoints,
            // capability_snapshots) are intentionally NOT deleted here. Both are
            // append-only with supersede-by-seq / MAX-per-height read semantics,
            // so a stale row is harmless once the hub pushes a higher-seq
            // replacement. The price_snapshots delete exists because a from-genesis
            // replay never regenerates orphaned rounds, so hub re-mirror alone
            // cannot close the divergence window on this table.
            // PRICE-SNAP-1 (mirror of xchain-indexer rollback.js): reference_block is always a BTC
            // anchor height regardless of the publishing chain, so only the BTC replica can prune
            // BTC-published rounds by it. Qualify by reference_chain and run only on the BTC replica so
            // a BTC reorg never deletes an off-BTC-published round the hub still keeps (price_snapshots
            // feeds getOracleDataForVM). Behavior-preserving today (price capability is BTC-only).
            try {
                if(this.coin === 'BTC')
                    await this.db.doQuery("DELETE FROM price_snapshots WHERE reference_chain = 'BTC' AND reference_block >= ?", [block_index]);
            } catch(e){
                // Schema-gap errors (missing table/column on older replicas) are safe to skip.
                // All other errors (deadlock, lock-wait, connection drop) must abort the reorg-reset.
                if(e.errno !== 1146 && e.errno !== 1054) throw e;
            }

            // oracle_prices is the per-action local mirror of PRICE v1 rows
            // (populated by hub_db_sync). Like price_snapshots, its rows are
            // tagged by source_chain + action_index and are NOT regenerated by a
            // from-genesis replay on the new chain. Deleting them here closes the
            // staleness window before hub-driven convergence catches up. The delete
            // MUST be qualified by source_chain (this.coin) because oracle_prices
            // holds rows from ALL chains and action_index is only unique within a
            // chain.
            if(firstActionIndex !== null){
                try {
                    await this.db.doQuery(
                        "DELETE FROM oracle_prices WHERE source_chain = ? AND action_index >= ?",
                        [this.coin, firstActionIndex]
                    );
                } catch(e){
                    // Schema-gap errors (missing table/column on older replicas) are safe to skip.
                    // All other errors (deadlock, lock-wait, connection drop) must abort the reorg-reset.
                    if(e.errno !== 1146 && e.errno !== 1054) throw e;
                }
            }

            // cross_chain_calls / cross_chain_matches are the per-action local mirrors
            // of hub-relayed XCALL + cross-chain DEX rows (populated by hub_db_sync).
            // Like oracle_prices above, they are tagged by source chain + a per-chain
            // action_index and are NOT regenerated by a from-genesis replay, so deleting
            // the orphaned range here closes the staleness window before hub-driven
            // convergence (row:deleted) catches up. cross_chain_matches is two-sided: a
            // match drops when EITHER leg on this chain was rolled back. Predicates are
            // byte-identical to xchain-indexer rollback.js (drift-guarded by the markers).
            if(firstActionIndex !== null){
                let crossChainFrom = firstActionIndex;
                //<CROSS-CHAIN-MIRROR-REORG-DELETE>
                try {
                    await this.db.doQuery(
                        `DELETE FROM cross_chain_calls WHERE source_chain = ? AND source_action_index >= ?`,
                        [this.coin, crossChainFrom]);
                    await this.db.doQuery(
                        `DELETE FROM cross_chain_matches WHERE (a_chain = ? AND a_action_index >= ?) OR (b_chain = ? AND b_action_index >= ?)`,
                        [this.coin, crossChainFrom, this.coin, crossChainFrom]);
                } catch(e){
                    // Schema-gap errors (missing table/column on older replicas) are safe to skip.
                    // All other errors (deadlock, lock-wait, connection drop) must abort the reorg-reset.
                    if(e.errno !== 1146 && e.errno !== 1054) throw e;
                }
                //</CROSS-CHAIN-MIRROR-REORG-DELETE>
            }

            try {
                await this.db.doQuery("DELETE FROM sync_meta WHERE block_index >= ?", [block_index]);
            } catch(e){
                // Schema-gap errors (missing table/column on older replicas) are safe to skip.
                // All other errors (deadlock, lock-wait, connection drop) must abort the reorg-reset.
                if(e.errno !== 1146 && e.errno !== 1054) throw e;
            }

            // Mirror the server's TransparencyLog.pruneFrom, which deletes BOTH
            // sync_meta AND merkle_epochs on reorg; the follower only mirrored
            // sync_meta above. merkle_epochs is sync-owned, reaches followers only
            // via the full-snapshot ride-along, and is applied INSERT IGNORE
            // (ClientApplier.ignoreTables), so a reorg that re-roots a closed epoch
            // would leave the stale root in place forever: the corrected re-dump
            // collides on UNIQUE epoch and is silently skipped. Deleting the
            // orphaned-range epochs lets the next catch-up re-insert the corrected
            // roots (and ids) cleanly. Keyed by end_block (not block_index), like
            // the server's prune. Falls outside the generic blockTables loop.
            try {
                await this.db.doQuery("DELETE FROM merkle_epochs WHERE end_block >= ?", [block_index]);
            } catch(e){
                // Schema-gap errors (missing table/column on older replicas) are safe to skip.
                // All other errors (deadlock, lock-wait, connection drop) must abort the reorg-reset.
                if(e.errno !== 1146 && e.errno !== 1054) throw e;
            }

            // attest_validator_stats: running per-validator aggregate counters
            // (fulfilled/missed/slashed). This table is NOT block-streamed to
            // replicas; it only arrives via full-snapshot ride-along, and the thin
            // replica DB has none of the capability/governance machinery the source
            // uses to derive missed_count (the responsible-set capability snapshot),
            // so it cannot recompute these rows; reproducing that math here would
            // re-introduce the indexer-mirror drift the rollback guard exists to
            // catch. On reorg we therefore drop the rows whose most-recent touch is
            // in the orphaned range, so the replica never serves overcounted values
            // and let the next full-snapshot ride-along restore correct counts
            // from the (now reorg-safe) source. Contrast markets: its VALUES converge
            // via the full-dump UPSERT (ON DUPLICATE KEY UPDATE) on the next snapshot,
            // so only its two source ROW deletes need mirroring here (above).
            // NOTE: between this DELETE and the next full snapshot, the replica
            // serves no attest_validator_stats rows for the affected validators;
            // this window is unbounded if full snapshots are infrequent. Acceptable
            // trade-off: correctness is preferred over availability for this table.
            // PHASE-4 GATE: this drop is safe only while quality_score/slashed_count
            // are display aggregates with no consensus reader. Before Phase-4 lets
            // quality_score drive live responsible-set selection or slashing, replace
            // this DELETE with a stale-pending-snapshot marking (a source-schema flag
            // carried by the snapshot ride-along), so a reorg cannot make a replica
            // serve a dropped row as a real zero score.
            try {
                await this.db.doQuery("DELETE FROM `attest_validator_stats` WHERE last_updated_block >= ?", [block_index]);
            } catch(e){
                // Schema-gap errors (missing table/column on older replicas) are safe to skip.
                // All other errors (deadlock, lock-wait, connection drop) must abort the reorg-reset.
                if(e.errno !== 1146 && e.errno !== 1054) throw e;
            }

            // Recalculate balances from the surviving credits/debits.
            // SQL is shared with ClientApplier via balance-helpers to prevent divergence.
            try {
                await balanceHelpers.rebuildBalances(this.db);
            } catch(e){
                // Only errno 1146 (missing table on an older-schema replica) is a
                // benign skip; log the step context and rethrow every real fault so
                // the outer catch aborts rather than committing a partial reorg-reset.
                if(e.errno !== 1146){ console.error('rebuildBalances after rollback failed:', e); throw e; }
            }

            // Recompute tokens.supply from the surviving credits/debits/escrows. The
            // orphaned MINT's credit row is deleted above, but tokens.supply was mutated
            // IN PLACE (no new row), so it survives the block-scoped deletes with the
            // inflated value; the source indexer's rollback recomputes it (updateTokens),
            // so without this the replica serves an over-inflated supply until the next
            // full snapshot. Mirrors xchain-indexer getTokenSupply (see balance-helpers).
            try {
                await balanceHelpers.recomputeTokenSupplies(this.db);
            } catch(e){
                // errno 1146 (missing table) is the only benign schema gap; log the
                // step and rethrow real faults so the outer catch rolls back.
                if(e.errno !== 1146){ console.error('recomputeTokenSupplies after rollback failed:', e); throw e; }
            }

            await this.db.commitTransaction();
            console.log('Indexer rollback to block ' + block_index + ' completed (' + this.util.getTimer(timer) + ')');

        } catch(e){
            await this.db.rollbackTransaction();
            console.error('Indexer rollback failed:', e);
            throw e;
        }
    }

    // Decoder rollback. Decoder schema has no actions / contract_emissions /
    // balances / sync_meta. Tx-scoped tables (transaction_outputs) must be
    // deleted BEFORE the parent transactions rows that gave them their
    // tx_index scope. events is left untouched: it has no block_index and no
    // monotonic cursor, so per-block rollback isn't possible without a schema
    // change (decoder review Finding D).
    async _rollbackDecoder(block_index){
        let timer = this.util.startTimer();
        console.log('Starting decoder rollback to block ' + block_index + '...');

        await this.db.beginTransaction();
        try {
            // Collect tx_indexes for the blocks being rolled back so we can
            // clean tx-scoped tables before the transactions row goes away.
            let txRows = await this.db.doQuery(
                "SELECT tx_index FROM transactions WHERE block_index >= ?",
                [block_index]
            );
            let txIndexes = txRows.map(r => Number(r.tx_index));

            if(txIndexes.length > 0){
                let placeholders = txIndexes.map(() => '?').join(',');
                for(let t of this.decoderTxScopedTables){
                    try {
                        await this.db.doQuery("DELETE FROM `" + t + "` WHERE tx_index IN (" + placeholders + ")", txIndexes);
                    } catch(e){
                        // Schema-gap errors (missing table/column on older replicas) are safe to skip.
                        // All other errors (deadlock, lock-wait, connection drop) must abort the reorg-reset.
                        if(e.errno !== 1146 && e.errno !== 1054) throw e;
                    }
                }
            }

            // Block-scoped: transactions before blocks (no declared FK in
            // current schema, but kept in dependency order for clarity).
            for(let t of this.decoderBlockTables){
                await this.db.doQuery("DELETE FROM `" + t + "` WHERE block_index >= ?", [block_index]);
            }

            // index_addresses, index_transactions, pubkeys: append-only;
            // orphan rows are harmless (the sync stream uses INSERT IGNORE to
            // re-introduce them when new blocks arrive). Skip.
            //
            // "Harmless" holds because these surrogate ids are local-only and feed no
            // consensus value; block hashes are computed from resolved strings, not lookup
            // ids (the current BLOCK_HASH_VERSION). Never let a lookup id back into a hashed projection.

            await this.db.commitTransaction();
            console.log('Decoder rollback to block ' + block_index + ' completed (' + this.util.getTimer(timer) + ')');

        } catch(e){
            await this.db.rollbackTransaction();
            console.error('Decoder rollback failed:', e);
            throw e;
        }
    }
}

// Re-derive tokens.escrow_action_index (the ownership-escrow gate) from the
// already-replicated offer/status tables. Shared by ClientRollback (reorg path,
// after orphaned offers/statuses are deleted) and ClientApplier (forward-apply
// path, after the block's offers/statuses are inserted) so both derive
// byte-identical gate values. The SQL between the //<ESCROW-REDERIVE-SQL> markers
// is kept logically identical with xchain-indexer/src/rollback.js (cross-repo drift
// guard in test/unit/rollback-coverage.test.js). Uses db.doQuery so it joins
// whatever transaction the caller already opened.
// Affected set = currently-escrowed tokens (Class A) UNION tokens with a
// surviving still-escrowed GIVE_OWNERSHIP offer (Class B).
async function rederiveEscrowGate(db){
    //<ESCROW-REDERIVE-SQL>
    const escrowAffectedTickersSql =
        `SELECT DISTINCT tk.tick FROM tokens t INNER JOIN index_tickers tk ON tk.id=t.tick_id WHERE t.escrow_action_index IS NOT NULL
         UNION
         SELECT DISTINCT tk.tick FROM index_tickers tk WHERE tk.id IN (
             SELECT o.give_tick_id FROM orders o INNER JOIN order_statuses st ON st.order_action_index=o.action_index INNER JOIN index_statuses si ON si.id=st.status_id WHERE o.give_ownership=1 AND st.action_index=(SELECT MAX(x.action_index) FROM order_statuses x WHERE x.order_action_index=o.action_index) AND si.status IN ('open','cancelling','expiring')
             UNION ALL
             SELECT s.give_tick_id FROM swaps s INNER JOIN swap_statuses st ON st.swap_action_index=s.action_index INNER JOIN index_statuses si ON si.id=st.status_id WHERE s.give_ownership=1 AND st.action_index=(SELECT MAX(x.action_index) FROM swap_statuses x WHERE x.swap_action_index=s.action_index) AND si.status IN ('open','cancelling','expiring')
             UNION ALL
             SELECT d.give_tick_id FROM dispensers d INNER JOIN dispenser_statuses st ON st.dispenser_action_index=d.action_index INNER JOIN index_statuses si ON si.id=st.status_id WHERE d.give_ownership=1 AND st.action_index=(SELECT MAX(x.action_index) FROM dispenser_statuses x WHERE x.dispenser_action_index=d.action_index) AND si.status IN ('open','cancelling','expiring')
         )`;
    const escrowOpenOfferSql =
        `SELECT o.action_index FROM orders o INNER JOIN order_statuses st ON st.order_action_index=o.action_index INNER JOIN index_statuses si ON si.id=st.status_id INNER JOIN index_tickers tk ON tk.id=o.give_tick_id WHERE tk.tick=? AND o.give_ownership=1 AND st.action_index=(SELECT MAX(x.action_index) FROM order_statuses x WHERE x.order_action_index=o.action_index) AND si.status IN ('open','cancelling','expiring')
         UNION ALL
         SELECT s.action_index FROM swaps s INNER JOIN swap_statuses st ON st.swap_action_index=s.action_index INNER JOIN index_statuses si ON si.id=st.status_id INNER JOIN index_tickers tk ON tk.id=s.give_tick_id WHERE tk.tick=? AND s.give_ownership=1 AND st.action_index=(SELECT MAX(x.action_index) FROM swap_statuses x WHERE x.swap_action_index=s.action_index) AND si.status IN ('open','cancelling','expiring')
         UNION ALL
         SELECT d.action_index FROM dispensers d INNER JOIN dispenser_statuses st ON st.dispenser_action_index=d.action_index INNER JOIN index_statuses si ON si.id=st.status_id INNER JOIN index_tickers tk ON tk.id=d.give_tick_id WHERE tk.tick=? AND d.give_ownership=1 AND st.action_index=(SELECT MAX(x.action_index) FROM dispenser_statuses x WHERE x.dispenser_action_index=d.action_index) AND si.status IN ('open','cancelling','expiring')
         ORDER BY action_index ASC
         LIMIT 1`;
    //</ESCROW-REDERIVE-SQL>
    let escrowTickers = await db.doQuery(escrowAffectedTickersSql, []);
    for(let row of escrowTickers){
        let offerRows = await db.doQuery(escrowOpenOfferSql, [row.tick, row.tick, row.tick]);
        let newEscrow = (offerRows.length > 0) ? offerRows[0].action_index : null;
        await db.doQuery("UPDATE tokens SET escrow_action_index=? WHERE tick_id=(SELECT id FROM index_tickers WHERE tick=? LIMIT 1)", [newEscrow, row.tick]);
    }
}

module.exports = ClientRollback;
module.exports.rederiveEscrowGate = rederiveEscrowGate;
