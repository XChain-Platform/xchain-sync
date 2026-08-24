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
 * XChain Sync - Server Poller
 *
 * Polls one database (indexer OR decoder) for new blocks, builds block
 * payloads, records hashes in the transparency log (indexer only), and
 * broadcasts to WebSocket subscribers via the BlockBroadcaster.
 *
 * One instance per chain/network/dbType.
 *
 * dbType is read from db.dbType:
 *   - 'indexer' (default): full block payload with actions, action-scoped
 *     tables, infra tables, and three-hash transparency log
 *   - 'decoder':           simpler payload with transactions + tx-scoped
 *     tables (transaction_outputs). No actions, no transparency log
 *     (decoder content is deterministic from the coin node).
 *
 ********************************************************************/

const replicatedTables = require('./replicatedTables');
const { collectUpdatedRows } = require('./updatedRows');
const { collectMaturedCooldownCredits } = require('./cooldownCredits');
const { collectRedrivenValidatorRewards } = require('./recoveryRewards');
const { collectDerivedAnchorRewards } = require('./derivedRewards');
const { activationDelayBlocks, coinTicker } = require('./consensus-constants');
const { isStateCommitmentActive } = require('./state_commitment_activation');
const { SCHEMA_VERSION } = require('./schema-version');

// How many recently broadcast block hashes to retain in memory for the
// net-forward reorg walk-back. Comfortably above the source
// indexer's MAX_ROLLBACK_DEPTH (100) so a deep same-interval reorg can be
// walked back one height per poll against the pre-reorg hash we recorded,
// rather than against a fresh (post-reorg) source read that always matches.
const RECENT_HASH_CAP = 256;

// A per-table read in _buildBlockPayload may legitimately fail because the source
// runs an older schema that lacks the table/column (errno 1146 missing table, 1054
// unknown column): that table is simply absent from this block's payload, harmless.
// EVERY OTHER error (deadlock 1213, lock-wait timeout 1205, connection drop, etc.)
// is a transient/operational fault, and swallowing it would broadcast a structurally
// valid but silently INCOMPLETE block that followers durably record (false
// VERIFY_RECOMPUTE halts on hashed tables; silent stake-state divergence on the
// unhashed slash-debit/reconcile tables ClientRollback restores from on reorg). Only
// schema gaps may be skipped; anything else must re-throw so _poll's loop freezes the
// cursor and retries the block. Mirrors SnapshotBuilder.streamIncrementalSnapshot's
// errno discrimination.
function isSchemaGapError(e){
    return !!(e && (e.errno === 1146 || e.errno === 1054));
}

class ServerPoller {

    constructor(chain, network, db, broadcaster, transparencyLog, config, util) {
        this.chain    = chain;
        // Canonical TICKER form of `chain` for the per-chain '<TICKER>:<network>'
        // activation lookup AND the state_tree_roots.chain column (the source indexer
        // writes tickers there). `this.chain` stays the caller's full-name form because
        // broadcast routing, payload `chain:` fields, and logging all use it. See
        // coinTicker: passing the full name here made the gate resolve to "off" and the
        // roots lookup miss its row, so the server published NULL roots and the
        // follower's state-commitment check never ran.
        this.coinTicker = coinTicker(chain);
        this.network  = network;
        this.db       = db;
        this.broadcaster    = broadcaster;
        this.transparencyLog = transparencyLog;  // null for decoder; non-null for indexer
        this.config   = config;
        this.util     = util;
        this.dbType   = (db && db.dbType) ? db.dbType : 'indexer';

        // Frozen per-chain ACTIVATION_DELAY_BLOCKS, needed to detect forward
        // deactivation_block stamps for the in-place updated-rows channel (see
        // updatedRows.js). A coin that is unrecognized (or omitted in a test
        // harness) yields undefined/null; collectUpdatedRows then skips the
        // deactivation_block class rather than scanning with a wrong delay.
        let delay = activationDelayBlocks(chain);
        this.activationDelay = (delay === undefined) ? null : delay;

        this.lastPolledBlock = null;
        // Hash of lastPolledBlock's content on the source, so a net-forward reorg
        // (rollback + readvance within one poll interval, which keeps the height
        // monotonic) is detectable by a changed hash, not just a lower height.
        this.lastPolledBlockHash = null;
        // Bounded map of recently broadcast block hashes (block_index -> content
        // hash WE broadcast for that height). On a net-forward reorg the walk-back
        // seeds lastPolledBlockHash from the PRE-reorg hash recorded here, so a
        // reorg deeper than one block keeps walking back over subsequent polls.
        // Works for both dbTypes (the decoder has no sync_meta to read a recorded
        // hash from). Capped to the last RECENT_HASH_CAP heights.
        this.recentBroadcastHashes = new Map();
        this.running = false;

        // Per-block replicated table topology (single source of truth shared with
        // the row-count completeness check; see src/replicatedTables.js).
        let topo = replicatedTables.getTopology(this.dbType);
        this.blockScopedTables  = topo.blockScoped;
        this.txScopedTables     = topo.txScoped;
        this.actionScopedTables = topo.actionScoped;
        this.indexTables        = topo.index;

        if(this.dbType === 'decoder'){
            // Decoder doesn't have cross-chain infrastructure tables
            this.infraTables = new Set();
        } else {
            // Infrastructure tables: always synced regardless of subscriber sync mode.
            // These tables provide cross-chain state that every node needs (validator set,
            // rewards) or that participate in cross-chain queries (PRICE actions on any chain).
            // Subscribers in 'infra-only' mode receive ONLY these tables for this chain.
            this.infraTables = new Set([
                'stakes', 'delegations', 'validator_rewards', 'prices', 'reward_claims',
                'index_pubkeys', 'index_addresses', 'index_actions', 'index_statuses', 'index_fiats'
            ]);
        }

        // Count of consecutive _poll() failures so _updateStatus can surface
        // a stale-status signal to /health callers when the poller is wedged.
        this.pollErrorCount = 0;

        // Throttle stamp for the action-scoped query-count metric (0 = never emitted,
        // so the first block of a process publishes a baseline). See
        // _reportActionScopedQueryMetric.
        this._lastQueryMetricAt = 0;
    }

    async start(){
        this.lastPolledBlock = await this._resumeCursor();
        this.lastPolledBlockHash = await this._seedReorgGuardHash(this.lastPolledBlock);
        this.running = true;
        console.log('ServerPoller started for ' + this.chain + '/' + this.network + '/' + this.dbType + ' at block ' + (this.lastPolledBlock || 'none'));

        // Repair any interior transparency-log holes left by a pre-fix restart that
        // resumed from the source tip. No-op on a healthy log (and on the decoder,
        // which has no transparency log). Failure here must not stop live polling;
        // the backfill is idempotent and retries on the next restart.
        try {
            await this.backfillGaps();
        } catch(e){
            console.error('Transparency backfill failed for ' + this.chain + '/' + this.network + '/' + this.dbType + ' (continuing with live polling):', e);
        }

        await this._updateStatus();

        while(this.running){
            let blocksProcessed = 0;
            try {
                blocksProcessed = await this._poll() || 0;
                this.pollErrorCount = 0;
            } catch(e){
                this.pollErrorCount++;
                console.error('ServerPoller error for ' + this.chain + '/' + this.network + '/' + this.dbType + ' (consecutive errors: ' + this.pollErrorCount + '):', e);
                // Update status so the poll_error_count field is current even while
                // lastPolledBlock is frozen at the last-good value.
                await this._updateStatus().catch(() => {});
            }
            // Skip sleep when the batch cap was hit (backlog likely remains)
            if(blocksProcessed < 100)
                await this.util.sleep(this.config['BLOCK_POLL_INTERVAL']);
        }
    }

    // Seed the broadcast cursor for a (re)start. For the indexer, resume from the
    // transparency log's own high-water mark, which is the durable record of how far
    // this poller actually recorded and broadcast (MAX(block_index) in sync_meta). The
    // source DB tip (db.getLastBlock) is NOT a record of broadcast progress: if the
    // sync server was down while the co-located indexer advanced, seeding from the
    // tip would jump the cursor past every missed block, so _poll's while-loop never
    // runs for them, recordBlock is never called, and any epoch boundary in the gap
    // is never committed, leaving a permanent hole in sync_meta and missing Merkle
    // proofs while the poller falsely reports caught-up. A null high-water mark (empty
    // sync_meta, fresh node) leaves the cursor null so _poll initialises from the
    // current tip on its first pass, as before. The decoder has no transparency log,
    // so it resumes from the source tip (decoder content is deterministic from the
    // coin node and carries no synthetic hash chain to keep gap-free).
    async _resumeCursor(){
        if(this.transparencyLog)
            return await this.transparencyLog.getHighWaterMark();
        return await this.db.getLastBlock();
    }

    // Repair interior holes in the transparency log left by the pre-fix restart
    // behaviour (resuming from the source tip skipped every block the indexer
    // advanced during downtime). Replays recordBlock for each missing block and
    // recomputes any Merkle epoch whose range spanned a hole. Indexer-only and
    // idempotent: a healthy log reports no gaps, so this is a no-op. Runs once at
    // startup, before the poll loop.
    async backfillGaps(){
        if(!this.transparencyLog) return 0;
        // A replicated log has no holes of this process's making, and every repair
        // below is a write. Skip the scan entirely rather than let it report gaps it
        // would then "repair" with no-op writes and log a false success.
        if(this.transparencyLog.readOnly) return 0;

        let gaps = await this.transparencyLog.findGaps();
        if(gaps.length === 0) return 0;

        console.log('Transparency backfill: ' + gaps.length + ' missing block(s) detected for ' +
            this.chain + '/' + this.network + '/' + this.dbType + '; repairing');

        let epochSize = this.transparencyLog.epochSize;
        let epochs = new Set();
        for(let block_index of gaps){
            let hashRow = await this.db.getBlockHashRow(block_index);
            if(!hashRow) continue;  // block no longer in source (reorg since scan); skip
            await this.transparencyLog.recordBlock(
                Number(hashRow.block_index), Number(hashRow.block_time),
                hashRow.ledger_hash, hashRow.actions_hash, hashRow.contract_hash
            );
            // Block N belongs to epoch ceil(N / epochSize); matches getProof's mapping.
            epochs.add(Math.ceil(block_index / epochSize));
        }

        // Recompute every epoch a hole touched. recordBlock auto-commits an epoch only
        // when the boundary block itself was the gap; an epoch whose boundary block was
        // already present would have been committed earlier with a partial tree (the
        // hole's blocks missing), so its root must be rebuilt explicitly now that the
        // range is complete. Pass highWaterMark so recommitEpoch skips any epoch whose
        // endBlock has not yet been reached, avoiding a permanent partial root for the
        // current in-progress epoch.
        let hwm = await this.transparencyLog.getHighWaterMark();
        for(let epoch of epochs)
            await this.transparencyLog.recommitEpoch(epoch, hwm);

        console.log('Transparency backfill complete for ' + this.chain + '/' + this.network + '/' + this.dbType +
            ': ' + gaps.length + ' block(s) recorded, ' + epochs.size + ' epoch(s) recomputed');
        return gaps.length;
    }

    stop(){
        this.running = false;
    }

    async _poll(){
        let currentBlock = await this.db.getLastBlock();
        if(currentBlock === null) return;

        if(this.lastPolledBlock === null){
            this.lastPolledBlock = currentBlock;
            this.lastPolledBlockHash = await this._sourceBlockHash(currentBlock);
            await this._updateStatus();
            return;
        }

        // Net-forward reorg guard: a rollback then readvance within one poll interval
        // leaves currentBlock >= lastPolledBlock, so the height-only check below never
        // fires, yet the block we already broadcast was orphaned and re-mined. Detect
        // it by re-reading the source hash at lastPolledBlock; a change means the chain
        // forked at or below it. Roll back one block and re-read the prior hash so a
        // deeper reorg is walked back over subsequent polls.
        if(this.lastPolledBlockHash !== null){
            let srcHash = await this._sourceBlockHash(this.lastPolledBlock);
            if(srcHash !== null && srcHash !== this.lastPolledBlockHash){
                // Net-forward reorg: the chain forked at or below lastPolledBlock. Resolve
                // the TRUE fork point WITHIN THIS POLL by walking down over the recorded
                // pre-reorg hashes, instead of one height per poll. The earlier per-poll
                // walk-back left a window on a reorg deeper than one block: while it
                // descended one height per ~poll, the chain could grow and the forward
                // re-stream below overwrote recentBroadcastHashes for a still-orphaned
                // lower block with its POST-reorg hash, so a later compare matched
                // (post vs post), the walk-back stopped short, and a too-shallow reorg
                // was broadcast. The follower then rolled back only to that shallow point,
                // kept stale lower blocks, and its chained recompute diverged. Resolving
                // the full depth in one poll closes that window: one deep reorg is
                // broadcast and the forward loop below re-streams every orphaned block
                // fresh. Bounded by the recorded-hash window (RECENT_HASH_CAP); a fork
                // below it stops at the deepest recorded height (cold-start fallback,
                // same as before), where the follower's recompute/remediation is the net.
                let forkBlock = await this._resolveForkPoint(this.lastPolledBlock);
                console.log('Net-forward reorg detected for ' + this.chain + '/' + this.network + '/' + this.dbType + ' at block ' + forkBlock + ' (content hash changed)');
                if(this.transparencyLog)
                    await this.transparencyLog.pruneFrom(forkBlock);
                // Reorg event message shape:
                //   { type: 'reorg', chain, network, dbType, block_index }
                //   block_index: the first orphaned block height (clients must roll back
                //     all blocks >= block_index and re-apply from the source).
                //   dbType: the database type this poller manages ('indexer' or 'decoder'),
                //     included so a subscriber receiving events for multiple db types can
                //     route the rollback to the correct replica without inspecting the
                //     subscription URL.
                this.broadcaster.broadcast(this.chain, this.network, {
                    type: 'reorg',
                    chain: this.chain,
                    network: this.network,
                    dbType: this.dbType,
                    block_index: forkBlock
                });
                // Re-stream from the fork point: the forward loop below re-broadcasts every
                // orphaned block (forkBlock..currentBlock) with its fresh post-reorg hash.
                this.lastPolledBlock = forkBlock - 1;
                // Seed from the recorded pre-reorg hash at the new height (a fresh source
                // read returns the post-reorg hash and would mask a still-deeper reorg). On
                // a miss (cold start / below the cap) fall back to null, disabling the guard
                // for that step rather than falsely confirming.
                this.lastPolledBlockHash = (this.lastPolledBlock >= 0 && this.recentBroadcastHashes.has(this.lastPolledBlock))
                    ? this.recentBroadcastHashes.get(this.lastPolledBlock) : null;
                await this._updateStatus();
                return;
            }
        }

        if(currentBlock < this.lastPolledBlock){
            // Resolve the TRUE fork point before broadcasting (same walk-back as the
            // net-forward path). A poll can observe the source MID-REWRITE: tip
            // dropped, but replacement blocks already committed at or below
            // currentBlock. Broadcasting reorg@currentBlock+1 in that state is too
            // shallow: the follower rolls back only above currentBlock, keeps stale
            // pre-reorg blocks at/below it, and a catch-up racing the next poll (which
            // would detect the deeper rewrite via the seeded pre-reorg hash) stitches
            // post-reorg blocks onto the stale range: the join recompute then halts on
            // a divergence no delivery interruption caused. Walking the
            // recorded pre-reorg hashes down from currentBlock resolves the full depth
            // in THIS poll, so the one reorg event carries the true fork point.
            let forkBlock = await this._resolveForkPoint(currentBlock + 1);
            console.log('Reorg detected for ' + this.chain + '/' + this.network + '/' + this.dbType + ': block went from ' + this.lastPolledBlock + ' to ' + currentBlock + ' (fork at ' + forkBlock + ')');

            // Prune the source's own transparency log first (indexer only; decoder
            // has no transparencyLog). The indexer rolls back its data tables on a
            // reorg but not these sync-service-owned tables, and recordBlock's
            // INSERT IGNORE would otherwise keep the orphaned blocks' stale hashes
            // and drop the re-added blocks' new ones, serving wrong Merkle proofs.
            // Throwing here leaves lastPolledBlock un-rewound so the next poll
            // re-detects the reorg and retries (prune + broadcast stay together).
            if(this.transparencyLog)
                await this.transparencyLog.pruneFrom(forkBlock);

            // Reorg event shape: see the net-forward reorg broadcast above for the
            // full field documentation. block_index is the first orphaned block.
            this.broadcaster.broadcast(this.chain, this.network, {
                type: 'reorg',
                chain: this.chain,
                network: this.network,
                dbType: this.dbType,
                block_index: forkBlock
            });
            this.lastPolledBlock = forkBlock - 1;
            // Seed from the RECORDED pre-reorg hash (mirror of the net-forward path
            // above): a fresh source read here returns the post-reorg hash, so the
            // next poll's content-change guard would compare post vs post and never
            // fire on a rewrite still deeper than the recorded-hash window resolved.
            // On a miss (cold start / below the cap) fall back to the source read,
            // disabling the guard for that step as before.
            this.lastPolledBlockHash = this.recentBroadcastHashes.has(this.lastPolledBlock)
                ? this.recentBroadcastHashes.get(this.lastPolledBlock)
                : await this._sourceBlockHash(this.lastPolledBlock);
            await this._updateStatus();
            return;
        }

        // Process new blocks (limit to 100 per poll to avoid large bursts)
        let blocksProcessed = 0;
        // Catch-up log throttle: on a fast/lagging chain the server can be thousands
        // of blocks behind and the per-block console line floods the journal. Log only
        // when catching up a batch (last block of the batch) or for individual blocks
        // during normal steady-state follow. This avoids burying real errors in noise
        // while still surfacing progress at batch boundaries.
        let catchUpStart = this.lastPolledBlock;
        let streamTo = currentBlock;
        if(this.lastPolledBlock < currentBlock){
            // Pin the whole forward batch to ONE consistent REPEATABLE READ view.
            // Every payload read (hash header, table rows, and above all the
            // updated_rows in-place-mutation channel) observes the same instant.
            // In steady state the snapshot tip IS the block being streamed, so
            // updated_rows carry exact state-at-B; without the pin they read the
            // source's live tip, and a row re-mutated right after B streamed its
            // future state under B's payload, halting a strict follower's
            // apply-time recompute (deepdive H-P2). During a catch-up burst the
            // pin still bounds every read to one view (the batch tip), so blocks
            // B < snapTip carry tip-state updated_rows; those blocks ship
            // state_hash NULL (burst exemption in _buildBlockPayload) because the
            // follower's apply-time recompute at B would otherwise halt on the
            // future value, while the batch as a whole converges to exact tip
            // state by its last block.
            let snapConn = await this.db.beginReadSnapshot();
            try {
                // The snapshot's own tip is the batch authority: it may sit ahead of
                // the pre-snapshot read (a block landed in between; safe to stream,
                // and it makes the batch end exact) or behind it (a reorg raced us;
                // never build a block the snapshot cannot see, the next poll
                // re-detects it).
                let snapTip = await this.db.getLastBlock(snapConn);
                if(snapTip != null) streamTo = snapTip;
                while(this.lastPolledBlock < streamTo && blocksProcessed < 100){
                    let nextBlock = this.lastPolledBlock + 1;
                    let payload = await this._buildBlockPayload(nextBlock, snapConn, snapTip);
                    if(payload){
                        // Record in transparency log (indexer only; decoder has no synthetic hashes)
                        if(this.transparencyLog){
                            await this.transparencyLog.recordBlock(
                                payload.block_index, payload.block_time,
                                payload.ledger_hash, payload.actions_hash, payload.contract_hash
                            );
                        }

                        // Broadcast to subscribers (infraTables enables filtering for infra-only subscribers)
                        this.broadcaster.broadcast(this.chain, this.network, payload, this.infraTables);

                        // Track the hash we just broadcast so the next poll can detect a
                        // net-forward reorg that rewrites this block.
                        this.lastPolledBlockHash = (this.dbType === 'decoder') ? payload.block_hash : payload.ledger_hash;
                        // Record it for the net-forward walk-back so a deeper reorg can be
                        // detected against this pre-reorg hash on a later poll.
                        this.recentBroadcastHashes.set(nextBlock, this.lastPolledBlockHash);
                        if(nextBlock > RECENT_HASH_CAP)
                            this.recentBroadcastHashes.delete(nextBlock - RECENT_HASH_CAP - 1);
                    } else {
                        // No payload (block vanished mid-poll): disable the hash check for this
                        // step rather than compare against a stale hash next poll.
                        this.lastPolledBlockHash = null;
                    }
                    this.lastPolledBlock = nextBlock;
                    blocksProcessed++;
                }
            } finally {
                await this.db.commitReadSnapshot(snapConn);
            }
        }

        // Log a single summary line for catch-up batches; log each block individually
        // only when following the tip one block at a time (steady-state, low noise).
        if(blocksProcessed > 0){
            let isBatch = (streamTo - catchUpStart) > 1 || blocksProcessed >= 100;
            if(isBatch){
                console.log('Synced blocks ' + (catchUpStart + 1) + '-' + this.lastPolledBlock +
                    ' (' + blocksProcessed + ' block(s)) for ' + this.chain + '/' + this.network + '/' + this.dbType);
            } else {
                console.log('Synced block ' + this.lastPolledBlock + ' for ' +
                    this.chain + '/' + this.network + '/' + this.dbType);
            }
            await this._updateStatus(streamTo);
        }

        return blocksProcessed;
    }

    // Walk the recorded pre-reorg broadcast hashes down from a candidate fork
    // height to the TRUE fork point: descend while the height below still exists
    // on the source with a content hash different from the one WE broadcast for
    // it. Shared by the net-forward and height-drop reorg paths; bounded by the
    // recorded-hash window (RECENT_HASH_CAP). A fork below the window stops at
    // the deepest recorded height (cold-start fallback), where the follower's
    // recompute/remediation is the net.
    async _resolveForkPoint(forkBlock){
        while(forkBlock - 1 >= 1 && this.recentBroadcastHashes.has(forkBlock - 1)){
            let belowSrc = await this._sourceBlockHash(forkBlock - 1);
            if(belowSrc !== null && belowSrc !== this.recentBroadcastHashes.get(forkBlock - 1))
                forkBlock = forkBlock - 1;   // this height also changed; fork is deeper
            else
                break;                       // forkBlock-1 unchanged: true fork point
        }
        return forkBlock;
    }

    // Source content hash at a block, for net-forward reorg detection. Indexer uses
    // the ledger_hash (primary content hash); decoder uses the blockchain block_hash.
    async _sourceBlockHash(blockIndex){
        let row = await this.db.getBlockHashRow(blockIndex);
        if(!row) return null;
        return (this.dbType === 'decoder') ? row.block_hash : row.ledger_hash;
    }

    // Seed the net-forward reorg guard (lastPolledBlockHash) for a (re)start. This
    // MUST come from the DURABLE recorded hash (sync_meta.ledger_hash via the
    // transparency log), NOT a fresh source read. A reorg that completed entirely
    // during downtime leaves the LIVE source content at lastPolledBlock in its
    // post-reorg form; seeding from that would match the first poll's re-read and
    // the guard would never fire, so sync_meta/merkle_epochs keep the pre-reorg
    // hashes and getProof serves internally-consistent but chain-WRONG proofs
    // forever (no reorg event is ever broadcast). Seeding from the recorded
    // (pre-reorg) hash makes the first poll compare recorded(pre) vs live(post) and
    // fire pruneFrom. Indexer-only: the decoder has no transparency log to record
    // from, so it falls back to the live read (its content is deterministic from the
    // coin node and carries no synthetic hash chain to protect). The recorded hash
    // is present for every block the poller broadcast, so the HWM resume point
    // always has one; the live fallback covers only the theoretical miss (and the
    // null-cursor fresh-node case), where disabling the guard for that step is safer
    // than seeding a wrong value.
    async _seedReorgGuardHash(blockIndex){
        if(blockIndex === null) return null;
        if(this.transparencyLog){
            let recorded = await this.transparencyLog.getRecordedHash(blockIndex);
            if(recorded !== null) return recorded;
        }
        return await this._sourceBlockHash(blockIndex);
    }

    // Build a complete block payload for broadcasting.
    // Indexer payload includes ledger_hash/actions_hash/contract_hash and actions-scoped tables.
    // Decoder payload includes the blockchain block hash and tx-scoped tables; no actions.
    //
    // conn: optional REPEATABLE READ snapshot connection (db.beginReadSnapshot). The
    // poll loop pins each forward batch to one snapshot so every read below (hash
    // header, table rows, updated_rows) observes a single point in time. Without it
    // the reads run at the source's live tip, so a surviving row mutated again right
    // after this block streamed its FUTURE state under this block's payload and a
    // strict follower's apply-time recompute halted on the mismatch (deepdive H-P2).
    // viewTip: the pinned snapshot's own tip (db.getLastBlock(conn)); when it sits
    // ahead of block_index (catch-up burst) the indexer payload's state_hash is
    // shipped NULL, see the burst-exemption comment at the state_hash assignment.
    async _buildBlockPayload(block_index, conn, viewTip){
        let hashRow = await this.db.getBlockHashRow(block_index, conn);
        if(!hashRow) return null;

        let payload = {
            type: 'block',
            chain: this.chain,
            network: this.network,
            dbType: this.dbType,
            schema_version: SCHEMA_VERSION[this.dbType],
            block_index: Number(hashRow.block_index),
            block_time: Number(hashRow.block_time),
            data: {}
        };

        if(this.dbType === 'decoder'){
            // Decoder payload: simpler hash field, tx-scoped joins
            payload.block_hash = hashRow.block_hash;
        } else {
            // Indexer payload: three-hash transparency model
            payload.ledger_hash   = hashRow.ledger_hash;
            payload.actions_hash  = hashRow.actions_hash;
            payload.contract_hash = hashRow.contract_hash;
            // Fourth, replication-integrity hash (the in-place mutations + backdated refund
            // credits the three hashes can't cover). Optional top-level field, NOT in
            // sync_meta / the Merkle leaf / the hub-signed checkpoint; a follower with
            // VERIFY_STATE_HASH recomputes it APPLY-TIME and halts on mismatch. May be NULL
            // for blocks indexed before the feature (the follower then skips the check).
            //
            // Burst exemption: during a catch-up batch the pinned view sits at the batch
            // tip, so updated_rows for every block B < viewTip carry row state as of the
            // tip, not as of B (the tick set is B-scoped but `SELECT t.*` reads the pinned
            // view). The follower's apply-time recompute of state_hash(B) reads those rows
            // back and would halt on a value the source never committed at B, even though
            // the replica converges to exact tip state by the end of the batch (each later
            // mutation re-emits its row under its own block). Ship NULL for those blocks so
            // the follower takes its existing pre-feature skip path -- the same posture the
            // incremental-snapshot channel has by design (state_hash-exempt, consistent at
            // its own view tip). Steady-state blocks (viewTip == B, the overwhelmingly
            // common case) keep the full check; ledger/actions/contract hashes and the
            // state-commitment roots are B-scoped committed rows and stay verified on every
            // path.
            payload.state_hash = (viewTip != null && Number(viewTip) > block_index)
                ? null
                : hashRow.state_hash;

            // Light-client state-commitment roots (SPV spec sec.4-5). Top-level fields
            // ONLY, like state_hash: NOT in payload.data (the follower computes its own
            // state_tree_nodes/roots), NOT in sync_meta, NOT in any Merkle leaf. NULL
            // before the flag-day (the follower then skips the check). The follower
            // verifies balances_root + block_merkle_root in Phase 1; state_root is carried
            // for the later full-state_root verification (no wire change needed then).
            if(isStateCommitmentActive(block_index, this.network, this.coinTicker)){
                let roots = await this.db.getStateRootsRow(this.coinTicker, this.network, block_index, conn);
                payload.balances_root    = roots ? roots.balances_root    : null;
                payload.block_merkle_root = roots ? roots.block_merkle_root : null;
                // state_root folds the BTC-only stakes_root, which the follower recomputes
                // every block from the live stakes/unstakes tables (stateCommitment
                // .gatherStakeEntries reading amount/deactivation_block). During a catch-up
                // burst those columns carry TIP-state (post-slash) values, not the values
                // committed at block B, so the follower would recompute state_root over a
                // future amount and durably HALT on a value the source never committed at B.
                // NULL state_root for burst blocks exactly like state_hash above; the
                // follower's per-field compare skips a null state_root. balances_root/
                // block_merkle_root stay live: they derive from B-scoped credit/debit/content
                // rows applied in block order and are not exposed to the tip-state drift.
                payload.state_root       = (viewTip != null && Number(viewTip) > block_index)
                    ? null
                    : (roots ? roots.state_root : null);
            } else {
                payload.balances_root    = null;
                payload.block_merkle_root = null;
                payload.state_root       = null;
            }

            // Replicate the per-block transparency-log row (sync_meta) live. The
            // table is otherwise only carried by snapshots (SnapshotBuilder includes
            // it; ClientRollback prunes it on reorg), so without this the replica's
            // sync_meta drifts behind the source between snapshots. Built inline from
            // the hashes rather than read from the table: the server's
            // transparencyLog.recordBlock runs AFTER this payload is built (see
            // _poll), so the row isn't in sync_meta yet at this point. id/logged_at
            // are node-local and intentionally omitted (the client assigns its own);
            // the client applies sync_meta with INSERT IGNORE on the unique
            // block_index, so re-sends are idempotent.
            payload.data['sync_meta'] = [{
                block_index:   payload.block_index,
                block_time:    payload.block_time,
                ledger_hash:   hashRow.ledger_hash,
                actions_hash:  hashRow.actions_hash,
                contract_hash: hashRow.contract_hash
            }];
        }

        // Block-scoped tables (both indexer and decoder)
        for(let table of this.blockScopedTables){
            if(table === 'transactions') continue;  // Handled below
            try {
                let rows = await this.db.getBlockScopedRows(table, block_index, conn);
                if(rows && rows.length > 0)
                    payload.data[table] = rows;
            } catch(e){
                // Skip a genuine schema gap; re-throw any transient fault so the
                // block is retried rather than broadcast incomplete.
                if(!isSchemaGapError(e)) throw e;
            }
        }

        // Transactions (both indexer and decoder)
        let txRows = await this.db.getTransactions(block_index, conn);
        if(txRows && txRows.length > 0)
            payload.data['transactions'] = txRows;

        if(this.dbType === 'decoder'){
            // Decoder: tx-scoped tables (transaction_outputs)
            for(let table of this.txScopedTables){
                try {
                    let rows = await this.db.getTxScopedRows(table, block_index, conn);
                    if(rows && rows.length > 0)
                        payload.data[table] = rows;
                } catch(e){
                    // Skip a genuine schema gap; re-throw any transient fault so the
                    // block is retried rather than broadcast incomplete.
                    if(!isSchemaGapError(e)) throw e;
                }
            }
        } else {
            // Indexer: actions and action-scoped tables
            let actionRows = await this.db.getActions(block_index, conn);
            if(actionRows && actionRows.length > 0)
                payload.data['actions'] = actionRows;

            // Counters for the action-scoped query-count metric emitted after the loop.
            // Read-only bookkeeping: nothing here reaches payload.data.
            let scopedQueries = 0, scopedNonEmpty = 0, probeQueries = 0;
            let scopedStartedAt = Date.now();

            // Discover in ONE round-trip which action-scoped tables carry rows this block,
            // then fetch only those, because the loop below otherwise queries all 86
            // registry tables, empty ones included, and grows with every table added.
            // Skipping a probe-absent table cannot change payload.data: the probe runs
            // getActionScopedRows' own predicate, so its verdict IS that fetch's row count,
            // and an empty fetch is already dropped by the length check below.
            //
            // scopedTables stays null on ANY probe failure, and on a db without the helper,
            // which restores the query-every-table behaviour verbatim. Swallowing a
            // transient fault here is safe precisely because the fallback re-issues the
            // real fetches: a fault that persists throws from those instead, freezing the
            // cursor rather than broadcasting an incomplete block.
            let scopedTables = null;
            if(typeof this.db.getNonEmptyActionScopedTables === 'function'){
                try {
                    probeQueries = 1;
                    let probed = await this.db.getNonEmptyActionScopedTables(
                        this.actionScopedTables.filter(t => t !== 'actions' && t !== 'contract_emissions'),
                        block_index, conn);
                    scopedTables = (probed && typeof probed.has === 'function') ? probed : null;
                } catch(e){
                    probeQueries = 0;
                    scopedTables = null;
                }
            }

            for(let table of this.actionScopedTables){
                if(table === 'actions') continue; // Already handled
                // contract_emissions has NULL action_index for internal emissions (e.g. SLASH).
                // getActionScopedRows joins on action_index and would drop those rows from the
                // payload, while the consensus hash includes them (via execution_index). A
                // follower would then recompute a divergent contract_hash and halt. Stream them
                // through the execution_index chain instead, matching BlockHasher exactly.
                if(table === 'contract_emissions'){
                    try {
                        scopedQueries++;
                        let rows = await this.db.getEmissionRowsForBlock(block_index, conn);
                        if(rows && rows.length > 0){
                            payload.data[table] = rows;
                            scopedNonEmpty++;
                        }
                    } catch(e){
                        // Skip a genuine schema gap; re-throw any transient fault so the
                        // block is retried rather than broadcast incomplete.
                        if(!isSchemaGapError(e)) throw e;
                    }
                    continue;
                }
                // Probe said this table has no rows for this block, so getActionScopedRows
                // would return [] and the length check below would drop it anyway.
                if(scopedTables && !scopedTables.has(table)) continue;
                try {
                    scopedQueries++;
                    let rows = await this.db.getActionScopedRows(table, block_index, conn);
                    if(rows && rows.length > 0){
                        payload.data[table] = rows;
                        scopedNonEmpty++;
                    }
                } catch(e){
                    // Skip a genuine schema gap; re-throw any transient fault so the
                    // block is retried rather than broadcast incomplete.
                    if(!isSchemaGapError(e)) throw e;
                }
            }

            this._reportActionScopedQueryMetric(scopedQueries, scopedNonEmpty,
                                               Date.now() - scopedStartedAt, probeQueries);

            // Cooldown-maturity refund credits mint AT this block but carry the
            // unstake's earlier-block action_index (and no block_index), so the
            // action-scoped join above misses them, leaving followers permanently
            // short by every matured refund. Select them by maturity block
            // (cooldown_end_block = this block), the forward mirror of
            // ClientRollback's reverse delete, and merge into the credits payload;
            // ClientApplier then upserts them and rebuilds balances like any other
            // credit. Disjoint from the action-scoped credits (those carry an action
            // in THIS block; a refund's action is in an earlier block), but dedup the
            // union defensively on the credit's logical identity.
            try {
                let matured = await collectMaturedCooldownCredits(this.db, block_index, block_index, conn);
                if(matured.length > 0){
                    let existing = payload.data['credits'] || [];
                    let seen = new Set(existing.map(c => c.action_index + ':' + c.address_id + ':' + c.tick_id));
                    for(let c of matured){
                        let k = c.action_index + ':' + c.address_id + ':' + c.tick_id;
                        if(!seen.has(k)){ seen.add(k); existing.push(c); }
                    }
                    payload.data['credits'] = existing;
                }
            } catch(e){
                // Skip a genuine schema gap; re-throw any transient fault so the
                // block is retried rather than broadcast incomplete.
                if(!isSchemaGapError(e)) throw e;
            }

            // Recovery-redriven validator rewards: a reorg re-drain re-materializes a
            // survivor reward at block_index = earn-block E < B, so the block-scoped
            // getBlockScopedRows path (forward from B) misses it. Select by applied_block
            // (= this block, the re-drain point), the forward analogue of ClientRollback's
            // block_index >= B delete, and merge into the validator_rewards payload deduped
            // on the row's UNIQUE identity. Disjoint from the block-scoped rows (those carry
            // block_index = this block; a survivor's earn-block is earlier).
            try {
                let redriven = await collectRedrivenValidatorRewards(this.db, block_index, block_index, conn);
                if(redriven.length > 0){
                    let existing = payload.data['validator_rewards'] || [];
                    let seen = new Set(existing.map(r => r.source_id + ':' + r.signing_pubkey_id + ':' + r.reward_type + ':' + r.round_reference));
                    for(let r of redriven){
                        let k = r.source_id + ':' + r.signing_pubkey_id + ':' + r.reward_type + ':' + r.round_reference;
                        if(!seen.has(k)){ seen.add(k); existing.push(r); }
                    }
                    payload.data['validator_rewards'] = existing;
                }
            } catch(e){
                // Skip a genuine schema gap; re-throw any transient fault so the
                // block is retried rather than broadcast incomplete.
                if(!isSchemaGapError(e)) throw e;
            }

            // Derived anchor/archive validator rewards: the BTC-side derivation writes the
            // row while processing THIS block but stamps block_index = the checkpoint's
            // SNAPSHOT_BLOCK E (< this block), so getBlockScopedRows never carries it.
            // Select by derive_block_index (= this block, the materialization point), the
            // forward twin of ClientRollback's derive_block_index >= B delete, and merge
            // deduped on the UNIQUE identity exactly like the redriven rows above. The
            // reconcile that collapses the round to its winner runs in the same block on the
            // source, so only survivors are read here; the losers' pre-images ride the
            // anchor_reward_reconcile_log rows this payload already carries.
            try {
                let derived = await collectDerivedAnchorRewards(this.db, block_index, block_index, conn);
                if(derived.length > 0){
                    let existing = payload.data['validator_rewards'] || [];
                    let seen = new Set(existing.map(r => r.source_id + ':' + r.signing_pubkey_id + ':' + r.reward_type + ':' + r.round_reference));
                    for(let r of derived){
                        let k = r.source_id + ':' + r.signing_pubkey_id + ':' + r.reward_type + ':' + r.round_reference;
                        if(!seen.has(k)){ seen.add(k); existing.push(r); }
                    }
                    payload.data['validator_rewards'] = existing;
                }
            } catch(e){
                // Skip a genuine schema gap; re-throw any transient fault so the
                // block is retried rather than broadcast incomplete.
                if(!isSchemaGapError(e)) throw e;
            }
        }

        // Index tables: get entries referenced by this block's data.
        // Both indexer and decoder use this pattern (decoder has fewer index tables).
        // The client uses INSERT IGNORE so duplicates are harmless.
        for(let table of this.indexTables){
            try {
                // index_transactions: every `*_hash_id` this block's own rows carry, DERIVED
                // from the column suffix rather than listed. The generic `*_id` scan below
                // skips this table, so a hash column absent here reaches a follower as a
                // permanently dangling reference: its blocks row is correct, its LEFT JOIN
                // (getBlockHashRow, and the explorer's identical join) resolves that hash to
                // NULL for every block above the last snapshot, which is the only thing that
                // re-dumps this table in full. One rule for both dbTypes, so a hash column
                // added later cannot re-open the gap.
                if(table === 'index_transactions'){
                    let ids = [];
                    let collectHashIds = (row) => {
                        for(let col in row){
                            if(col.length > 8 && col.slice(-8) === '_hash_id' && row[col] != null)
                                ids.push(row[col]);
                        }
                    };
                    // Decoder: blocks carry block_hash_id + previous_block_hash_id.
                    // Indexer: blocks carry ledger/actions/contract/state hash ids.
                    // Both: transactions carry tx_hash_id.
                    if(payload.data['blocks'])
                        for(let b of payload.data['blocks']) collectHashIds(b);
                    if(payload.data['transactions'])
                        for(let tx of payload.data['transactions']) collectHashIds(tx);
                    if(ids.length > 0){
                        let unique = [...new Set(ids)];
                        let rows = await this.db.doQuery("SELECT * FROM index_transactions WHERE id IN (" + unique.map(() => '?').join(',') + ")", unique, conn);
                        if(rows && rows.length > 0)
                            payload.data[table] = rows;
                    }
                }
                // index_addresses: collect referenced address IDs from transactions
                else if(table === 'index_addresses' && payload.data['transactions']){
                    let ids = [];
                    for(let tx of payload.data['transactions']){
                        if(tx.source_id) ids.push(tx.source_id);
                        if(tx.destination_id) ids.push(tx.destination_id);
                    }
                    // Decoder: also collect from transaction_outputs
                    if(this.dbType === 'decoder'){
                        if(payload.data['transaction_outputs']){
                            for(let o of payload.data['transaction_outputs'])
                                if(o.destination_id) ids.push(o.destination_id);
                        }
                    }
                    if(ids.length > 0){
                        let unique = [...new Set(ids)];
                        let rows = await this.db.doQuery("SELECT * FROM index_addresses WHERE id IN (" + unique.map(() => '?').join(',') + ")", unique, conn);
                        if(rows && rows.length > 0)
                            payload.data[table] = rows;
                    }
                }
                // pubkeys: decoder-only; fetch any pubkeys for addresses referenced this block
                else if(table === 'pubkeys' && this.dbType === 'decoder' && payload.data['index_addresses']){
                    let ids = payload.data['index_addresses'].map(a => a.id).filter(id => id != null);
                    if(ids.length > 0){
                        let rows = await this.db.doQuery("SELECT * FROM pubkeys WHERE address_id IN (" + ids.map(() => '?').join(',') + ")", ids, conn);
                        if(rows && rows.length > 0)
                            payload.data[table] = rows;
                    }
                }
                // events: decoder-only operational log with no block_index/tx_index
                // cursor, so it can't be scoped per-block; it is intentionally skipped
                // here. It converges via snapshots instead: both the full snapshot and
                // every incremental snapshot re-dump the events table in full (the client
                // applies them with INSERT IGNORE on the AUTO_INCREMENT id PK, so repeated
                // dumps are idempotent). See SnapshotBuilder.streamIncrementalSnapshot.
                // For other index tables, the generic _id-reference pass below
                // (indexer only) extracts them; see the comment there.
            } catch(e){
                // Skip a genuine schema gap; re-throw any transient fault so the
                // block is retried rather than broadcast incomplete.
                if(!isSchemaGapError(e)) throw e;
            }
        }

        // Indexer only: extract the remaining interned-lookup index tables
        // (index_actions, index_statuses, index_tickers, index_fiats, index_coins,
        // index_memos, index_mime_types, index_pubkeys). Each is an append-only
        // string-interning table referenced by `*_id` columns scattered across
        // dozens of action/block-scoped tables. The references are NOT a clean
        // suffix convention (e.g. lists.item_id and orders.give_tick_id/get_coin_id
        // all point at index_tickers/index_coins). Hardcoding every referencing
        // column would silently drop a brand-new interned value the first time a new
        // action type appears mid-stream, until the next snapshot backfilled it.
        // Instead, pool every `*_id` value present in this block's already-assembled
        // payload and fetch the matching rows from each remaining index table.
        // Over-fetch is harmless: the rows exist on the source, the client applies
        // them INSERT IGNORE on the PK (ClientApplier.ignoreTables), so the replica's
        // index_* set stays a subset of the source's and never overshoots the
        // row-count completeness check.
        // index_transactions keeps its explicit-only join above (it is referenced by
        // block-hash/tx-hash IDs the generic _id scan can't see). index_addresses IS
        // re-fetched here: the explicit join above sees only tx source/dest, but an
        // address can first receive its in-block id via a non-tx column (credits.address_id,
        // contract_executions.caller_id, XCALL/XEXEC counterparties, action-data recipients),
        // which only the generic _id scan reaches.
        if(this.dbType !== 'decoder'){
            let refIds = new Set();
            for(let t in payload.data){
                let rows = payload.data[t];
                if(!Array.isArray(rows)) continue;
                for(let row of rows){
                    for(let col in row){
                        if(col.length > 3 && col.slice(-3) === '_id'){
                            let v = row[col];
                            if(v !== null && v !== undefined) refIds.add(v);
                        }
                    }
                }
            }
            if(refIds.size > 0){
                let idList = [...refIds];
                let placeholders = idList.map(() => '?').join(',');
                for(let table of this.indexTables){
                    // index_transactions carries block-hash/tx-hash IDs the generic _id
                    // scan can't see, so it keeps its explicit join above and is skipped here.
                    if(table === 'index_transactions') continue;
                    // index_addresses is pre-populated tx-only by the explicit join above;
                    // re-fetch it here over the full ref set (a superset of the tx-only set,
                    // since the scan also sees transactions.source_id/destination_id) so a
                    // non-tx-interned address is streamed at its intern block. Without this it
                    // is never delivered, forking the follower's index map (reorg-gated
                    // divergence today; a per-block halt once the index-map state_hash class
                    // is armed). For every other table the already-populated skip stands.
                    if(table !== 'index_addresses' && payload.data[table]) continue;  // defensive: already populated
                    try {
                        let rows = await this.db.doQuery("SELECT * FROM `" + table + "` WHERE id IN (" + placeholders + ")", idList, conn);
                        if(rows && rows.length > 0)
                            payload.data[table] = rows;
                    } catch(e){
                        // Skip a genuine schema gap; re-throw any transient fault so the
                        // block is retried rather than broadcast incomplete.
                        if(!isSchemaGapError(e)) throw e;
                    }
                }
            }
        }

        // In-place mutations to SURVIVING (below-window) rows: deactivation_block
        // stamps, SLASH amount reductions, and v0 request_status flips are not
        // reachable by the action_index-scoped joins above (those rows were created
        // by an earlier block's action). Carry their current full state in a separate
        // top-level `updated_rows` map so the follower can UPSERT them; without this
        // every forward in-place mutation is silently dropped on the replica. Indexer
        // only (decoder has none of these tables). tokens.escrow_action_index rides
        // along (the tokens class carries the full row); the follower additionally
        // re-derives it from the replicated offer/status tables when a payload
        // touches an escrow table (ClientApplier._maybeRederiveEscrow), so the wire
        // value is a convergent carry, not the gate's only writer. Kept OUT of payload.data so an
        // old follower that doesn't recognise the field simply ignores it (its apply
        // loop iterates payload.data only) rather than mis-applying a non-row map.
        if(this.dbType !== 'decoder'){
            try {
                // conn matters most HERE: these tables are exactly the ones mutated in
                // place, so tip-reads (the pre-snapshot behavior) could stream a row's
                // post-B state under block B's payload (deepdive H-P2).
                let updated = await collectUpdatedRows(this.db, block_index, block_index, this.activationDelay, conn);
                if(updated && Object.keys(updated).length > 0)
                    payload.updated_rows = updated;
            } catch(e){
                // Only a genuine schema gap may be skipped; any transient fault must
                // re-throw so the block is retried rather than broadcast without
                // updated_rows (a dropped in-place mutation forks every follower).
                console.error('updated_rows collection failed for block ' + block_index + ':', e);
                if(!isSchemaGapError(e)) throw e;
            }
        }

        return payload;
    }

    // Emit a throttled [METRIC] line recording how many action-scoped round-trips a
    // block cost and how many of them carried rows. probe_queries is 1 when the
    // non-empty-table probe answered (so `queries` is content-shaped) and 0 when the
    // build fell back to querying every registry table, which is what makes a silent
    // regression to the old N+1 visible rather than merely slow. Reads
    // counters only, never payload.data, so the consensus hash is untouched. Interval is
    // SYNC_QUERY_METRIC_INTERVAL_MS (default 15m, 0 disables), so this is one line per
    // interval, not per block. Twin of SyncService's STATE_TREE_METRIC_INTERVAL_MS.
    _reportActionScopedQueryMetric(queries, nonEmpty, elapsedMs, probeQueries){
        let raw = parseInt(process.env.SYNC_QUERY_METRIC_INTERVAL_MS, 10);
        let intervalMs = Number.isFinite(raw) ? raw : (15 * 60 * 1000);
        if(intervalMs === 0) return;   // explicitly disabled
        let now = Date.now();
        if(this._lastQueryMetricAt && (now - this._lastQueryMetricAt) < intervalMs) return;
        this._lastQueryMetricAt = now;
        console.log('[METRIC] ' + JSON.stringify({
            metric: 'sync_action_scoped_queries_per_block', component: 'sync',
            chain: this.chain, network: this.network, db_type: this.dbType,
            candidate_tables: this.actionScopedTables ? this.actionScopedTables.length : 0,
            queries: queries, probe_queries: probeQueries || 0,
            non_empty_tables: nonEmpty, elapsed_ms: elapsedMs,
            ts: now
        }));
    }

    async _updateStatus(sourceBlockHeight){
        let hashRow = this.lastPolledBlock ? await this.db.getBlockHashRow(this.lastPolledBlock) : null;
        let status = {
            dbType:              this.dbType,
            block_height:        this.lastPolledBlock,
            block_time:          hashRow ? Number(hashRow.block_time) : null,
            source_block_height: sourceBlockHeight != null ? sourceBlockHeight : (await this.db.getLastBlock()),
            poll_error_count:    this.pollErrorCount
        };
        // Replication freshness. source_block_height above is read from the
        // SERVED database, so on a node fronting a native SQL replica both heights
        // freeze together when replication stalls and the derived lag reads 0.
        let rep = await this._readReplicaStatus();
        status.replica_seconds_behind = rep.secondsBehind;
        status.replica_stale          = rep.stale;
        if(this.dbType === 'decoder'){
            status.block_hash = hashRow ? hashRow.block_hash : null;
        } else {
            status.ledger_hash   = hashRow ? hashRow.ledger_hash : null;
            status.actions_hash  = hashRow ? hashRow.actions_hash : null;
            status.contract_hash = hashRow ? hashRow.contract_hash : null;
        }
        status.subscriber_count = this.broadcaster.getSubscriberCount(this.chain, this.network, this.dbType);
        this.broadcaster.updateStatus(this.chain, this.network, status);
    }

    // Fail closed on everything except a confirmed primary. A stopped SQL thread
    // reports Seconds_Behind_Source NULL, which is unbounded lag, never zero; an
    // unreadable status (no grant, older db object without the method) is unknown
    // and must not certify freshness either.
    async _readReplicaStatus(){
        let rep = null;
        try {
            if(typeof this.db.getReplicaStatus === 'function')
                rep = await this.db.getReplicaStatus();
        } catch (e){
            this.util.logError('Replication status read failed:', e);
        }
        if(!rep) return { secondsBehind: null, stale: true };
        if(rep.isReplica === false) return { secondsBehind: null, stale: false };
        let maxLag = Number(this.config.SYNC_REPLICA_MAX_LAG_S);
        if(!Number.isFinite(maxLag)) maxLag = 120;
        let stale = rep.isReplica !== true
            || !rep.running
            || rep.secondsBehind == null
            || rep.secondsBehind > maxLag;
        return { secondsBehind: rep.secondsBehind, stale };
    }
}

module.exports = ServerPoller;
