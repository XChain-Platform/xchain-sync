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

const replicatedTables = require('../schema/replicated_tables');
const lifecycle = require('../table_lifecycle');
const { collectUpdatedRows } = require('./updated_rows');
const { collectMaturedCooldownCredits, collectMaturedCooldownEscrows, mergeMaturedRows } = require('./cooldown_credits');
const { collectRedrivenValidatorRewards } = require('./recovery_rewards');
const { collectDerivedAnchorRewards } = require('./derived_rewards');
const seedReorgWindow = require('./poller/reorg_window_seed');
const { activationDelayBlocks, coinTicker } = require('../consensus-constants');
const { isStateCommitmentActive } = require('../consensus/gates/state_commitment_gate');
const { SCHEMA_VERSION } = require('../schema/version');
const util = require('node:util');
const { getLogger } = require('../observability');
const envConfig = require('../config');
const logger = getLogger();

// Retain at least this many broadcast hashes for the reorg walk-back, raised per chain to the
// source reorg ceiling plus a margin so every reorg an honest source emits resolves (recentHashCap).
const RECENT_HASH_CAP_FLOOR = 256;
const RECENT_HASH_CAP_MARGIN = 16;

// Derive the reorg-scoped lookups from the lifecycle registry, so forward streaming,
// both rollbacks and the content-parity bound always name the same tables.
const BLOCK_SCOPED_INDEX_TABLES = lifecycle.tablesWhere(t => t.rollback === 'index' && t.replication === 'stream:index');

// A per-table read in buildBlockPayload may legitimately fail because the source
// runs an older schema that lacks the table/column (errno 1146 missing table, 1054
// unknown column): that table is simply absent from this block's payload, harmless.
// EVERY OTHER error (deadlock 1213, lock-wait timeout 1205, connection drop, etc.)
// is a transient/operational fault, and swallowing it would broadcast a structurally
// valid but silently INCOMPLETE block that followers durably record (false
// VERIFY_RECOMPUTE halts on hashed tables; silent stake-state divergence on the
// unhashed slash-debit/reconcile tables ClientRollback restores from on reorg). Only
// schema gaps may be skipped; anything else must re-throw so poll's loop freezes the
// cursor and retries the block. Mirrors SnapshotBuilder.streamIncrementalSnapshot's
// errno discrimination.
function isSchemaGapError(e){
    return !!(e && (e.errno === 1146 || e.errno === 1054));
}

function initializePollerIdentity(poller, chain, network, db, broadcaster,
    transparencyLog, config, util){
    poller.chain = chain;
    // Keep caller `chain` form for routing, payload fields, and logs.
    // Resolve a ticker separately for per-chain activation lookups.

    // Match state_tree_roots.chain to the ticker written by the source indexer.
    // Use the same ticker for the state-commitment activation lookup.
    // Prevent null roots from bypassing the follower commitment check.

    // Preserve caller form for broadcast routing and payload `chain:` fields.
    // Route log messages under the caller's full chain name.
    // Keep ticker conversion isolated to fields that require canonical form.
    poller.coinTicker = coinTicker(chain);
    poller.network = network;
    poller.db = db;
    poller.broadcaster = broadcaster;
    poller.transparencyLog = transparencyLog;  // null for decoder; non-null for indexer
    poller.config = config;
    poller.util = util;
    poller.dbType = (db && db.dbType) ? db.dbType : 'indexer';
}

function initializeActivationDelay(poller, chain){
    // Freeze the per-chain activation delay for forward deactivation stamps.
    // Support the in-place updated-rows channel with its consensus delay.
    // Keep null only for an omitted coin (test harnesses) and for decoders.

    // Refuse an indexer coin with no frozen delay, as ClientRollback does: a null here
    // makes collectUpdatedRows silently drop the deactivation class from every payload.
    // SyncService.discoverChains skips such a chain first, so this is the backstop.
    let delay = activationDelayBlocks(chain);
    if(delay === undefined && poller.dbType === 'indexer'){
        throw new Error('ServerPoller: unrecognized coin "' + chain + '" - no frozen ACTIVATION_DELAY_BLOCKS (see src/consensus-constants.js)');
    }
    poller.activationDelay = (delay === undefined) ? null : delay;
}

function initializeCursorState(poller, chain, network){
    poller.lastPolledBlock = null;
    // Track the source content hash for the last polled block.
    // Detect net-forward reorgs whose height remains monotonic.
    // Compare content hashes after rollback and readvance in one interval.
    poller.lastPolledBlockHash = null;

    // Bound recently broadcast hashes by block index and content hash.
    // Seed walk-back from the pre-reorg hash for net-forward reorgs.
    // Continue deep walk-back across later polls.

    // Support both database types without relying on decoder sync metadata.
    // Retain the chain-specific safe rollback ceiling plus a margin.
    // Enforce the global minimum window for short rollback configurations.

    // Cap retained entries to recentHashCap heights.
    // Initialize the map before computing its chain-specific capacity.
    poller.recentBroadcastHashes = new Map();
    poller.recentHashCap = Math.max(RECENT_HASH_CAP_FLOOR,
        envConfig.rollbackDepthSafeCeiling(chain, network) + RECENT_HASH_CAP_MARGIN);
    poller.running = false;
}

function initializeTableTopology(poller){
    // Share replicated topology with row-count completeness checks.
    // Read every scope from src/schema/replicated_tables.js.
    let topo = replicatedTables.getTopology(poller.dbType);
    poller.blockScopedTables = topo.blockScoped;
    poller.txScopedTables = topo.txScoped;
    poller.actionScopedTables = topo.actionScoped;
    poller.indexTables = topo.index;
}

function initializeInfrastructureTables(poller){
    if(poller.dbType === 'decoder'){
        // Leave decoder without cross-chain infrastructure tables.
        poller.infraTables = new Set();
        return;
    }

    // Sync infrastructure tables regardless of subscriber mode.
    // Provide cross-chain validator, reward, and price-query state.

    // Send only these tables to infra-only subscribers for this chain.
    // Keep the set identical for every indexer poller instance.
    poller.infraTables = new Set([
        'stakes', 'delegations', 'validator_rewards', 'prices', 'reward_claims',
        'index_pubkeys', 'index_addresses', 'index_actions', 'index_statuses', 'index_fiats'
    ]);
}

function initializePollerHealthState(poller){
    // Count consecutive poll failures for stale /health status.
    // Reset the count after a successful poll cycle.
    poller.pollErrorCount = 0;

    // Throttle the action-scoped query-count metric.
    // Use zero so the first block publishes a baseline.
    // Update the stamp in reportActionScopedQueryMetric.
    poller._lastQueryMetricAt = 0;
}

class ServerPoller {

    constructor(chain, network, db, broadcaster, transparencyLog, config, util) {
        initializePollerIdentity(this, chain, network, db, broadcaster,
            transparencyLog, config, util);
        initializeActivationDelay(this, chain);
        initializeCursorState(this, chain, network);
        initializeTableTopology(this);
        initializeInfrastructureTables(this);
        initializePollerHealthState(this);
    }

    async start(){
        this.lastPolledBlock = await this.resumeCursor();
        this.lastPolledBlockHash = await seedReorgWindow(this, logger);
        this.running = true;
        logger.info('ServerPoller started for ' + this.chain + '/' + this.network + '/' + this.dbType + ' at block ' + (this.lastPolledBlock || 'none'));

        // Repair any interior transparency-log holes left by a pre-fix restart that
        // resumed from the source tip. No-op on a healthy log (and on the decoder,
        // which has no transparency log). Failure here must not stop live polling;
        // the backfill is idempotent and retries on the next restart.
        try {
            await this.backfillGaps();
        } catch(e){
            logger.error(util.format('Transparency backfill failed for ' + this.chain + '/' + this.network + '/' + this.dbType + ' (continuing with live polling):', e));
        }

        await this.updateStatus();

        while(this.running){
            let blocksProcessed = 0;
            try {
                blocksProcessed = await this.poll() || 0;
                this.pollErrorCount = 0;
            } catch(e){
                this.pollErrorCount++;
                logger.error(util.format('ServerPoller error for ' + this.chain + '/' + this.network + '/' + this.dbType + ' (consecutive errors: ' + this.pollErrorCount + '):', e));
                // Update status so the poll_error_count field is current even while
                // lastPolledBlock is frozen at the last-good value.
                await this.updateStatus().catch(() => {});
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
    // tip would jump the cursor past every missed block, so poll's while-loop never
    // runs for them, recordBlock is never called, and any epoch boundary in the gap
    // is never committed, leaving a permanent hole in sync_meta and missing Merkle
    // proofs while the poller falsely reports caught-up. A null high-water mark (empty
    // sync_meta, fresh node) leaves the cursor null so poll initialises from the
    // current tip on its first pass, as before. The decoder has no transparency log,
    // so it resumes from the source tip (decoder content is deterministic from the
    // coin node and carries no synthetic hash chain to keep gap-free).
    async resumeCursor(){
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

        logger.info('Transparency backfill: ' + gaps.length + ' missing block(s) detected for ' +
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

        logger.info('Transparency backfill complete for ' + this.chain + '/' + this.network + '/' + this.dbType +
            ': ' + gaps.length + ' block(s) recorded, ' + epochs.size + ' epoch(s) recomputed');
        return gaps.length;
    }

    stop(){
        this.running = false;
    }

    async poll(){
        // A failed cursor read throws so an outage cannot look like an idle chain.
        let currentBlock = await this.db.getLastBlock(null, { rethrow: true });
        if(currentBlock === null) return;

        // Cursor initialization observes the current source tip without replaying it.
        // Later polls compare its content hash before considering height movement,
        // which detects replacement blocks even when the source remains ahead.
        if(this.lastPolledBlock === null){
            await this.initializePollCursor(currentBlock);
            return;
        }

        if(this.lastPolledBlockHash !== null && await this.handleNetForwardReorg()) return;

        if(currentBlock < this.lastPolledBlock){
            await this.handleHeightDropReorg(currentBlock);
            return;
        }

        let blocksProcessed = 0;
        let catchUpStart = this.lastPolledBlock;
        let streamTo = currentBlock;
        if(this.lastPolledBlock < currentBlock)
            ({ blocksProcessed, catchUpStart, streamTo } = await this.streamNewBlocks(currentBlock));
        this.logSyncedBlocks(blocksProcessed, catchUpStart, streamTo);

        // Every poll refreshes replica health, including polls where the tip is idle.
        await this.updateStatus(streamTo);

        return blocksProcessed;
    }

    async initializePollCursor(currentBlock){
        // A fresh cursor begins at the visible tip and records that tip's content hash.
        // Status publication follows both reads, so observers never see an initialized
        // height paired with an uninitialized reorg guard.
        this.lastPolledBlock = currentBlock;
        this.lastPolledBlockHash = await this.sourceBlockHash(currentBlock);
        await this.updateStatus();
    }

    async handleNetForwardReorg(){
        // The guard compares the last broadcast content with the source's current row.
        // A missing source row leaves height-drop handling to a later poll, while an
        // equal hash proves that this specific height still belongs to the same branch.
        let srcHash = await this.sourceBlockHash(this.lastPolledBlock);
        if(srcHash === null || srcHash === this.lastPolledBlockHash) return false;

        // The recorded hashes locate the fork even when the source tip stays ahead.
        let forkBlock = await this.resolveForkPoint(this.lastPolledBlock);
        logger.info('Net-forward reorg detected for ' + this.chain + '/' + this.network + '/' + this.dbType + ' at block ' + forkBlock + ' (content hash changed)');
        if(this.transparencyLog)
            await this.transparencyLog.pruneFrom(forkBlock);
        // Pruning completes before publication so clients cannot begin rollback while
        // the server still exposes orphaned transparency leaves. The cursor then moves
        // below the fork, making the next forward pass replay every replacement block.
        this.broadcastReorg(forkBlock);
        this.lastPolledBlock = forkBlock - 1;
        // A recorded pre-reorg hash keeps deeper rewrites visible on the next poll.
        this.lastPolledBlockHash = (this.lastPolledBlock >= 0 && this.recentBroadcastHashes.has(this.lastPolledBlock))
            ? this.recentBroadcastHashes.get(this.lastPolledBlock) : null;
        await this.updateStatus();
        return true;
    }

    async handleHeightDropReorg(currentBlock){
        // A falling tip can expose replacement content below the new height, so the
        // recorded window identifies the true fork instead of treating currentBlock + 1
        // as authoritative, keeping the rollback deep enough for joined hashes.
        let forkBlock = await this.resolveForkPoint(currentBlock + 1);
        logger.info('Reorg detected for ' + this.chain + '/' + this.network + '/' + this.dbType + ': block went from ' + this.lastPolledBlock + ' to ' + currentBlock + ' (fork at ' + forkBlock + ')');
        if(this.transparencyLog)
            await this.transparencyLog.pruneFrom(forkBlock);

        // The broadcast occurs only after transparency cleanup succeeds. Any cleanup
        // error leaves the cursor untouched, allowing the enclosing poll loop to retry
        // the complete prune, notification, cursor, hash and status sequence.
        this.broadcastReorg(forkBlock);
        this.lastPolledBlock = forkBlock - 1;
        // The source hash is a fallback when the bounded recorded window has no entry.
        this.lastPolledBlockHash = this.recentBroadcastHashes.has(this.lastPolledBlock)
            ? this.recentBroadcastHashes.get(this.lastPolledBlock)
            : await this.sourceBlockHash(this.lastPolledBlock);
        await this.updateStatus();
    }

    broadcastReorg(forkBlock){
        // block_index identifies the first orphaned height, so a subscriber removes
        // that height and every successor before accepting replacement block events.
        // dbType routes the rollback when one subscriber follows both database tracks.
        this.broadcaster.broadcast(this.chain, this.network, {
            type: 'reorg',
            chain: this.chain,
            network: this.network,
            dbType: this.dbType,
            block_index: forkBlock
        });
    }

    async streamNewBlocks(currentBlock){
        // The result carries both progress and logging bounds back to poll. Keeping the
        // outer read as the initial ceiling preserves idle behavior when the snapshot
        // reports no usable tip, while a visible snapshot tip becomes authoritative.
        let result = {
            blocksProcessed: 0,
            catchUpStart: this.lastPolledBlock,
            streamTo: currentBlock
        };

        // One repeatable-read snapshot pins every payload read in the forward batch.
        let snapConn = await this.db.beginReadSnapshot();
        try {
            // The snapshot tip bounds the batch when a block or reorg races the first read.
            let snapTip = await this.db.getLastBlock(snapConn);
            if(snapTip != null) result.streamTo = snapTip;
            // The hundred-block cap yields control to the service loop without changing
            // snapshot consistency inside this batch. start() skips its sleep when the
            // cap is reached, allowing a backlog to continue on a newly pinned view.
            while(this.lastPolledBlock < result.streamTo && result.blocksProcessed < 100){
                let nextBlock = this.lastPolledBlock + 1;
                let payload = await this.buildBlockPayload(nextBlock, snapConn, snapTip);
                if(payload){
                    // Transparency recording precedes publication, ensuring a delivered
                    // indexer block already has its durable proof leaf. Decoder payloads
                    // omit that step because their source block hash is canonical.
                    if(this.transparencyLog){
                        await this.transparencyLog.recordBlock(
                            payload.block_index, payload.block_time,
                            payload.ledger_hash, payload.actions_hash, payload.contract_hash
                        );
                    }
                    this.publishBlockPayload(payload, nextBlock);
                } else {
                    // A vanished block disables content comparison for this cursor step.
                    // Retaining an older hash here could turn an incomplete snapshot read
                    // into a false net-forward reorg on the next polling iteration.
                    this.lastPolledBlockHash = null;
                }
                // Cursor advancement happens after all payload awaits and publication.
                // The ordering keeps a rejected record operation retryable and counts a
                // missing payload as examined without claiming a broadcast hash for it.
                this.lastPolledBlock = nextBlock;
                result.blocksProcessed++;
            }
        } finally {
            // Snapshot release is the final await in forward processing and also runs
            // when payload construction or transparency recording throws. A release
            // failure propagates so the polling loop reports the database fault.
            await this.db.commitReadSnapshot(snapConn);
        }
        return result;
    }

    publishBlockPayload(payload, nextBlock){
        // Publication happens before local hash bookkeeping, matching the externally
        // visible event order. The selected hash is the same content identity that the
        // next poll reads from each source database type.
        this.broadcaster.broadcast(this.chain, this.network, payload, this.infraTables);
        this.lastPolledBlockHash = (this.dbType === 'decoder') ? payload.block_hash : payload.ledger_hash;
        this.recentBroadcastHashes.set(nextBlock, this.lastPolledBlockHash);
        // The bounded map retains enough pre-reorg identities for the configured source
        // ceiling plus its safety margin. Eviction removes only the single height that
        // falls beyond the moving window after a successful publication.
        if(nextBlock > this.recentHashCap)
            this.recentBroadcastHashes.delete(nextBlock - this.recentHashCap - 1);
    }

    logSyncedBlocks(blocksProcessed, catchUpStart, streamTo){
        // Idle polls stay silent. Multi-block ranges and capped batches produce one
        // summary, while normal tip following retains the concise single-block event
        // that operators use to confirm steady source progress.
        if(blocksProcessed === 0) return;

        let isBatch = (streamTo - catchUpStart) > 1 || blocksProcessed >= 100;
        if(isBatch){
            logger.info('Synced blocks ' + (catchUpStart + 1) + '-' + this.lastPolledBlock +
                ' (' + blocksProcessed + ' block(s)) for ' + this.chain + '/' + this.network + '/' + this.dbType);
        } else {
            logger.info('Synced block ' + this.lastPolledBlock + ' for ' +
                this.chain + '/' + this.network + '/' + this.dbType);
        }
    }

    // Walk the recorded pre-reorg broadcast hashes down from a candidate fork
    // height to the TRUE fork point: descend while the height below still exists
    // on the source with a content hash different from the one WE broadcast for
    // it. Shared by the net-forward and height-drop reorg paths; bounded by the
    // recorded-hash window (recentHashCap). A fork below the window stops at
    // the deepest recorded height (cold-start fallback), where the follower's
    // recompute/remediation is the net.
    async resolveForkPoint(forkBlock){
        let start = forkBlock;
        while(forkBlock - 1 >= 1 && this.recentBroadcastHashes.has(forkBlock - 1)){
            let belowSrc = await this.sourceBlockHash(forkBlock - 1);
            if(belowSrc !== null && belowSrc !== this.recentBroadcastHashes.get(forkBlock - 1))
                forkBlock = forkBlock - 1;   // this height also changed; fork is deeper
            else
                return forkBlock;            // forkBlock-1 unchanged: true fork point
        }
        // Warn when recorded hashes ran out before an unchanged height confirmed the fork point.
        if(forkBlock - 1 >= 1)
            logger.warn('Reorg fork point for ' + this.chain + '/' + this.network + '/' + this.dbType + ' may be too shallow: '
                + 'no recorded hash below block ' + forkBlock + ' after walking ' + (start - forkBlock) + ' height(s)');
        return forkBlock;
    }

    // Source content hash at a block, for net-forward reorg detection. Indexer uses
    // the ledger_hash (primary content hash); decoder uses the blockchain block_hash.
    async sourceBlockHash(blockIndex, conn, opts){
        let row = await this.db.getBlockHashRow(blockIndex, conn, opts);
        if(!row) return null;
        return (this.dbType === 'decoder') ? row.block_hash : row.ledger_hash;
    }

    async readReorgWindow(floor, cursor){
        let rangeDb;
        let readRange;
        if(this.transparencyLog){
            rangeDb = this.transparencyLog.db;
            if(!rangeDb || typeof rangeDb.findSyncMetaLeaves !== 'function')
                return await this.readStableReorgWindow(floor, cursor);
            readRange = async conn => {
                const rows = await rangeDb.findSyncMetaLeaves(floor, cursor, conn);
                return rows.map(row => ({
                    block_index: row.block_index,
                    hash: row.ledger_hash
                }));
            };
        } else {
            rangeDb = this.db;
            if(typeof rangeDb.findBlockHashesBetween !== 'function')
                return await this.readStableReorgWindow(floor, cursor);
            readRange = async conn => await rangeDb.findBlockHashesBetween(floor, cursor, conn);
        }

        const supportsSnapshot = typeof rangeDb.beginReadSnapshot === 'function' &&
            typeof rangeDb.commitReadSnapshot === 'function' &&
            typeof rangeDb.rollbackReadSnapshot === 'function';
        if(!supportsSnapshot)
            return await readRange();

        const conn = await rangeDb.beginReadSnapshot();
        let snapshotOpen = true;
        try {
            const rows = await readRange(conn);
            await rangeDb.commitReadSnapshot(conn);
            snapshotOpen = false;
            return rows;
        } catch(e){
            if(snapshotOpen)
                await rangeDb.rollbackReadSnapshot(conn);
            throw e;
        }
    }

    async readStableReorgWindow(floor, cursor){
        const maxAttempts = 3;
        for(let attempt = 0; attempt < maxAttempts; attempt++){
            const tipBefore = await this.db.getLastBlock(null, { rethrow: true });
            const cursorBefore = await this.sourceBlockHash(cursor, null, { rethrow: true });
            const rows = [];

            for(let blockIndex = cursor; blockIndex >= floor; blockIndex--){
                const hash = this.transparencyLog
                    ? await this.transparencyLog.getRecordedHash(blockIndex)
                    : await this.sourceBlockHash(blockIndex, null, { rethrow: true });
                if(hash === null) break;
                rows.push({ block_index: blockIndex, hash });
            }

            const tipAfter = await this.db.getLastBlock(null, { rethrow: true });
            const cursorAfter = await this.sourceBlockHash(cursor, null, { rethrow: true });
            if(tipBefore === tipAfter && cursorBefore === cursorAfter)
                return rows;
        }
        throw new Error('Reorg window changed while seeding');
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
    async seedReorgGuardHash(blockIndex){
        if(blockIndex === null) return null;
        if(this.transparencyLog){
            let recorded = await this.transparencyLog.getRecordedHash(blockIndex);
            if(recorded !== null) return recorded;
        }
        return await this.sourceBlockHash(blockIndex);
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
    async buildBlockPayload(block_index, conn, viewTip){
        let hashRow = await this.db.getBlockHashRow(block_index, conn);
        if(!hashRow) return null;

        let payload = this.createBlockPayload(hashRow);
        this.addPayloadHashFields(payload, hashRow, block_index, viewTip);
        return this.addPayloadStateRoots(payload, hashRow, block_index, conn, viewTip);
    }

    createBlockPayload(hashRow){
        return {
            type: 'block',
            chain: this.chain,
            network: this.network,
            dbType: this.dbType,
            schema_version: SCHEMA_VERSION[this.dbType],
            block_index: Number(hashRow.block_index),
            block_time: Number(hashRow.block_time),
            data: {}
        };
    }

    // Fourth, replication-integrity hash (the in-place mutations + backdated refund
    // credits the three hashes can't cover). Optional top-level field, NOT in
    // sync_meta / the Merkle leaf / the hub-signed checkpoint; a follower with

    // VERIFY_STATE_HASH recomputes it APPLY-TIME and halts on mismatch. May be NULL
    // for blocks indexed before the feature (the follower then skips the check).
    //

    // Burst exemption: during a catch-up batch the pinned view sits at the batch
    // tip, so updated_rows for every block B < viewTip carry row state as of the
    // tip, not as of B (the tick set is B-scoped but the row read takes every column

    // from the pinned view). The follower's apply-time recompute of state_hash(B) reads those rows
    // back and would halt on a value the source never committed at B, even though
    // the replica converges to exact tip state by the end of the batch (each later

    // mutation re-emits its row under its own block). Ship NULL for those blocks so
    // the follower takes its existing pre-feature skip path -- the same posture the
    // incremental-snapshot channel has by design (state_hash-exempt, consistent at

    // its own view tip). Steady-state blocks (viewTip == B, the overwhelmingly
    // common case) keep the full check; ledger/actions/contract hashes and the
    // state-commitment roots are B-scoped committed rows and stay verified on every

    // path.
    addPayloadHashFields(payload, hashRow, block_index, viewTip){
        if(this.dbType === 'decoder'){
            // Decoder payload: simpler hash field, tx-scoped joins
            payload.block_hash = hashRow.block_hash;
            return;
        }
        // Indexer payload: three-hash transparency model
        payload.ledger_hash   = hashRow.ledger_hash;
        payload.actions_hash  = hashRow.actions_hash;
        payload.contract_hash = hashRow.contract_hash;
        payload.state_hash = (viewTip != null && Number(viewTip) > block_index)
            ? null
            : hashRow.state_hash;
    }

    // Light-client state-commitment roots (SPV spec sec.4-5). Top-level fields
    // ONLY, like state_hash: NOT in payload.data (the follower computes its own
    // state_tree_nodes/roots), NOT in sync_meta, NOT in any Merkle leaf. NULL

    // before the flag-day (the follower then skips the check). The follower
    // verifies balances_root + block_merkle_root in Phase 1; state_root is carried
    // for the later full-state_root verification (no wire change needed then).

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
    async addPayloadStateRoots(payload, hashRow, block_index, conn, viewTip){
        if(this.dbType !== 'decoder' && isStateCommitmentActive(block_index, this.network, this.coinTicker)){
            let roots = await this.db.getStateRootsRow(this.coinTicker, this.network, block_index, conn);
            payload.balances_root    = roots ? roots.balances_root    : null;
            payload.block_merkle_root = roots ? roots.block_merkle_root : null;
            payload.state_root       = (viewTip != null && Number(viewTip) > block_index)
                ? null
                : (roots ? roots.state_root : null);
        } else if(this.dbType !== 'decoder'){
            payload.balances_root    = null;
            payload.block_merkle_root = null;
            payload.state_root       = null;
        }
        this.addSyncMetaPayloadRow(payload, hashRow);
        return this.addBlockScopedPayloadRows(payload, block_index, conn);
    }

    // Replicate the per-block transparency-log row (sync_meta) live. The
    // table is otherwise only carried by snapshots (SnapshotBuilder includes
    // it; ClientRollback prunes it on reorg), so without this the replica's

    // sync_meta drifts behind the source between snapshots. Built inline from
    // the hashes rather than read from the table: the server's
    // transparencyLog.recordBlock runs AFTER this payload is built (see

    // poll), so the row isn't in sync_meta yet at this point. id/logged_at
    // are node-local and intentionally omitted (the client assigns its own);
    // the client applies sync_meta with INSERT IGNORE on the unique

    // block_index, so re-sends are idempotent.
    addSyncMetaPayloadRow(payload, hashRow){
        if(this.dbType === 'decoder') return;
        payload.data['sync_meta'] = [{
            block_index:   payload.block_index,
            block_time:    payload.block_time,
            ledger_hash:   hashRow.ledger_hash,
            actions_hash:  hashRow.actions_hash,
            contract_hash: hashRow.contract_hash
        }];
    }

    // Block-scoped tables (both indexer and decoder)
    async addBlockScopedPayloadRows(payload, block_index, conn){
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
        return this.addTransactionPayloadRows(payload, block_index, conn);
    }

    // Transactions (both indexer and decoder)
    async addTransactionPayloadRows(payload, block_index, conn){
        let txRows = await this.db.getTransactions(block_index, conn);
        if(txRows && txRows.length > 0)
            payload.data['transactions'] = txRows;
        if(this.dbType === 'decoder')
            return this.addDecoderPayloadRows(payload, block_index, conn);
        return this.addActionPayloadRows(payload, block_index, conn);
    }

    // Decoder: tx-scoped tables (transaction_outputs)
    async addDecoderPayloadRows(payload, block_index, conn){
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
        return this.addReferencedIndexRows(payload, block_index, conn);
    }

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
    async actionScopedProbe(payload, block_index, conn, result){
        if(typeof this.db.getNonEmptyActionScopedTables === 'function'){
            try {
                result.probeQueries = 1;
                let probed = await this.db.getNonEmptyActionScopedTables(
                    this.actionScopedTables.filter(t => t !== 'actions' && t !== 'contract_emissions'),
                    block_index, conn);
                result.scopedTables = (probed && typeof probed.has === 'function') ? probed : null;
            } catch(e){
                result.probeQueries = 0;
                result.scopedTables = null;
            }
        }
        return this.addActionScopedTableRows(payload, block_index, conn, result);
    }

    // contract_emissions has NULL action_index for internal emissions (e.g. SLASH).
    // getActionScopedRows joins on action_index and would drop those rows from the
    // payload, while the consensus hash includes them (via execution_index). A

    // follower would then recompute a divergent contract_hash and halt. Stream them
    // through the execution_index chain instead, matching BlockHasher exactly.
    async addActionScopedTableRows(payload, block_index, conn, metric){
        for(let table of this.actionScopedTables){
            if(table === 'actions') continue; // Already handled
            if(table === 'contract_emissions'){
                try {
                    metric.scopedQueries++;
                    let rows = await this.db.getEmissionRowsForBlock(block_index, conn);
                    if(rows && rows.length > 0){
                        payload.data[table] = rows;
                        metric.scopedNonEmpty++;
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
            if(metric.scopedTables && !metric.scopedTables.has(table)) continue;
            try {
                metric.scopedQueries++;
                let rows = await this.db.getActionScopedRows(table, block_index, conn);
                if(rows && rows.length > 0){
                    payload.data[table] = rows;
                    metric.scopedNonEmpty++;
                }
            } catch(e){
                // Skip a genuine schema gap; re-throw any transient fault so the
                // block is retried rather than broadcast incomplete.
                if(!isSchemaGapError(e)) throw e;
            }
        }
        this.reportActionScopedQueryMetric(metric.scopedQueries, metric.scopedNonEmpty,
                                           Date.now() - metric.scopedStartedAt, metric.probeQueries);
        return this.addCooldownPayloadRows(payload, block_index, conn);
    }

    async addActionPayloadRows(payload, block_index, conn){
        // Indexer: actions and action-scoped tables
        let actionRows = await this.db.getActions(block_index, conn);
        if(actionRows && actionRows.length > 0)
            payload.data['actions'] = actionRows;
        // Counters for the action-scoped query-count metric emitted after the loop.
        // Read-only bookkeeping: nothing here reaches payload.data.
        let metric = {
            scopedQueries: 0,
            scopedNonEmpty: 0,
            probeQueries: 0,
            scopedStartedAt: Date.now(),
            scopedTables: null
        };
        return this.actionScopedProbe(payload, block_index, conn, metric);
    }

    // Cooldown-maturity refund credits mint AT this block but carry the
    // unstake's earlier-block action_index (and no block_index), so the
    // action-scoped join above misses them, leaving followers permanently

    // short by every matured refund. Select them by maturity block
    // (cooldown_end_block = this block), the forward mirror of
    // ClientRollback's reverse delete, and merge into the credits payload;

    // ClientApplier then upserts them and rebuilds balances like any other
    // credit. Disjoint from the action-scoped credits (those carry an action
    // in THIS block; a refund's action is in an earlier block), but dedup the

    // union defensively on the credit's logical identity. The escrow release
    // written beside each refund shares its backdated action_index, so it
    // rides the same way into the escrows payload.
    async addCooldownPayloadRows(payload, block_index, conn){
        try {
            let refunds  = await collectMaturedCooldownCredits(this.db, block_index, block_index, conn);
            let releases = await collectMaturedCooldownEscrows(this.db, block_index, block_index, conn);
            if(refunds.length > 0) payload.data['credits'] = mergeMaturedRows(payload.data['credits'], refunds);
            if(releases.length > 0) payload.data['escrows'] = mergeMaturedRows(payload.data['escrows'], releases);
        } catch(e){
            // Skip a genuine schema gap; re-throw any transient fault so the
            // block is retried rather than broadcast incomplete.
            if(!isSchemaGapError(e)) throw e;
        }
        return this.addRedrivenRewardPayloadRows(payload, block_index, conn);
    }

    // The dedup key is the FULL five-column identity. round_qualifier is the
    // archive leg's snapshot_block, and its round_reference (MATCH_BATCH_SEQ) is
    // a dense hub counter a rebase reissues, so two distinct archive rewards can

    // share the four older columns; on the narrower key the second is treated as
    // a duplicate and dropped from the payload before it ever reaches a replica.
    mergeValidatorRewardRows(payload, rows){
        let existing = payload.data['validator_rewards'] || [];
        let seen = new Set(existing.map(r => r.source_id + ':' + r.signing_pubkey_id + ':' + r.reward_type + ':' + r.round_reference + ':' + r.round_qualifier));
        for(let r of rows){
            let k = r.source_id + ':' + r.signing_pubkey_id + ':' + r.reward_type + ':' + r.round_reference + ':' + r.round_qualifier;
            if(!seen.has(k)){ seen.add(k); existing.push(r); }
        }
        payload.data['validator_rewards'] = existing;
    }

    // Recovery-redriven validator rewards: a reorg re-drain re-materializes a
    // survivor reward at block_index = earn-block E < B, so the block-scoped
    // getBlockScopedRows path (forward from B) misses it. Select by applied_block

    // (= this block, the re-drain point), the forward analogue of ClientRollback's
    // block_index >= B delete, and merge into the validator_rewards payload deduped
    // on the row's UNIQUE identity. Disjoint from the block-scoped rows (those carry

    // block_index = this block; a survivor's earn-block is earlier).
    async addRedrivenRewardPayloadRows(payload, block_index, conn){
        try {
            let redriven = await collectRedrivenValidatorRewards(this.db, block_index, block_index, conn);
            if(redriven.length > 0) this.mergeValidatorRewardRows(payload, redriven);
        } catch(e){
            // Skip a genuine schema gap; re-throw any transient fault so the
            // block is retried rather than broadcast incomplete.
            if(!isSchemaGapError(e)) throw e;
        }
        return this.addDerivedRewardPayloadRows(payload, block_index, conn);
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

    // Five-column identity, same reason as the redriven merge above: the
    // archive leg is exactly the channel that can present two distinct rewards
    // differing only in round_qualifier.
    async addDerivedRewardPayloadRows(payload, block_index, conn){
        try {
            let derived = await collectDerivedAnchorRewards(this.db, block_index, block_index, conn);
            if(derived.length > 0) this.mergeValidatorRewardRows(payload, derived);
        } catch(e){
            // Skip a genuine schema gap; re-throw any transient fault so the
            // block is retried rather than broadcast incomplete.
            if(!isSchemaGapError(e)) throw e;
        }
        return this.addReferencedIndexRows(payload, block_index, conn);
    }

    // Index tables: get entries referenced by this block's data.
    // Both indexer and decoder use this pattern (decoder has fewer index tables).
    // The client uses INSERT IGNORE so duplicates are harmless.

    // events: decoder-only operational log with no block_index/tx_index
    // cursor, so it can't be scoped per-block; it is intentionally skipped
    // here. It converges via snapshots instead: both the full snapshot and

    // every incremental snapshot re-dump the events table in full (the client
    // applies them with INSERT IGNORE on the AUTO_INCREMENT id PK, so repeated
    // dumps are idempotent). See SnapshotBuilder.streamIncrementalSnapshot.

    // For other index tables, the generic _id-reference pass below
    // (indexer only) extracts them; see the comment there.
    addReferencedIndexRows(payload, block_index, conn, tableIndex = 0){
        if(tableIndex >= this.indexTables.length)
            return this.addGenericIndexRows(payload, block_index, conn);
        let table = this.indexTables[tableIndex];
        if(table === 'index_transactions')
            return this.addIndexTransactionRows(payload, table, block_index, conn, tableIndex);
        if(table === 'index_addresses' && payload.data['transactions'])
            return this.addIndexAddressRows(payload, table, block_index, conn, tableIndex);
        if(table === 'pubkeys' && this.dbType === 'decoder' && payload.data['index_addresses'])
            return this.addIndexPubkeyRows(payload, table, block_index, conn, tableIndex);
        return this.addReferencedIndexRows(payload, block_index, conn, tableIndex + 1);
    }

    // index_transactions: every `*_hash_id` this block's own rows carry, DERIVED
    // from the column suffix rather than listed. The generic `*_id` scan below
    // skips this table, so a hash column absent here reaches a follower as a

    // permanently dangling reference: its blocks row is correct, its LEFT JOIN
    // (getBlockHashRow, and the explorer's identical join) resolves that hash to
    // NULL for every block above the last snapshot, which is the only thing that

    // re-dumps this table in full. One rule for both dbTypes, so a hash column
    // added later cannot re-open the gap.
    async addIndexTransactionRows(payload, table, block_index, conn, tableIndex){
        try {
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
                let rows = await this.db.findIndexTransactionsByIds(unique, conn);
                if(rows && rows.length > 0)
                    payload.data[table] = rows;
            }
        } catch(e){
            // Skip a genuine schema gap; re-throw any transient fault so the
            // block is retried rather than broadcast incomplete.
            if(!isSchemaGapError(e)) throw e;
        }
        return this.addReferencedIndexRows(payload, block_index, conn, tableIndex + 1);
    }

    // index_addresses: collect referenced address IDs from transactions
    // Decoder: also collect from transaction_outputs
    // A DISPENSER create interns its GET_ADDRESS and oracle address in

    // this block, but dispensers never streams, so ship those ids here or
    // the replica's MAX(id) cursor passes them and leaves a hole. Its own
    // schema-gap guard, so a source without dispensers keeps the tx ids.
    async addIndexAddressRows(payload, table, block_index, conn, tableIndex){
        try {
            let ids = [];
            for(let tx of payload.data['transactions']){
                if(tx.source_id) ids.push(tx.source_id);
                if(tx.destination_id) ids.push(tx.destination_id);
            }
            if(this.dbType === 'decoder'){
                if(payload.data['transaction_outputs']){
                    for(let o of payload.data['transaction_outputs'])
                        if(o.destination_id) ids.push(o.destination_id);
                }
                let dispenserRows = [];
                try {
                    dispenserRows = await this.db.getTxScopedRows('dispensers', block_index, conn);
                } catch(e){
                    if(!isSchemaGapError(e)) throw e;
                }
                for(let d of (dispenserRows || [])){
                    if(d.address_id) ids.push(d.address_id);
                    if(d.oracle_address_id) ids.push(d.oracle_address_id);
                    if(d.source_address_id) ids.push(d.source_address_id);
                }
            }
            if(ids.length > 0){
                let unique = [...new Set(ids)];
                let rows = await this.db.findIndexAddressesByIds(unique, conn);
                if(rows && rows.length > 0)
                    payload.data[table] = rows;
            }
        } catch(e){
            // Skip a genuine schema gap; re-throw any transient fault so the
            // block is retried rather than broadcast incomplete.
            if(!isSchemaGapError(e)) throw e;
        }
        return this.addReferencedIndexRows(payload, block_index, conn, tableIndex + 1);
    }

    // pubkeys: decoder-only; fetch any pubkeys for addresses referenced this block
    async addIndexPubkeyRows(payload, table, block_index, conn, tableIndex){
        try {
            let ids = payload.data['index_addresses'].map(a => a.id).filter(id => id != null);
            if(ids.length > 0){
                let rows = await this.db.findPubkeysByAddressIds(ids, conn);
                if(rows && rows.length > 0)
                    payload.data[table] = rows;
            }
        } catch(e){
            // Skip a genuine schema gap; re-throw any transient fault so the
            // block is retried rather than broadcast incomplete.
            if(!isSchemaGapError(e)) throw e;
        }
        return this.addReferencedIndexRows(payload, block_index, conn, tableIndex + 1);
    }

    collectPayloadReferenceIds(payload){
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
        return refIds;
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
    async addGenericIndexRows(payload, block_index, conn){
        if(this.dbType === 'decoder')
            return this.addBlockScopedIndexRows(payload, block_index, conn);
        let refIds = this.collectPayloadReferenceIds(payload);
        if(refIds.size > 0){
            let idList = [...refIds];
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
                    let rows = await this.db.findRowsByIds(table, idList, conn);
                    if(rows && rows.length > 0)
                        payload.data[table] = rows;
                } catch(e){
                    // Skip a genuine schema gap; re-throw any transient fault so the
                    // block is retried rather than broadcast incomplete.
                    if(!isSchemaGapError(e)) throw e;
                }
            }
        }
        return this.addBlockScopedIndexRows(payload, block_index, conn);
    }

    async addBlockScopedIndexRows(payload, block_index, conn){
        if(this.dbType !== 'decoder'){
            for(const table of BLOCK_SCOPED_INDEX_TABLES){
                try {
                    const rows = await this.db.getBlockScopedRows(table, block_index, conn);
                    if(!rows || rows.length === 0) continue;
                    const existing = payload.data[table] || [];
                    const ids = new Set(existing.map(row => row.id));
                    for(const row of rows){
                        if(!ids.has(row.id)){
                            ids.add(row.id);
                            existing.push(row);
                        }
                    }
                    payload.data[table] = existing;
                } catch(e){
                    if(!isSchemaGapError(e)) throw e;
                }
            }
        }
        return this.addUpdatedPayloadRows(payload, block_index, conn);
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

    // touches an escrow table (ClientApplier.maybeRederiveEscrow), so the wire
    // value is a convergent carry, not the gate's only writer. Kept OUT of payload.data so an
    // old follower that doesn't recognise the field simply ignores it (its apply

    // loop iterates payload.data only) rather than mis-applying a non-row map.
    async addUpdatedPayloadRows(payload, block_index, conn){
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
                logger.error(util.format('updated_rows collection failed for block ' + block_index + ':', e));
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
    reportActionScopedQueryMetric(queries, nonEmpty, elapsedMs, probeQueries){
        let raw = envConfig.syncQueryMetricIntervalMsFromEnv();
        let intervalMs = Number.isFinite(raw) ? raw : (15 * 60 * 1000);
        if(intervalMs === 0) return;   // explicitly disabled
        let now = Date.now();
        if(this._lastQueryMetricAt && (now - this._lastQueryMetricAt) < intervalMs) return;
        this._lastQueryMetricAt = now;
        logger.info('[METRIC] ' + JSON.stringify({
            metric: 'sync_action_scoped_queries_per_block', component: 'sync',
            chain: this.chain, network: this.network, db_type: this.dbType,
            candidate_tables: this.actionScopedTables ? this.actionScopedTables.length : 0,
            queries: queries, probe_queries: probeQueries || 0,
            non_empty_tables: nonEmpty, elapsed_ms: elapsedMs,
            ts: now
        }));
    }

    async updateStatus(sourceBlockHeight){
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
        let rep = await this.readReplicaStatus();
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
        // When this measurement was actually taken. Every read above has succeeded by the
        // time we reach here (a throw skips the updateStatus call entirely), so the stamp
        // only ever dates a real observation. Without it the cached object is undated and
        // no reader can tell a status measured a second ago from one measured before the
        // database went away, which is what let a failed poll keep certifying freshness
        // indefinitely. BlockBroadcaster.getStatus expires the verdict against it.
        status.measured_at = Date.now();
        this.broadcaster.updateStatus(this.chain, this.network, status);
    }

    // Fail closed on everything except a confirmed primary. A stopped SQL thread
    // reports Seconds_Behind_Source NULL, which is unbounded lag, never zero; an
    // unreadable status (no grant, older db object without the method) is unknown
    // and must not certify freshness either.
    async readReplicaStatus(){
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
