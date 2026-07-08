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
 *     (decoder content is deterministic from the coin node; see
 *     xchain-sync-decoder-db-decisions memory).
 *
 ********************************************************************/

const replicatedTables = require('./replicatedTables');
const { collectUpdatedRows } = require('./updatedRows');
const { collectMaturedCooldownCredits } = require('./cooldownCredits');
const { collectRedrivenValidatorRewards } = require('./recoveryRewards');
const { activationDelayBlocks } = require('./consensus-constants');
const { isStateCommitmentActive } = require('./state_commitment_activation');
const { SCHEMA_VERSION } = require('./schema-version');

// How many recently broadcast block hashes to retain in memory for the
// net-forward reorg walk-back (item 4830). Comfortably above the source
// indexer's MAX_ROLLBACK_DEPTH (100) so a deep same-interval reorg can be
// walked back one height per poll against the pre-reorg hash we recorded,
// rather than against a fresh (post-reorg) source read that always matches.
const RECENT_HASH_CAP = 256;

class ServerPoller {

    constructor(chain, network, db, broadcaster, transparencyLog, config, util) {
        this.chain    = chain;
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
        // monotonic) is detectable by a changed hash, not just a lower height (4623).
        this.lastPolledBlockHash = null;
        // Bounded map of recently broadcast block hashes (block_index -> content
        // hash WE broadcast for that height). On a net-forward reorg the walk-back
        // seeds lastPolledBlockHash from the PRE-reorg hash recorded here, so a
        // reorg deeper than one block keeps walking back over subsequent polls
        // (item 4830). Works for both dbTypes (the decoder has no sync_meta to read
        // a recorded hash from). Capped to the last RECENT_HASH_CAP heights.
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
        // deeper reorg is walked back over subsequent polls (item 4623).
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
                let forkBlock = this.lastPolledBlock;
                while(forkBlock - 1 >= 1 && this.recentBroadcastHashes.has(forkBlock - 1)){
                    let belowSrc = await this._sourceBlockHash(forkBlock - 1);
                    if(belowSrc !== null && belowSrc !== this.recentBroadcastHashes.get(forkBlock - 1))
                        forkBlock = forkBlock - 1;   // this height also changed; fork is deeper
                    else
                        break;                       // forkBlock-1 unchanged: true fork point
                }
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

        // Detect reorgs: if current block is less than last polled, a rollback occurred
        if(currentBlock < this.lastPolledBlock){
            console.log('Reorg detected for ' + this.chain + '/' + this.network + '/' + this.dbType + ': block went from ' + this.lastPolledBlock + ' to ' + currentBlock);

            // Prune the source's own transparency log first (indexer only; decoder
            // has no transparencyLog). The indexer rolls back its data tables on a
            // reorg but not these sync-service-owned tables, and recordBlock's
            // INSERT IGNORE would otherwise keep the orphaned blocks' stale hashes
            // and drop the re-added blocks' new ones, serving wrong Merkle proofs.
            // Throwing here leaves lastPolledBlock un-rewound so the next poll
            // re-detects the reorg and retries (prune + broadcast stay together).
            if(this.transparencyLog)
                await this.transparencyLog.pruneFrom(currentBlock + 1);

            // Reorg event shape: see the net-forward reorg broadcast above for the
            // full field documentation. block_index here is the first orphaned block
            // (currentBlock + 1), consistent with the net-forward path.
            this.broadcaster.broadcast(this.chain, this.network, {
                type: 'reorg',
                chain: this.chain,
                network: this.network,
                dbType: this.dbType,
                block_index: currentBlock + 1
            });
            this.lastPolledBlock = currentBlock;
            this.lastPolledBlockHash = await this._sourceBlockHash(currentBlock);
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
            // pin still bounds every read to one view (the batch tip), which
            // re-emission at each subsequent window then converges.
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
                    let payload = await this._buildBlockPayload(nextBlock, snapConn);
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
                        // net-forward reorg that rewrites this block (item 4623).
                        this.lastPolledBlockHash = (this.dbType === 'decoder') ? payload.block_hash : payload.ledger_hash;
                        // Record it for the net-forward walk-back so a deeper reorg can be
                        // detected against this pre-reorg hash on a later poll (item 4830).
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
    async _buildBlockPayload(block_index, conn){
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
            payload.state_hash    = hashRow.state_hash;

            // Light-client state-commitment roots (SPV spec sec.4-5). Top-level fields
            // ONLY, like state_hash: NOT in payload.data (the follower computes its own
            // state_tree_nodes/roots), NOT in sync_meta, NOT in any Merkle leaf. NULL
            // before the flag-day (the follower then skips the check). The follower
            // verifies balances_root + block_merkle_root in Phase 1; state_root is carried
            // for the later full-state_root verification (no wire change needed then).
            if(isStateCommitmentActive(block_index, this.network, this.chain)){
                let roots = await this.db.getStateRootsRow(this.chain, this.network, block_index, conn);
                payload.balances_root    = roots ? roots.balances_root    : null;
                payload.block_merkle_root = roots ? roots.block_merkle_root : null;
                payload.state_root       = roots ? roots.state_root       : null;
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
                // Table may not exist in older schemas; skip silently
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
                    // Skip silently
                }
            }
        } else {
            // Indexer: actions and action-scoped tables
            let actionRows = await this.db.getActions(block_index, conn);
            if(actionRows && actionRows.length > 0)
                payload.data['actions'] = actionRows;

            for(let table of this.actionScopedTables){
                if(table === 'actions') continue; // Already handled
                // contract_emissions has NULL action_index for internal emissions (e.g. SLASH).
                // getActionScopedRows joins on action_index and would drop those rows from the
                // payload, while the consensus hash includes them (via execution_index). A
                // follower would then recompute a divergent contract_hash and halt. Stream them
                // through the execution_index chain instead, matching BlockHasher exactly.
                if(table === 'contract_emissions'){
                    try {
                        let rows = await this.db.getEmissionRowsForBlock(block_index, conn);
                        if(rows && rows.length > 0)
                            payload.data[table] = rows;
                    } catch(e){
                        // Table may not exist; skip silently
                    }
                    continue;
                }
                try {
                    let rows = await this.db.getActionScopedRows(table, block_index, conn);
                    if(rows && rows.length > 0)
                        payload.data[table] = rows;
                } catch(e){
                    // Table may not exist; skip silently
                }
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
                // Tables may not exist on older schemas; skip silently
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
                // Tables may not exist on older schemas; skip silently
            }
        }

        // Index tables: get entries referenced by this block's data.
        // Both indexer and decoder use this pattern (decoder has fewer index tables).
        // The client uses INSERT IGNORE so duplicates are harmless.
        for(let table of this.indexTables){
            try {
                // index_transactions: collect referenced IDs
                if(table === 'index_transactions'){
                    let ids = [];
                    if(this.dbType === 'decoder'){
                        // Decoder: block_hash_id + previous_block_hash_id from blocks, tx_hash_id from txns
                        if(payload.data['blocks']){
                            for(let b of payload.data['blocks']){
                                if(b.block_hash_id) ids.push(b.block_hash_id);
                                if(b.previous_block_hash_id) ids.push(b.previous_block_hash_id);
                            }
                        }
                        if(payload.data['transactions']){
                            for(let tx of payload.data['transactions'])
                                if(tx.tx_hash_id) ids.push(tx.tx_hash_id);
                        }
                    } else {
                        // Indexer: ledger/actions/contract hash IDs + tx_hash_id
                        if(payload.data['blocks']){
                            let block = payload.data['blocks'][0];
                            if(block){
                                ids = [block.ledger_hash_id, block.actions_hash_id, block.contract_hash_id].filter(id => id != null);
                            }
                        }
                        if(payload.data['transactions']){
                            for(let tx of payload.data['transactions'])
                                if(tx.tx_hash_id) ids.push(tx.tx_hash_id);
                        }
                    }
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
                // Skip silently
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
                        // Table may not exist in older schemas; skip silently
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
        // only (decoder has none of these tables). tokens.escrow_action_index is NOT
        // included; the follower re-derives it from the replicated offer/status
        // tables (ClientApplier._maybeRederiveEscrow). Kept OUT of payload.data so an
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
                // Never let updated-rows collection break live block broadcasting.
                console.error('updated_rows collection failed for block ' + block_index + ':', e);
            }
        }

        return payload;
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
}

module.exports = ServerPoller;
