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
const { activationDelayBlocks } = require('./consensus-constants');
const { isStateCommitmentActive } = require('./state_commitment_activation');

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

    // Start the polling loop
    async start(){
        this.lastPolledBlock = await this._resumeCursor();
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

        // Update initial status
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

    // Stop the polling loop
    stop(){
        this.running = false;
    }

    // Poll for new blocks
    async _poll(){
        let currentBlock = await this.db.getLastBlock();
        if(currentBlock === null) return;

        // Initialize if first poll
        if(this.lastPolledBlock === null){
            this.lastPolledBlock = currentBlock;
            await this._updateStatus();
            return;
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

            this.broadcaster.broadcast(this.chain, this.network, {
                type: 'reorg',
                chain: this.chain,
                network: this.network,
                dbType: this.dbType,
                block_index: currentBlock + 1
            });
            this.lastPolledBlock = currentBlock;
            await this._updateStatus();
            return;
        }

        // Process new blocks (limit to 100 per poll to avoid large bursts)
        let blocksProcessed = 0;
        while(this.lastPolledBlock < currentBlock && blocksProcessed < 100){
            let nextBlock = this.lastPolledBlock + 1;
            let payload = await this._buildBlockPayload(nextBlock);
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

                console.log('Synced block ' + nextBlock + ' for ' + this.chain + '/' + this.network + '/' + this.dbType);
            }
            this.lastPolledBlock = nextBlock;
            blocksProcessed++;
        }

        if(blocksProcessed > 0)
            await this._updateStatus(currentBlock);

        return blocksProcessed;
    }

    // Build a complete block payload for broadcasting.
    // Indexer payload includes ledger_hash/actions_hash/contract_hash and actions-scoped tables.
    // Decoder payload includes the blockchain block hash and tx-scoped tables; no actions.
    async _buildBlockPayload(block_index){
        let hashRow = await this.db.getBlockHashRow(block_index);
        if(!hashRow) return null;

        let payload = {
            type: 'block',
            chain: this.chain,
            network: this.network,
            dbType: this.dbType,
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
            if(isStateCommitmentActive(block_index, this.network)){
                let roots = await this.db.getStateRootsRow(this.chain, this.network, block_index);
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
                let rows = await this.db.getBlockScopedRows(table, block_index);
                if(rows && rows.length > 0)
                    payload.data[table] = rows;
            } catch(e){
                // Table may not exist in older schemas; skip silently
            }
        }

        // Transactions (both indexer and decoder)
        let txRows = await this.db.getTransactions(block_index);
        if(txRows && txRows.length > 0)
            payload.data['transactions'] = txRows;

        if(this.dbType === 'decoder'){
            // Decoder: tx-scoped tables (transaction_outputs)
            for(let table of this.txScopedTables){
                try {
                    let rows = await this.db.getTxScopedRows(table, block_index);
                    if(rows && rows.length > 0)
                        payload.data[table] = rows;
                } catch(e){
                    // Skip silently
                }
            }
        } else {
            // Indexer: actions and action-scoped tables
            let actionRows = await this.db.getActions(block_index);
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
                        let rows = await this.db.getEmissionRowsForBlock(block_index);
                        if(rows && rows.length > 0)
                            payload.data[table] = rows;
                    } catch(e){
                        // Table may not exist; skip silently
                    }
                    continue;
                }
                try {
                    let rows = await this.db.getActionScopedRows(table, block_index);
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
                let matured = await collectMaturedCooldownCredits(this.db, block_index, block_index);
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
                        let rows = await this.db.doQuery("SELECT * FROM index_transactions WHERE id IN (" + unique.map(() => '?').join(',') + ")", unique);
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
                        let rows = await this.db.doQuery("SELECT * FROM index_addresses WHERE id IN (" + unique.map(() => '?').join(',') + ")", unique);
                        if(rows && rows.length > 0)
                            payload.data[table] = rows;
                    }
                }
                // pubkeys: decoder-only; fetch any pubkeys for addresses referenced this block
                else if(table === 'pubkeys' && this.dbType === 'decoder' && payload.data['index_addresses']){
                    let ids = payload.data['index_addresses'].map(a => a.id).filter(id => id != null);
                    if(ids.length > 0){
                        let rows = await this.db.doQuery("SELECT * FROM pubkeys WHERE address_id IN (" + ids.map(() => '?').join(',') + ")", ids);
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
        // index_transactions/index_addresses are handled explicitly above.
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
                    // Explicitly-handled above (they carry block-hash/tx-hash IDs the
                    // generic _id scan can't see, so leave their precise joins intact).
                    if(table === 'index_transactions' || table === 'index_addresses') continue;
                    if(payload.data[table]) continue;  // defensive: already populated
                    try {
                        let rows = await this.db.doQuery("SELECT * FROM `" + table + "` WHERE id IN (" + placeholders + ")", idList);
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
                let updated = await collectUpdatedRows(this.db, block_index, block_index, this.activationDelay);
                if(updated && Object.keys(updated).length > 0)
                    payload.updated_rows = updated;
            } catch(e){
                // Never let updated-rows collection break live block broadcasting.
                console.error('updated_rows collection failed for block ' + block_index + ':', e);
            }
        }

        return payload;
    }

    // Update the broadcaster's status data for this chain/network/dbType
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
