/*********************************************************************
 *
 * Copyright © 2025 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC – https://dankest.llc
 *
 * Licensed under the Dankest Community License (Apache License 2.0 + Additional Terms).
 * You may not use this file except in compliance with that License.
 *
 * A copy of the License is available at:
 *     https://dankest.llc/license
 *
 * This software is provided "AS IS", without warranties or conditions of any kind.
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
 *     tables (transaction_outputs, dispensers). No actions, no
 *     transparency log (decoder content is deterministic from the coin
 *     node — see xchain-sync-decoder-db-decisions memory).
 *
 ********************************************************************/

const replicatedTables = require('./replicatedTables');

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

        this.lastPolledBlock = null;
        this.running = false;

        // Per-block replicated table topology (single source of truth shared with
        // the row-count completeness check — see src/replicatedTables.js).
        let topo = replicatedTables.getTopology(this.dbType);
        this.blockScopedTables  = topo.blockScoped;
        this.txScopedTables     = topo.txScoped;
        this.actionScopedTables = topo.actionScoped;
        this.indexTables        = topo.index;

        if(this.dbType === 'decoder'){
            // Decoder doesn't have cross-chain infrastructure tables
            this.infraTables = new Set();
        } else {
            // Infrastructure tables — always synced regardless of subscriber sync mode
            // These tables provide cross-chain state that every node needs (validator set,
            // rewards) or that participate in cross-chain queries (PRICE actions on any chain).
            // Subscribers in 'infra-only' mode receive ONLY these tables for this chain.
            this.infraTables = new Set([
                'stakes', 'delegations', 'validator_rewards', 'prices', 'reward_claims',
                'index_pubkeys', 'index_addresses', 'index_actions', 'index_statuses', 'index_fiats'
            ]);
        }
    }

    // Start the polling loop
    async start(){
        this.lastPolledBlock = await this.db.getLastBlock();
        this.running = true;
        console.log('ServerPoller started for ' + this.chain + '/' + this.network + '/' + this.dbType + ' at block ' + (this.lastPolledBlock || 'none'));

        // Update initial status
        await this._updateStatus();

        while(this.running){
            let blocksProcessed = 0;
            try {
                blocksProcessed = await this._poll() || 0;
            } catch(e){
                console.error('ServerPoller error for ' + this.chain + '/' + this.network + '/' + this.dbType + ':', e);
            }
            // Skip sleep when the batch cap was hit — backlog likely remains
            if(blocksProcessed < 100)
                await this.util.sleep(this.config['BLOCK_POLL_INTERVAL']);
        }
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
                // Record in transparency log — indexer only (decoder has no synthetic hashes)
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
                // Table may not exist in older schemas — skip silently
            }
        }

        // Transactions (both indexer and decoder)
        let txRows = await this.db.getTransactions(block_index);
        if(txRows && txRows.length > 0)
            payload.data['transactions'] = txRows;

        if(this.dbType === 'decoder'){
            // Decoder: tx-scoped tables (transaction_outputs, dispensers)
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
                try {
                    let rows = await this.db.getActionScopedRows(table, block_index);
                    if(rows && rows.length > 0)
                        payload.data[table] = rows;
                } catch(e){
                    // Table may not exist — skip silently
                }
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
                    // Decoder: also collect from transaction_outputs and dispensers
                    if(this.dbType === 'decoder'){
                        if(payload.data['transaction_outputs']){
                            for(let o of payload.data['transaction_outputs'])
                                if(o.destination_id) ids.push(o.destination_id);
                        }
                        if(payload.data['dispensers']){
                            for(let d of payload.data['dispensers'])
                                if(d.address_id) ids.push(d.address_id);
                        }
                    }
                    if(ids.length > 0){
                        let unique = [...new Set(ids)];
                        let rows = await this.db.doQuery("SELECT * FROM index_addresses WHERE id IN (" + unique.map(() => '?').join(',') + ")", unique);
                        if(rows && rows.length > 0)
                            payload.data[table] = rows;
                    }
                }
                // pubkeys: decoder-only — fetch any pubkeys for addresses referenced this block
                else if(table === 'pubkeys' && this.dbType === 'decoder' && payload.data['index_addresses']){
                    let ids = payload.data['index_addresses'].map(a => a.id).filter(id => id != null);
                    if(ids.length > 0){
                        let rows = await this.db.doQuery("SELECT * FROM pubkeys WHERE address_id IN (" + ids.map(() => '?').join(',') + ")", ids);
                        if(rows && rows.length > 0)
                            payload.data[table] = rows;
                    }
                }
                // events: decoder-only — operational log with no block_index/tx_index
                // cursor, so it can't be scoped per-block; it is intentionally skipped
                // here. It converges via snapshots instead: both the full snapshot and
                // every incremental snapshot re-dump the events table in full (the client
                // applies them with INSERT IGNORE on the AUTO_INCREMENT id PK, so repeated
                // dumps are idempotent). See SnapshotBuilder.streamIncrementalSnapshot.
                // For other index tables, we skip per-block extraction (they're included in full/incremental snapshots)
            } catch(e){
                // Skip silently
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
            source_block_height: sourceBlockHeight != null ? sourceBlockHeight : (await this.db.getLastBlock())
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
