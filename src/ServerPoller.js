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
 * XChain Indexer Sync - Server Poller
 *
 * Polls one indexer database for new blocks, builds block payloads,
 * records hashes in the transparency log, and broadcasts to WebSocket
 * subscribers via the BlockBroadcaster.
 *
 * One instance per chain/network.
 *
 ********************************************************************/

class ServerPoller {

    constructor(chain, network, db, broadcaster, transparencyLog, config, util) {
        this.chain    = chain;
        this.network  = network;
        this.db       = db;
        this.broadcaster    = broadcaster;
        this.transparencyLog = transparencyLog;
        this.config   = config;
        this.util     = util;

        this.lastPolledBlock = null;
        this.running = false;

        // Tables that are scoped by block_index (not action_index)
        this.blockScopedTables = ['blocks', 'transactions', 'validator_rewards', 'contract_state'];

        // Action-scoped tables to include in block payloads
        this.actionScopedTables = [
            'actions', 'addresses', 'airdrops', 'batches', 'broadcasts', 'callbacks',
            'coinpays', 'coinpay_obligations', 'coinpay_expires', 'coinpay_statuses',
            'contracts', 'contract_executions', 'contract_emissions', 'contract_balances',
            'credits', 'debits', 'escrows',
            'delegates', 'delegations',
            'deposits', 'destroys',
            'dispensers', 'dispenser_cancels', 'dispenser_closes', 'dispenser_edits',
            'dispenser_expires', 'dispenser_statuses', 'dispenses',
            'dividends', 'events',
            'fees', 'files',
            'issues',
            'links', 'lists', 'list_edits', 'list_items', 'list_items_invalid',
            'mappings_actions', 'mappings_files',
            'markets', 'messages', 'mints',
            'orders', 'order_cancels', 'order_edits', 'order_expires', 'order_matches', 'order_statuses',
            'reward_claims',
            'sends', 'sleeps',
            'stakes',
            'swaps', 'swap_cancels', 'swap_edits', 'swap_expires', 'swap_matches', 'swap_statuses',
            'sweeps', 'tokens', 'unstakes',
            'withdrawals',
            'balances'
        ];

        // Index tables that may have new entries per block
        this.indexTables = [
            'index_actions', 'index_addresses', 'index_coins', 'index_fiats',
            'index_memos', 'index_mime_types', 'index_pubkeys', 'index_statuses',
            'index_tickers', 'index_transactions'
        ];
    }

    // Start the polling loop
    async start(){
        this.lastPolledBlock = await this.db.getLastBlock();
        this.running = true;
        console.log('ServerPoller started for ' + this.chain + '/' + this.network + ' at block ' + (this.lastPolledBlock || 'none'));

        // Update initial status
        await this._updateStatus();

        while(this.running){
            try {
                await this._poll();
            } catch(e){
                console.error('ServerPoller error for ' + this.chain + '/' + this.network + ':', e.message);
            }
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
            console.log('Reorg detected for ' + this.chain + '/' + this.network + ': block went from ' + this.lastPolledBlock + ' to ' + currentBlock);
            this.broadcaster.broadcast(this.chain, this.network, {
                type: 'reorg',
                chain: this.chain,
                network: this.network,
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
                // Record in transparency log
                await this.transparencyLog.recordBlock(
                    payload.block_index, payload.block_time,
                    payload.ledger_hash, payload.actions_hash, payload.contract_hash
                );

                // Broadcast to subscribers
                this.broadcaster.broadcast(this.chain, this.network, payload);

                console.log('Synced block ' + nextBlock + ' for ' + this.chain + '/' + this.network);
            }
            this.lastPolledBlock = nextBlock;
            blocksProcessed++;
        }

        if(blocksProcessed > 0)
            await this._updateStatus();
    }

    // Build a complete block payload for broadcasting
    async _buildBlockPayload(block_index){
        let hashRow = await this.db.getBlockHashRow(block_index);
        if(!hashRow) return null;

        let payload = {
            type: 'block',
            chain: this.chain,
            network: this.network,
            block_index: Number(hashRow.block_index),
            block_time: Number(hashRow.block_time),
            ledger_hash: hashRow.ledger_hash,
            actions_hash: hashRow.actions_hash,
            contract_hash: hashRow.contract_hash,
            data: {}
        };

        // Block-scoped tables
        for(let table of this.blockScopedTables){
            try {
                let rows = await this.db.getBlockScopedRows(table, block_index);
                if(rows && rows.length > 0)
                    payload.data[table] = rows;
            } catch(e){
                // Table may not exist in older schemas — skip silently
            }
        }

        // Transactions
        let txRows = await this.db.getTransactions(block_index);
        if(txRows && txRows.length > 0)
            payload.data['transactions'] = txRows;

        // Actions and action-scoped tables
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

        // Index tables: get entries that were created for this block's actions
        // These don't have block_index columns, so we query by ID range
        // For simplicity, we include any index entries referenced by this block's data
        // The client uses INSERT IGNORE so duplicates are harmless
        for(let table of this.indexTables){
            try {
                // For index_transactions, get entries referenced by this block's transactions
                if(table === 'index_transactions' && payload.data['blocks']){
                    let block = payload.data['blocks'][0];
                    if(block){
                        let ids = [block.ledger_hash_id, block.actions_hash_id, block.contract_hash_id].filter(id => id != null);
                        if(payload.data['transactions']){
                            for(let tx of payload.data['transactions'])
                                if(tx.tx_hash_id) ids.push(tx.tx_hash_id);
                        }
                        if(ids.length > 0){
                            let rows = await this.db.doQuery("SELECT * FROM index_transactions WHERE id IN (" + ids.map(() => '?').join(',') + ")", ids);
                            if(rows && rows.length > 0)
                                payload.data[table] = rows;
                        }
                    }
                }
                // For index_addresses, get entries referenced by this block's transactions
                else if(table === 'index_addresses' && payload.data['transactions']){
                    let ids = payload.data['transactions'].map(tx => tx.source_id).filter(id => id != null);
                    if(ids.length > 0){
                        let unique = [...new Set(ids)];
                        let rows = await this.db.doQuery("SELECT * FROM index_addresses WHERE id IN (" + unique.map(() => '?').join(',') + ")", unique);
                        if(rows && rows.length > 0)
                            payload.data[table] = rows;
                    }
                }
                // For other index tables, we skip per-block extraction (they're included in full/incremental snapshots)
            } catch(e){
                // Skip silently
            }
        }

        return payload;
    }

    // Update the broadcaster's status data for this chain/network
    async _updateStatus(){
        let hashRow = this.lastPolledBlock ? await this.db.getBlockHashRow(this.lastPolledBlock) : null;
        this.broadcaster.updateStatus(this.chain, this.network, {
            block_height: this.lastPolledBlock,
            block_time: hashRow ? Number(hashRow.block_time) : null,
            ledger_hash: hashRow ? hashRow.ledger_hash : null,
            actions_hash: hashRow ? hashRow.actions_hash : null,
            contract_hash: hashRow ? hashRow.contract_hash : null
        });
    }
}

module.exports = ServerPoller;
