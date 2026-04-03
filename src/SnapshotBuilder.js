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
 * XChain Indexer Sync - Snapshot Builder
 *
 * Builds full and incremental JSON snapshots from the indexer database.
 * Snapshots are streamed with gzip compression to avoid OOM on large DBs.
 *
 ********************************************************************/

const zlib = require('zlib');

class SnapshotBuilder {

    constructor(util) {
        this.util = util;

        // Table ordering for correct INSERT dependency (index/dedup tables first)
        this.tableOrder = [
            // Index/dedup tables (must exist before referencing tables)
            'index_actions', 'index_addresses', 'index_coins', 'index_fiats',
            'index_memos', 'index_mime_types', 'index_pubkeys', 'index_statuses',
            'index_tickers', 'index_transactions',
            // Core tables
            'blocks', 'transactions', 'actions',
            // Action-specific tables
            'addresses', 'airdrops', 'batches', 'broadcasts', 'callbacks',
            'coinpays', 'coinpay_obligations', 'coinpay_expires', 'coinpay_statuses',
            'contracts', 'contract_state', 'contract_executions', 'contract_emissions', 'contract_balances',
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
            'sweeps',
            'tokens', 'unstakes',
            'validator_rewards',
            'withdrawals',
            // Derived/computed tables
            'balances',
            // Sync service metadata
            'sync_meta'
        ];

        // Page size for paginated reads
        this.pageSize = 10000;
    }

    // Stream a full snapshot to an HTTP response
    async streamFullSnapshot(db, res){
        let lastBlock = await db.getLastBlock();
        if(lastBlock === null){
            res.status(404).json({ error: 'No blocks in database' });
            return;
        }

        let hashRow = await db.getBlockHashRow(lastBlock);

        // Set response headers
        res.setHeader('Content-Type', 'application/json');
        res.setHeader('Content-Encoding', 'gzip');
        res.setHeader('X-Block-Height', lastBlock);
        if(hashRow){
            res.setHeader('X-Ledger-Hash', hashRow.ledger_hash || '');
            res.setHeader('X-Actions-Hash', hashRow.actions_hash || '');
            res.setHeader('X-Contract-Hash', hashRow.contract_hash || '');
        }

        let gzip = zlib.createGzip();
        gzip.pipe(res);

        // Start JSON structure
        gzip.write('{"block_height":' + lastBlock + ',"tables":{');

        let first = true;
        for(let table of this.tableOrder){
            try {
                let count = await db.getTableCount(table);
                if(count === 0) continue;

                if(!first) gzip.write(',');
                first = false;
                gzip.write('"' + table + '":[');

                let offset = 0;
                let firstRow = true;
                while(offset < count){
                    let rows = await db.getTablePage(table, this.pageSize, offset);
                    for(let row of rows){
                        if(!firstRow) gzip.write(',');
                        firstRow = false;
                        gzip.write(JSON.stringify(row));
                    }
                    offset += this.pageSize;
                }

                gzip.write(']');
            } catch(e){
                console.error('Error reading table ' + table + ':', e.message);
            }
        }

        gzip.write('}}');
        gzip.end();
    }

    // Stream an incremental snapshot to an HTTP response
    async streamIncrementalSnapshot(db, sinceBlock, res){
        let lastBlock = await db.getLastBlock();
        if(lastBlock === null || sinceBlock > lastBlock){
            res.status(404).json({ error: 'No data available after block ' + sinceBlock });
            return;
        }

        let hashRow = await db.getBlockHashRow(lastBlock);
        let firstActionIndex = await db.getFirstActionIndex(sinceBlock);

        // Set response headers
        res.setHeader('Content-Type', 'application/json');
        res.setHeader('Content-Encoding', 'gzip');
        res.setHeader('X-Block-Height', lastBlock);
        res.setHeader('X-Since-Block', sinceBlock);
        if(hashRow){
            res.setHeader('X-Ledger-Hash', hashRow.ledger_hash || '');
            res.setHeader('X-Actions-Hash', hashRow.actions_hash || '');
            res.setHeader('X-Contract-Hash', hashRow.contract_hash || '');
        }

        let gzip = zlib.createGzip();
        gzip.pipe(res);

        gzip.write('{"block_height":' + lastBlock + ',"since_block":' + sinceBlock + ',"tables":{');

        // Block-scoped tables
        let blockTables = ['blocks', 'transactions', 'validator_rewards', 'contract_state'];
        // Everything else is action-scoped
        let first = true;

        for(let table of this.tableOrder){
            try {
                let rows;
                if(blockTables.includes(table)){
                    rows = await db.doQuery("SELECT * FROM `" + table + "` WHERE block_index >= ?", [sinceBlock]);
                } else if(table === 'sync_meta'){
                    rows = await db.doQuery("SELECT * FROM sync_meta WHERE block_index >= ?", [sinceBlock]);
                } else if(firstActionIndex !== null){
                    rows = await db.doQuery("SELECT * FROM `" + table + "` WHERE action_index >= ?", [firstActionIndex]);
                } else {
                    continue;
                }

                if(!rows || rows.length === 0) continue;

                if(!first) gzip.write(',');
                first = false;
                gzip.write('"' + table + '":[');

                for(let i = 0; i < rows.length; i++){
                    if(i > 0) gzip.write(',');
                    gzip.write(JSON.stringify(rows[i]));
                }

                gzip.write(']');
            } catch(e){
                console.error('Error reading table ' + table + ' for incremental:', e.message);
            }
        }

        gzip.write('}}');
        gzip.end();
    }

    // Get the table order (used by ClientApplier for INSERT ordering)
    getTableOrder(){
        return this.tableOrder;
    }
}

module.exports = SnapshotBuilder;
