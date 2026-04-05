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

// JSON replacer that converts BigInt to string (mariadb driver returns BigInt for BIGINT columns)
const bigIntReplacer = (k, v) => typeof v === 'bigint' ? v.toString() : v;

class SnapshotBuilder {

    constructor(util) {
        this.util = util;

        // Priority ordering for tables that must come first (index/dedup, then core).
        // Any tables not in this list are included alphabetically after these.
        this.priorityTables = [
            'index_actions', 'index_addresses', 'index_coins', 'index_fiats',
            'index_memos', 'index_mime_types', 'index_pubkeys', 'index_statuses',
            'index_tickers', 'index_transactions',
            'blocks', 'transactions', 'actions'
        ];

        // Tables to put last (derived/computed — depend on everything else)
        this.trailingTables = ['balances', 'sync_meta'];

        // Page size for paginated reads
        this.pageSize = 10000;
    }

    // Discover all tables in the database and return them in dependency order.
    // Priority tables come first, trailing tables last, everything else alphabetically in between.
    async _getOrderedTables(db){
        let rows = await db.doQuery(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_type = 'BASE TABLE'",
            [db.dbName]
        );
        let allTables = rows.map(r => r.table_name || r.TABLE_NAME);

        let prioritySet  = new Set(this.priorityTables);
        let trailingSet  = new Set(this.trailingTables);

        let ordered = [];
        // Priority tables first (in defined order)
        for(let t of this.priorityTables){
            if(allTables.includes(t)) ordered.push(t);
        }
        // Middle tables alphabetically
        let middle = allTables.filter(t => !prioritySet.has(t) && !trailingSet.has(t)).sort();
        ordered.push(...middle);
        // Trailing tables last
        for(let t of this.trailingTables){
            if(allTables.includes(t)) ordered.push(t);
        }

        return ordered;
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

        let tableOrder = await this._getOrderedTables(db);
        let first = true;
        for(let table of tableOrder){
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
                        gzip.write(JSON.stringify(row, bigIntReplacer));
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

        // Tables scoped by block_index (not action_index)
        let blockScopedSet = new Set(['blocks', 'transactions', 'validator_rewards', 'contract_state', 'sync_meta']);
        let tableOrder = await this._getOrderedTables(db);
        let first = true;

        for(let table of tableOrder){
            try {
                let rows;
                if(blockScopedSet.has(table)){
                    rows = await db.doQuery("SELECT * FROM `" + table + "` WHERE block_index >= ?", [sinceBlock]);
                } else if(firstActionIndex !== null){
                    // Try action_index column — not all tables may have it
                    try {
                        rows = await db.doQuery("SELECT * FROM `" + table + "` WHERE action_index >= ?", [firstActionIndex]);
                    } catch(e){
                        // Table doesn't have action_index — skip it for incremental
                        continue;
                    }
                } else {
                    continue;
                }

                if(!rows || rows.length === 0) continue;

                if(!first) gzip.write(',');
                first = false;
                gzip.write('"' + table + '":[');

                for(let i = 0; i < rows.length; i++){
                    if(i > 0) gzip.write(',');
                    gzip.write(JSON.stringify(rows[i], bigIntReplacer));
                }

                gzip.write(']');
            } catch(e){
                console.error('Error reading table ' + table + ' for incremental:', e.message);
            }
        }

        gzip.write('}}');
        gzip.end();
    }

}

module.exports = SnapshotBuilder;
