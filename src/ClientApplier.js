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
 * XChain Indexer Sync - Client Applier
 *
 * Applies block payloads and snapshots to a local replica MariaDB.
 * Uses INSERT IGNORE for dedup/index tables and standard INSERT for data tables.
 *
 ********************************************************************/

class ClientApplier {

    constructor(db, util) {
        this.db   = db;
        this.util = util;

        // Index/dedup tables use INSERT IGNORE (rows may already exist)
        this.ignoreTables = new Set([
            'index_actions', 'index_addresses', 'index_coins', 'index_fiats',
            'index_memos', 'index_mime_types', 'index_pubkeys', 'index_statuses',
            'index_tickers', 'index_transactions'
        ]);
    }

    // Apply a single block payload from a WebSocket event
    async applyBlock(payload){
        if(!payload || !payload.data || !payload.block_index) return;

        // Check if this block already exists (duplicate detection)
        let existing = await this.db.getBlockHashRow(payload.block_index);
        if(existing){
            console.log('Block ' + payload.block_index + ' already exists, skipping');
            return;
        }

        await this.db.beginTransaction();
        try {
            let data = payload.data;
            // Apply tables in the order they appear in the payload
            for(let table in data){
                let rows = data[table];
                if(!rows || rows.length === 0) continue;
                await this._insertRows(table, rows);
            }
            await this.db.commitTransaction();
        } catch(e){
            await this.db.rollbackTransaction();
            console.error('Error applying block ' + payload.block_index + ':', e.message);
            throw e;
        }
    }

    // Apply a full snapshot (used for initial bootstrap)
    // snapshotData: parsed JSON object with { tables: { tableName: [rows...] } }
    async applyFullSnapshot(snapshotData){
        if(!snapshotData || !snapshotData.tables) return;

        console.log('Applying full snapshot (block height: ' + snapshotData.block_height + ')...');
        let timer = this.util.startTimer();

        await this.db.beginTransaction();
        try {
            // Truncate all tables first (reverse order to handle any logical dependencies)
            let tables = Object.keys(snapshotData.tables);
            for(let i = tables.length - 1; i >= 0; i--){
                await this.db.truncateTable(tables[i]);
            }

            // Insert rows in forward order
            for(let table of tables){
                let rows = snapshotData.tables[table];
                if(!rows || rows.length === 0) continue;
                await this._insertRows(table, rows);
                if(rows.length > 100)
                    console.log('  ' + table + ': ' + rows.length + ' rows');
            }

            await this.db.commitTransaction();
            console.log('Full snapshot applied (' + this.util.getTimer(timer) + ')');
        } catch(e){
            await this.db.rollbackTransaction();
            console.error('Error applying full snapshot:', e.message);
            throw e;
        }
    }

    // Apply an incremental snapshot
    async applyIncrementalSnapshot(snapshotData){
        if(!snapshotData || !snapshotData.tables) return;

        console.log('Applying incremental snapshot (since block ' + snapshotData.since_block + ')...');
        let timer = this.util.startTimer();

        await this.db.beginTransaction();
        try {
            for(let table in snapshotData.tables){
                let rows = snapshotData.tables[table];
                if(!rows || rows.length === 0) continue;
                await this._insertRows(table, rows);
            }
            await this.db.commitTransaction();
            console.log('Incremental snapshot applied (' + this.util.getTimer(timer) + ')');
        } catch(e){
            await this.db.rollbackTransaction();
            console.error('Error applying incremental snapshot:', e.message);
            throw e;
        }
    }

    // Insert rows into a table
    async _insertRows(table, rows){
        if(!rows || rows.length === 0) return;

        let useIgnore = this.ignoreTables.has(table);
        let columns   = Object.keys(rows[0]);
        let colList   = columns.map(c => '`' + c + '`').join(', ');
        let placeholders = columns.map(() => '?').join(', ');

        let insertPrefix = useIgnore
            ? 'INSERT IGNORE INTO `' + table + '` (' + colList + ') VALUES '
            : 'INSERT INTO `' + table + '` (' + colList + ') VALUES ';

        // Batch inserts in groups of 100 for efficiency
        let batchSize = 100;
        for(let i = 0; i < rows.length; i += batchSize){
            let batch = rows.slice(i, i + batchSize);
            let valueClauses = [];
            let args = [];

            for(let row of batch){
                valueClauses.push('(' + placeholders + ')');
                for(let col of columns){
                    args.push(row[col] !== undefined ? row[col] : null);
                }
            }

            let query = insertPrefix + valueClauses.join(', ');
            await this.db.doQuery(query, args);
        }
    }
}

module.exports = ClientApplier;
