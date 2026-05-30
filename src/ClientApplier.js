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

const validation     = require('./validation');
const balanceHelpers = require('./balance-helpers');

class ClientApplier {

    constructor(db, util) {
        this.db   = db;
        this.util = util;

        // Index/dedup tables use INSERT IGNORE (rows may already exist).
        // pubkeys (decoder DB) is included: incremental snapshots and per-block
        // payloads can re-send the same address's pubkey row across multiple
        // blocks, and the PK on address_id would otherwise collide.
        this.ignoreTables = new Set([
            'index_actions', 'index_addresses', 'index_coins', 'index_fiats',
            'index_memos', 'index_mime_types', 'index_pubkeys', 'index_statuses',
            'index_tickers', 'index_transactions',
            'pubkeys'
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
            // Rebuild balances if this payload touched credits/debits.
            // ServerPoller's per-block payload can't scope balances via the
            // action-scoped JOIN (the balances table has no action_index
            // column), so the only way the replica's balances table can stay
            // consistent with credits/debits during live sync is to recompute
            // it from the (now-updated) credit/debit data. Indexer-shaped DBs
            // only — decoder has no balances/credits/debits tables.
            let dbType = (this.db && this.db.dbType) || 'indexer';
            if(dbType === 'indexer' && (data.credits || data.debits)){
                await this._rebuildBalances();
            }
            // contract_balances is the deposits/withdrawals analogue of balances:
            // a derived aggregate with no action_index column, so the source poller
            // can't stream it per-block (its action_index JOIN errors). Recompute it
            // here whenever the payload touched deposits/withdrawals, exactly as we do
            // for balances above — otherwise replica custody balances stay stale until
            // the next full snapshot.
            if(dbType === 'indexer' && (data.deposits || data.withdrawals)){
                await this._rebuildContractBalances();
            }
            await this.db.commitTransaction();
        } catch(e){
            await this.db.rollbackTransaction();
            console.error('Error applying block ' + payload.block_index + ':', e.message);
            throw e;
        }
    }

    // Recompute the balances table from the current credits/debits rows.
    // SQL lives in balance-helpers so ClientRollback uses the same query.
    async _rebuildBalances(){
        try {
            await balanceHelpers.rebuildBalances(this.db);
        } catch(e){
            if(e.errno !== 1146) throw e;
            // Tables may not exist on a decoder replica — the dbType guard above
            // should prevent this from being reached, but the catch keeps the
            // applier's containing transaction from blowing up if it is.
        }
    }

    // Recompute the contract_balances table from the current deposits/withdrawals
    // rows (valid status only).  SQL lives in balance-helpers so ClientRollback
    // uses the same query.  Indexer-shaped DBs only.
    async _rebuildContractBalances(){
        try {
            await balanceHelpers.rebuildContractBalances(this.db);
        } catch(e){
            if(e.errno !== 1146) throw e;
            // Tables may not exist on a decoder replica — the dbType guard above
            // should prevent this from being reached, but the catch keeps the
            // applier's containing transaction from blowing up if it is.
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
            // Clear all snapshot tables first (reverse order: child rows before parents).
            // DELETE rather than TRUNCATE: MariaDB rejects TRUNCATE on any table referenced
            // by a foreign key, even when the referencing table is empty. The decoder DB
            // declares such a FK (pubkeys.address_id → index_addresses.id), so TRUNCATE
            // would crash the bootstrap; DELETE honours FK constraints row-by-row.
            let tables = Object.keys(snapshotData.tables);
            for(let i = tables.length - 1; i >= 0; i--){
                let tCheck = validation.validateIdentifier(tables[i]);
                if(!tCheck.valid){
                    console.error('Skipping clear of invalid table: ' + tables[i]);
                    continue;
                }
                await this.db.doQuery('DELETE FROM `' + tables[i] + '`');
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
            // Rebuild balances if this snapshot touched credits/debits. The
            // incremental catch-up inserts new credit/debit rows, but the
            // balances table is a derived aggregate — without recomputing it
            // here the replica's balances stay stale until the next live block
            // happens to touch credits/debits (mirrors applyBlock above).
            // Indexer-shaped DBs only — decoder has no balances table.
            let dbType = (this.db && this.db.dbType) || 'indexer';
            if(dbType === 'indexer' && (snapshotData.tables.credits || snapshotData.tables.debits)){
                await this._rebuildBalances();
            }
            // Same rationale as applyBlock: contract_balances is a derived aggregate
            // with no action_index cursor, so refresh it from deposits/withdrawals
            // whenever the incremental catch-up touched either.
            if(dbType === 'indexer' && (snapshotData.tables.deposits || snapshotData.tables.withdrawals)){
                await this._rebuildContractBalances();
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

        // Validate table name
        let tableCheck = validation.validateIdentifier(table);
        if(!tableCheck.valid){
            console.error('Rejected table name in _insertRows: ' + table + ' (' + tableCheck.reason + ')');
            return;
        }

        let useIgnore = this.ignoreTables.has(table);
        let columns   = Object.keys(rows[0]);

        // Validate all column names
        for(let col of columns){
            let colCheck = validation.validateIdentifier(col);
            if(!colCheck.valid){
                console.error('Rejected column name in _insertRows: ' + col + ' (' + colCheck.reason + ')');
                return;
            }
        }
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
