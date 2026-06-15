/*********************************************************************
 *
 * Copyright © 2025–2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC – https://dankest.llc
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This file is part of XChain Platform. Licensed under the GNU Affero
 * General Public License v3.0 or later; see LICENSE.md. A commercial
 * license (without AGPL source-disclosure terms) is available —
 * contact legal@dankest.llc.
 *
 **********************************************************************
 *
 * XChain Indexer Sync - Client Applier
 *
 * Applies block payloads and snapshots to a local replica MariaDB.
 * Uses INSERT IGNORE for dedup/index tables and standard INSERT for data tables.
 *
 ********************************************************************/

const validation          = require('./validation');
const balanceHelpers      = require('./balance-helpers');
const { SCHEMA_VERSION }  = require('./schema-version');
const { decodeValue }     = require('./wireCodec');

// Above this many distinct ids per dimension a scoped rebuild's IN-lists stop
// being worth it (and a catch-up that touched that much of the table is close
// to a full recompute anyway) — fall back to the unscoped rebuild.
const MAX_SCOPED_REBUILD_IDS = 1000;

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
            'pubkeys',
            // events is an append-only operational log that incremental snapshots
            // re-dump in full (it has no block_index/action_index cursor to scope by).
            // Its AUTO_INCREMENT id PK collides on already-applied rows on every
            // catch-up; without INSERT IGNORE the whole catch-up transaction fails
            // with "Duplicate entry for PRIMARY". IGNORE makes the re-dump idempotent.
            'events',
            // sync_meta (transparency log) has a UNIQUE index on block_index. It is
            // now streamed live per block AND carried by snapshots; INSERT IGNORE
            // makes a row already present (bootstrap snapshot, or a catch-up/live
            // overlap) a no-op, mirroring the server's recordBlock INSERT IGNORE.
            'sync_meta'
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
            // it from the (now-updated) credit/debit data — scoped to the ids
            // the new rows touched where possible. Indexer-shaped DBs only —
            // decoder has no balances/credits/debits tables.
            let dbType = (this.db && this.db.dbType) || 'indexer';
            if(dbType === 'indexer' && (data.credits || data.debits)){
                await this._rebuildBalancesTouchedBy(data.credits, data.debits);
            }
            await this.db.commitTransaction();
        } catch(e){
            await this.db.rollbackTransaction();
            console.error('Error applying block ' + payload.block_index + ':', e);
            throw e;
        }
    }

    // Collect the distinct (keyField, tick_id) ids touched by freshly applied
    // rows so the derived-aggregate rebuild can be limited to them. Returns
    // null when the rows can't be scoped — a row missing either id (NULL ids
    // can't be matched by an IN-list), or more distinct ids than an IN-list
    // should carry — in which case the caller falls back to the full rebuild.
    _collectRebuildScope(rowArrays, keyField){
        let keys  = new Set();
        let ticks = new Set();
        for(let rows of rowArrays){
            for(let row of (rows || [])){
                let k = row ? row[keyField] : undefined;
                let t = row ? row.tick_id   : undefined;
                if(k === undefined || k === null || t === undefined || t === null) return null;
                keys.add(k);
                ticks.add(t);
                if(keys.size > MAX_SCOPED_REBUILD_IDS || ticks.size > MAX_SCOPED_REBUILD_IDS) return null;
            }
        }
        return { keys: Array.from(keys), ticks: Array.from(ticks) };
    }

    // Rebuild balances scoped to the ids the given credit/debit rows touched;
    // unscopable rows fall back to the full rebuild, empty arrays touch
    // nothing and skip the rebuild entirely.
    async _rebuildBalancesTouchedBy(credits, debits){
        let scope = this._collectRebuildScope([credits, debits], 'address_id');
        if(scope && !scope.keys.length) return;
        await this._rebuildBalances(scope);
    }

    // Recompute the balances table from the current credits/debits rows.
    // SQL lives in balance-helpers so ClientRollback uses the same query.
    // scope (optional): { keys, ticks } from _collectRebuildScope; null/absent
    // recomputes the whole table.
    async _rebuildBalances(scope){
        try {
            await balanceHelpers.rebuildBalances(this.db,
                scope ? { addressIds: scope.keys, tickIds: scope.ticks } : undefined);
        } catch(e){
            if(e.errno !== 1146) throw e;
            // Tables may not exist on a decoder replica — the dbType guard above
            // should prevent this from being reached, but the catch keeps the
            // applier's containing transaction from blowing up if it is.
        }
    }

    // Apply a full snapshot (used for initial bootstrap)
    // snapshotData: parsed JSON object with { schema_version, block_height, tables: { tableName: [rows...] } }
    //
    // On schema_version mismatch the validator must be restarted after the server is upgraded so
    // that _fetchAndApplySchema re-runs against the new DDL before any rows are applied.
    async applyFullSnapshot(snapshotData){
        if(!snapshotData || !snapshotData.tables) return;

        let dbType = (this.db && this.db.dbType) || 'indexer';
        let expectedVersion = SCHEMA_VERSION[dbType];
        if(snapshotData.schema_version !== expectedVersion){
            throw new Error('Schema version mismatch: server=' + snapshotData.schema_version + ' client=' + expectedVersion + ' — restart the validator after upgrading the server');
        }

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
            console.error('Error applying full snapshot:', e);
            throw e;
        }
    }

    // Apply an incremental snapshot
    async applyIncrementalSnapshot(snapshotData){
        if(!snapshotData || !snapshotData.tables) return;

        let dbType = (this.db && this.db.dbType) || 'indexer';
        let expectedVersion = SCHEMA_VERSION[dbType];
        if(snapshotData.schema_version !== expectedVersion){
            throw new Error('Schema version mismatch: server=' + snapshotData.schema_version + ' client=' + expectedVersion + ' — restart the validator after upgrading the server');
        }

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
                await this._rebuildBalancesTouchedBy(snapshotData.tables.credits, snapshotData.tables.debits);
            }
            await this.db.commitTransaction();
            console.log('Incremental snapshot applied (' + this.util.getTimer(timer) + ')');
        } catch(e){
            await this.db.rollbackTransaction();
            console.error('Error applying incremental snapshot:', e);
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
                    // decodeValue restores base64 binary sentinels back to Buffers
                    // before insert (the inverse of SnapshotBuilder/BlockBroadcaster
                    // encoding); non-binary values pass through unchanged.
                    args.push(decodeValue(row[col] !== undefined ? row[col] : null));
                }
            }

            let query = insertPrefix + valueClauses.join(', ');
            await this.db.doQuery(query, args);
        }
    }
}

module.exports = ClientApplier;
