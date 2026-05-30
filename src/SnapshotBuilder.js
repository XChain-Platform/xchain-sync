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
const { SCHEMA_VERSION } = require('./schema-version');

// JSON replacer that converts BigInt to string (mariadb driver returns BigInt for BIGINT columns)
const bigIntReplacer = (k, v) => typeof v === 'bigint' ? v.toString() : v;

// Tables holding operator-local bookkeeping state (fetch timestamps, retry counters) that
// legitimately diverges between nodes and must not appear in consensus snapshots.
const OPERATOR_LOCAL_TABLES = new Set(['icons', 'price_snapshots']);

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
        // pubkeys trails index_addresses: pubkeys.sql declares a FK on index_addresses(id),
        // so the reverse-delete path in applyFullSnapshot must drop pubkeys rows before
        // index_addresses rows.  Appended after sync_meta because neither balances nor
        // sync_meta carries a FK on pubkeys.
        this.trailingTables = ['balances', 'sync_meta', 'pubkeys'];

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
        let allTables = rows.map(r => r.table_name || r.TABLE_NAME).filter(t => !OPERATOR_LOCAL_TABLES.has(t));

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

    // Stream a full snapshot to an HTTP response.
    //
    // The block-height anchor, the hash headers, and every paginated table read
    // run inside a single REPEATABLE READ snapshot (opened before getLastBlock).
    // This keeps the advertised hashes and the streamed rows consistent to one
    // block height even if the source commits new blocks mid-stream; without it
    // a busy source produces mixed-block payloads that fail validator hash
    // verification on bootstrap.
    async streamFullSnapshot(db, res){
        await db.beginReadSnapshot();
        let snapshotOpen = true;
        try {
            let lastBlock = await db.getLastBlock();
            if(lastBlock === null){
                await db.commitTransaction();
                snapshotOpen = false;
                res.status(404).json({ error: 'No blocks in database' });
                return;
            }

            let hashRow = await db.getBlockHashRow(lastBlock);

            // Set response headers
            res.setHeader('Content-Type', 'application/json');
            res.setHeader('Content-Encoding', 'gzip');
            res.setHeader('X-Block-Height', lastBlock);
            res.setHeader('X-Snapshot-Schema-Version', SCHEMA_VERSION);
            if(hashRow){
                res.setHeader('X-Ledger-Hash', hashRow.ledger_hash || '');
                res.setHeader('X-Actions-Hash', hashRow.actions_hash || '');
                res.setHeader('X-Contract-Hash', hashRow.contract_hash || '');
            }

            let gzip = zlib.createGzip();
            gzip.pipe(res);

            // Start JSON structure
            gzip.write('{"schema_version":' + SCHEMA_VERSION + ',"block_height":' + lastBlock + ',"tables":{');

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
                    console.error('Error reading table ' + table + ':', e);
                }
            }

            // Release the read view only after the final page is read. The data
            // is already buffered into gzip, so committing before gzip.end()
            // does not race the stream flush.
            gzip.write('}}');
            await db.commitTransaction();
            snapshotOpen = false;
            gzip.end();
        } catch(e){
            if(snapshotOpen) await db.rollbackTransaction();
            throw e;
        }
    }

    // Stream an incremental snapshot to an HTTP response.
    // Behaviour branches on db.dbType:
    //   - indexer: block-scoped tables filtered by block_index, action-scoped
    //     tables filtered by action_index (the original logic).
    //   - decoder: block-scoped tables filtered by block_index, tx-scoped tables
    //     joined through transactions, and the small append-only index_* / pubkeys
    //     tables included in full (the client uses INSERT IGNORE on those).
    //     `events` is skipped — it has no block cursor and no monotonic id we
    //     can scope by (see decoder review Finding D).
    async streamIncrementalSnapshot(db, sinceBlock, res){
        // Same REPEATABLE READ snapshot discipline as streamFullSnapshot: the
        // anchor, hash headers, and all per-table reads must observe one block
        // height so the catch-up payload can't mix rows from two heights while
        // the headers advertise only one.
        await db.beginReadSnapshot();
        let snapshotOpen = true;
        try {
            let lastBlock = await db.getLastBlock();
            if(lastBlock === null || sinceBlock > lastBlock){
                await db.commitTransaction();
                snapshotOpen = false;
                res.status(404).json({ error: 'No data available after block ' + sinceBlock });
                return;
            }

            let dbType  = (db && db.dbType) || 'indexer';
            let hashRow = await db.getBlockHashRow(lastBlock);

            // Set response headers
            res.setHeader('Content-Type', 'application/json');
            res.setHeader('Content-Encoding', 'gzip');
            res.setHeader('X-Block-Height', lastBlock);
            res.setHeader('X-Since-Block', sinceBlock);
            res.setHeader('X-Snapshot-Schema-Version', SCHEMA_VERSION);
            if(hashRow){
                if(dbType === 'decoder'){
                    res.setHeader('X-Block-Hash', hashRow.block_hash || '');
                } else {
                    res.setHeader('X-Ledger-Hash',   hashRow.ledger_hash   || '');
                    res.setHeader('X-Actions-Hash',  hashRow.actions_hash  || '');
                    res.setHeader('X-Contract-Hash', hashRow.contract_hash || '');
                }
            }

            let gzip = zlib.createGzip();
            gzip.pipe(res);

            gzip.write('{"schema_version":' + SCHEMA_VERSION + ',"block_height":' + lastBlock + ',"since_block":' + sinceBlock + ',"tables":{');

            let tableOrder = await this._getOrderedTables(db);
            let first = true;

            // Scoping rules per dbType.
            // Decoder full-dump tables: index_* and pubkeys are small + append-only;
            //   the client uses INSERT IGNORE so re-sending existing rows is a no-op.
            let decoderBlockScoped = new Set(['blocks', 'transactions']);
            let decoderTxScoped    = new Set(['transaction_outputs', 'dispensers']);
            let decoderFullDump    = new Set(['index_addresses', 'index_transactions', 'pubkeys']);
            let decoderSkip        = new Set(['events', 'mempool_transactions']);

            // Indexer block-scoped set. These tables carry a block_index but no
            // action_index, so the action_index branch below cannot reach them —
            // they must be filtered by block_index here to appear in incremental
            // snapshots. attestation_validator_signatures and slash_events are
            // block-scoped for the same reason (see ServerPoller.blockScopedTables).
            //
            // Tables with neither a block_index nor an action_index cursor —
            // icons (token-icon processing state, keyed by token_id),
            // attestation_validator_stats (running per-validator aggregates), and
            // price_snapshots (mirrored from the cross-chain hub's price channel,
            // keyed by round_number/coin_pair) — cannot be scoped incrementally and
            // are intentionally omitted. They ride along in the full snapshot only;
            // for price_snapshots, live convergence is handled by the hub DB sync
            // mirror, not this block stream.
            let indexerBlockScoped = new Set(['blocks', 'transactions', 'validator_rewards', 'contract_state', 'attestation_validator_signatures', 'slash_events', 'sync_meta']);
            let firstActionIndex   = (dbType === 'indexer') ? await db.getFirstActionIndex(sinceBlock) : null;

            for(let table of tableOrder){
                try {
                    let rows;
                    if(dbType === 'decoder'){
                        if(decoderSkip.has(table)){
                            continue;
                        } else if(decoderBlockScoped.has(table)){
                            rows = await db.doQuery("SELECT * FROM `" + table + "` WHERE block_index >= ?", [sinceBlock]);
                        } else if(decoderTxScoped.has(table)){
                            rows = await db.doQuery(
                                "SELECT t.* FROM `" + table + "` t " +
                                "INNER JOIN transactions tx ON (tx.tx_index = t.tx_index) " +
                                "WHERE tx.block_index >= ?",
                                [sinceBlock]
                            );
                        } else if(decoderFullDump.has(table)){
                            rows = await db.doQuery("SELECT * FROM `" + table + "`");
                        } else {
                            continue;
                        }
                    } else {
                        if(indexerBlockScoped.has(table)){
                            rows = await db.doQuery("SELECT * FROM `" + table + "` WHERE block_index >= ?", [sinceBlock]);
                        } else if(firstActionIndex !== null){
                            try {
                                rows = await db.doQuery("SELECT * FROM `" + table + "` WHERE action_index >= ?", [firstActionIndex]);
                            } catch(e){
                                // Table doesn't have action_index — skip it for incremental
                                continue;
                            }
                        } else {
                            continue;
                        }
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
                    console.error('Error reading table ' + table + ' for incremental:', e);
                }
            }

            // Release the read view only after the final table read (see the
            // matching note in streamFullSnapshot).
            gzip.write('}}');
            await db.commitTransaction();
            snapshotOpen = false;
            gzip.end();
        } catch(e){
            if(snapshotOpen) await db.rollbackTransaction();
            throw e;
        }
    }

}

module.exports = SnapshotBuilder;
