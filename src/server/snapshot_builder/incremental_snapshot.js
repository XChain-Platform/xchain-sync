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
 * XChain Sync - Incremental Snapshot
 * SnapshotBuilder mixin: streams a catch-up snapshot under one REPEATABLE READ view.
 * Installed on SnapshotBuilder.prototype, so `this` is the builder.
 *
 ********************************************************************/

const zlib = require('zlib');
const util = require('node:util');
const { SCHEMA_VERSION } = require('../../schema/version');
const { encodeRow, encodeTables } = require('../../util/wire_codec');
const replicatedTables = require('../../schema/replicated_tables');
const { collectUpdatedRows } = require('../updated_rows');
const { activationDelayBlocks } = require('../../consensus-constants');
const { getLogger } = require('../../observability');
const { SnapshotStreamWriter, bigIntReplacer } = require('./stream_writer');
const { decoderIncrementalSets, indexerBlockScopedSet, indexerFullDumpSet, INDEXER_INBAND_PAGED } = require('./table_sets');
const logger = getLogger();

// Resolve the frozen activation delay for an indexer catch-up: null for an omitted coin,
// the integer for a known one, and a throw for an unrecognized one (hard misconfiguration).
function requireIncrementalActivationDelay(coin){
    let delay = activationDelayBlocks(coin);
    if(delay === undefined)
        throw new Error('incremental snapshot: unrecognized coin "' + coin + '" - no frozen ACTIVATION_DELAY_BLOCKS (see src/consensus-constants.js)');
    return delay;
}

module.exports = {

    // Stream an incremental snapshot to an HTTP response.
    // Behaviour branches on db.dbType:
    //   - indexer: block-scoped tables filtered by block_index, action-scoped
    //     tables filtered by action_index (the original logic).
    //   - decoder: block-scoped tables filtered by block_index, tx-scoped tables
    //     joined through transactions, and the small append-only index_* / pubkeys
    //     tables included in full (the client uses INSERT IGNORE on those).
    //     `events` (operational log) has no block cursor to scope by, so it is
    //     re-dumped in full each increment. Its AUTO_INCREMENT `id` PK rides the
    //     payload and the client applies incremental rows with INSERT IGNORE, so a
    //     repeated full dump is idempotent (existing ids no-op, new ones insert).
    //     Without this, an incrementally-caught-up follower would never receive
    //     events rows for the gap and would silently drift behind the source.
    async streamIncrementalSnapshot(db, sinceBlock, res, coin, opts){
        // Concurrency gate: same per-Database stream cap as the full
        // snapshot; both hold a REPEATABLE READ pool connection end-to-end.
        if(!this.acquireSnapshotSlot(db, res)) return;
        try {
            await this.streamIncrementalSnapshotLocked(db, sinceBlock, res, coin, opts);
        } finally {
            this.releaseSnapshotSlot(db);
        }
    },

    async streamIncrementalSnapshotLocked(db, sinceBlock, res, coin, opts){
        // opts.skipLookups: omit the append-only `.index` lookup tables (index_*,
        // and for the decoder pubkeys/events) from this response. A truncated /
        // fast-chain replica syncs those separately via the id-cursor paged route
        // (streamTableRowsById) because a single full-dump of a multi-million-row
        // lookup table (e.g. DOGE-testnet index_transactions ~8.5M rows) alone
        // exceeds SNAPSHOT_MAX_CONTENT and aborts the client download. Default off:
        // existing callers (4 args) keep the full bundled behaviour unchanged.
        let skipLookups = !!(opts && opts.skipLookups);
        // Refuse an indexer coin with no frozen activation delay before any header is sent,
        // so the route answers a clean 500 rather than a catch-up missing its deactivation rows.
        if(((db && db.dbType) || 'indexer') === 'indexer') requireIncrementalActivationDelay(coin);
        // Same REPEATABLE READ snapshot discipline as streamFullSnapshot: the
        // anchor, hash headers, and all per-table reads must observe one block
        // height so the catch-up payload can't mix rows from two heights while
        // the headers advertise only one.
        let conn = await db.beginReadSnapshot();
        let snapshotOpen = true;
        let writer = null;
        try {
            let lastBlock = await db.getLastBlock(conn);
            if(lastBlock === null || sinceBlock > lastBlock){
                await db.commitReadSnapshot(conn);
                snapshotOpen = false;
                res.status(404).json({ error: 'No data available after block ' + sinceBlock });
                return;
            }

            let dbType  = (db && db.dbType) || 'indexer';
            let schemaVersion = SCHEMA_VERSION[dbType];
            let hashRow = await db.getBlockHashRow(lastBlock, conn);

            this.setIncrementalHeaders(res, dbType, schemaVersion, lastBlock, sinceBlock, hashRow);

            let gzip = zlib.createGzip();
            gzip.pipe(res);
            writer = new SnapshotStreamWriter(gzip, res);

            await writer.write('{"schema_version":' + schemaVersion + ',"block_height":' + lastBlock + ',"since_block":' + sinceBlock + ',"tables":{');

            let tableOrder = await this.getOrderedTables(db, conn);
            let first = true;

            let selectCtx = await this.buildIncrementalSelectCtx(db, dbType, sinceBlock, lastBlock, conn, skipLookups);

            for(let table of tableOrder){
                first = await this.streamIncrementalTable(writer, db, table, selectCtx, first);
            }

            await writer.write('}');
            await this.writeIncrementalUpdatedRows(writer, db, dbType, sinceBlock, lastBlock, coin, conn);
            await writer.write('}');
            await db.commitReadSnapshot(conn);
            snapshotOpen = false;
            writer.finish();
        } catch(e){
            if(writer) writer.dispose();
            if(snapshotOpen) await db.rollbackReadSnapshot(conn);
            // A client abort mid-stream is not a server error (see streamFullSnapshot).
            if(e && e.aborted) return;
            throw e;
        }
    },

    setIncrementalHeaders(res, dbType, schemaVersion, lastBlock, sinceBlock, hashRow){
        res.setHeader('Content-Type', 'application/json');
        res.setHeader('Content-Encoding', 'gzip');
        res.setHeader('X-Block-Height', lastBlock);
        res.setHeader('X-Since-Block', sinceBlock);
        res.setHeader('X-Snapshot-Schema-Version', schemaVersion);
        if(hashRow){
            if(dbType === 'decoder'){
                res.setHeader('X-Block-Hash', hashRow.block_hash || '');
            } else {
                res.setHeader('X-Ledger-Hash',   hashRow.ledger_hash   || '');
                res.setHeader('X-Actions-Hash',  hashRow.actions_hash  || '');
                res.setHeader('X-Contract-Hash', hashRow.contract_hash || '');
            }
        }
    },

    async buildIncrementalSelectCtx(db, dbType, sinceBlock, lastBlock, conn, skipLookups){
        // Scoping rules per dbType. Decoder buckets: see decoderIncrementalSets.
        let decoderSets        = decoderIncrementalSets();
        let decoderBlockScoped = decoderSets.blockScoped;
        let decoderTxScoped    = decoderSets.txScoped;
        let decoderFullDump    = decoderSets.fullDump;
        let decoderSkip        = decoderSets.skip;

        let indexerBlockScoped = indexerBlockScopedSet();
        let indexerFullDump    = indexerFullDumpSet();
        // The append-only, id-PK lookup tables a skipLookups caller syncs out of
        // band via streamTableRowsById. Only these are omitted; the mutated
        // aggregates folded into indexerFullDump (merkle_epochs/markets/
        // attest_validator_stats) stay in the bundled response (small, no id cursor).
        let lookupSet          = new Set(replicatedTables.getTopology(dbType).index);
        let firstActionIndex   = (dbType === 'indexer') ? await db.getFirstActionIndex(sinceBlock, conn) : null;

        return {
            dbType, sinceBlock, lastBlock, conn, skipLookups, lookupSet, firstActionIndex,
            decoderSkip, decoderBlockScoped, decoderTxScoped, decoderFullDump,
            indexerBlockScoped, indexerFullDump,
        };
    },

    // Stream one table of an incremental snapshot and return the updated `first` flag.
    async streamIncrementalTable(writer, db, table, selectCtx, first){
        const { dbType, skipLookups, lookupSet, decoderFullDump, indexerFullDump, conn } = selectCtx;
        try {
            // Append-only id-PK lookup tables (index_*, and decoder pubkeys/
            // events) are full-dumped, but a single unbounded SELECT * would
            // materialize the whole table - millions of rows on a fast chain
            // (e.g. DOGE-testnet index_transactions ~8.5M) - into the driver
            // array before a byte is written, a cheap unauthenticated OOM path
            // on /since/0. Stream them by id cursor in pageSize batches instead
            // (mirrors streamTableRowsById), so no more than one page is resident
            // and the gzip backpressure above bounds the buffered output. A
            // skipLookups caller syncs these out of band, so omit them here. The
            // non-lookup indexer full-dumps (merkle_epochs/markets/
            // attest_validator_stats) are small aggregates with no id cursor and
            // stay in the bundled path below.
            if(lookupSet.has(table) &&
               ((dbType === 'decoder' && decoderFullDump.has(table)) ||
                (dbType === 'indexer' && indexerFullDump.has(table)))){
                if(skipLookups) return first;
                first = await this.streamLookupPaged(writer, db, table, conn, first);
                return first;
            }

            // The indexer `events` log and `pubkeys` cache are the indexerFullDump
            // members that are append-only yet absent from lookupSet: their
            // replication class is 'snapshot', not 'stream:index', so the branch
            // above cannot reach them and the bundled SELECT * below would
            // materialize the whole table per catch-up. Page each by its
            // INDEXER_INBAND_PAGED cursor, which emits a byte-identical key, so no
            // client, protocol, or schema change is implied. Unlike the lookupSet
            // tables they are NOT synced out of band, so they stream under skipLookups.
            if(dbType === 'indexer' && Object.hasOwn(INDEXER_INBAND_PAGED, table) && indexerFullDump.has(table)){
                first = await this.streamLookupPaged(writer, db, table, conn, first, INDEXER_INBAND_PAGED[table]);
                return first;
            }

            let rows = await this.selectIncrementalRows(db, table, selectCtx);
            if(rows === null) return first;

            if(!rows || rows.length === 0) return first;

            return await this.writeIncrementalTableRows(writer, table, rows, first);
        } catch(e){
            this.tolerateIncrementalTableError(e, table);
        }
        return first;
    },

    // After the "tables" object closes, emit the in-place updated-rows channel
    // as a sibling key. The action_index window above can't reach a surviving
    // row (created below the window) that was mutated in place during the
    // catch-up range, so carry its current full state here for the follower to
    // UPSERT (ClientApplier.applyUpdatedRows). Indexer only; decoder has none
    // of these tables. Collected on the SAME REPEATABLE READ conn so it reads at
    // the snapshot's block height. Window starts at sinceBlock to match the
    // `block_index >= sinceBlock` data scoping above (over-inclusion is a
    // harmless UPSERT). tokens.escrow_action_index rides along (full-row
    // carry); the follower also re-derives it locally when escrow tables
    // move (ClientApplier.maybeRederiveEscrow), so the carried value is
    // convergent, not the gate's only writer.
    async writeIncrementalTableRows(writer, table, rows, first){
        if(!first) await writer.write(',');
        await writer.write('"' + table + '":[');

        for(let i = 0; i < rows.length; i++){
            if(i > 0) await writer.write(',');
            await writer.write(JSON.stringify(encodeRow(rows[i]), bigIntReplacer));
        }

        await writer.write(']');
        return false;
    },

    tolerateIncrementalTableError(e, table){
        // A client-disconnect abort must break out of the whole stream, not
        // be swallowed as a per-table read error (which would keep writing to
        // a dead stream and pinning the read view).
        if(e && e.aborted) throw e;
        // The catch tolerates only a genuine schema gap (1146 missing table /
        // 1054 missing column) on an older source: log-and-continue leaves that
        // table out of the payload harmlessly. Any other error is a transient/
        // operational fault (deadlock 1213, lock-wait 1205, connection drop)
        // that would otherwise ship a structurally-valid but silently INCOMPLETE
        // catch-up (rows are fully fetched before any byte is written, so the
        // table's window vanishes while the payload still parses). Re-throw so
        // the stream aborts and the follower fails closed on truncated JSON,
        // matching ClientRollback's errno discrimination.
        if(e && e.errno !== 1146 && e.errno !== 1054) throw e;
        logger.error(util.format('Error reading table ' + table + ' for incremental:', e));
    },

    async writeIncrementalUpdatedRows(writer, db, dbType, sinceBlock, lastBlock, coin, conn){
        if(dbType !== 'indexer') return;
        let delay = requireIncrementalActivationDelay(coin);
        let updated = await collectUpdatedRows(db, sinceBlock, lastBlock, delay, conn);
        await writer.write(',"updated_rows":' + JSON.stringify(encodeTables(updated), bigIntReplacer));
    },

};
