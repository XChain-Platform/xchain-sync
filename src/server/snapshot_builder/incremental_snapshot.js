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
const { encodeRow, encodeTables, bigIntReplacer } = require('../../util/wire_codec');
const { isSchemaGapError } = require('../../db/tables/schema_gap');
const replicatedTables = require('../../schema/replicated_tables');
const { collectUpdatedRows } = require('../updated_rows');
const { activationDelayBlocks } = require('../../consensus-constants');
const { getLogger } = require('../../observability');
const { SnapshotStreamWriter } = require('./stream_writer');
const { setHashHeaders } = require('./hash_headers');
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
            await this.streamIncrementalTables(writer, db, dbType, sinceBlock, lastBlock, conn, skipLookups);
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
            // A fault after the headers went out (a streamed table read cut off
            // by the source, such as net_write_timeout behind a stalled reader)
            // leaves a partial body. Destroying the response makes the follower
            // fail at once instead of waiting on an open, silent transfer.
            if(res.headersSent && typeof res.destroy === 'function') res.destroy();
            throw e;
        }
    },

    // Write every table of the "tables" object, in dependency order, under the
    // per-dbType scoping rules built by buildIncrementalSelectCtx.
    async streamIncrementalTables(writer, db, dbType, sinceBlock, lastBlock, conn, skipLookups){
        let tableOrder = await this.getOrderedTables(db, conn);
        let first = true;

        let selectCtx = await this.buildIncrementalSelectCtx(db, dbType, sinceBlock, lastBlock, conn, skipLookups);

        for(let table of tableOrder){
            first = await this.streamIncrementalTable(writer, db, table, selectCtx, first);
        }
    },

    setIncrementalHeaders(res, dbType, schemaVersion, lastBlock, sinceBlock, hashRow){
        res.setHeader('Content-Type', 'application/json');
        res.setHeader('Content-Encoding', 'gzip');
        res.setHeader('X-Block-Height', lastBlock);
        res.setHeader('X-Since-Block', sinceBlock);
        res.setHeader('X-Snapshot-Schema-Version', schemaVersion);
        setHashHeaders(res, dbType, hashRow);
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

            const selection = await this.selectIncrementalRows(db, table, selectCtx);
            if(selection === null) return first;
            return await this.writeIncrementalTableRows(writer, table, selection, first);
        } catch(e){
            this.tolerateIncrementalTableError(e, table, !!(e && e.incrementalTableStarted));
        }
        return first;
    },

    async writeIncrementalTableRows(writer, table, selection, first){
        if(Array.isArray(selection)) selection = { rows: selection };
        const extras = selection.extras || [];
        const stream = selection.open ? selection.open() : selection.stream;
        const source = stream || selection.rows || [];
        const dedupeKey = selection.dedupeKey;
        const pendingExtraKeys = dedupeKey ? new Set(extras.map(dedupeKey)) : null;
        let started = false;
        let rowCount = 0;

        const writeRow = async (row) => {
            if(!started){
                if(!first) await writer.write(',');
                await writer.write('"' + table + '":[');
                started = true;
            }
            if(rowCount > 0) await writer.write(',');
            await writer.write(JSON.stringify(encodeRow(row), bigIntReplacer));
            rowCount++;
        };

        try {
            try {
                for await (const row of source){
                    if(pendingExtraKeys) pendingExtraKeys.delete(dedupeKey(row));
                    await writeRow(row);
                }
            } finally {
                // The driver's destroy() leaves the socket paused, so the
                // rollback queued behind an abandoned stream would never run;
                // close() discards the remaining rows and resumes it first.
                if(stream && typeof stream.close === 'function') stream.close();
                if(stream && typeof stream.destroy === 'function') stream.destroy();
            }

            for(const row of extras){
                if(pendingExtraKeys){
                    const key = dedupeKey(row);
                    if(!pendingExtraKeys.has(key)) continue;
                    pendingExtraKeys.delete(key);
                }
                await writeRow(row);
            }

            if(!started) return first;
            await writer.write(']');
            return false;
        } catch(e){
            if(started && e) e.incrementalTableStarted = true;
            throw e;
        }
    },

    tolerateIncrementalTableError(e, table, tableStarted){
        // A client-disconnect abort must break out of the whole stream, not
        // be swallowed as a per-table read error (which would keep writing to
        // a dead stream and pinning the read view).
        if(e && e.aborted) throw e;
        // The catch tolerates only a genuine schema gap (1146 missing table /
        // 1054 missing column) on an older source: log-and-continue leaves that
        // table out of the payload harmlessly, but only before its header has
        // been written. Any error after that point must abort the response so a
        // follower cannot accept truncated table contents. Any other error is a
        // transient/operational fault (deadlock 1213, lock-wait 1205, connection
        // drop) that would otherwise ship a structurally-valid but silently
        // INCOMPLETE catch-up. Re-throw so the stream aborts and the follower
        // fails closed on truncated JSON, matching ClientRollback's errno
        // discrimination.
        if(tableStarted || !isSchemaGapError(e)) throw e;
        logger.error(util.format('Error reading table ' + table + ' for incremental:', e));
    },

    // After the "tables" object closes, emit the in-place updated-rows channel
    // as a sibling key. The action_index window above can't reach a surviving
    // row (created below the window) that was mutated in place during the
    // catch-up range, so carry its current full state here for the follower to
    // UPSERT (ClientApplier.applyUpdatedRows). Indexer only; decoder has none
    // of these tables. Collected on the SAME REPEATABLE READ conn so it reads at
    // the snapshot's block height. Window starts at sinceBlock to match the
    // `block_index >= sinceBlock` data scoping above (over-inclusion is a
    // harmless UPSERT). tokens.escrow_action_index rides along for a token
    // selected by a ledger touch or ISSUE edit. Offer transitions alone do not
    // select that token, so the follower also derives the gate locally. On a
    // truncated replica the derive preserves a pre-floor gate unless a local
    // later status proves the offer closed.
    async writeIncrementalUpdatedRows(writer, db, dbType, sinceBlock, lastBlock, coin, conn){
        if(dbType !== 'indexer') return;
        let delay = requireIncrementalActivationDelay(coin);
        let updated = await collectUpdatedRows(db, sinceBlock, lastBlock, delay, conn);
        await writer.write(',"updated_rows":' + JSON.stringify(encodeTables(updated), bigIntReplacer));
    },

};
