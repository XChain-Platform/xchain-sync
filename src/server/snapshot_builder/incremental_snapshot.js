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
const { decoderIncrementalSets, INDEXER_INBAND_PAGED } = require('./table_sets');
const logger = getLogger();

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

            let gzip = zlib.createGzip();
            gzip.pipe(res);
            writer = new SnapshotStreamWriter(gzip, res);

            await writer.write('{"schema_version":' + schemaVersion + ',"block_height":' + lastBlock + ',"since_block":' + sinceBlock + ',"tables":{');

            let tableOrder = await this.getOrderedTables(db, conn);
            let first = true;

            // Scoping rules per dbType. Decoder buckets: see decoderIncrementalSets.
            let decoderSets        = decoderIncrementalSets();
            let decoderBlockScoped = decoderSets.blockScoped;
            let decoderTxScoped    = decoderSets.txScoped;
            let decoderFullDump    = decoderSets.fullDump;
            let decoderSkip        = decoderSets.skip;

            // Indexer block-scoped set. These tables carry a block_index but no
            // action_index, so the action_index branch below cannot reach them.
            // They must be filtered by block_index here to appear in incremental
            // snapshots. slash_events is block-scoped for the same reason
            // (see ServerPoller.blockScopedTables).
            //
            // Tables with neither a block_index nor an action_index cursor, such as
            // icons (token-icon processing state, keyed by token_id) and
            // price_snapshots (mirrored from the cross-chain hub's price channel,
            // keyed by round_number/coin_pair), cannot be scoped incrementally and
            // are intentionally omitted. They ride along in the full snapshot only;
            // for price_snapshots, live convergence is handled by the hub DB sync
            // mirror, not this block stream.
            // attest_validator_stats, markets, and merkle_epochs are also unscoped
            // but are included in indexerFullDump below so a follower does not
            // freeze those tables at bootstrap height (see comment there).
            //
            // Every block_index-scoped streamed table must be filtered by block_index here:
            // these tables carry NO action_index column (e.g. the slash debit logs key off
            // execution_index / slash_action_index, not action_index), so the action_index
            // branch below cannot reach them. A follower catching up incrementally over their
            // range would hit SELECT ... WHERE action_index >= ? -> ER_BAD_FIELD_ERROR ->
            // caught -> continue, silently dropping every row (short /status count; the reorg
            // restore, which JOINs the debit logs, then finds nothing to restore).
            //
            // Derive the set from the streaming topology (the single source of truth) rather
            // than a hand-maintained literal, so the next block-scoped table added to
            // replicatedTables can never re-open this gap. sync_meta is appended because it is
            // streamed inline by ServerPoller (not via the blockScoped topology) but is still
            // block_index-scoped for incremental catch-up.
            let indexerBlockScoped = new Set([...replicatedTables.getTopology('indexer').blockScoped, 'sync_meta']);

            // Append-only lookup/dedup tables (index_actions, index_addresses,
            // index_transactions, ...). They carry neither a block_index nor an
            // action_index cursor, so they can't be range-scoped. A follower that
            // heals a gap via incremental still needs the index_* rows those blocks
            // reference, or it is left short on them (row-count + ledger-hash mismatch
            // after the heal; blocks/transactions carry *_hash_id FKs into
            // index_transactions, so even action-less blocks need it). They are
            // therefore re-dumped in full; the client applies index_* with INSERT
            // IGNORE (ClientApplier.ignoreTables), so re-sending existing rows is a
            // no-op. Mirrors the decoder full-dump path. Sourced from the replicated
            // topology so it can't drift from the per-block streamed set.
            //
            // Also included:
            //   - merkle_epochs: transparency epoch records with no block_index or
            //     action_index cursor. Absent from every topology bucket, so it never
            //     enters the action_index or block_index scoping branches. Without an
            //     explicit full-dump here, a relay follower's merkle_epochs table
            //     freezes at bootstrap height and getProof returns "epoch not yet
            //     committed" for every post-bootstrap epoch.
            //   - markets: derived OHLCV aggregate keyed by tick pair with no
            //     action_index. Without a full-dump here, a follower's markets table
            //     freezes at bootstrap height. VALUE changes converge post-reorg via
            //     this full-dump UPSERT (ON DUPLICATE KEY UPDATE); ROW REMOVAL cannot,
            //     which is why ClientRollback mirrors both of the source's markets
            //     deletes (orphaned-tick sweep and the pair-scoped IDX-2 delete).
            //   - attest_validator_stats: running per-validator aggregate counters
            //     with no action_index. Without a full-dump here, these counters
            //     freeze at bootstrap height. ClientRollback drops affected rows on
            //     reorg; the next incremental catch-up restores current values.
            //   - events: append-only operational audit log (records REORG events)
            //     with no block_index or action_index cursor, so it never enters the
            //     scoping branches and, without a full-dump here, freezes at bootstrap
            //     height on an incrementally-caught-up follower (source count grows,
            //     replica frozen, and it is replication:'snapshot' so it never shows in
            //     the /status TABLE_COUNT_MISMATCH signal). Mirrors the decoder events
            //     full-dump. It is in ClientApplier.ignoreTables, so the re-dump is
            //     idempotent (INSERT IGNORE on the AUTO_INCREMENT id PK). rollback:
            //     'exempt', so nothing rolls it back; the full re-dump is its only
            //     convergence path.
            //   - pubkeys: INSERT IGNORE cache keyed by address_id, replication:
            //     'snapshot', so streamTopology() puts it in no per-block bucket and
            //     it reaches neither the spread above nor indexerBlockScoped. Without
            //     a full-dump here it fell to the action_index branch, where the
            //     missing column raised errno 1054 and was swallowed as a schema gap,
            //     so it rode NO incremental snapshot and froze at bootstrap height on
            //     every incrementally-caught-up follower (silent: replication:
            //     'snapshot' keeps it out of the /status count check, and it is not
            //     consensus-hashed). Mirrors the decoder pubkeys full-dump. It is in
            //     ClientApplier.ignoreTables, so the re-dump is idempotent.
            let indexerFullDump    = new Set([
                ...replicatedTables.getTopology('indexer').index,
                'merkle_epochs',
                'markets',
                'attest_validator_stats',
                'events',
                'pubkeys',
            ]);
            // The append-only, id-PK lookup tables a skipLookups caller syncs out of
            // band via streamTableRowsById. Only these are omitted; the mutated
            // aggregates folded into indexerFullDump (merkle_epochs/markets/
            // attest_validator_stats) stay in the bundled response (small, no id cursor).
            let lookupSet          = new Set(replicatedTables.getTopology(dbType).index);
            let firstActionIndex   = (dbType === 'indexer') ? await db.getFirstActionIndex(sinceBlock, conn) : null;

            let selectCtx = {
                dbType, sinceBlock, lastBlock, conn, skipLookups, lookupSet, firstActionIndex,
                decoderSkip, decoderBlockScoped, decoderTxScoped, decoderFullDump,
                indexerBlockScoped, indexerFullDump,
            };

            for(let table of tableOrder){
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
                        if(skipLookups) continue;
                        first = await this.streamLookupPaged(writer, db, table, conn, first);
                        continue;
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
                        continue;
                    }

                    let rows = await this.selectIncrementalRows(db, table, selectCtx);
                    if(rows === null) continue;

                    if(!rows || rows.length === 0) continue;

                    if(!first) await writer.write(',');
                    first = false;
                    await writer.write('"' + table + '":[');

                    for(let i = 0; i < rows.length; i++){
                        if(i > 0) await writer.write(',');
                        await writer.write(JSON.stringify(encodeRow(rows[i]), bigIntReplacer));
                    }

                    await writer.write(']');
                } catch(e){
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
                }
            }

            // Close the "tables" object, then emit the in-place updated-rows channel
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
            await writer.write('}');
            if(dbType === 'indexer'){
                let delay = activationDelayBlocks(coin);
                if(delay === undefined) delay = null;
                let updated = await collectUpdatedRows(db, sinceBlock, lastBlock, delay, conn);
                await writer.write(',"updated_rows":' + JSON.stringify(encodeTables(updated), bigIntReplacer));
            }
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

};
