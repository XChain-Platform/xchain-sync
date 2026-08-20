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
 * XChain Indexer Sync - Snapshot Builder
 *
 * Builds full and incremental JSON snapshots from the indexer database.
 * Snapshots are streamed with gzip compression to avoid OOM on large DBs.
 *
 ********************************************************************/

const zlib = require('zlib');
const { once } = require('events');
const { SCHEMA_VERSION } = require('./schema-version');
const { encodeRow, encodeTables } = require('./wireCodec');
const replicatedTables = require('./replicatedTables');
const tableLifecycle = require('./tableLifecycle');
const { collectUpdatedRows } = require('./updatedRows');
const { collectMaturedCooldownCredits } = require('./cooldownCredits');
const { collectRedrivenValidatorRewards } = require('./recoveryRewards');
const { activationDelayBlocks } = require('./consensus-constants');
const poolSizing = require('./poolSizing');

// JSON replacer that converts BigInt to string (mariadb driver returns BigInt for BIGINT columns)
const bigIntReplacer = (k, v) => typeof v === 'bigint' ? v.toString() : v;

// Tables holding operator-local bookkeeping state (fetch timestamps, retry counters) that
// legitimately diverges between nodes and must not appear in consensus snapshots.
//
// DERIVED from the tableLifecycle registry: every entry whose `replication`
// mode is 'local', 'hub-mirror', or 'follower-derived' is snapshot-excluded,
// the same registry binding the per-block stream (streamTopology) and the
// rollback lists (replicaRollbackTables) already have. A new registry entry
// with one of those modes is excluded automatically; before this derivation a
// new local table passed every guard and silently rode consensus snapshots
// unless someone also hand-edited this Set. The three names appended
// after the spread have no registry entry (mempool_transactions is a
// decoder-DB table; sync_halt / sync_state are replica-created control
// tables) and are the ONLY permitted non-registry members; rollback-coverage
// F-5 pins both directions against the registry.
//
// Why the registry-derived members are excluded:
//  - icons, price_snapshots, pending_hub_pushes ('local'): operator-local
//    fetch/push bookkeeping that legitimately diverges between nodes.
//  - recovery_pending_rewards ('local'): restore-time scratch staging for archived
//    validator rewards (xchain-indexer F1a id-determinism fix). Recovery-local, drained
//    into validator_rewards by the reindex apply hook, and never consensus-hashed. It
//    must not ride snapshots (a follower has no recovery in progress, so its count
//    legitimately differs).
//  - Hub-mirrored tables ('hub-mirror': oracle_prices, capability_snapshots,
//    cross_chain_calls, cross_chain_matches, state_checkpoints): pushed/retracted by
//    hub_db_sync out-of-band with block apply, so they vary by WS arrival timing and
//    must not appear in consensus snapshots. These are NEVER replicated by xchain-sync
//    (excluded from the per-block stream, the incremental catch-up, AND these
//    snapshots). A serving node does not converge them via sync: the explorer serves
//    the consensus-relevant ones (state_checkpoints, capability_snapshots,
//    cross_chain_matches) from the MANDATORY co-located hub DB on the same server,
//    with no local-mirror fallback (it fails loud if the hub DB is absent).
//    IMPORTANT: a follower node that lacks a co-located hub DB (hub-less validator)
//    will have these five tables empty. This is by design and not a replication gap:
//    these rows are hub-driven state that the node must receive from its OWN hub
//    subscription (hub_db_sync), not from the sync snapshot stream. A hub-less
//    validator cannot serve hub-dependent API surfaces correctly; provision a hub
//    connection before running in validator mode.
//  - state_tree_roots ('follower-derived'): the follower recomputes its OWN row at
//    every snapshot apply (ClientApplier.seedSnapshotRoots, rebuilt from local
//    balances/stakes; never compared to the source). The table also carries
//    computed_at (CURRENT_TIMESTAMP), a per-node wall-clock value, so shipping the
//    source's rows adds pure nondeterminism to the snapshot and is never consumed by
//    the follower.
const OPERATOR_LOCAL_TABLES = new Set([
    ...tableLifecycle.tablesWhere(t =>
        ['local', 'hub-mirror', 'follower-derived'].includes(t.replication)),
    // mempool_transactions: node-local, non-deterministic observation state (its own
    // schema comment forbids sharing raw values across nodes). It is a DECODER-DB
    // table with no registry entry (the registry covers the indexer DB). Every other
    // channel already excludes it: the per-block stream (replicatedTables.js), the
    // incremental snapshot (decoderSkip in streamIncrementalSnapshot), and the
    // /status completeness count (getReplicatedTables). Listing it here closes the
    // one remaining leak: the FULL snapshot used to ship the source's
    // bootstrap-instant mempool, freezing it forever on full-bootstrap decoder
    // replicas while incremental-bootstrap replicas held zero rows for the same table.
    'mempool_transactions',
    // sync_halt, sync_state: replica-local durable CONTROL tables the source never
    // ships (created by db.verifySyncTables for both dbTypes; no registry entry).
    // sync_halt holds the durable divergence-halt audit record; sync_state holds the
    // bootstrap_base:<dbType> truncation-floor marker and the
    // index_map_mismatch_count counters. They are not in any snapshot payload, so
    // without listing them here the full-snapshot clear loop (ClientApplier:
    // enumerate all BASE tables, DELETE every one not in this set) would wipe both
    // on every full-snapshot apply, including the runtime oversized-incremental
    // recovery fallback on a live replica: erasing halt/forensic history and the
    // persisted bootstrap/verification posture _persistBootstrapBase wrote. They are
    // exactly the node-local control state this exclusion set exists to protect.
    'sync_halt', 'sync_state',
]);

// Tables the SOURCE never streams (full or incremental) but that are deliberately
// NOT in OPERATOR_LOCAL_TABLES: the applier's full-snapshot clear loop must keep
// clearing them so a replica that imported a foreign copy before the exclusion
// existed is healed on its next full-snapshot apply (OPERATOR_LOCAL_TABLES is
// clear-protected, which would freeze those foreign rows forever).
//
// merkle_reorgs: node-local transparency-log reorg audit (server-side/indexer-only
// per its schema header; written by TransparencyLog.pruneFrom, new_root backfilled
// by commitEpoch). No cursor column, no replica reader, not in getReplicatedTables.
// Before this exclusion it rode full snapshots (shipping another node's audit
// trail, wall-clock detected_at included) and hit the incremental path's
// action_index fall-through, where its errno 1054 was silently swallowed; now the
// omission is a classification on record. The registry-exhaustiveness test
// (test/unit/syncTableClassification.test.js) keeps the next sync-owned table from
// falling through the same way.
const SOURCE_UNSTREAMED_TABLES = new Set([
    'merkle_reorgs',
]);

// Guards a gzip snapshot stream against a slow or vanished reader. The snapshot
// routes are unauthenticated behind only a per-IP/hr limiter, so a half-open
// reader that never drains would otherwise (a) buffer the entire snapshot in the
// server's RAM and (b) pin the REPEATABLE READ connection open the whole time,
// bloating InnoDB undo history; a few concurrent slow reads exhaust the pool and
// push the source toward OOM. write() applies real backpressure - it awaits
// 'drain' when the gzip buffer fills instead of writing unconditionally - and
// rejects with an aborted-tagged error the moment the client disconnects, so the
// caller tears the read snapshot down rather than blocking on a drain that will
// never come.
class SnapshotStreamWriter {
    constructor(gzip, res){
        this.gzip = gzip;
        this.aborted = false;
        this._ac = new AbortController();
        this._res = res;
        // A disconnect fires 'close' on the response before the stream drains.
        // Flag it, wake any pending drain wait (via the abort signal), and destroy
        // the gzip stream so its buffered chunks are freed.
        this._onClose = () => this._abort();
        // A gzip error (e.g. a write after the piped socket died) must also release
        // a pending drain wait rather than hang it, and must not throw unhandled.
        this._onError = () => this._abort();
        res.once('close', this._onClose);
        gzip.once('error', this._onError);
    }

    _abort(){
        if(this.aborted) return;
        this.aborted = true;
        this._ac.abort();
        if(!this.gzip.destroyed) this.gzip.destroy();
    }

    _abortError(){
        let e = new Error('snapshot stream aborted by client disconnect');
        e.aborted = true;
        return e;
    }

    // Write one chunk, applying backpressure. Resolves once the chunk is accepted
    // (immediately if the buffer has room, else after 'drain'). Rejects with an
    // aborted-tagged error if the client has gone away, so the caller stops.
    async write(chunk){
        if(this.aborted) throw this._abortError();
        // write() returns false when the internal buffer crosses the high-water
        // mark: pause and wait for 'drain' so we never outrun a slow reader.
        if(this.gzip.write(chunk)) return;
        try {
            await once(this.gzip, 'drain', { signal: this._ac.signal });
        } catch(e){
            // AbortError (client closed) or a gzip error surfaced by once(): either
            // way the stream is gone, so report it as an abort.
            throw this._abortError();
        }
    }

    // Normal completion: detach the disconnect handlers and flush-close the stream.
    finish(){
        this._detach();
        if(!this.gzip.destroyed) this.gzip.end();
    }

    // Error/abort teardown: detach handlers and drop any buffered output.
    dispose(){
        this._detach();
        if(!this.gzip.destroyed) this.gzip.destroy();
    }

    _detach(){
        this._res.removeListener('close', this._onClose);
        this.gzip.removeListener('error', this._onError);
    }
}

// Priority ordering for tables that must come first (index/dedup, then core).
// Any tables not in this list are included alphabetically after these.
const PRIORITY_TABLES = [
    'index_actions', 'index_addresses', 'index_coins', 'index_fiats',
    'index_memos', 'index_mime_types', 'index_pubkeys', 'index_statuses',
    'index_tickers', 'index_transactions',
    'blocks', 'transactions', 'actions'
];

// Tables to put last (derived/computed; they depend on everything else).
// pubkeys trails index_addresses: pubkeys.sql declares a FK on index_addresses(id),
// so the reverse-delete path in applyFullSnapshot must drop pubkeys rows before
// index_addresses rows.  Appended after sync_meta because neither balances nor
// sync_meta carries a FK on pubkeys.
const TRAILING_TABLES = ['balances', 'sync_meta', 'pubkeys'];

// Order a set of snapshot table names into the builder's dependency order:
// priority tables first (in declared order), everything else alphabetically,
// trailing tables last. Exported (alongside OPERATOR_LOCAL_TABLES) so
// ClientApplier.applyFullSnapshot can clear the union of payload tables and
// the local snapshot-eligible set in the same FK-safe order the builder
// streams them (reverse-delete: children before parents).
function orderSnapshotTables(allTables){
    let prioritySet  = new Set(PRIORITY_TABLES);
    let trailingSet  = new Set(TRAILING_TABLES);

    let ordered = [];
    for(let t of PRIORITY_TABLES){
        if(allTables.includes(t)) ordered.push(t);
    }
    let middle = allTables.filter(t => !prioritySet.has(t) && !trailingSet.has(t)).sort();
    ordered.push(...middle);
    for(let t of TRAILING_TABLES){
        if(allTables.includes(t)) ordered.push(t);
    }

    return ordered;
}

class SnapshotBuilder {

    constructor(util) {
        this.util = util;

        this.priorityTables = PRIORITY_TABLES;
        this.trailingTables = TRAILING_TABLES;

        this.pageSize = 10000;

        // In-flight long-lived snapshot streams per Database instance.
        // ServerPoller and the /snapshot routes share ONE Database (pool sized
        // per dbType, see poolSizing.js) per chain:network:dbType, and each full or
        // incremental snapshot pins a pool connection for the whole stream
        // (beginReadSnapshot). The rate limiter is per-IP only, so N validators
        // bootstrapping at once (a flag-day cohort) could pin every connection
        // and starve the poller's ~3s getLastBlock acquire, stalling live block
        // broadcast. This semaphore caps concurrent streams per Database so at
        // least one connection is always free for the poller; excess requests
        // get 503 + Retry-After and the client retries.
        this._inflightSnapshots = new Map();
    }

    // Per-Database cap on concurrent long-lived snapshot streams. Default is
    // poolSize - 2 (one connection reserved for ServerPoller, one for other
    // short reads like /status). MAX_CONCURRENT_SNAPSHOTS overrides, but is
    // always clamped to [1, poolSize - 1] so no configuration can hand the
    // poller's last connection to a snapshot stampede.
    _snapshotCap(db){
        let poolSize = (db && db.connectionPoolParams && db.connectionPoolParams.connectionLimit)
            || poolSizing.resolvePoolSize(db && db.dbType);
        let cap = parseInt(process.env.MAX_CONCURRENT_SNAPSHOTS);
        if(!Number.isFinite(cap)) cap = poolSize - 2;
        return Math.max(1, Math.min(cap, poolSize - 1));
    }

    // Try to reserve a snapshot-stream slot for this Database. On saturation,
    // answers the request itself with 503 + Retry-After (fail fast rather than
    // queue: a queued acquire would still pin the caller and hide the overload
    // from the client's retry logic) and returns false.
    _acquireSnapshotSlot(db, res){
        let inflight = this._inflightSnapshots.get(db) || 0;
        if(inflight >= this._snapshotCap(db)){
            this.snapshotsRejected = (this.snapshotsRejected || 0) + 1;
            res.setHeader('Retry-After', '30');
            res.status(503).json({
                error: 'Too many concurrent snapshot streams; retry later',
                code: 'SNAPSHOT_BUSY'
            });
            return false;
        }
        this._inflightSnapshots.set(db, inflight + 1);
        return true;
    }

    _releaseSnapshotSlot(db){
        let inflight = this._inflightSnapshots.get(db) || 0;
        if(inflight <= 1) this._inflightSnapshots.delete(db);
        else this._inflightSnapshots.set(db, inflight - 1);
    }

    // Discover all tables in the database and return them in dependency order.
    // Priority tables come first, trailing tables last, everything else alphabetically in between.
    async _getOrderedTables(db, conn){
        let rows = await db.doQuery(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_type = 'BASE TABLE'",
            [db.dbName],
            conn
        );
        let allTables = rows.map(r => r.table_name || r.TABLE_NAME)
            .filter(t => !OPERATOR_LOCAL_TABLES.has(t) && !SOURCE_UNSTREAMED_TABLES.has(t));
        return orderSnapshotTables(allTables);
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
        // Concurrency gate: reject with 503 instead of pinning yet
        // another pool connection when the per-Database stream cap is reached.
        if(!this._acquireSnapshotSlot(db, res)) return;
        try {
            await this._streamFullSnapshotLocked(db, res);
        } finally {
            this._releaseSnapshotSlot(db);
        }
    }

    async _streamFullSnapshotLocked(db, res){
        let startedAt = Date.now();
        let conn = await db.beginReadSnapshot();
        let snapshotOpen = true;
        let writer = null;
        try {
            let lastBlock = await db.getLastBlock(conn);
            if(lastBlock === null){
                await db.commitReadSnapshot(conn);
                snapshotOpen = false;
                res.status(404).json({ error: 'No blocks in database' });
                return;
            }

            let dbType  = (db && db.dbType) || 'indexer';
            let schemaVersion = SCHEMA_VERSION[dbType];
            let hashRow = await db.getBlockHashRow(lastBlock, conn);

            res.setHeader('Content-Type', 'application/json');
            res.setHeader('Content-Encoding', 'gzip');
            res.setHeader('X-Block-Height', lastBlock);
            res.setHeader('X-Snapshot-Schema-Version', schemaVersion);
            if(hashRow){
                res.setHeader('X-Ledger-Hash', hashRow.ledger_hash || '');
                res.setHeader('X-Actions-Hash', hashRow.actions_hash || '');
                res.setHeader('X-Contract-Hash', hashRow.contract_hash || '');
            }

            let gzip = zlib.createGzip();
            gzip.pipe(res);
            writer = new SnapshotStreamWriter(gzip, res);

            await writer.write('{"schema_version":' + schemaVersion + ',"block_height":' + lastBlock + ',"tables":{');

            let tableOrder = await this._getOrderedTables(db, conn);
            let first = true;
            let totalRows = 0;
            // NO per-table catch: any read error here fails the WHOLE snapshot. This loop
            // used to log 'Error reading table X' and continue, which published
            // syntactically valid JSON with that table simply absent (a COUNT(*)
            // lock-wait/timeout on one large table was enough) while still advertising
            // block_height at the tip. ClientApplier.applyFullSnapshot DELETEs every
            // snapshot-eligible local table and re-inserts only the tables the payload
            // carries, so a populated `actions`/`markets` reached the replica EMPTY and the
            // replica then advanced to the advertised tip; a single-source deployment runs
            // no post-apply content check to notice (ClientSync._bootstrapRotateSources
            // cross-verifies only when sources.length > 1), so the divergence was permanent
            // and silent. Failing loud instead: the outer catch disposes the writer, so the
            // closing '}}' is never emitted, and the client's JSON.parse of the truncated
            // download throws and retries. The legitimate `if(count === 0) continue;` below
            // stands - a genuinely empty table is still omitted; only real errors abort.
            // A client-disconnect abort already broke out of the loop before this change
            // and still does, now via the same path.
            for(let table of tableOrder){
                let count = await db.getTableCount(table, conn);
                if(count === 0) continue;

                if(!first) await writer.write(',');
                first = false;
                await writer.write('"' + table + '":[');

                // One ordered streaming pass per table (no LIMIT/OFFSET repaging):
                // most replicated tables are keyless with a non-unique first
                // column, so offset paging by `ORDER BY 1` had no total order and
                // could duplicate or skip a page-boundary tie (see
                // db.streamTableRows). A single query execution reads each row
                // exactly once; for-await gives row-at-a-time backpressure so a
                // multi-million-row table never lands in the driver array.
                let firstRow = true;
                let rowStream = db.streamTableRows(table, conn);
                try {
                    for await (let row of rowStream){
                        if(!firstRow) await writer.write(',');
                        firstRow = false;
                        await writer.write(JSON.stringify(encodeRow(row), bigIntReplacer));
                        totalRows++;
                    }
                } finally {
                    // On a writer abort mid-table, stop the driver from buffering
                    // the rest of the result set (no-op once the stream has ended).
                    rowStream.destroy();
                }

                await writer.write(']');
            }

            // Release the read view only after the final page is read. The data
            // is already buffered into gzip, so committing before gzip.end()
            // does not race the stream flush.
            await writer.write('}}');
            await db.commitReadSnapshot(conn);
            snapshotOpen = false;

            // Record completion: log dbType/chain/block_height/duration/rows and
            // increment the lifetime served counter exposed on /status.
            let duration = Date.now() - startedAt;
            let chain  = (db && db.chain)  || '?';
            let network = (db && db.network) || '?';
            console.log('[SnapshotBuilder] full-snapshot served: dbType=' + (db && db.dbType) +
                ' chain=' + chain + '/' + network +
                ' block_height=' + lastBlock +
                ' rows=' + totalRows +
                ' duration=' + duration + 'ms');
            this.snapshotsServed = (this.snapshotsServed || 0) + 1;

            writer.finish();
        } catch(e){
            if(writer) writer.dispose();
            if(snapshotOpen) await db.rollbackReadSnapshot(conn);
            // A client abort mid-stream is not a server error: the read view is
            // already released, and the response is gone, so return quietly rather
            // than surfacing a 500-shaped error to the (absent) caller.
            if(e && e.aborted) return;
            // Tear the response down once the headers are out. writer.dispose()
            // destroys the gzip stream, but pipe() does not propagate a destroy to
            // its destination, so `res` would stay open with no further writes and
            // the client would sit on it until SNAPSHOT download timeout (600s) -
            // one stalled attempt per source, per retry. Destroying the socket makes
            // the truncated transfer fail immediately so the bootstrap retry ladder
            // moves. Headers-not-sent is left to the route, which answers 500.
            if(res.headersSent && typeof res.destroy === 'function') res.destroy();
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
    //     `events` (operational log) has no block cursor to scope by, so it is
    //     re-dumped in full each increment. Its AUTO_INCREMENT `id` PK rides the
    //     payload and the client applies incremental rows with INSERT IGNORE, so a
    //     repeated full dump is idempotent (existing ids no-op, new ones insert).
    //     Without this, an incrementally-caught-up follower would never receive
    //     events rows for the gap and would silently drift behind the source.
    async streamIncrementalSnapshot(db, sinceBlock, res, coin, opts){
        // Concurrency gate: same per-Database stream cap as the full
        // snapshot; both hold a REPEATABLE READ pool connection end-to-end.
        if(!this._acquireSnapshotSlot(db, res)) return;
        try {
            await this._streamIncrementalSnapshotLocked(db, sinceBlock, res, coin, opts);
        } finally {
            this._releaseSnapshotSlot(db);
        }
    }

    async _streamIncrementalSnapshotLocked(db, sinceBlock, res, coin, opts){
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

            let tableOrder = await this._getOrderedTables(db, conn);
            let first = true;

            // Scoping rules per dbType.
            // Decoder full-dump tables: index_* and pubkeys are small + append-only;
            //   the client uses INSERT IGNORE so re-sending existing rows is a no-op.
            //   `events` is full-dumped too: it carries no block_index/tx_index cursor
            //   to scope incrementally, so the only way an incrementally-caught-up
            //   follower converges its events table is a complete re-dump. It is safe
            //   to re-send because events has an AUTO_INCREMENT `id` PK and the client
            //   applies all incremental rows with INSERT IGNORE (existing ids are no-ops).
            let decoderBlockScoped = new Set(['blocks', 'transactions']);
            let decoderTxScoped    = new Set(['transaction_outputs']);
            let decoderFullDump    = new Set(['index_addresses', 'index_transactions', 'pubkeys', 'events']);
            // dispensers is skipped here for the same reason it is not per-block
            // streamed: the decoder soft-expires dispensers (UPDATE expired_block_index)
            // and defers the hard-purge to purgeExpiredDispensers. An insert-only
            // incremental delta (the tx_index->block_index join) would re-introduce the
            // count divergence on any follower that catches up incrementally, and a plain
            // re-dump would collide on the (tx_index, address_id) PK (dispensers is not in
            // ClientApplier.ignoreTables, so it is not INSERT IGNORE). dispensers seeds
            // from the full snapshot and is then held in parity SOLELY by the apply-side
            // reconcile: ClientApplier.applyDispensersReplace via
            // ClientSync._reconcileDispensers, gated by DISPENSERS_RECONCILE_EVERY /
            // DISPENSERS_RECONCILE_MAX_INTERVAL_MS. Its decoder /status completeness count
            // (replicatedTables `special`) is a post-replace equality sanity check, not a
            // backstop: a soft-expire UPDATE leaves counts equal and a hard-purge DELETE
            // leaves the replica ahead, which _verifyTableCounts does not report.
            let decoderSkip        = new Set(['mempool_transactions', 'dispensers']);

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
            // range would hit `SELECT ... WHERE action_index >= ?` -> ER_BAD_FIELD_ERROR ->
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

            for(let table of tableOrder){
                try {
                    // Append-only id-PK lookup tables (index_*, and decoder pubkeys/
                    // events) are full-dumped, but a single unbounded `SELECT *` would
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
                        first = await this._streamLookupPaged(writer, db, table, conn, first);
                        continue;
                    }

                    // The indexer `events` log is the one indexerFullDump member that is
                    // append-only on an AUTO_INCREMENT id yet absent from lookupSet: its
                    // replication class is 'snapshot', not 'stream:index', so the branch
                    // above cannot reach it and it fell to the bundled `SELECT *` below,
                    // materializing the whole audit log per catch-up. Page it
                    // by the same id cursor, which emits a byte-identical "events":[...]
                    // key, so no client, protocol, or schema change is implied. Unlike the
                    // lookupSet tables it is NOT synced out of band, so it must still be
                    // streamed under skipLookups rather than skipped.
                    if(dbType === 'indexer' && table === 'events' && indexerFullDump.has(table)){
                        first = await this._streamLookupPaged(writer, db, table, conn, first);
                        continue;
                    }

                    let rows;
                    if(dbType === 'decoder'){
                        if(decoderSkip.has(table)){
                            continue;
                        } else if(decoderBlockScoped.has(table)){
                            rows = await db.doQuery("SELECT * FROM `" + table + "` WHERE block_index >= ? ORDER BY block_index", [sinceBlock], conn);
                        } else if(decoderTxScoped.has(table)){
                            rows = await db.doQuery(
                                "SELECT t.* FROM `" + table + "` t " +
                                "INNER JOIN transactions tx ON (tx.tx_index = t.tx_index) " +
                                "WHERE tx.block_index >= ? ORDER BY t.tx_index",
                                [sinceBlock],
                                conn
                            );
                        } else if(decoderFullDump.has(table)){
                            if(skipLookups && lookupSet.has(table)) continue;
                            rows = await db.doQuery("SELECT * FROM `" + table + "`", null, conn);
                        } else {
                            continue;
                        }
                    } else {
                        if(indexerBlockScoped.has(table)){
                            rows = await db.doQuery("SELECT * FROM `" + table + "` WHERE block_index >= ? ORDER BY block_index", [sinceBlock], conn);
                        } else if(indexerFullDump.has(table)){
                            if(skipLookups && lookupSet.has(table)) continue;
                            rows = await db.doQuery("SELECT * FROM `" + table + "`", null, conn);
                        } else if(table === 'contract_emissions'){
                            // contract_emissions.action_index is NULL for INTERNAL emissions
                            // (SLASH and friends, which move ledger state without minting an
                            // on-wire action), so the generic `action_index >= ?` branch below
                            // silently drops every one of them from a catch-up window. The
                            // consensus contract_hash counts them (BlockHasher reaches them
                            // through the execution_index chain), and interior blocks arrive
                            // with their committed hashes verbatim, so nothing halts: the
                            // follower just carries a short table and any later recompute of an
                            // interior block diverges. Reach them the same way the live stream
                            // does (db.getEmissionRowsForBlock), widened from one block to the
                            // catch-up window.
                            //
                            // Block-scoped, not action-scoped, so it is correct with a null
                            // firstActionIndex too, and it names the four protocol columns
                            // rather than `em.*`: the AUTO_INCREMENT id is local to each node
                            // (the live stream never carries it, so the follower's sequence is
                            // already offset from the source's), and shipping the source's id
                            // into a plain INSERT is the ER_DUP_ENTRY freeze that
                            // ClientApplier.localSurrogateIdTables documents.
                            try {
                                rows = await db.doQuery(
                                    "SELECT em.execution_index, em.emitted_action, em.action_index, em.position " +
                                    "FROM `contract_emissions` em " +
                                    "INNER JOIN contract_executions ce ON (ce.action_index = em.execution_index) " +
                                    "INNER JOIN actions a ON (a.action_index = ce.action_index) " +
                                    "WHERE a.block_index >= ? " +
                                    "ORDER BY em.execution_index ASC, em.position ASC",
                                    [sinceBlock], conn);
                            } catch(e){
                                // Same rule as the generic branch: swallow ONLY a genuine
                                // schema gap on an older source (1146 missing table / 1054
                                // missing column), propagate anything transient.
                                if(e && e.errno !== 1146 && e.errno !== 1054) throw e;
                                continue;
                            }
                        } else if(firstActionIndex !== null){
                            try {
                                rows = await db.doQuery("SELECT * FROM `" + table + "` WHERE action_index >= ? ORDER BY action_index", [firstActionIndex], conn);
                            } catch(e){
                                // Swallow ONLY a genuine schema gap (1146 missing table /
                                // 1054 missing action_index column on an older source);
                                // skip the table for incremental. A transient/operational
                                // error (deadlock 1213, lock-wait 1205, connection drop)
                                // must propagate so the stream aborts rather than silently
                                // omitting the table's window (matches ClientRollback).
                                if(e && e.errno !== 1146 && e.errno !== 1054) throw e;
                                continue;
                            }
                        } else if(table === 'credits'){
                            // firstActionIndex is null: a catch-up window with zero actions.
                            // The action-scoped base query would be empty, but the matured-
                            // cooldown merge below keys off the maturity block, not
                            // action_index, and a legacy-era cooldown maturity mints NO
                            // actions row (ClientRollback moves its reverse cooldown delete
                            // OUTSIDE this same firstActionIndex guard for exactly this
                            // reason). Fall through with an empty base so the merge still
                            // ships the backdated refund credit; without this the follower
                            // receives the updated_rows status flip but not the credit and
                            // its balances silently diverge over quiet windows.
                            rows = [];
                        } else {
                            continue;
                        }
                    }

                    // Cooldown-maturity refund credits reuse the unstake's earlier-block
                    // action_index (and carry no block_index), so they sit below the
                    // action_index cursor above and are missed. Merge them by maturity
                    // block over the same [sinceBlock, lastBlock] window the in-place
                    // updated_rows channel uses, deduped on the credit's logical identity
                    // (the forward mirror of ClientRollback's reverse cooldown delete). An
                    // unstake created AND matured inside this window can be reached both
                    // here and by the action_index scope, hence the dedup.
                    if(dbType === 'indexer' && table === 'credits'){
                        try {
                            let matured = await collectMaturedCooldownCredits(db, sinceBlock, lastBlock, conn);
                            if(matured.length > 0){
                                rows = rows || [];
                                let seen = new Set(rows.map(c => c.action_index + ':' + c.address_id + ':' + c.tick_id));
                                for(let c of matured){
                                    let k = c.action_index + ':' + c.address_id + ':' + c.tick_id;
                                    if(!seen.has(k)){ seen.add(k); rows.push(c); }
                                }
                            }
                        } catch(e){
                            // Swallow ONLY a genuine schema gap (1146 missing table / 1054
                            // missing column on an older source). A transient/operational
                            // error must abort the stream, not silently drop the matured
                            // credits from the catch-up payload (mirrors ClientRollback).
                            if(e && e.errno !== 1146 && e.errno !== 1054) throw e;
                        }
                    }

                    // Recovery-redriven validator rewards: a reorg re-drain re-materializes
                    // a survivor at block_index = earn-block E < B, so the block_index >=
                    // sinceBlock scope above misses it. Merge by applied_block over the same
                    // [sinceBlock, lastBlock] window, deduped on the row's UNIQUE identity
                    // (the forward analogue of ClientRollback's reverse block_index delete).
                    if(dbType === 'indexer' && table === 'validator_rewards'){
                        try {
                            let redriven = await collectRedrivenValidatorRewards(db, sinceBlock, lastBlock, conn);
                            if(redriven.length > 0){
                                rows = rows || [];
                                let seen = new Set(rows.map(r => r.source_id + ':' + r.signing_pubkey_id + ':' + r.reward_type + ':' + r.round_reference));
                                for(let r of redriven){
                                    let k = r.source_id + ':' + r.signing_pubkey_id + ':' + r.reward_type + ':' + r.round_reference;
                                    if(!seen.has(k)){ seen.add(k); rows.push(r); }
                                }
                            }
                        } catch(e){
                            // Swallow ONLY a genuine schema gap (1146 missing table / 1054
                            // missing column on an older source). A transient/operational
                            // error must abort the stream, not silently drop the redriven
                            // rewards from the catch-up payload (mirrors ClientRollback).
                            if(e && e.errno !== 1146 && e.errno !== 1054) throw e;
                        }
                    }

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
                    console.error('Error reading table ' + table + ' for incremental:', e);
                }
            }

            // Close the "tables" object, then emit the in-place updated-rows channel
            // as a sibling key. The action_index window above can't reach a surviving
            // row (created below the window) that was mutated in place during the
            // catch-up range, so carry its current full state here for the follower to
            // UPSERT (ClientApplier._applyUpdatedRows). Indexer only; decoder has none
            // of these tables. Collected on the SAME REPEATABLE READ conn so it reads at
            // the snapshot's block height. Window starts at sinceBlock to match the
            // `block_index >= sinceBlock` data scoping above (over-inclusion is a
            // harmless UPSERT). tokens.escrow_action_index rides along (full-row
            // carry); the follower also re-derives it locally when escrow tables
            // move (ClientApplier._maybeRederiveEscrow), so the carried value is
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
    }

    // Stream one append-only id-PK lookup table into the snapshot body by id cursor
    // in pageSize batches, so a multi-million-row table never lands in the driver
    // array (or gzip buffer) all at once. `first` tracks whether any table key has
    // been written yet (for the inter-table comma); returns the updated value.
    // Uses the shared REPEATABLE READ conn, so paging is consistent across batches.
    async _streamLookupPaged(writer, db, table, conn, first){
        let col = replicatedTables.lookupCursorColumn(table);
        let after = 0;
        let wrote = false;
        let firstRow = true;
        while(true){
            let page = await db.doQuery(
                "SELECT * FROM `" + table + "` WHERE `" + col + "` > ? ORDER BY `" + col + "` ASC LIMIT ?",
                [after, this.pageSize],
                conn
            );
            if(!page.length) break;
            if(!wrote){
                if(!first) await writer.write(',');
                first = false;
                await writer.write('"' + table + '":[');
                wrote = true;
            }
            for(let r of page){
                if(!firstRow) await writer.write(',');
                firstRow = false;
                await writer.write(JSON.stringify(encodeRow(r), bigIntReplacer));
            }
            after = Number(page[page.length - 1][col]);
            if(page.length < this.pageSize) break;
        }
        if(wrote) await writer.write(']');
        return first;
    }

    // Default + ceiling page sizes for streamTableRowsById. The ceiling bounds the
    // server's single-query result set and the client's per-page buffer so no one
    // request can approach SNAPSHOT_MAX_CONTENT, which is the whole point.
    static get ROWS_PAGE_DEFAULT(){ return 50000; }
    static get ROWS_PAGE_MAX(){ return 100000; }

    // Stream one id-ordered page of an append-only lookup table to an HTTP response.
    // Powers the truncated/fast-chain replica's out-of-band lookup sync: instead of
    // a single full-dump of a multi-million-row table (which exceeds
    // SNAPSHOT_MAX_CONTENT and aborts the client), the client pages by id cursor and
    // applies each bounded page. Only the append-only id-PK lookup tables (the
    // topology `.index` set: index_*, plus pubkeys/events for the decoder) are
    // pageable; they are INSERT-only with a monotonic AUTO_INCREMENT `id`, so
    // `id > cursor ORDER BY id` is stable across requests without a long-lived read
    // view. (Decoder pubkeys also pages by this surrogate `id`: its PK address_id is
    // non-monotonic w.r.t. insert order, so it would skip rows; see
    // replicatedTables.lookupCursorColumn and the pubkeys monotonic-id migration.)
    // Returns {schema_version, table, max_id, has_more, rows:[...]}.
    async streamTableRowsById(db, table, afterId, limit, res){
        let dbType = (db && db.dbType) || 'indexer';
        let allowed = new Set(replicatedTables.getTopology(dbType).index);
        if(!allowed.has(table)){
            // Not an allowlisted append-only lookup table. Refuse rather than run an
            // arbitrary `SELECT *` (the allowlist is also the SQL-identifier guard:
            // only these hardcoded names are ever interpolated into the query).
            return res.status(400).json({ error: 'Table not pageable: ' + table });
        }
        let after = Number.isFinite(afterId) ? Math.max(0, Math.floor(afterId)) : 0;
        let lim   = Number.isFinite(limit) ? Math.floor(limit) : SnapshotBuilder.ROWS_PAGE_DEFAULT;
        lim = Math.max(1, Math.min(SnapshotBuilder.ROWS_PAGE_MAX, lim));

        // Per-table cursor column. All pageable lookup tables (including decoder
        // pubkeys, via its surrogate monotonic `id`) page by `id`; address_id is
        // non-monotonic w.r.t. insert order and must NOT be used as the cursor.
        let col = replicatedTables.lookupCursorColumn(table);
        let rows = await db.doQuery(
            "SELECT * FROM `" + table + "` WHERE `" + col + "` > ? ORDER BY `" + col + "` ASC LIMIT ?",
            [after, lim]
        );
        let maxId   = rows.length ? Number(rows[rows.length - 1][col]) : after;
        let hasMore = rows.length === lim;
        let schemaVersion = SCHEMA_VERSION[dbType];

        res.setHeader('Content-Type', 'application/json');
        res.setHeader('Content-Encoding', 'gzip');
        res.setHeader('X-Snapshot-Schema-Version', schemaVersion);
        res.setHeader('X-Max-Id', String(maxId));
        res.setHeader('X-Has-More', hasMore ? 'true' : 'false');

        let gzip = zlib.createGzip();
        gzip.pipe(res);
        // Backpressure + client-abort teardown, same as the snapshot streams. No read
        // view is held here (the doQuery connection is released before streaming), but
        // the writer still bounds gzip's buffered output on a slow reader and stops
        // writing to a socket the client already closed.
        let writer = new SnapshotStreamWriter(gzip, res);
        try {
            await writer.write('{"schema_version":' + schemaVersion + ',"table":"' + table +
                '","max_id":' + maxId + ',"has_more":' + (hasMore ? 'true' : 'false') + ',"rows":[');
            for(let i = 0; i < rows.length; i++){
                if(i > 0) await writer.write(',');
                await writer.write(JSON.stringify(encodeRow(rows[i]), bigIntReplacer));
            }
            await writer.write(']}');
            writer.finish();
        } catch(e){
            writer.dispose();
            if(e && e.aborted) return;
            throw e;
        }
    }

    // Stream one keyset-ordered page of the decoder `dispensers` table. dispensers
    // is excluded from both the incremental block stream and the id-cursor lookup
    // paging (streamTableRowsById): it has no monotonic surrogate id (PK is
    // (tx_index, address_id)) and the decoder soft-expires then hard-purges rows, so
    // an insert-only delta cannot replay the UPDATE/DELETE convergence. A truncated
    // replica therefore never seeds it and an incrementally-caught-up replica drifts.
    // This endpoint powers the client's periodic replace-table reconcile
    // (ClientSync._reconcileDispensers): it serves the FULL current table in ONE
    // response so the client rebuilds it from a single point-in-time image.
    // Decoder-only. Returns {schema_version, max_tx, max_addr, has_more, rows:[...]}.
    //
    // Deliberately NOT paged: the keyset-cursor stability rationale
    // (borrowed from streamTableRowsById) only holds for INSERT-only tables, and
    // dispensers is exactly the table it does not hold for - the decoder
    // soft-expires rows in place (UPDATE expired_block_index), so a soft-expire
    // landing on an already-served page after that page shipped left the client's
    // assembled walk carrying a torn cross-instant image, which
    // applyDispensersReplace then wrote in as authoritative. The count backstop
    // (_verifyTableCounts) structurally cannot see it: an UPDATE leaves counts
    // equal on both sides. A single SELECT is statement-consistent in InnoDB, so
    // one response can never mix instants; dispensers is small next to the
    // multi-million-row lookups that forced paging onto the id-cursor rail. The
    // cursor params stay honoured (filtered within the same single query) and
    // has_more is always false, so an old paging client simply completes its walk
    // in one round trip.
    async streamDispensers(db, afterTx, afterAddr, limit, res){
        let dbType = (db && db.dbType) || 'indexer';
        if(dbType !== 'decoder'){
            return res.status(400).json({ error: 'dispensers reconcile is decoder-only' });
        }

        let rows;
        if(Number.isFinite(afterTx) && Number.isFinite(afterAddr)){
            rows = await db.doQuery(
                "SELECT * FROM `dispensers` WHERE (tx_index > ? OR (tx_index = ? AND address_id > ?)) " +
                "ORDER BY tx_index ASC, address_id ASC",
                [afterTx, afterTx, afterAddr]
            );
        } else {
            rows = await db.doQuery(
                "SELECT * FROM `dispensers` ORDER BY tx_index ASC, address_id ASC"
            );
        }
        let hasMore = false;
        let maxTx   = rows.length ? Number(rows[rows.length - 1].tx_index)   : (Number.isFinite(afterTx)   ? afterTx   : 0);
        let maxAddr = rows.length ? Number(rows[rows.length - 1].address_id) : (Number.isFinite(afterAddr) ? afterAddr : 0);
        let schemaVersion = SCHEMA_VERSION[dbType];

        res.setHeader('Content-Type', 'application/json');
        res.setHeader('Content-Encoding', 'gzip');
        res.setHeader('X-Snapshot-Schema-Version', schemaVersion);
        res.setHeader('X-Has-More', hasMore ? 'true' : 'false');

        let gzip = zlib.createGzip();
        gzip.pipe(res);
        // Backpressure + client-abort teardown (see streamTableRowsById).
        let writer = new SnapshotStreamWriter(gzip, res);
        try {
            await writer.write('{"schema_version":' + schemaVersion + ',"max_tx":' + maxTx +
                ',"max_addr":' + maxAddr + ',"has_more":' + (hasMore ? 'true' : 'false') + ',"rows":[');
            for(let i = 0; i < rows.length; i++){
                if(i > 0) await writer.write(',');
                await writer.write(JSON.stringify(encodeRow(rows[i]), bigIntReplacer));
            }
            await writer.write(']}');
            writer.finish();
        } catch(e){
            writer.dispose();
            if(e && e.aborted) return;
            throw e;
        }
    }

}

module.exports = SnapshotBuilder;
module.exports.SnapshotStreamWriter = SnapshotStreamWriter;
module.exports.OPERATOR_LOCAL_TABLES = OPERATOR_LOCAL_TABLES;
module.exports.SOURCE_UNSTREAMED_TABLES = SOURCE_UNSTREAMED_TABLES;
module.exports.orderSnapshotTables = orderSnapshotTables;
