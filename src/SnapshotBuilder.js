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
const { SCHEMA_VERSION } = require('./schema-version');
const { encodeRow, encodeTables } = require('./wireCodec');
const replicatedTables = require('./replicatedTables');
const { collectUpdatedRows } = require('./updatedRows');
const { collectMaturedCooldownCredits } = require('./cooldownCredits');
const { activationDelayBlocks } = require('./consensus-constants');

// JSON replacer that converts BigInt to string (mariadb driver returns BigInt for BIGINT columns)
const bigIntReplacer = (k, v) => typeof v === 'bigint' ? v.toString() : v;

// Tables holding operator-local bookkeeping state (fetch timestamps, retry counters) that
// legitimately diverges between nodes and must not appear in consensus snapshots.
const OPERATOR_LOCAL_TABLES = new Set([
    'icons', 'price_snapshots', 'pending_hub_pushes',
    // recovery_pending_rewards: restore-time scratch staging for archived validator rewards
    // (xchain-indexer F1a id-determinism fix). Recovery-local, drained into validator_rewards
    // by the reindex apply hook, and never consensus-hashed. It must not ride snapshots (a
    // follower has no recovery in progress, so its count legitimately differs).
    'recovery_pending_rewards',
    // Hub-mirrored tables: pushed/retracted by hub_db_sync out-of-band with block
    // apply, so they vary by WS arrival timing and must not appear in consensus snapshots.
    // These are NEVER replicated by xchain-sync (excluded from the per-block stream, the
    // incremental catch-up, AND these snapshots). A serving node does not converge them via
    // sync: the explorer serves the consensus-relevant ones (state_checkpoints,
    // capability_snapshots, cross_chain_matches) from the MANDATORY co-located hub DB on the
    // same server, with no local-mirror fallback (fails loud if the hub DB is absent; #4138).
    'oracle_prices', 'capability_snapshots', 'cross_chain_calls', 'cross_chain_matches',
    'state_checkpoints',
    // state_tree_roots: the follower recomputes its OWN row at every snapshot apply
    // (ClientApplier.seedSnapshotRoots, rebuilt from local balances/stakes; never
    // compared to the source). The table also carries computed_at (CURRENT_TIMESTAMP),
    // a per-node wall-clock value, so shipping the source's rows adds pure
    // nondeterminism to the snapshot and is never consumed by the follower.
    'state_tree_roots',
]);

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

        // Tables to put last (derived/computed; they depend on everything else).
        // pubkeys trails index_addresses: pubkeys.sql declares a FK on index_addresses(id),
        // so the reverse-delete path in applyFullSnapshot must drop pubkeys rows before
        // index_addresses rows.  Appended after sync_meta because neither balances nor
        // sync_meta carries a FK on pubkeys.
        this.trailingTables = ['balances', 'sync_meta', 'pubkeys'];

        this.pageSize = 10000;
    }

    // Discover all tables in the database and return them in dependency order.
    // Priority tables come first, trailing tables last, everything else alphabetically in between.
    async _getOrderedTables(db, conn){
        let rows = await db.doQuery(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_type = 'BASE TABLE'",
            [db.dbName],
            conn
        );
        let allTables = rows.map(r => r.table_name || r.TABLE_NAME).filter(t => !OPERATOR_LOCAL_TABLES.has(t));

        let prioritySet  = new Set(this.priorityTables);
        let trailingSet  = new Set(this.trailingTables);

        let ordered = [];
        for(let t of this.priorityTables){
            if(allTables.includes(t)) ordered.push(t);
        }
        let middle = allTables.filter(t => !prioritySet.has(t) && !trailingSet.has(t)).sort();
        ordered.push(...middle);
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
        let conn = await db.beginReadSnapshot();
        let snapshotOpen = true;
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

            gzip.write('{"schema_version":' + schemaVersion + ',"block_height":' + lastBlock + ',"tables":{');

            let tableOrder = await this._getOrderedTables(db, conn);
            let first = true;
            for(let table of tableOrder){
                try {
                    let count = await db.getTableCount(table, conn);
                    if(count === 0) continue;

                    if(!first) gzip.write(',');
                    first = false;
                    gzip.write('"' + table + '":[');

                    let offset = 0;
                    let firstRow = true;
                    while(offset < count){
                        let rows = await db.getTablePage(table, this.pageSize, offset, conn);
                        for(let row of rows){
                            if(!firstRow) gzip.write(',');
                            firstRow = false;
                            gzip.write(JSON.stringify(encodeRow(row), bigIntReplacer));
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
            await db.commitReadSnapshot(conn);
            snapshotOpen = false;
            gzip.end();
        } catch(e){
            if(snapshotOpen) await db.rollbackReadSnapshot(conn);
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

            gzip.write('{"schema_version":' + schemaVersion + ',"block_height":' + lastBlock + ',"since_block":' + sinceBlock + ',"tables":{');

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
            // from the full snapshot; it is now in the decoder /status completeness count
            // (replicatedTables `special`), so the soft-expire/hard-purge drift the
            // bootstrap dump cannot replay surfaces as a TABLE_COUNT_MISMATCH rather than
            // drifting silently. The apply-side reconcile has landed:
            // ClientApplier.applyDispensersReplace via ClientSync._reconcileDispensers,
            // gated by DISPENSERS_RECONCILE_EVERY / DISPENSERS_RECONCILE_MAX_INTERVAL_MS.
            // The count signal is now a backstop for detecting residual drift.
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
            //     freezes at bootstrap height. markets has NO ClientRollback drop;
            //     it converges post-reorg via the full-dump UPSERT (ON DUPLICATE KEY
            //     UPDATE) on the next snapshot.
            //   - attest_validator_stats: running per-validator aggregate counters
            //     with no action_index. Without a full-dump here, these counters
            //     freeze at bootstrap height. ClientRollback drops affected rows on
            //     reorg; the next incremental catch-up restores current values.
            let indexerFullDump    = new Set([
                ...replicatedTables.getTopology('indexer').index,
                'merkle_epochs',
                'markets',
                'attest_validator_stats',
            ]);
            // The append-only, id-PK lookup tables a skipLookups caller syncs out of
            // band via streamTableRowsById. Only these are omitted; the mutated
            // aggregates folded into indexerFullDump (merkle_epochs/markets/
            // attest_validator_stats) stay in the bundled response (small, no id cursor).
            let lookupSet          = new Set(replicatedTables.getTopology(dbType).index);
            let firstActionIndex   = (dbType === 'indexer') ? await db.getFirstActionIndex(sinceBlock, conn) : null;

            for(let table of tableOrder){
                try {
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
                        } else if(firstActionIndex !== null){
                            try {
                                rows = await db.doQuery("SELECT * FROM `" + table + "` WHERE action_index >= ? ORDER BY action_index", [firstActionIndex], conn);
                            } catch(e){
                                // Table doesn't have action_index; skip it for incremental
                                continue;
                            }
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
                            // Tables may not exist on older schemas; skip
                        }
                    }

                    if(!rows || rows.length === 0) continue;

                    if(!first) gzip.write(',');
                    first = false;
                    gzip.write('"' + table + '":[');

                    for(let i = 0; i < rows.length; i++){
                        if(i > 0) gzip.write(',');
                        gzip.write(JSON.stringify(encodeRow(rows[i]), bigIntReplacer));
                    }

                    gzip.write(']');
                } catch(e){
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
            // harmless UPSERT). tokens.escrow_action_index is omitted; the follower
            // re-derives it locally (ClientApplier._maybeRederiveEscrow).
            gzip.write('}');
            if(dbType === 'indexer'){
                let delay = activationDelayBlocks(coin);
                if(delay === undefined) delay = null;
                let updated = await collectUpdatedRows(db, sinceBlock, lastBlock, delay, conn);
                gzip.write(',"updated_rows":' + JSON.stringify(encodeTables(updated), bigIntReplacer));
            }
            gzip.write('}');
            await db.commitReadSnapshot(conn);
            snapshotOpen = false;
            gzip.end();
        } catch(e){
            if(snapshotOpen) await db.rollbackReadSnapshot(conn);
            throw e;
        }
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
        gzip.write('{"schema_version":' + schemaVersion + ',"table":"' + table +
            '","max_id":' + maxId + ',"has_more":' + (hasMore ? 'true' : 'false') + ',"rows":[');
        for(let i = 0; i < rows.length; i++){
            if(i > 0) gzip.write(',');
            gzip.write(JSON.stringify(encodeRow(rows[i]), bigIntReplacer));
        }
        gzip.write(']}');
        gzip.end();
    }

    // Stream one keyset-ordered page of the decoder `dispensers` table. dispensers
    // is excluded from both the incremental block stream and the id-cursor lookup
    // paging (streamTableRowsById): it has no monotonic surrogate id (PK is
    // (tx_index, address_id)) and the decoder soft-expires then hard-purges rows, so
    // an insert-only delta cannot replay the UPDATE/DELETE convergence. A truncated
    // replica therefore never seeds it and an incrementally-caught-up replica drifts.
    // This endpoint powers the client's periodic replace-table reconcile
    // (ClientSync._reconcileDispensers): it pages the FULL current table by the
    // composite (tx_index, address_id) keyset so the client can rebuild it verbatim.
    // Decoder-only. Returns {schema_version, max_tx, max_addr, has_more, rows:[...]}.
    async streamDispensers(db, afterTx, afterAddr, limit, res){
        let dbType = (db && db.dbType) || 'indexer';
        if(dbType !== 'decoder'){
            return res.status(400).json({ error: 'dispensers reconcile is decoder-only' });
        }
        let lim = Number.isFinite(limit) ? Math.floor(limit) : SnapshotBuilder.ROWS_PAGE_DEFAULT;
        lim = Math.max(1, Math.min(SnapshotBuilder.ROWS_PAGE_MAX, lim));

        // Keyset pagination on the composite PK. The first page (no cursor) has no
        // lower bound; later pages use the strict (tx_index, address_id) > (last)
        // tuple, which ORDER BY tx_index, address_id makes stable across requests.
        let rows;
        if(Number.isFinite(afterTx) && Number.isFinite(afterAddr)){
            rows = await db.doQuery(
                "SELECT * FROM `dispensers` WHERE (tx_index > ? OR (tx_index = ? AND address_id > ?)) " +
                "ORDER BY tx_index ASC, address_id ASC LIMIT ?",
                [afterTx, afterTx, afterAddr, lim]
            );
        } else {
            rows = await db.doQuery(
                "SELECT * FROM `dispensers` ORDER BY tx_index ASC, address_id ASC LIMIT ?",
                [lim]
            );
        }
        let hasMore = rows.length === lim;
        let maxTx   = rows.length ? Number(rows[rows.length - 1].tx_index)   : (Number.isFinite(afterTx)   ? afterTx   : 0);
        let maxAddr = rows.length ? Number(rows[rows.length - 1].address_id) : (Number.isFinite(afterAddr) ? afterAddr : 0);
        let schemaVersion = SCHEMA_VERSION[dbType];

        res.setHeader('Content-Type', 'application/json');
        res.setHeader('Content-Encoding', 'gzip');
        res.setHeader('X-Snapshot-Schema-Version', schemaVersion);
        res.setHeader('X-Has-More', hasMore ? 'true' : 'false');

        let gzip = zlib.createGzip();
        gzip.pipe(res);
        gzip.write('{"schema_version":' + schemaVersion + ',"max_tx":' + maxTx +
            ',"max_addr":' + maxAddr + ',"has_more":' + (hasMore ? 'true' : 'false') + ',"rows":[');
        for(let i = 0; i < rows.length; i++){
            if(i > 0) gzip.write(',');
            gzip.write(JSON.stringify(encodeRow(rows[i]), bigIntReplacer));
        }
        gzip.write(']}');
        gzip.end();
    }

}

module.exports = SnapshotBuilder;
