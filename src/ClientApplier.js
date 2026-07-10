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
const { rederiveEscrowGate } = require('./ClientRollback');
const { computeFollowerRoots, seedSnapshotRoots } = require('./stateCommitment');
const { isStateCommitmentActive, isStateCommitmentActivationBlock } = require('./state_commitment_activation');
const { OPERATOR_LOCAL_TABLES, orderSnapshotTables } = require('./SnapshotBuilder');

// Above this many distinct ids per dimension a scoped rebuild's IN-lists stop
// being worth it (and a catch-up that touched that much of the table is close
// to a full recompute anyway); fall back to the unscoped rebuild.
const MAX_SCOPED_REBUILD_IDS = 1000;

// Tables whose presence in a block/catch-up payload can change a token's
// ownership-escrow gate (a GIVE_OWNERSHIP offer opening or its status moving to a
// closed state). When any appears, re-derive tokens.escrow_action_index from the
// already-replicated offer/status tables (the forward-apply counterpart to the
// reorg re-derive in ClientRollback; the gate is never carried on the wire because
// it is fully replica-derivable). See _maybeRederiveEscrow.
const ESCROW_TRIGGER_TABLES = new Set([
    'orders', 'order_statuses', 'swaps', 'swap_statuses',
    'dispensers', 'dispenser_statuses', 'tokens'
]);

class ClientApplier {

    constructor(db, util, chain, network) {
        this.db   = db;
        this.util = util;
        // chain (COIN) + network are needed to key the light-client state_tree_roots
        // rows and the SMT balance/escrow keys (SPV spec sec.4). null on callers that
        // predate the feature; the state-commitment path is then simply skipped.
        this.chain   = chain || null;
        this.network = network || null;
        // Roots the most recent applyBlock computed over the replica, for ClientSync's
        // VERIFY_STATE_COMMITMENT comparison; null when the block predates the flag-day.
        this._lastComputedRoots = null;

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
            'sync_meta',
            // merkle_epochs is append-only (epoch UNIQUE); INSERT IGNORE makes its
            // full-dump re-send on an incremental catch-up idempotent (item 4622).
            'merkle_epochs',
            // validator_rewards has a UNIQUE key (source_id, signing_pubkey_id,
            // reward_type, round_reference). The recovery-redriven collector
            // (recoveryRewards.js) can re-inject a backdated survivor row via BOTH the
            // live per-block and incremental-snapshot channels when their windows overlap;
            // INSERT IGNORE makes that re-injection idempotent. Safe for the normal path
            // (each row streams once in its earn-block; mirrors createValidatorReward's
            // own INSERT IGNORE on the source).
            'validator_rewards'
        ]);

        // Mutable aggregates that the indexer full-dump re-sends with their CURRENT
        // value (markets = OHLCV; attest_validator_stats = running counters). On a
        // non-empty replica a plain INSERT collides on their UNIQUE key (ER_DUP_ENTRY,
        // which aborts the catch-up transaction) and INSERT IGNORE would keep the
        // STALE row, so they must UPSERT to overwrite with the source values (4622).
        this.upsertFullDumpTables = new Set([
            'markets',
            'attest_validator_stats'
        ]);
    }

    // Apply a single block payload from a WebSocket event
    async applyBlock(payload){
        // Clear any prior block's computed roots up front: on an early return
        // (malformed payload or an already-applied duplicate) ClientSync must NOT
        // compare stale roots against this event. A null here means "not recomputed
        // this apply" and the state-commitment check is skipped, never treated as a
        // divergence (a duplicate block was already verified when first applied).
        this._lastComputedRoots = null;
        // block_index is null-checked (not truthiness-checked): the genesis block is
        // a legitimate block_index 0 (ServerPoller emits Number(0)), and ClientSync
        // documents block 0 as the one valid from-empty apply target. `!0` would
        // silently drop it before any transaction opened.
        if(!payload || !payload.data || payload.block_index == null) return;

        // Schema-version gate: reject live block payloads with a mismatched schema
        // version the same way snapshot applies do, so a server-side schema bump
        // fails closed on live blocks rather than silently accepting rows whose
        // encoding or column set the local replica cannot correctly apply.
        // Gated on schema_version != null so old payloads (pre-5250) pass through.
        let dbType = (this.db && this.db.dbType) || 'indexer';
        if(payload.schema_version != null && payload.schema_version !== SCHEMA_VERSION[dbType]){
            throw new Error('Schema version mismatch: server=' + payload.schema_version +
                ' client=' + SCHEMA_VERSION[dbType] + '; restart the validator after upgrading the server');
        }

        let existing = await this.db.getBlockHashRow(payload.block_index);
        if(existing){
            console.log('Block ' + payload.block_index + ' already exists, skipping');
            return;
        }

        await this.db.beginTransaction();
        try {
            let data = payload.data;
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
            // it from the (now-updated) credit/debit data, scoped to the ids
            // the new rows touched where possible. Indexer-shaped DBs only;
            // decoder has no balances/credits/debits tables.
            let dbType = (this.db && this.db.dbType) || 'indexer';
            if(dbType === 'indexer'){
                // Apply in-place mutations to SURVIVING rows (deactivation_block,
                // SLASH amounts, request_status) the action-scoped insert loop above
                // can't reach (they live on rows created by an earlier block). Without
                // this UPSERT every forward in-place mutation is silently dropped.
                if(payload.updated_rows)
                    await this._applyUpdatedRows(payload.updated_rows);
                // Re-derive tokens.escrow_action_index when this block moved any
                // offer/status (the gate is replica-derived, not wire-carried).
                await this._maybeRederiveEscrow(data);
                if(data.credits || data.debits)
                    await this._rebuildBalancesTouchedBy(data.credits, data.debits);
                // Light-client state commitment (SPV spec sec.4-5): recompute + persist
                // the per-block SMT roots over the replica INSIDE this txn (atomic with
                // the data apply, so block B+1's incremental update always finds B's
                // balances_root). The touched (address,tick) set comes from the applied
                // event rows (credits/debits/escrows; cooldown refunds already merged by
                // the source), mirroring the indexer's ledger-choke-point set. ClientSync
                // reads _lastComputedRoots after commit and HALTs on divergence.
                if(isStateCommitmentActive(payload.block_index, this.network, this.chain)){
                    let isActivation = isStateCommitmentActivationBlock(payload.block_index, this.network, this.chain);
                    let touchedKeys  = isActivation ? [] : await this._collectSmtTouchedKeys(data);
                    this._lastComputedRoots = await computeFollowerRoots(
                        this.db, this.chain, this.network, payload.block_index, touchedKeys, isActivation);
                } else {
                    this._lastComputedRoots = null;
                }
            } else {
                this._lastComputedRoots = null;
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
    // null when the rows can't be scoped: a row missing either id (NULL ids
    // can't be matched by an IN-list), or more distinct ids than an IN-list
    // should carry (in which case the caller falls back to the full rebuild).
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

    // Distinct (address, tick) string pairs the applied block touched, for the
    // light-client SMT update (SPV spec sec.4). Mirrors the indexer's _smtTouched
    // set, which captures EXACT pairs at its ledger choke point. Collects pairs
    // (not the address x tick cross-product) from the applied credits/debits/escrows
    // rows (the source has already merged backdated cooldown-refund credits into
    // data.credits), skips native-coin rows with a NULL address_id/tick_id (matching
    // the indexer guard `address != null && tick != null && tick !== ''`), and
    // resolves the surrogate ids to canonical strings. NO cap: every touched pair
    // must be recomputed (unlike the balance-cache rebuild, which can fall back to a
    // full recompute). Runs inside the apply txn so freshly-inserted index rows resolve.
    async _collectSmtTouchedKeys(data){
        let pairs   = new Set();
        let addrIds = new Set();
        let tickIds = new Set();
        for(let arr of [data.credits, data.debits, data.escrows]){
            for(let row of (arr || [])){
                if(!row || row.address_id == null || row.tick_id == null) continue;
                pairs.add(row.address_id + '\t' + row.tick_id);
                addrIds.add(row.address_id);
                tickIds.add(row.tick_id);
            }
        }
        if(!pairs.size) return [];
        let aIn = Array.from(addrIds);
        let tIn = Array.from(tickIds);
        let addrRows = await this.db.doQuery(
            'SELECT id, address FROM index_addresses WHERE id IN (' + aIn.map(() => '?').join(',') + ')', aIn);
        let tickRows = await this.db.doQuery(
            'SELECT id, tick FROM index_tickers WHERE id IN (' + tIn.map(() => '?').join(',') + ')', tIn);
        let addrMap = new Map(); for(let r of addrRows) addrMap.set(String(r.id), r.address);
        let tickMap = new Map(); for(let r of tickRows) tickMap.set(String(r.id), r.tick);
        let out = [];
        for(let key of pairs){
            let parts   = key.split('\t');
            let address = addrMap.get(parts[0]);
            let tick    = tickMap.get(parts[1]);
            if(address == null || tick == null || tick === '') continue;
            out.push({ address: address, tick: tick });
        }
        return out;
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
            // Tables may not exist on a decoder replica. The dbType guard above
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
            throw new Error('Schema version mismatch: server=' + snapshotData.schema_version + ' client=' + expectedVersion + '; restart the validator after upgrading the server');
        }

        console.log('Applying full snapshot (block height: ' + snapshotData.block_height + ')...');
        let timer = this.util.startTimer();

        await this.db.beginTransaction();
        try {
            // Clear the FULL local snapshot-eligible table set, not just the tables
            // named in the payload: streamFullSnapshot omits any table with zero
            // source rows, so a table emptied on the source but still populated on
            // this replica would otherwise survive a re-bootstrap (the oversized-
            // incremental fallback applies a full snapshot over a NON-empty replica),
            // and _verifyTableCounts only reports remote>local, so the stale rows
            // would never surface. schema_version equality with the source was
            // enforced above, so the local table set mirrors the source's. The
            // enumeration is best-effort: payload-named tables are always cleared.
            let localTables = [];
            try {
                let schemaRows = await this.db.doQuery(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_type = 'BASE TABLE'",
                    [this.db.dbName]
                );
                localTables = (schemaRows || [])
                    .map(r => r.table_name || r.TABLE_NAME)
                    .filter(t => t && !OPERATOR_LOCAL_TABLES.has(t));
            } catch(e){
                console.error('Full-snapshot clear: local table enumeration failed; clearing payload tables only:', e.message);
            }

            // Node-local tables (OPERATOR_LOCAL_TABLES) never ride snapshots; if an
            // older source still ships one (e.g. mempool_transactions before it was
            // excluded), drop it here rather than clobbering node-local state with
            // the source's copy.
            let payloadTables = Object.keys(snapshotData.tables).filter(t => {
                if(!OPERATOR_LOCAL_TABLES.has(t)) return true;
                console.log('Ignoring node-local table shipped in full snapshot: ' + t);
                return false;
            });

            // Clear all snapshot tables first (reverse dependency order: child rows
            // before parents; orderSnapshotTables mirrors the builder's stream order).
            // DELETE rather than TRUNCATE: MariaDB rejects TRUNCATE on any table referenced
            // by a foreign key, even when the referencing table is empty. The decoder DB
            // declares such a FK (pubkeys.address_id → index_addresses.id), so TRUNCATE
            // would crash the bootstrap; DELETE honours FK constraints row-by-row.
            let tables = orderSnapshotTables([...new Set([...payloadTables, ...localTables])]);
            for(let i = tables.length - 1; i >= 0; i--){
                let tCheck = validation.validateIdentifier(tables[i]);
                if(!tCheck.valid){
                    console.error('Skipping clear of invalid table: ' + tables[i]);
                    continue;
                }
                await this.db.doQuery('DELETE FROM `' + tables[i] + '`');
            }

            for(let table of tables){
                let rows = snapshotData.tables[table];
                if(!rows || rows.length === 0) continue;
                await this._insertRows(table, rows);
                if(rows.length > 100)
                    console.log('  ' + table + ': ' + rows.length + ' rows');
            }

            // Seed the light-client SMT at the snapshot tip so the first live block
            // (block_height+1) finds a prior balances_root (SPV spec sec.4.3). Full
            // build over the replicated state; block_merkle_root is NULL (state-at-
            // height, not the tip block's content rows). Indexer + post-flag-day only.
            if(dbType === 'indexer' && isStateCommitmentActive(snapshotData.block_height, this.network, this.chain))
                await seedSnapshotRoots(this.db, this.chain, this.network, snapshotData.block_height);

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
            throw new Error('Schema version mismatch: server=' + snapshotData.schema_version + ' client=' + expectedVersion + '; restart the validator after upgrading the server');
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
            // balances table is a derived aggregate. Without recomputing it
            // here the replica's balances stay stale until the next live block
            // happens to touch credits/debits (mirrors applyBlock above).
            // Indexer-shaped DBs only; decoder has no balances table.
            if(dbType === 'indexer'){
                // Mirror applyBlock: in-place mutations to below-window surviving rows
                // ride a separate updated_rows map (the incremental's action_index
                // window can't reach them), and the escrow gate is re-derived locally.
                if(snapshotData.updated_rows)
                    await this._applyUpdatedRows(snapshotData.updated_rows);
                await this._maybeRederiveEscrow(snapshotData.tables);
                if(snapshotData.tables.credits || snapshotData.tables.debits)
                    await this._rebuildBalancesTouchedBy(snapshotData.tables.credits, snapshotData.tables.debits);
                // Re-seed the SMT at the new tip: the incremental window's blocks never
                // had per-block roots computed, so without this the next live block would
                // find no prior balances_root. Full build over the now-complete replica
                // (correct only on a non-truncated replica; truncated / incremental-
                // bootstrapped replicas must run VERIFY_STATE_COMMITMENT=false).
                if(isStateCommitmentActive(snapshotData.block_height, this.network, this.chain))
                    await seedSnapshotRoots(this.db, this.chain, this.network, snapshotData.block_height);
            }
            await this.db.commitTransaction();
            console.log('Incremental snapshot applied (' + this.util.getTimer(timer) + ')');
        } catch(e){
            await this.db.rollbackTransaction();
            console.error('Error applying incremental snapshot:', e);
            throw e;
        }
    }

    // Replace the decoder `dispensers` table wholesale from a freshly-fetched full
    // set. dispensers is excluded from the block stream and the id-cursor lookup
    // paging (no monotonic id; the decoder soft-expires then hard-purges rows), so
    // neither the incremental catch-up nor the truncated bootstrap can converge it.
    // The client re-fetches the full table (ClientSync._reconcileDispensers) and
    // swaps it in atomically: DELETE + INSERT inside one transaction, so a reader
    // outside the txn never observes an empty table and a mid-apply failure rolls
    // back to the prior contents. dispensers is not in ignoreTables, so the post-
    // DELETE INSERT is a plain INSERT (no PK collisions against the emptied table).
    // Decoder-only; a no-op (and a safety guard) on indexer-shaped DBs.
    async applyDispensersReplace(rows){
        if((this.db && this.db.dbType) !== 'decoder') return;
        if(!Array.isArray(rows)) return;
        await this.db.beginTransaction();
        try {
            await this.db.doQuery('DELETE FROM `dispensers`');
            if(rows.length) await this._insertRows('dispensers', rows);
            await this.db.commitTransaction();
        } catch(e){
            await this.db.rollbackTransaction();
            console.error('Error applying dispensers reconcile:', e);
            throw e;
        }
    }

    async _insertRows(table, rows){
        if(!rows || rows.length === 0) return;

        let tableCheck = validation.validateIdentifier(table);
        if(!tableCheck.valid){
            console.error('Rejected table name in _insertRows: ' + table + ' (' + tableCheck.reason + ')');
            return;
        }

        let useIgnore = this.ignoreTables.has(table);
        let useUpsert = this.upsertFullDumpTables.has(table);
        let columns   = Object.keys(rows[0]);

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
        // Mutable-aggregate full-dump tables overwrite their existing row so a
        // re-dump on a non-empty replica refreshes (not skips) stale values.
        let updateSuffix = useUpsert
            ? ' ON DUPLICATE KEY UPDATE ' + columns.map(c => '`' + c + '` = VALUES(`' + c + '`)').join(', ')
            : '';

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

            let query = insertPrefix + valueClauses.join(', ') + updateSuffix;
            await this.db.doQuery(query, args);

            // 5284: events rows >64KB silently truncate on a still-TEXT (pre-migration)
            // replica when INSERT IGNORE is used: the id collision guard skips the row
            // on re-send, so the truncated copy is never healed. Detect this by reading
            // SHOW WARNINGS immediately after (SHOW WARNINGS is session-scoped and is
            // valid on the same connection the INSERT just ran on; we are inside a
            // beginTransaction so this.db.transactionConnection is the live connection).
            // Throw (halt the apply transaction) on any 1265 truncation warning so
            // operators see the exact row rather than a silently corrupt events log.
            if(table === 'events'){
                let warnings = await this.db.doQuery('SHOW WARNINGS');
                for(let w of (warnings || [])){
                    let code = Number(w.Code || w.code || 0);
                    if(code === 1265){
                        throw new Error('events row truncated (errno 1265) during INSERT IGNORE: ' +
                            'replica column is still TEXT (64KB); run the MEDIUMTEXT migration. ' +
                            'Warning: ' + (w.Message || w.message || ''));
                    }
                }
            }
        }
    }

    // Apply the in-place "updated rows" channel: each entry is the CURRENT full
    // state of a surviving row the source mutated in place (deactivation_block,
    // SLASH amount, request_status). These rows already exist on the replica with
    // their stale values, so they must be UPSERTed (INSERT ... ON DUPLICATE KEY
    // UPDATE); a plain INSERT would collide on the row's UNIQUE action_index.
    // updated is a { table: [rows] } map; an old payload simply omits it.
    async _applyUpdatedRows(updated){
        if(!updated || typeof updated !== 'object') return;
        for(let table in updated){
            let rows = updated[table];
            if(!rows || rows.length === 0) continue;
            await this._upsertRows(table, rows);
        }
    }

    // INSERT ... ON DUPLICATE KEY UPDATE for a batch of full rows. Every column is
    // written on both insert and update, so an already-present surviving row has
    // its mutated columns overwritten to the source's current values while a
    // not-yet-present row (e.g. created and mutated within the same window) is
    // inserted. Identifier validation + binary decode mirror _insertRows.
    async _upsertRows(table, rows){
        if(!rows || rows.length === 0) return;

        let tableCheck = validation.validateIdentifier(table);
        if(!tableCheck.valid){
            console.error('Rejected table name in _upsertRows: ' + table + ' (' + tableCheck.reason + ')');
            return;
        }

        let columns = Object.keys(rows[0]);
        for(let col of columns){
            let colCheck = validation.validateIdentifier(col);
            if(!colCheck.valid){
                console.error('Rejected column name in _upsertRows: ' + col + ' (' + colCheck.reason + ')');
                return;
            }
        }
        let colList      = columns.map(c => '`' + c + '`').join(', ');
        let placeholders = columns.map(() => '?').join(', ');
        // VALUES(col) back-reference is the MariaDB idiom for "the value this row
        // would have inserted"; updating the key column to itself is a harmless no-op.
        let updateList   = columns.map(c => '`' + c + '` = VALUES(`' + c + '`)').join(', ');

        let insertPrefix = 'INSERT INTO `' + table + '` (' + colList + ') VALUES ';
        let updateSuffix = ' ON DUPLICATE KEY UPDATE ' + updateList;

        let batchSize = 100;
        for(let i = 0; i < rows.length; i += batchSize){
            let batch = rows.slice(i, i + batchSize);
            let valueClauses = [];
            let args = [];
            for(let row of batch){
                valueClauses.push('(' + placeholders + ')');
                for(let col of columns)
                    args.push(decodeValue(row[col] !== undefined ? row[col] : null));
            }
            let query = insertPrefix + valueClauses.join(', ') + updateSuffix;
            await this.db.doQuery(query, args);
        }
    }

    // Re-derive tokens.escrow_action_index from the already-replicated offer/status
    // tables when this payload moved any escrow-relevant row. The gate is fully
    // replica-derivable (it is never carried on the wire), so the forward-apply
    // path runs the SAME re-derive ClientRollback runs on reorg, keeping source
    // and replica byte-identical with no new payload field. `tables` is the payload's
    // table map (live block `data` or incremental `tables`).
    async _maybeRederiveEscrow(tables){
        if(!tables) return;
        let touched = false;
        for(let t in tables){
            if(ESCROW_TRIGGER_TABLES.has(t)){ touched = true; break; }
        }
        if(!touched) return;
        try {
            await rederiveEscrowGate(this.db);
        } catch(e){
            // Only a genuine schema gap (missing table/column on an older/thin replica)
            // is safe to skip. Any other error (deadlock, lock-wait timeout, connection
            // drop) must propagate so the surrounding apply transaction rolls back and
            // the block is retried: tokens.escrow_action_index is replica-derived and is
            // NOT covered by any hash / SMT / recompute check, so a swallowed error here
            // commits a stale or half-derived ownership-escrow gate with no divergence
            // signal. Mirrors _rebuildBalances' narrow catch.
            if(e.errno !== 1146 && e.errno !== 1054) throw e;
        }
    }
}

module.exports = ClientApplier;
