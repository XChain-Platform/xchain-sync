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
const { generatedColumns }   = require('./generatedColumns');
const { computeFollowerRoots, seedSnapshotRoots } = require('./stateCommitment');
const { isStateCommitmentActive, isStateCommitmentActivationBlock } = require('./state_commitment_activation');
const { coinTicker }      = require('./consensus-constants');
const { OPERATOR_LOCAL_TABLES, SOURCE_UNSTREAMED_TABLES, orderSnapshotTables } = require('./SnapshotBuilder');
const lifecycle           = require('./tableLifecycle');

// Above this many distinct ids per dimension a scoped rebuild's IN-lists stop
// being worth it (and a catch-up that touched that much of the table is close
// to a full recompute anyway); fall back to the unscoped rebuild.
const MAX_SCOPED_REBUILD_IDS = 1000;

// Tables whose presence in a block/catch-up payload can change a token's
// ownership-escrow gate (a GIVE_OWNERSHIP offer opening or its status moving to a
// closed state). When any appears, re-derive tokens.escrow_action_index from the
// already-replicated offer/status tables (the forward-apply counterpart to the
// reorg re-derive in ClientRollback). The gate ALSO rides the wire via the
// updated_rows tokens class (full-row carry, all-column upsert), so this derive
// is the corrective pass over a convergent carried value, not the gate's sole
// writer. Checked against the payload's data/tables map only; updated_rows keys
// do not trigger it. See _maybeRederiveEscrow.
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
        // Canonical TICKER form of `chain` for the per-chain '<TICKER>:<network>'
        // activation lookups and the state_tree_roots chain column, matching what the
        // SOURCE indexer writes (config['COIN']). Passing the full name made
        // isStateCommitmentActive resolve to "off" on every production chain, so the
        // follower silently never computed roots or ran the commitment check.
        this.coinTicker = coinTicker(chain) || null;
        this.network = network || null;
        // Roots the most recent applyBlock computed over the replica, for ClientSync's
        // VERIFY_STATE_COMMITMENT comparison; null when the block predates the flag-day.
        this._lastComputedRoots = null;

        // Index/dedup tables use INSERT IGNORE (rows may already exist).
        // pubkeys (decoder DB) is included: incremental snapshots and per-block
        // payloads can re-send the same address's pubkey row across multiple
        // blocks, and the PK on address_id would otherwise collide.
        this.ignoreTables = new Set([
            // The index_* lookup set is derived from the table-lifecycle registry
            // (replication: 'stream:index') rather than hand-listed: the index bucket
            // is re-sent every block by design and the lookups are NOT rolled back
            // (replicaRollback: 'lookup'), so a new stream:index registry entry missing
            // here would collide on its PK on the next referencing block and stall the
            // apply transaction. Deriving it means a one-line registry add is picked up
            // on both the stream side (ServerPoller/TOPOLOGY.indexer) and the apply side
            // automatically, with no second source of truth to drift.
            ...lifecycle.tablesWhere(t => t.replication === 'stream:index'),
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
            // full-dump re-send on an incremental catch-up idempotent.
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
        // STALE row, so they must UPSERT to overwrite with the source values.
        this.upsertFullDumpTables = new Set([
            'markets',
            'attest_validator_stats'
        ]);

        // Tables whose PRIMARY KEY is a purely LOCAL AUTO_INCREMENT surrogate that the
        // replica must NOT inherit from the source, mapped to the natural key that
        // actually identifies the row. Replicating such an id verbatim forces an
        // agreement the protocol explicitly does not require: BlockHasher hashes the
        // resolved canonical strings "rather than raw AUTO_INCREMENT lookup ids (which
        // diverge across nodes after a reorg); it is id-independent". Once a rollback
        // has deleted rows and let re-application renumber them, the replica's sequence
        // is permanently offset from the source's, and every later apply collides on
        // the same PK forever (ER_DUP_ENTRY 1062), aborting the whole transaction and
        // freezing the replica while it still reports halted:false. That was observed
        // on a production litecoin/mainnet replica as ~1,400 identical failures on
        // `Duplicate entry '27681' for key 'PRIMARY'`.
        //
        // Neither existing escape hatch fits `blocks`. INSERT IGNORE would SKIP the
        // block, leaving the replica silently short a consensus-relevant row with no
        // divergence signal; ON DUPLICATE KEY UPDATE would OVERWRITE whichever unrelated
        // block already occupies that id. The correct treatment is to drop the source's
        // id and let the replica assign its own, deleting any existing row for the same
        // natural key first so the re-send stays idempotent. A scoped DELETE (not a
        // UNIQUE constraint) is what this needs, because `blocks.block_index` carries a
        // plain INDEX only, so ON DUPLICATE KEY has nothing to fire on and a bare INSERT
        // of an already-present block would silently duplicate it. Same DELETE+INSERT
        // in-one-transaction shape as applyDispensersReplace below.
        //
        // Nothing joins `blocks.id`: the source's own createBlock INSERTs without it,
        // the other *_hash_id columns point into index_transactions, and the client
        // cursor is `SELECT MAX(block_index)` (db.js getLastBlock), never an id.
        this.localSurrogateIdTables = new Map([
            ['blocks', 'block_index']
        ]);

        // Same class as localSurrogateIdTables above, minus the DELETE: tables whose
        // `id` is a node-local AUTO_INCREMENT surrogate the replica must not inherit,
        // but which already carry a REAL unique natural key and already ride
        // upsertFullDumpTables, so the existing ON DUPLICATE KEY UPDATE identifies the
        // row and nothing has to be cleared first.
        //
        // attest_validator_stats gained `id ... AUTO_INCREMENT PRIMARY KEY` in indexer
        // migration 2026-08-19-attest-validator-stats-surrogate-id, which states the id
        // is node-local ("NOT consensus-visible ... Nothing reads or signs over the id")
        // and that the source reassigns ids wholesale on reorg
        // (Rollback._recomputeAttestationValidatorStats). The replica mints its own
        // instead: sync runs no migrations, so an aged replica takes the column through
        // db.addMissingColumns + _autoIncrementKeyAction, which lets the engine backfill
        // the sequence in LOCAL row order. The two id spaces then disagree, and because
        // the table is full-dumped with SELECT * and upserted over every carried column,
        // the applier would emit `id` = VALUES(`id`) against the replica's PRIMARY KEY:
        // on a source id another surviving replica row already holds that is ER_DUP_ENTRY
        // (1062), outside ClientSync._healSchemaIfStale's {1146, 1054} heal set, so the
        // apply transaction aborts and re-fails on every retry. Exactly the production
        // wedge `blocks.id` was stripped for.
        //
        // It cannot use localSurrogateIdTables: that map's value is a SINGLE natural-key
        // column driving DELETE ... WHERE <key> IN (...), and this table's natural key is
        // the COMPOSITE UNIQUE (validator_pubkey, provider_id). Deleting by
        // validator_pubkey alone would drop that validator's rows for every OTHER
        // provider, and since _insertRows runs per batch a later batch's DELETE could
        // remove rows an earlier one just inserted: a recoverable wedge traded for silent
        // data loss.
        //
        // `markets` is deliberately NOT here even though it shares the id-PK + upsert
        // shape. Its unique natural key uq_markets_pair arrives from indexer migration
        // 2026-07-15-markets-dedup-unique-pair, and secondary indexes are NOT propagated
        // to replicas (db.ensureReplicaSecondaryIndexes carries a hand-listed few), so an
        // aged replica may hold markets with NO unique key at all. Stripping the id there
        // would leave the re-dump with nothing to collide on and append duplicate rows
        // silently. attest_validator_stats has no such gap: validator_pubkey_provider has
        // been in its CREATE TABLE since the table was introduced and no migration adds
        // it, so every replica that has the table has the key.
        this.localSurrogateIdOnlyTables = new Set([
            'attest_validator_stats'
        ]);
    }

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

        // rethrow, not the fail-soft default: this guard runs before beginTransaction, where
        // doQuery turns a query error into [], so a transient fault would read as "block not
        // applied yet" and re-run _insertRows for a block already in the replica - credits,
        // debits and escrows take a plain INSERT (they are in neither ignoreTables nor
        // upsertFullDumpTables), and _rebuildBalancesTouchedBy would then run over the
        // duplicated rows. Let the error propagate: _applyBlockEvent's catch logs it and
        // leaves lastAppliedBlock unadvanced, so gap detection re-attempts the block.
        let existing = await this.db.getBlockHashRow(payload.block_index, null, { rethrow: true });
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
                // Mirror the anchor-reward winner collapse: the source DELETEd the
                // loser validator_rewards rows this block's reconcile-log rows pre-image
                // (rows from EARLIER blocks the action-scoped delete never reaches).
                if(data.anchor_reward_reconcile_log && data.anchor_reward_reconcile_log.length)
                    await this._mirrorAnchorRewardReconcile('d.block_index = ?', [payload.block_index]);
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
                if(isStateCommitmentActive(payload.block_index, this.network, this.coinTicker)){
                    let isActivation = isStateCommitmentActivationBlock(payload.block_index, this.network, this.coinTicker);
                    let touchedKeys  = isActivation ? [] : await this._collectSmtTouchedKeys(data);
                    this._lastComputedRoots = await computeFollowerRoots(
                        this.db, this.coinTicker, this.network, payload.block_index, touchedKeys, isActivation);
                } else {
                    this._lastComputedRoots = null;
                }
            } else {
                this._lastComputedRoots = null;
            }
            await this.db.commitTransaction();
        } catch(e){
            await this.db.rollbackTransaction();
            console.error('Error applying block %s:', payload.block_index, e);
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
            // enforced above, so the local table set mirrors the source's.
            // Enumeration failure must abort the apply: a missing table/column
            // (1146/1054) is the only schema-gap failure worth tolerating, and
            // information_schema has no such failure mode, so realistic failures
            // here are transient/operational (connection drop, lock-wait,
            // permissions). Those MUST propagate so the surrounding catch rolls
            // the transaction back and the bootstrap is retried; committing with
            // an un-enumerated table set leaves tables emptied on the source
            // still populated locally, invisible to _verifyTableCounts. Mirrors
            // the narrow catch at the escrow-gate rederive below.
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
                console.error('Full-snapshot clear: local table enumeration failed:', e.message);
                if(e.errno !== 1146 && e.errno !== 1054) throw e;
            }

            // Node-local tables (OPERATOR_LOCAL_TABLES and SOURCE_UNSTREAMED_TABLES)
            // never ride snapshots; if an older source still ships one (e.g.
            // mempool_transactions or merkle_reorgs before their exclusions), drop it
            // here rather than importing another node's local state. Note the two sets
            // differ on the clear loop above: SOURCE_UNSTREAMED_TABLES stays in
            // localTables so a previously-imported foreign copy is purged by this very
            // apply, while OPERATOR_LOCAL_TABLES is clear-protected.
            let payloadTables = Object.keys(snapshotData.tables).filter(t => {
                if(!OPERATOR_LOCAL_TABLES.has(t) && !SOURCE_UNSTREAMED_TABLES.has(t)) return true;
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

            // Scoped clear of state_tree_roots at/above the snapshot height. This table
            // is 'follower-derived' (OPERATOR_LOCAL_TABLES), so the clear loop above is
            // clear-protected and leaves it untouched, while its backing state_tree_nodes
            // store (replication 'snapshot') was wiped and re-imported wholesale. On a
            // full-snapshot apply over a NON-empty replica (the oversized-incremental
            // recovery fallback), pre-existing root rows at heights ABOVE the snapshot
            // height survive as future-dated, orphaned-fork roots the follower would serve
            // as authoritative SPV commitments until a live block upserts over each height.
            // Delete block_index >= snapshot height (seedSnapshotRoots re-inserts the row
            // at the height right below), mirroring the predicate ClientRollback already
            // applies to this same table (blockTables, DELETE WHERE block_index >= N).
            // Run unconditionally, NOT gated on dbType/isStateCommitmentActive: the orphan
            // roots must go even when state commitment is inactive on this node. Swallow
            // only schema gaps (1146 table missing on decoder / older schemas, 1054 missing
            // column); any other error must propagate so the outer catch rolls the txn back.
            try {
                await this.db.doQuery(
                    'DELETE FROM state_tree_roots WHERE chain = ? AND network = ? AND block_index >= ?',
                    [this.chain, this.network, snapshotData.block_height]);
            } catch(e){
                if(e.errno !== 1146 && e.errno !== 1054) throw e;
            }

            // Seed the light-client SMT at the snapshot tip so the first live block
            // (block_height+1) finds a prior balances_root (SPV spec sec.4.3). Full
            // build over the replicated state; block_merkle_root is NULL (state-at-
            // height, not the tip block's content rows). Indexer + post-flag-day only.
            if(dbType === 'indexer' && isStateCommitmentActive(snapshotData.block_height, this.network, this.coinTicker))
                await seedSnapshotRoots(this.db, this.coinTicker, this.network, snapshotData.block_height);

            await this.db.commitTransaction();
            console.log('Full snapshot applied (' + this.util.getTimer(timer) + ')');
        } catch(e){
            await this.db.rollbackTransaction();
            console.error('Error applying full snapshot:', e);
            throw e;
        }
    }

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
                // Mirror the anchor-reward winner collapses the catch-up window carried
                // (reconcile-log rows at/above since_block pre-image the rows the source
                // DELETEd); a replica that held the losers at since_block converges.
                if(snapshotData.tables.anchor_reward_reconcile_log && snapshotData.tables.anchor_reward_reconcile_log.length
                        && snapshotData.since_block != null)
                    await this._mirrorAnchorRewardReconcile('d.block_index >= ?', [snapshotData.since_block]);
                await this._maybeRederiveEscrow(snapshotData.tables);
                if(snapshotData.tables.credits || snapshotData.tables.debits)
                    await this._rebuildBalancesTouchedBy(snapshotData.tables.credits, snapshotData.tables.debits);
                // Re-seed the SMT at the new tip: the incremental window's blocks never
                // had per-block roots computed, so without this the next live block would
                // find no prior balances_root. Full build over the now-complete replica
                // (correct only on a non-truncated replica; truncated / incremental-
                // bootstrapped replicas must run VERIFY_STATE_COMMITMENT=false).
                if(isStateCommitmentActive(snapshotData.block_height, this.network, this.coinTicker))
                    await seedSnapshotRoots(this.db, this.coinTicker, this.network, snapshotData.block_height);
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
            // Fail closed, not open: a `return` here silently drops every row for this
            // table while the enclosing apply transaction still commits and the block's
            // duplicate guard prevents any retry, leaving the replica permanently short
            // those rows with no divergence signal. Throw so the apply transaction rolls
            // back and the block is retried or the client halts.
            throw new Error('Rejected table name in _insertRows: ' + table + ' (' + tableCheck.reason + ')');
        }

        let useIgnore = this.ignoreTables.has(table);
        let useUpsert = this.upsertFullDumpTables.has(table);
        let columns   = Object.keys(rows[0]);

        // Generated columns are the DATABASE's to compute. The source reads its rows
        // with SELECT *, so one rides the wire like any other column, and naming it in
        // the INSERT is errno 1906: harmless on a permissive server, a hard error under
        // STRICT_TRANS_TABLES, which every modern MariaDB defaults to. See
        // src/generatedColumns.js for why this is a frozen map and not a schema probe.
        let generated = generatedColumns(table);
        if(generated.size){
            columns = columns.filter(c => !generated.has(c));
            if(columns.length === 0)
                throw new Error('Refusing to insert into ' + table + ': every carried column is generated');
        }

        // Drop the source's local surrogate id and let the replica keep its own. No
        // DELETE: this class already upserts on a real unique natural key, so the
        // existing ON DUPLICATE KEY UPDATE identifies the row. Writing the id here is
        // what would rewrite the replica's PRIMARY KEY onto a number another surviving
        // row holds (ER_DUP_ENTRY 1062). See localSurrogateIdOnlyTables.
        if(this.localSurrogateIdOnlyTables.has(table) && columns.includes('id')){
            columns = columns.filter(c => c !== 'id');
            if(columns.length === 0)
                throw new Error('Refusing to insert into ' + table + ': the row carries only the stripped surrogate id');
        }

        // Drop the source's local surrogate id and clear any row already holding the
        // same natural key, so a re-sent row replaces rather than collides. See
        // localSurrogateIdTables for why this table cannot use IGNORE or UPSERT.
        let naturalKey = this.localSurrogateIdTables.get(table);
        if(naturalKey && columns.includes('id')){
            let keyCheck = validation.validateIdentifier(naturalKey);
            if(!keyCheck.valid)
                throw new Error('Rejected natural key in _insertRows: ' + naturalKey + ' (' + keyCheck.reason + ')');

            columns = columns.filter(c => c !== 'id');
            if(columns.length === 0)
                throw new Error('Refusing to insert into ' + table + ': the row carries only the stripped surrogate id');

            // Fail closed on a row that cannot be identified: inserting it would append a
            // duplicate the DELETE could never scope to (block_index is not UNIQUE).
            let keyValues = [];
            for(let row of rows){
                let v = row[naturalKey];
                if(v === undefined || v === null)
                    throw new Error('Row for ' + table + ' is missing its natural key ' + naturalKey);
                if(!keyValues.includes(v)) keyValues.push(v);
            }

            // Chunked to keep the IN list bounded on a large catch-up window.
            let deleteBatch = 500;
            for(let i = 0; i < keyValues.length; i += deleteBatch){
                let slice = keyValues.slice(i, i + deleteBatch);
                await this.db.doQuery(
                    'DELETE FROM `' + table + '` WHERE `' + naturalKey + '` IN (' +
                        slice.map(() => '?').join(', ') + ')',
                    slice);
            }
        }

        for(let col of columns){
            let colCheck = validation.validateIdentifier(col);
            if(!colCheck.valid){
                // Fail closed, not open: a `return` here drops the entire table's rows
                // while the apply transaction still commits (see the table check above).
                throw new Error('Rejected column name in _insertRows: ' + col + ' (' + colCheck.reason + ')');
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

    // Mirror the source's anchor-reward winner collapse (xchain-indexer db.js
    // reconcileAnchorRewardWinner): the source pre-images every loser validator_rewards
    // row into anchor_reward_reconcile_log, then DELETEs it. The log rows replicate
    // (stream:block / mirror) but the DELETE never did, so a replica that held a loser
    // (bootstrap snapshot, or the pre-flag-day ANCHOR write) kept it forever, strictly
    // AHEAD of the source and invisible to the source-ahead-only count check. The log
    // row carries the loser's full UNIQUE identity (source_id, signing_pubkey_id,
    // reward_type, round_reference), so this is a keyed delete with no winner predicate
    // to reproduce; rows the source still holds (winners) never match a pre-image.
    // Runs AFTER the insert loop (the log rows of this apply are in place) and INSIDE
    // the apply transaction. The reverse twin is ClientRollback's RB-ANCHOR restore,
    // which re-INSERTs these pre-images when the reconcile block is orphaned. `scopeSql`
    // / `scopeArgs` bound the log rows to the window this apply carried
    // (d.block_index = B live; d.block_index >= since on an incremental catch-up).
    async _mirrorAnchorRewardReconcile(scopeSql, scopeArgs){
        try {
            await this.db.doQuery(
                "DELETE vr FROM validator_rewards vr " +
                "JOIN anchor_reward_reconcile_log d " +
                "  ON d.source_id = vr.source_id AND d.signing_pubkey_id = vr.signing_pubkey_id " +
                " AND d.reward_type = vr.reward_type AND d.round_reference <=> vr.round_reference " +
                "WHERE " + scopeSql,
                scopeArgs);
        } catch(e){
            // Schema-gap errors (log table / columns absent on an older replica) are safe
            // to skip: such a replica received no log rows either. Anything else must
            // abort the apply so the block is retried, never applied half-mirrored.
            if(e && e.errno !== 1146 && e.errno !== 1054) throw e;
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
            // Fail closed, not open: a `return` here silently drops every row for this
            // table while the apply transaction commits, permanently diverging the replica
            // with no signal. Throw so the transaction rolls back and the block is retried.
            throw new Error('Rejected table name in _upsertRows: ' + table + ' (' + tableCheck.reason + ')');
        }

        let columns = Object.keys(rows[0]);
        // Same errno-1906 rule as _insertRows: a generated column may ride the wire
        // (the source reads with SELECT *) and must not be named in the write.
        let generated = generatedColumns(table);
        if(generated.size){
            columns = columns.filter(c => !generated.has(c));
            if(columns.length === 0)
                throw new Error('Refusing to upsert into ' + table + ': every carried column is generated');
        }
        for(let col of columns){
            let colCheck = validation.validateIdentifier(col);
            if(!colCheck.valid){
                // Fail closed, not open: a `return` here drops the entire table's rows.
                throw new Error('Rejected column name in _upsertRows: ' + col + ' (' + colCheck.reason + ')');
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
    // replica-derivable, and it ALSO arrives on the wire via the updated_rows
    // tokens full-row carry (the source's own authoritative value, so the carry
    // converges rather than forks); this forward-apply pass runs the SAME
    // re-derive ClientRollback runs on reorg, keeping source and replica
    // byte-identical. `tables` is the payload's table map (live block `data` or
    // incremental `tables`); updated_rows does not trigger it.
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
