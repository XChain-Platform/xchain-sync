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

const validation          = require('../util/validation');
const balanceHelpers      = require('../db/balance_helpers');
const { SCHEMA_VERSION }  = require('../schema/version');
const { decodeValue }     = require('../util/wire_codec');
const { rederiveEscrowGate } = require('./rollback');
const { generatedColumns }   = require('../schema/generated_columns');
const { computeFollowerRoots, seedSnapshotRoots } = require('../state_commitment');
const { isStateCommitmentActive, isStateCommitmentActivationBlock } = require('../consensus/gates/state_commitment_gate');
const { coinTicker }      = require('../consensus-constants');
const { OPERATOR_LOCAL_TABLES, SOURCE_UNSTREAMED_TABLES, orderSnapshotTables } = require('../server/snapshot_builder');
const lifecycle           = require('../table_lifecycle');
const util = require('node:util');
const { getLogger } = require('../observability');
const logger = getLogger();

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
// do not trigger it. See maybeRederiveEscrow.
const ESCROW_TRIGGER_TABLES = new Set([
    'orders', 'order_statuses', 'swaps', 'swap_statuses',
    'dispensers', 'dispenser_statuses', 'tokens'
]);

function initializeCoreState(applier, db, util, chain, network) {
    applier.db = db;
    applier.util = util;
    // Keep chain and network nullable so callers without commitment context
    // leave the state-commitment path disabled.
    applier.chain = chain || null;
    // Normalize the chain to the ticker form shared by activation lookups and
    // state_tree_roots rows, matching the source indexer's configured coin.
    applier.coinTicker = coinTicker(chain) || null;
    applier.network = network || null;
    // Reset computed roots until applyBlock produces a commitment-enabled result.
    applier._lastComputedRoots = null;
}

function initializeIgnoreTables(applier) {
    // Index and dedup tables use INSERT IGNORE because rows may already exist.
    applier.ignoreTables = new Set([
        // The lifecycle registry supplies every lookup that streams in each block.
        ...lifecycle.tablesWhere(t => t.replication === 'stream:index'),
        // Pubkeys can recur in incremental snapshots and per-block payloads.
        'pubkeys',
        // Events are a full-dumped append-only log, so repeat IDs are idempotent.
        'events',
        // Sync metadata streams live and through snapshots with a unique block index.
        'sync_meta',
        // Merkle epochs are full-dumped and carry a unique epoch.
        'merkle_epochs',
        // Reward windows can overlap the live and incremental snapshot channels.
        'validator_rewards',
        // Rollcall rows can overlap the bootstrap dump and close-block stream.
        'rollcalls',
        'rollcall_absences',
        'rollcall_gates'
    ]);
}

function initializeIdKeyedIgnoreTables(applier) {
    // Limit strict collision checks to tables deduplicated by numeric primary ID,
    // excluding tables with surrogate IDs and separate natural-key uniqueness where
    // a warning on the natural key represents expected re-delivery.
    applier.idKeyedIgnoreTables = new Set([
        // Keep streamed lookups visible to repair when a natural value holds the wrong ID.
        ...lifecycle.tablesWhere(t => t.replication === 'stream:index'),
        'pubkeys',
        'rollcalls',
        'rollcall_absences',
        'rollcall_gates'
    ]);
}

function initializeRepairNaturalKeyColumns(applier) {
    // Map lookup tables to natural columns so from-zero repair reconciles a carried
    // ID with its value instead of letting INSERT IGNORE preserve the same natural
    // value under a different local ID.
    applier.repairNaturalKeyColumns = new Map([
        ['index_statuses', ['status']]
    ]);
}

function initializeUpsertFullDumpTables(applier) {
    // Upsert mutable full-dump aggregates through their unique natural keys so
    // current source values replace stale rows after re-delivery.
    applier.upsertFullDumpTables = new Set([
        'markets',
        'attest_validator_stats'
    ]);
}

function initializeLocalSurrogateIdTables(applier) {
    // Map source rows whose numeric IDs are local-only to natural keys, dropping
    // each carried ID and replacing the matching natural row in one transaction
    // to avoid either a primary-key collision or a duplicate block.
    applier.localSurrogateIdTables = new Map([
        // Keep blocks keyed by block_index because no relation targets blocks.id,
        // letting each replica allocate its own ID after rollback or re-application.
        ['blocks', 'block_index']
    ]);
}

function initializeLocalSurrogateIdOnlyTables(applier) {
    // Mark upserted tables whose local IDs must be stripped before insertion,
    // relying on an existing composite unique key instead of the single-column
    // delete path above.
    applier.localSurrogateIdOnlyTables = new Set([
        // Keep attest validator stats keyed by validator and provider while every
        // replica assigns its own surrogate sequence; source reorg recomputation
        // may allocate the same numeric ID to a different surviving row.
        'attest_validator_stats'
    ]);
}

class ClientApplier {

    constructor(db, util, chain, network) {
        initializeCoreState(this, db, util, chain, network);
        initializeIgnoreTables(this);
        initializeIdKeyedIgnoreTables(this);
        initializeRepairNaturalKeyColumns(this);
        initializeUpsertFullDumpTables(this);
        initializeLocalSurrogateIdTables(this);
        initializeLocalSurrogateIdOnlyTables(this);
    }

    /**
     * Apply a single block payload from a WebSocket event.
     *
     * Runs the whole block inside one transaction: a duplicate or malformed payload
     * returns before anything is written, and any failure rolls the block back so
     * ClientSync retries it rather than committing part of it.
     *
     * @param {object} payload the server's block event: block_index, data (a
     *                         { table: [rows] } map) and any updated_rows
     */
    async applyBlock(payload){
        // Clear computed roots before every attempt so early returns cannot expose
        // roots from an earlier block to ClientSync.
        this._lastComputedRoots = null;
        // A null check keeps the genesis block_index 0 valid.
        if(!payload || !payload.data || payload.block_index == null) return;

        this.assertBlockSchemaVersion(payload);
        let existing = await this.db.getBlockHashRow(payload.block_index, null, { rethrow: true });
        if(existing){
            logger.info('Block ' + payload.block_index + ' already exists, skipping');
            return;
        }

        await this.applyBlockTransaction(payload);
    }

    assertBlockSchemaVersion(payload){
        let dbType = (this.db && this.db.dbType) || 'indexer';
        if(payload.schema_version != null && payload.schema_version !== SCHEMA_VERSION[dbType]){
            throw new Error('Schema version mismatch: server=' + payload.schema_version +
                ' client=' + SCHEMA_VERSION[dbType] + '; restart the validator after upgrading the server');
        }
    }

    async applyBlockTransaction(payload){
        await this.db.beginTransaction();
        try {
            let data = payload.data;
            await this.insertBlockRows(data);
            let dbType = (this.db && this.db.dbType) || 'indexer';
            if(dbType === 'indexer'){
                await this.applyIndexerBlockState(payload, data);
            } else {
                this._lastComputedRoots = null;
            }
            await this.db.commitTransaction();
        } catch(e){
            await this.db.rollbackTransaction();
            logger.error(util.format('Error applying block %s:', payload.block_index, e));
            throw e;
        }
    }

    async insertBlockRows(data){
        for(let table in data){
            let rows = data[table];
            if(!rows || rows.length === 0) continue;
            await this.insertRows(table, rows);
        }
    }

    async applyIndexerBlockState(payload, data){
        if(payload.updated_rows)
            await this.applyUpdatedRows(payload.updated_rows);
        if(data.anchor_reward_reconcile_log && data.anchor_reward_reconcile_log.length)
            await this.mirrorAnchorRewardReconcile('d.block_index = ?', [payload.block_index]);
        await this.maybeRederiveEscrow(data);
        if(data.credits || data.debits)
            await this.rebuildBalancesTouchedBy(data.credits, data.debits);
        await this.updateBlockStateCommitment(payload, data);
    }

    async updateBlockStateCommitment(payload, data){
        if(!isStateCommitmentActive(payload.block_index, this.network, this.coinTicker)){
            this._lastComputedRoots = null;
            return;
        }
        let isActivation = isStateCommitmentActivationBlock(payload.block_index, this.network, this.coinTicker);
        let touchedKeys  = isActivation ? [] : await this.collectSmtTouchedKeys(data);
        this._lastComputedRoots = await computeFollowerRoots(
            this.db, this.coinTicker, this.network, payload.block_index, touchedKeys, isActivation);
    }

    // Collect the distinct (keyField, tick_id) ids touched by freshly applied
    // rows so the derived-aggregate rebuild can be limited to them. Returns
    // null when the rows can't be scoped: a row missing either id (NULL ids
    // can't be matched by an IN-list), or more distinct ids than an IN-list
    // should carry (in which case the caller falls back to the full rebuild).
    collectRebuildScope(rowArrays, keyField){
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
    async rebuildBalancesTouchedBy(credits, debits){
        let scope = this.collectRebuildScope([credits, debits], 'address_id');
        if(scope && !scope.keys.length) return;
        await this.rebuildBalances(scope);
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
    async collectSmtTouchedKeys(data){
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
        let addrRows = await this.db.findIndexAddressTextByIds(aIn);
        let tickRows = await this.db.findIndexTickTextByIds(tIn);
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
    // scope (optional): { keys, ticks } from collectRebuildScope; null/absent
    // recomputes the whole table.
    async rebuildBalances(scope){
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

    // Names of the local snapshot-eligible tables; operator-local tables are excluded.
    localSnapshotTableNames(schemaRows){
        return (schemaRows || [])
            .map(r => r.table_name || r.TABLE_NAME)
            .filter(t => t && !OPERATOR_LOCAL_TABLES.has(t));
    }

    // Payload tables minus node-local ones; a source still shipping one is ignored.
    payloadSnapshotTableNames(snapshotData){
        return Object.keys(snapshotData.tables).filter(t => {
            if(!OPERATOR_LOCAL_TABLES.has(t) && !SOURCE_UNSTREAMED_TABLES.has(t)) return true;
            logger.info('Ignoring node-local table shipped in full snapshot: ' + t);
            return false;
        });
    }

    // Tolerates only a missing table or column (1146/1054); any other error propagates.
    ignoreSchemaGap(e){
        if(e.errno !== 1146 && e.errno !== 1054) throw e;
    }

    isClearableTable(table){
        let tCheck = validation.validateIdentifier(table);
        if(!tCheck.valid) logger.error('Skipping clear of invalid table: ' + table);
        return tCheck.valid;
    }

    logLargeTableInsert(table, rows){
        if(rows.length > 100)
            logger.info('  ' + table + ': ' + rows.length + ' rows');
    }

    // Seeds the light-client SMT at the snapshot tip on indexer replicas past the flag day.
    shouldSeedSnapshotRoots(snapshotData, dbType){
        return dbType === 'indexer' && isStateCommitmentActive(snapshotData.block_height, this.network, this.coinTicker);
    }

    // Apply a full snapshot after confirming that its schema matches this replica.
    async applyFullSnapshot(snapshotData){
        if(!snapshotData || !snapshotData.tables) return;

        let dbType = (this.db && this.db.dbType) || 'indexer';
        let expectedVersion = SCHEMA_VERSION[dbType];
        if(snapshotData.schema_version !== expectedVersion){
            throw new Error('Schema version mismatch: server=' + snapshotData.schema_version + ' client=' + expectedVersion + '; restart the validator after upgrading the server');
        }

        logger.info('Applying full snapshot (block height: ' + snapshotData.block_height + ')...');
        let timer = this.util.startTimer();

        await this.db.beginTransaction();
        try {
            // The full local table set is cleared, not just the payload's: tables empty on the
            // source are omitted from the payload. Enumeration failures abort the apply.
            let localTables = [];
            try {
                localTables = this.localSnapshotTableNames(await this.db.findStreamableTableNames());
            } catch(e){
                logger.error(util.format('Full-snapshot clear: local table enumeration failed:', e.message));
                this.ignoreSchemaGap(e);
            }

            let tables = orderSnapshotTables([...new Set([...this.payloadSnapshotTableNames(snapshotData), ...localTables])]);
            // Reverse dependency order, using DELETE since TRUNCATE fails on FK-referenced tables.
            for(let i = tables.length - 1; i >= 0; i--){
                if(!this.isClearableTable(tables[i])) continue;
                await this.db.deleteAllRows(tables[i]);
            }

            for(let table of tables){
                let rows = snapshotData.tables[table];
                if(!rows || rows.length === 0) continue;
                await this.insertRows(table, rows);
                this.logLargeTableInsert(table, rows);
            }

            // Drops orphaned state_tree_roots at or above the snapshot height, keyed by ticker.
            try {
                await this.db.deleteStateTreeRootsFromBlock(this.coinTicker, this.network, snapshotData.block_height);
            } catch(e){
                this.ignoreSchemaGap(e);
            }

            if(this.shouldSeedSnapshotRoots(snapshotData, dbType))
                await seedSnapshotRoots(this.db, this.coinTicker, this.network, snapshotData.block_height);

            await this.db.commitTransaction();
            logger.info('Full snapshot applied (' + this.util.getTimer(timer) + ')');
        } catch(e){
            await this.db.rollbackTransaction();
            logger.error(util.format('Error applying full snapshot:', e));
            throw e;
        }
    }

    /**
     * Apply an incremental snapshot.
     *
     * opts.strictIgnoreCheck: see the SHOW WARNINGS block in insertRows.
     * Set only by ClientSync's from-zero lookup repair; every other caller (ordinary
     * live/catch-up apply) omits it and keeps the cheap, silent INSERT IGNORE path.
     *
     * @param {object} snapshotData the server's catch-up payload since a block
     * @param {object} [opts]
     */
    async applyIncrementalSnapshot(snapshotData, opts){
        if(!snapshotData || !snapshotData.tables) return;

        let dbType = (this.db && this.db.dbType) || 'indexer';
        let expectedVersion = SCHEMA_VERSION[dbType];
        if(snapshotData.schema_version !== expectedVersion){
            throw new Error('Schema version mismatch: server=' + snapshotData.schema_version + ' client=' + expectedVersion + '; restart the validator after upgrading the server');
        }

        logger.info('Applying incremental snapshot (since block ' + snapshotData.since_block + ')...');
        let timer = this.util.startTimer();

        await this.db.beginTransaction();
        try {
            await this.insertSnapshotTables(snapshotData.tables, opts);
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
                    await this.applyUpdatedRows(snapshotData.updated_rows);
                // Mirror the anchor-reward winner collapses the catch-up window carried
                // (reconcile-log rows at/above since_block pre-image the rows the source
                // DELETEd); a replica that held the losers at since_block converges.
                if(snapshotData.tables.anchor_reward_reconcile_log && snapshotData.tables.anchor_reward_reconcile_log.length
                        && snapshotData.since_block != null)
                    await this.mirrorAnchorRewardReconcile('d.block_index >= ?', [snapshotData.since_block]);
                await this.maybeRederiveEscrow(snapshotData.tables);
                if(snapshotData.tables.credits || snapshotData.tables.debits)
                    await this.rebuildBalancesTouchedBy(snapshotData.tables.credits, snapshotData.tables.debits);
                // Re-seed the SMT at the new tip: the incremental window's blocks never
                // had per-block roots computed, so without this the next live block would
                // find no prior balances_root. Full build over the now-complete replica
                // (correct only on a non-truncated replica; truncated / incremental-
                // bootstrapped replicas must run VERIFY_STATE_COMMITMENT=false).
                if(isStateCommitmentActive(snapshotData.block_height, this.network, this.coinTicker))
                    await seedSnapshotRoots(this.db, this.coinTicker, this.network, snapshotData.block_height);
            }
            await this.db.commitTransaction();
            logger.info('Incremental snapshot applied (' + this.util.getTimer(timer) + ')');
        } catch(e){
            await this.db.rollbackTransaction();
            logger.error(util.format('Error applying incremental snapshot:', e));
            throw e;
        }
    }

    // One incremental snapshot's tables, inserted in payload order inside the
    // caller's transaction. The strict option is only set by the from-zero lookup
    // repair: reconcile the carried id/status pairs before INSERT IGNORE so both a
    // natural-key collision and a wrong row hidden by a PRIMARY collision are
    // corrected.
    async insertSnapshotTables(tables, opts){
        for(let table in tables){
            let rows = tables[table];
            if(!rows || rows.length === 0) continue;
            let repairKeyColumns = this.repairNaturalKeyColumns.get(table);
            if(opts && opts.strictIgnoreCheck && repairKeyColumns)
                await this.reconcileLookupRows(table, rows, repairKeyColumns);
            await this.insertRows(table, rows, opts);
        }
    }

    // Replace the decoder `dispensers` table wholesale from a freshly-fetched full
    // set. dispensers is excluded from the block stream and the id-cursor lookup
    // paging (no monotonic id; the decoder soft-expires then hard-purges rows), so
    // neither the incremental catch-up nor the truncated bootstrap can converge it.
    // The client re-fetches the full table (ClientSync.reconcileDispensers) and
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
            await this.db.deleteAllDispensers();
            if(rows.length) await this.insertRows('dispensers', rows);
            await this.db.commitTransaction();
        } catch(e){
            await this.db.rollbackTransaction();
            logger.error(util.format('Error applying dispensers reconcile:', e));
            throw e;
        }
    }

    validateInsertTable(table){
        // The table name is spliced into every statement below rather than bound as a
        // parameter, so refuse anything that is not a plain identifier before it can
        // reach the database.
        let tableCheck = validation.validateIdentifier(table);
        if(!tableCheck.valid){
            // Fail closed, not open: a `return` here silently drops every row for this
            // table while the enclosing apply transaction still commits and the block's
            // duplicate guard prevents any retry, leaving the replica permanently short
            // those rows with no divergence signal. Throw so the apply transaction rolls
            // back and the block is retried or the client halts.
            throw new Error('Rejected table name in insertRows: ' + table + ' (' + tableCheck.reason + ')');
        }
    }

    prepareInsertColumns(table, rows){
        let columns = Object.keys(rows[0]);
        // Generated columns are the DATABASE's to compute. The source reads its rows
        // with SELECT *, so one rides the wire like any other column, and naming it in
        // the INSERT is errno 1906: harmless on a permissive server, a hard error under
        // STRICT_TRANS_TABLES, which every modern MariaDB defaults to. See
        // src/schema/generated_columns.js for why this is a frozen map and not a schema probe.
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
        return columns;
    }

    prepareNaturalKeyRows(table, rows, columns){
        // Drop the source's local surrogate id and clear any row already holding the
        // same natural key, so a re-sent row replaces rather than collides. See
        // localSurrogateIdTables for why this table cannot use IGNORE or UPSERT.
        let naturalKey = this.localSurrogateIdTables.get(table);
        if(!naturalKey || !columns.includes('id')) return { columns, naturalKey: null, keyValues: [] };

        let keyCheck = validation.validateIdentifier(naturalKey);
        if(!keyCheck.valid)
            throw new Error('Rejected natural key in insertRows: ' + naturalKey + ' (' + keyCheck.reason + ')');

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
        return { columns, naturalKey, keyValues };
    }

    validateInsertColumns(columns){
        // Column names are spliced in the same way, so each one gets the same check.
        for(let col of columns){
            let colCheck = validation.validateIdentifier(col);
            if(!colCheck.valid){
                // Fail closed, not open: a `return` here drops the entire table's rows
                // while the apply transaction still commits (see the table check above).
                throw new Error('Rejected column name in insertRows: ' + col + ' (' + colCheck.reason + ')');
            }
        }
    }

    prepareTableRows(table, rows){
        this.validateInsertTable(table);
        let useIgnore = this.ignoreTables.has(table);
        let useUpsert = this.upsertFullDumpTables.has(table);
        let prepared = this.prepareNaturalKeyRows(table, rows, this.prepareInsertColumns(table, rows));
        return { ...prepared, useIgnore, useUpsert };
    }

    prepareInsertBatch(rows, columns, start, batchSize){
        let batch = rows.slice(start, start + batchSize);
        let args = [];
        for(let row of batch){
            for(let col of columns){
                // decodeValue restores base64 binary sentinels back to Buffers
                // before insert (the inverse of SnapshotBuilder/BlockBroadcaster
                // encoding); non-binary values pass through unchanged.
                args.push(decodeValue(row[col] !== undefined ? row[col] : null));
            }
        }
        return { batch, args };
    }

    insertRowBatch(table, columns, batch, args, useIgnore, useUpsert){
        return this.db.insertRowValues(table, columns, batch.length, args, useIgnore, useUpsert);
    }

    assertNoEventTruncation(warnings){
        // events rows >64KB silently truncate on a still-TEXT (pre-migration)
        // replica when INSERT IGNORE is used: the id collision guard skips the row
        // on re-send, so the truncated copy is never healed. Detect this by reading
        // SHOW WARNINGS immediately after (SHOW WARNINGS is session-scoped and is
        // valid on the same connection the INSERT just ran on; we are inside a
        // beginTransaction so this.db.transactionConnection is the live connection).
        // Throw (halt the apply transaction) on any 1265 truncation warning so
        // operators see the exact row rather than a silently corrupt events log.
        for(let w of (warnings || [])){
            let code = Number(w.Code || w.code || 0);
            if(code === 1265){
                throw new Error('events row truncated (errno 1265) during INSERT IGNORE: ' +
                    'replica column is still TEXT (64KB); run the MEDIUMTEXT migration. ' +
                    'Warning: ' + (w.Message || w.message || ''));
            }
        }
    }

    shouldCheckStrictIgnoreWarnings(table, opts){
        // gated on opts.strictIgnoreCheck (set only by ClientSync's
        // from-zero lookup repair, ClientApplier.applyIncrementalSnapshot's
        // caller) rather than running on every ordinary per-block apply: this is
        // an extra SHOW WARNINGS round-trip per batch, and the hot streaming path
        // re-sends these tables' rows constantly by design (that is the whole
        // point of ignoreTables), so it must stay cheap there. A REPAIR pass is
        // different - ClientSync only pages a table from-zero because the
        // completeness check already measured it short, so every row in that page
        // is expected to be either already-correct or genuinely missing, never a
        // silent conflict.
        //
        // For this class of table the row's own id/PRIMARY KEY is the ENTIRE
        // re-send contract - a benign re-delivery can only ever warn "Duplicate
        // entry '<id>' for key 'PRIMARY'". Any other warning (a collision on a
        // DIFFERENT unique key, e.g. index_statuses' `status`, or a non-duplicate
        // error like a truncated/NULL column) means IGNORE just silently dropped
        // a row the repair needed to land, with the replica left short and no
        // signal anywhere that it happened. Fail loud instead, so the repair's
        // caller (ClientSync.maybeVerifyCompleteness) sees exactly which
        // table/row collided rather than reporting the same short count forever.

        // Keep warning classification scoped to repair passes.
        return opts && opts.strictIgnoreCheck && this.idKeyedIgnoreTables.has(table);
    }

    assertNoStrictIgnoreConflicts(table, suspect){
        // A natural-key collision on this class has two causes and only one of
        // them needs a human. A STALE GENERATION is the healable one: the source
        // re-interned its lookup rows (the ids are node-local AUTO_INCREMENT
        // surrogates assigned in first-seen order and the table is never rolled
        // back), so the replica holds the natural value at an id the source no
        // longer has. Retiring that dead row and landing the source's converges
        // the table. A GENUINE conflict is the other: the id the local row holds
        // is one the source ALSO serves, so retiring it would destroy a live row.
        for(let w of suspect){
            let code = Number(w.Code || w.code || 0);
            let message = w.Message || w.message || '';
            throw new Error('INSERT IGNORE silently dropped a row applying to `' + table +
                '` (errno ' + code + '): ' + message + '. This table\'s re-send contract is a ' +
                'PRIMARY-key duplicate only; the local row holding this one\'s natural key is ' +
                'still in the source\'s own row set, so it cannot be retired automatically and ' +
                'needs a human to reconcile it.');
        }
    }

    async insertRows(table, rows, opts){
        if(!rows || rows.length === 0) return;
        let prepared = this.prepareTableRows(table, rows);

        // Chunked to keep the IN list bounded on a large catch-up window.
        let deleteBatch = 500;
        for(let i = 0; i < prepared.keyValues.length; i += deleteBatch){
            let slice = prepared.keyValues.slice(i, i + deleteBatch);
            await this.db.deleteRowsByKeyValues(table, prepared.naturalKey, slice);
        }
        this.validateInsertColumns(prepared.columns);

        // Mutable-aggregate full-dump tables (useUpsert) overwrite their existing row so
        // a re-dump on a non-empty replica refreshes (not skips) stale values.
        // Batch inserts in groups of 100 for efficiency
        let batchSize = 100;
        for(let i = 0; i < rows.length; i += batchSize){
            let { batch, args } = this.prepareInsertBatch(rows, prepared.columns, i, batchSize);

            try {
                await this.insertRowBatch(table, prepared.columns, batch, args,
                    prepared.useIgnore, prepared.useUpsert);
            } catch(e){
                // Name the upsert table on the error so ClientSync can tell a repeating
                // full-dump duplicate key apart from any other apply failure.
                if(prepared.useUpsert && e && typeof e === 'object' && e.upsertTable === undefined) e.upsertTable = table;
                throw e;
            }

            if(table === 'events'){
                let warnings = await this.db.doQuery('SHOW WARNINGS');
                this.assertNoEventTruncation(warnings);
            } else if(this.shouldCheckStrictIgnoreWarnings(table, opts)){
                let suspect = this.suspectIgnoreWarnings(await this.db.doQuery('SHOW WARNINGS'));

                if(suspect.length){
                    let retired = await this.retireStaleNaturalKeyRows(table, batch, rows, suspect);
                    if(retired.length){
                        await this.insertRowBatch(table, prepared.columns, batch, args,
                            prepared.useIgnore, prepared.useUpsert);
                        suspect = this.suspectIgnoreWarnings(await this.db.doQuery('SHOW WARNINGS'));
                    }
                    this.assertNoStrictIgnoreConflicts(table, suspect);
                }
            }
        }
    }

    // Warnings from an id-keyed INSERT IGNORE that the re-send contract does NOT admit.
    // A benign re-delivery can only ever warn "Duplicate entry '<id>' for key 'PRIMARY'";
    // everything else means IGNORE dropped a row the caller needed to land.
    suspectIgnoreWarnings(warnings){
        let suspect = [];
        for(let w of (warnings || [])){
            let code    = Number(w.Code || w.code || 0);
            let message = String(w.Message || w.message || '');
            if(code === 1062 && /for key ['"`]?(?:[\w-]+\.)?PRIMARY['"`]?/i.test(message)) continue;
            suspect.push(w);
        }
        return suspect;
    }

    // Remove rows that conflict with the source page by either surrogate id or natural
    // key. The caller immediately re-inserts the page in the same transaction. Probing
    // exact pairs first keeps an already-converged repair idempotent and avoids writes.
    async reconcileLookupRows(table, rows, keyColumns){
        let retireIds = new Set();
        for(let row of rows){
            let id = row ? row.id : undefined;
            if(id === undefined || id === null)
                throw new Error('Lookup repair row for ' + table + ' is missing id');

            let values = [];
            for(let column of keyColumns){
                if(row[column] === undefined)
                    throw new Error('Lookup repair row for ' + table + ' is missing natural key ' + column);
                values.push(row[column]);
            }

            let keyHolders = await this.db.findRowIdsByKeyColumns(table, keyColumns, values);
            if(keyHolders && keyHolders.length === 1 && Number(keyHolders[0].id) === Number(id))
                continue;

            let idHolder = await this.db.findRowIdById(table, id);
            for(let holder of (idHolder || [])) retireIds.add(Number(holder.id));

            for(let holder of (keyHolders || [])) retireIds.add(Number(holder.id));
        }

        for(let id of retireIds){
            await this.db.deleteRowById(table, id);
            logger.warn('LOOKUP_ID_STATUS_RECONCILED table=' + table + ' retired_id=' + id +
                ' cause=first_seen_auto_increment_id_instability');
        }
    }

    // Retire the rows of a superseded lookup generation so the source's rows can land.
    //
    // Bounded by the page's own id window, which is what makes the "the source does not
    // have this id" conclusion sound: a repair page is a contiguous id-ordered slice of
    // the source table, so an id inside [min, max] that the page did not carry is absent
    // upstream. An id outside that window may simply be on another page, and an id the
    // page DID carry is a live row, so both are left for the throw.
    //
    // Safe to delete: the replica's data rows carry the SOURCE's status/lookup ids
    // verbatim (they are replicated, not minted locally), so nothing the source still
    // serves points at a retired generation's id.
    async retireStaleNaturalKeyRows(table, batch, pageRows, warnings){
        let retired = [];
        let indexNames = [];
        for(let w of warnings){
            if(Number(w.Code || w.code || 0) !== 1062) return retired;   // not a key collision at all
            let m = /for key ['"`]?(?:[\w-]+\.)?([\w-]+)['"`]?/i.exec(String(w.Message || w.message || ''));
            if(!m || m[1].toUpperCase() === 'PRIMARY') return retired;
            if(!indexNames.includes(m[1])) indexNames.push(m[1]);
        }

        // The source's id set for this table, and the window it proves.
        let carried = new Set();
        let low = null, high = null;
        for(let row of pageRows){
            let id = row ? row.id : undefined;
            if(id === undefined || id === null) return retired;   // no surrogate id: not this shape
            id = Number(id);
            carried.add(id);
            if(low === null  || id < low)  low  = id;
            if(high === null || id > high) high = id;
        }

        for(let indexName of indexNames){
            let keyColumns = await this.uniqueKeyColumns(table, indexName);
            if(!keyColumns.length) continue;

            for(let row of batch){
                let id = Number(row.id);
                let mine = await this.db.findRowIdById(table, id);
                if(mine && mine.length) continue;                 // the source's row is already here

                let values = keyColumns.map(c => row[c]);
                if(values.some(v => v === undefined)) continue;
                let holder = await this.db.findRowIdsByKeyColumns(table, keyColumns, values);
                if(!holder || holder.length !== 1) continue;       // absent, or ambiguous: not this shape

                let holderId = Number(holder[0].id);
                if(holderId === id || carried.has(holderId)) continue;
                if(holderId < low || holderId > high) continue;

                await this.db.deleteRowById(table, holderId);
                retired.push(holderId);
                logger.warn('STALE_LOOKUP_GENERATION_RETIRED table=' + table + ' key=' + indexName +
                    ' natural_key=' + JSON.stringify(keyColumns.map((c, i) => c + '=' + values[i]).join(',')) +
                    ' retired_id=' + holderId + ' landed_id=' + id +
                    ' (the source no longer serves the retired id within this page\'s id window)');
            }
        }
        return retired;
    }

    // Ordered column list of one index, for building the natural-key predicate. Names come
    // from information_schema and are re-validated before they reach the query string.
    async uniqueKeyColumns(table, indexName){
        let check = validation.validateIdentifier(indexName);
        if(!check.valid) return [];
        let rows = await this.db.findIndexColumnNames(table, indexName);
        let columns = [];
        for(let r of (rows || [])){
            let name = String(r.column_name || r.COLUMN_NAME || '');
            if(!validation.validateIdentifier(name).valid) return [];
            columns.push(name);
        }
        return columns;
    }

    // Mirror the source's anchor-reward winner collapse (xchain-indexer db.js
    // reconcileAnchorRewardWinner): the source pre-images every loser validator_rewards
    // row into anchor_reward_reconcile_log, then DELETEs it. The log rows replicate
    // (stream:block / mirror) but the DELETE never did, so a replica that held a loser
    // (bootstrap snapshot, or the pre-flag-day ANCHOR write) kept it forever, strictly
    // AHEAD of the source and invisible to the source-ahead-only count check. The log
    // row carries the loser's full UNIQUE identity (source_id, signing_pubkey_id,
    // reward_type, round_reference, round_qualifier), so this is a keyed delete with no
    // winner predicate to reproduce; rows the source still holds (winners) never match a
    // pre-image.
    //
    // round_qualifier is load-bearing here, not decoration. The archive leg keys
    // round_reference on MATCH_BATCH_SEQ, a dense hub counter a rebase reissues, so two
    // genuinely distinct archive rewards can share all four older columns and differ only
    // in qualifier (the snapshot_block). Keyed on the four alone this DELETE also reaches
    // the OTHER snapshot's row and destroys a reward the source still holds: the exact
    // inverse of the drift the mirror exists to close, and silent, because
    // validator_rewards declares no hash class (src/table_lifecycle.js).
    // Runs AFTER the insert loop (the log rows of this apply are in place) and INSIDE
    // the apply transaction. The reverse twin is ClientRollback's RB-ANCHOR restore,
    // which re-INSERTs these pre-images when the reconcile block is orphaned. `scopeSql`
    // / `scopeArgs` bound the log rows to the window this apply carried
    // (d.block_index = B live; d.block_index >= since on an incremental catch-up).
    async mirrorAnchorRewardReconcile(scopeSql, scopeArgs){
        try {
            await this.db.deleteReconciledValidatorRewards(scopeSql, scopeArgs);
        } catch(e){
            // Schema-gap errors (log table / columns absent on an older replica) are safe
            // to skip: such a replica received no log rows either. Anything else must
            // abort the apply so the block is retried, never applied half-mirrored.
            if(e && e.errno !== 1146 && e.errno !== 1054) throw e;
            // A replica whose log table or validator_rewards predates round_qualifier now
            // raises 1054 on the whole statement, so the mirror stops rather than deleting
            // on the stale four-column key. That leaves the replica AHEAD, which the
            // source-ahead-only count check cannot see, so say it once per apply instead of
            // skipping in silence; schema replication (ensureReplicatedColumns) adds the
            // column on the next pass and the mirror resumes.
            if(e && e.errno === 1054)
                logger.warn('anchor-reward reconcile mirror skipped: an identity column ' +
                    '(round_qualifier) is missing from validator_rewards or ' +
                    'anchor_reward_reconcile_log on this replica, so reconcile losers stay ' +
                    'until schema replication adds it');
        }
    }

    // Apply the in-place "updated rows" channel: each entry is the CURRENT full
    // state of a surviving row the source mutated in place (deactivation_block,
    // SLASH amount, request_status). These rows already exist on the replica with
    // their stale values, so they must be UPSERTed (INSERT ... ON DUPLICATE KEY
    // UPDATE); a plain INSERT would collide on the row's UNIQUE action_index.
    // updated is a { table: [rows] } map; an old payload simply omits it.
    async applyUpdatedRows(updated){
        if(!updated || typeof updated !== 'object') return;
        for(let table in updated){
            let rows = updated[table];
            if(!rows || rows.length === 0) continue;
            await this.upsertRows(table, rows);
        }
    }

    // INSERT ... ON DUPLICATE KEY UPDATE for a batch of full rows. Every column is
    // written on both insert and update, so an already-present surviving row has
    // its mutated columns overwritten to the source's current values while a
    // not-yet-present row (e.g. created and mutated within the same window) is
    // inserted. Identifier validation + binary decode mirror insertRows.
    async upsertRows(table, rows){
        if(!rows || rows.length === 0) return;

        let tableCheck = validation.validateIdentifier(table);
        if(!tableCheck.valid){
            // Fail closed, not open: a `return` here silently drops every row for this
            // table while the apply transaction commits, permanently diverging the replica
            // with no signal. Throw so the transaction rolls back and the block is retried.
            throw new Error('Rejected table name in upsertRows: ' + table + ' (' + tableCheck.reason + ')');
        }

        let columns = Object.keys(rows[0]);
        // Same errno-1906 rule as insertRows: a generated column may ride the wire
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
                throw new Error('Rejected column name in upsertRows: ' + col + ' (' + colCheck.reason + ')');
            }
        }
        // A plain INSERT with the ON DUPLICATE KEY UPDATE suffix: every carried column
        // is written on both insert and update.
        let batchSize = 100;
        for(let i = 0; i < rows.length; i += batchSize){
            let batch = rows.slice(i, i + batchSize);
            let args = [];
            for(let row of batch){
                for(let col of columns)
                    args.push(decodeValue(row[col] !== undefined ? row[col] : null));
            }
            await this.db.insertRowValues(table, columns, batch.length, args, false, true);
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
    async maybeRederiveEscrow(tables){
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
            // signal. Mirrors rebuildBalances' narrow catch.
            if(e.errno !== 1146 && e.errno !== 1054) throw e;
        }
    }
}

module.exports = ClientApplier;
