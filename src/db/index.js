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
 * XChain Sync - Database Class: connection, schema and plumbing.
 *
 * Connecting to MariaDB, reconciling the replica schema against the source,
 * and the pool, circuit breaker and transaction plumbing every query runs on.
 * Simplified from the indexer's own database class (no action processing).
 *
 * THE QUERIES THEMSELVES LIVE IN THE MIXINS beside this file, one per table
 * family and named for the DDL file in src/sql/ where the table has one. They
 * are installed on Database.prototype below, so a caller still writes
 * this.db.getX() and the split is invisible to it.
 *
 * Supports both 'indexer' and 'decoder' DBs via the dbType parameter.
 * Most queries are schema-agnostic; the indexer-specific block-hash join
 * (ledger_hash/actions_hash/contract_hash) only runs for dbType='indexer'.
 *
 ********************************************************************/

const mariadb    = require('mariadb');
const fs         = require('fs');
const path       = require('path');
const validation = require('../util/validation');
const { splitSqlStatements } = require('./sql_util');
const poolSizing = require('./pool_sizing');
const stakeWeightCollation = require('../stake_weight_collation_activation');
const utf8mb4Columns = require('../schema/utf8mb4_columns');
const lifecycle = require('../table_lifecycle');
const { assertValidIdentifier, requireStakeWeight } = require('./shared.js');
const util = require('node:util');
const { getLogger } = require('../observability');
const envConfig = require('../config');
const logger = getLogger();

// Columns that a key rebuild in ensureReplicaSecondaryIndexes NAMES, with the
// authoritative definition from the indexer migration that introduced each one. A
// rebuild whose column is absent is errno 1072 and the whole ALTER is refused, so the
// column has to land first; the source-DDL heal that would carry it (addMissingColumns)
// runs LATER in startup on the common client topology, and never at all on a replica
// holding a durable halt, which is why these definitions are declared here and healed
// beside the rebuild instead of deferred to a next startup that repeats the same order.
const KEY_REBUILD_PRECONDITION_COLUMNS = [
    // xchain-indexer 2026-08-28-anchor-actions-section-index-pk.
    { table: 'anchor_actions', column: 'section_index',
      definition: 'TINYINT UNSIGNED NOT NULL DEFAULT 0 AFTER `action_index`' },
    // xchain-indexer 2026-08-24-validator-rewards-round-qualifier, both tables it alters:
    // the reward identity and the reconcile-log pre-image the replica mirror joins on
    // (ClientApplier.mirrorAnchorRewardReconcile).
    { table: 'validator_rewards', column: 'round_qualifier',
      definition: 'BIGINT UNSIGNED NOT NULL DEFAULT 0 AFTER `round_reference`' },
    { table: 'anchor_reward_reconcile_log', column: 'round_qualifier',
      definition: 'BIGINT UNSIGNED NOT NULL DEFAULT 0 AFTER `round_reference`' }
];

// Delay between attempts in the infinite DB-connection retry loops
// (verifyDatabase / createDatabase). Named so the cadence lives in one place
// and is not confused with the unrelated connectTimeout in the pool config.
const DB_RETRY_DELAY_MS = 5000;

// One mixin per table family, keyed to the DDL files in src/sql/ where the
// table has one. Required by computed path, so nothing here names a mixin as a
// literal any tool could follow: the list IS the registry.
const MIXIN_FILES = [
    './sync_meta.js',
    './sync_halt.js',
    './state_tree_roots.js',
    './blocks.js',
    './actions.js',
    './transactions.js',
    './stakes.js',
    './tables.js',
    './validator_rewards.js',
    './credits.js',
    './index_lookups.js',
    './state_checkpoints.js',
    './merkle_epochs.js',
];

class Database {

    constructor(host, port, dbName, user, pass, util, dbType) {
        this.host   = host;
        this.port   = port;
        this.dbName = dbName;
        this.user   = user;
        this.pass   = pass;
        this.util   = util;
        this.dbType = dbType || 'indexer';  // 'indexer' (default) or 'decoder'

        // Name of the replication connection carrying the served schemas. Unset
        // reduces across every connection, worst-case; naming one measures that
        // stream alone so an unrelated lagging connection cannot drag the reading.
        this.replicaConnectionName = (envConfig.replicaConnectionFromEnv() || '').trim();

        // Connection pool parameters.
        // Sizing is per dbType (see poolSizing.js): the indexer pool absorbs
        // ServerPoller's ~113-query-per-block fan-out plus concurrent snapshot
        // streams, the decoder pool replicates 8 narrow tables. Each knob honours
        // DB_POOL_SIZE_<DBTYPE> first, then the legacy global DB_POOL_SIZE, then
        // the per-dbType default.
        let poolParams = poolSizing.resolvePoolParams(this.dbType);
        this.connectionPoolParams = {
            host:               this.host,
            user:               this.user,
            password:           this.pass,
            database:           this.dbName,
            port:               this.port,
            connectionLimit:    poolParams.connectionLimit,
            connectTimeout:     poolParams.connectTimeout,
            acquireTimeout:     poolParams.acquireTimeout,
            idleTimeout:        60000,
            insertIdAsNumber:   true,
            bigIntAsNumber:     true,
            // Return DATETIME/TIMESTAMP columns as MariaDB-format strings rather
            // than JS Dates. JSON.stringify would otherwise emit Date as ISO
            // ('2023-11-15T06:13:21.000Z'), which MariaDB strict mode rejects
            // on re-insert, and BlockHasher.contentDigest / the wire codec would
            // see a timezone-dependent value. GLOBAL on purpose: it covers every
            // replicated DATETIME column on BOTH dbTypes, today decoder events.time
            // and indexer events.time + events.witness_time (indexer `events` rides
            // the snapshot channel; SnapshotBuilder full-dumps it). Do not scope or
            // drop it per dbType; test/unit/replicated_datetime_columns.test.js pins
            // the inventory and this flag. (dispensers.expiration is a BIGINT unix
            // timestamp, replicated as a number via bigIntAsNumber.)
            dateStrings:        true,
            minDelayValidation: 3000,
            queryTimeout:       poolParams.queryTimeout
        };

        this.pool = mariadb.createPool(this.connectionPoolParams);
        this.transactionConnection = null;

        this.circuitState     = 'closed';
        this.circuitFailures  = 0;
        this.circuitThreshold = 10;
        this.circuitCooldown  = 30000;
        this.circuitOpenUntil = 0;
    }

    async verifyDatabase(){
        let connectionParams = {
            host:     this.host,
            user:     this.user,
            password: this.pass,
            port:     this.port
        };
        while(true){
            try {
                let db      = await mariadb.createConnection(connectionParams);
                let results = await db.query("SELECT * FROM information_schema.schemata WHERE schema_name = ?", [this.dbName]);
                await db.end();
                return results.length > 0;
            } catch (e){
                logger.error(util.format('Error checking if database ' + this.dbName + ' exists:', e))
                await this.util.sleep(DB_RETRY_DELAY_MS);
            }
        }
    }

    // Single-attempt existence check that THROWS on failure instead of retrying
    // forever (unlike verifyDatabase). Used to probe a SOURCE DB during client
    // discovery: when the source is unreachable (e.g. a node-internal DB host that
    // doesn't resolve from the replica box), the caller needs a thrown error so it
    // can fall back to the server /schema endpoint instead of blocking forever.
    async verifyDatabaseOnce(){
        let connectionParams = {
            host:           this.host,
            user:           this.user,
            password:       this.pass,
            port:           this.port,
            connectTimeout: 5000
        };
        let db = await mariadb.createConnection(connectionParams);
        try {
            let results = await db.query("SELECT * FROM information_schema.schemata WHERE schema_name = ?", [this.dbName]);
            return results.length > 0;
        } finally {
            await db.end();
        }
    }

    async createDatabase(){
        let connectionParams = {
            host:     this.host,
            user:     this.user,
            password: this.pass,
            port:     this.port
        };
        let dbCheck = validation.validateIdentifier(this.dbName);
        if(!dbCheck.valid)
            throw new Error('Invalid database name: ' + this.dbName + ' (' + dbCheck.reason + ')');
        logger.info("Creating " + this.dbName + " database!");
        while(true){
            try {
                let db = await mariadb.createConnection(connectionParams);
                await db.query("CREATE DATABASE IF NOT EXISTS `" + this.dbName + "`");
                await db.end();
                return true;
            } catch(e){
                logger.error(util.format('Error creating database ' + this.dbName + ':', e))
                await this.util.sleep(DB_RETRY_DELAY_MS);
            }
        }
    }

    // Verify sync-service-owned tables exist (replicated tables are created
    // dynamically via replicateSchema / the /schema fetch, not from local SQL
    // files). Which sync-owned tables apply depends on the DB shape: the
    // transparency log (sync_meta + merkle_epochs) is indexer-only, but the
    // durable divergence halt (sync_halt) applies to BOTH db types.
    // ClientSync checks and records halts for decoder replicas too, and
    // without the table every decoder client start logged a 1146 probe error.
    async verifySyncTables(){
        let dir  = path.join(__dirname, 'sql');
        let files = fs.readdirSync(dir);
        let db    = await this.getConnection();
        // One summary line instead of a per-table pair; the error path below still
        // names the table, so a failure stays attributable.
        logger.info('Verifying database and tables...');
        let checked = 0;
        let created = 0;
        for(let file of files){
            if(file.indexOf('.sql') !== -1){
                let table = file.substring(0, file.indexOf('.sql'));
                if(this.dbType !== 'indexer' && table !== 'sync_halt') continue;
                checked++;
                try {
                    let results = await db.query("SELECT * FROM information_schema.tables WHERE table_schema = ? AND table_name = ?", [this.dbName, table]);
                    if(results.length === 0){
                        await this.createTableFromFile(file);
                        created++;
                    }
                } catch(e){
                    this.util.throwError('Error verifying ' + table + ' table: ' + e);
                    return false;
                }
            }
        }
        await db.release();
        logger.info('Database and tables verified (' + checked + ' tables, ' + created + ' created).');
        return true;
    }

    // Only for sync-service-owned tables such as sync_meta; replicated tables come
    // from the source's own DDL.
    async createTableFromFile(file){
        let dir     = path.join(__dirname, 'sql');
        let data    = fs.readFileSync(dir + '/' + file, "utf8");
        let queries = splitSqlStatements(data);
        for(let query of queries){
            await this.doQuery(query);
        }
    }

    // Bring an already-existing table on this (target) database up to the
    // source schema by adding any columns the source has and the target
    // lacks. Schema replication only ever CREATEd missing tables; a column
    // added to a table that the replica had already bootstrapped from older
    // DDL was never propagated, so the first snapshot/block carrying the new
    // column failed with "Unknown column '...' in 'field list'" and rolled
    // back permanently. Source column names + definitions are derived from
    // the source's CREATE TABLE DDL (validated by validateDdl by the caller);
    // the target's columns come from INFORMATION_SCHEMA. Each gap is closed
    // with an ALTER TABLE ADD COLUMN. A column whose definition cannot be
    // cleanly parsed is logged and skipped rather than aborting the whole sync;
    // a column the SERVER refuses is a different thing and fails loudly (see
    // below). DDL auto-commits, so this must run before any snapshot
    // transaction is opened. Returns the number of columns added, and THROWS
    // when a generated ALTER was refused by the server: the column is
    // still missing, so every later row carrying it fails with errno 1054, and
    // a swallowed error here is a replica that stalls silently for days.
    // Callers route the throw into their own fail-closed path (ClientSync's
    // schema-apply fixpoint records a durable halt).
    //
    // The ALTER is deliberately UNQUALIFIED (the table name alone, never
    // db.table) and runs on the pool, whose connections carry a default
    // database. On a replica that is itself a replication source for a
    // downstream tier, a fully-qualified DDL statement executed with no default
    // database is dropped by the downstream Replicate_Do_DB filter and never
    // reaches it, so the self-heal would appear to work here while the tier
    // below stayed broken.
    //
    // NOTE FOR OPERATORS: a replica that stalled BEFORE this fix shipped will
    // not self-heal until it next runs schema replication. If one is wedged on
    // an "Unknown column" error, restart it (replication re-runs on startup)
    // or apply the missing `ALTER TABLE ... ADD COLUMN` on it manually once.
    async addMissingColumns(tableName, sourceDdl){
        let sourceColumns = validation.extractColumnNames(sourceDdl);
        if(sourceColumns.length === 0) return 0;

        let destRows = await this.doQuery(
            "SELECT column_name FROM information_schema.columns WHERE table_schema = ? AND table_name = ?",
            [this.dbName, tableName]
        );
        let destSet = new Set(destRows.map(r => r.column_name || r.COLUMN_NAME));

        let added  = 0;
        let failed = [];
        for(let col of sourceColumns){
            if(destSet.has(col)) continue;

            let colCheck = validation.validateIdentifier(col);
            if(!colCheck.valid){
                logger.error('Skipping invalid column name ' + col + ' on ' + tableName + ' (' + colCheck.reason + ')');
                continue;
            }

            let def = validation.extractColumnDefinition(sourceDdl, col);
            if(!def){
                logger.warn('Could not extract definition for column ' + col + ' on ' + tableName + '; skipping (manual ALTER may be required)');
                continue;
            }

            let actions = ['ADD COLUMN ' + def];
            if(validation.isAutoIncrementDefinition(def)){
                let keyAction = await this.autoIncrementKeyAction(tableName, col, sourceDdl);
                actions.push(keyAction);
            }
            let alter = "ALTER TABLE `" + tableName + "` " + actions.join(', ');

            try {
                // doQueryStrict, not doQuery: outside a transaction doQuery logs the
                // error and returns [], so the success log below fired on a REFUSED
                // ALTER and reported "Added column" for a column that does not exist.
                await this.doQueryStrict(alter);
                logger.info('Added column ' + col + ' to ' + tableName);
                added++;
            } catch(e){
                failed.push({ column: col, errno: (e && e.errno) || null, message: (e && e.message) || String(e) });
                logger.error(util.format('FAILED to add column ' + col + ' to ' + tableName +
                    ' (errno ' + ((e && e.errno) || 'unknown') + '); the column is still missing. ALTER was: ' + alter, e));
            }
        }

        if(failed.length){
            let err = new Error('Schema column self-heal failed on ' + tableName + ': ' +
                failed.map(f => f.column + ' (errno ' + f.errno + ')').join(', '));
            err.errno         = failed[0].errno;
            err.failedColumns = failed;
            throw err;
        }
        return added;
    }

    // Build the key clause that must accompany an ADD COLUMN for an
    // AUTO_INCREMENT column. MariaDB rejects the bare add with errno 1075
    // ("there can be only one auto column and it must be defined as a key"),
    // which is what wedged the decoder replicas' pubkeys.id self-heal.
    //
    // The key is taken from the SOURCE DDL so the replica converges on the
    // source's own definition rather than a guess. A source PRIMARY KEY is only
    // reproducible when the replica has no primary key yet; otherwise (and when
    // the source's covering key is multi-column or absent) a UNIQUE key on the
    // column alone satisfies the auto-increment requirement without disturbing
    // the existing keys.
    async autoIncrementKeyAction(tableName, col, sourceDdl){
        let key = validation.extractKeyForColumn(sourceDdl, col);

        if(key && key.type === 'primary' && !(await this.hasPrimaryKey(tableName)))
            return 'ADD PRIMARY KEY (`' + col + '`)';

        if(key && key.type === 'unique' && key.name)
            return 'ADD UNIQUE KEY `' + key.name + '` (`' + col + '`)';

        if(key && key.type === 'index' && key.name)
            return 'ADD KEY `' + key.name + '` (`' + col + '`)';

        return 'ADD UNIQUE KEY `' + col + '` (`' + col + '`)';
    }

    // Whether this table already carries a PRIMARY KEY on this database. A
    // failed probe answers "yes": the caller then adds a UNIQUE key, which is
    // valid either way, while a wrong "no" produces an ADD PRIMARY KEY that a
    // table with one rejects outright (errno 1068).
    async hasPrimaryKey(tableName){
        try {
            let rows = await this.doQueryStrict(
                "SELECT index_name FROM information_schema.statistics WHERE table_schema = ? AND table_name = ? AND index_name = 'PRIMARY' LIMIT 1",
                [this.dbName, tableName]
            );
            return rows.length > 0;
        } catch(e){
            logger.error(util.format('Could not read primary-key state for ' + tableName + '; assuming one exists:', e));
            return true;
        }
    }

    // Add a key-rebuild precondition column that is still absent, and answer whether the
    // column is there afterwards. Definitions come from KEY_REBUILD_PRECONDITION_COLUMNS,
    // never from a guess, and the ADD is IF NOT EXISTS so a concurrent startup that won
    // the race is not an error. A refused ADD returns false and the caller leaves the
    // stale key alone: the rebuild would only be refused too, with no signal.
    async ensureKeyRebuildColumn(table, column){
        let spec = KEY_REBUILD_PRECONDITION_COLUMNS.find(c => c.table === table && c.column === column);
        if(!spec) return false;
        try {
            await this.doQueryStrict('ALTER TABLE `' + table + '` ADD COLUMN IF NOT EXISTS `' +
                column + '` ' + spec.definition);
            logger.info('Added ' + table + '.' + column + ' on ' + this.dbName +
                '; the key rebuild that names it can run in this same startup.');
            return true;
        } catch(e){
            logger.error(util.format('Failed to add ' + table + '.' + column + ' on ' + this.dbName +
                ' (errno ' + ((e && e.errno) || 'unknown') + '); the key rebuild that names it cannot run', e));
            return false;
        }
    }

    // Replicate schema from a source database into this database.
    // Reads all table DDLs from the source via SHOW CREATE TABLE and
    // creates any missing tables locally. For tables that already exist,
    // propagates any columns the source has added since the replica was
    // bootstrapped (see addMissingColumns). This ensures the replica always
    // matches the authoritative indexer schema (no copied SQL files needed).
    async replicateSchema(sourceDb){
        logger.info('Replicating schema from ' + sourceDb.dbName + ' into ' + this.dbName + '...');

        let sourceTables = await sourceDb.doQuery(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_type = 'BASE TABLE' ORDER BY table_name",
            [sourceDb.dbName]
        );

        let existingTables = await this.doQuery(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_type = 'BASE TABLE'",
            [this.dbName]
        );
        let existingSet = new Set(existingTables.map(r => r.table_name || r.TABLE_NAME));

        let created = 0;
        let columnFailures = [];
        for(let row of sourceTables){
            let tableName = row.table_name || row.TABLE_NAME;

            // Validate table name before using in SQL
            let idCheck = validation.validateIdentifier(tableName);
            if(!idCheck.valid){
                logger.error('Skipping invalid table name: ' + tableName + ' (' + idCheck.reason + ')');
                continue;
            }

            let ddlRows = await sourceDb.doQuery("SHOW CREATE TABLE `" + tableName + "`");
            if(ddlRows.length === 0) continue;

            let createSql = ddlRows[0]['Create Table'];
            if(!createSql) continue;

            // Validate DDL before executing
            let ddlCheck = validation.validateDdl(createSql);
            if(!ddlCheck.valid){
                logger.error('Rejected DDL for ' + tableName + ': ' + ddlCheck.reason);
                continue;
            }

            // Table already exists on the replica: don't recreate it, but
            // propagate any columns the source has added since it was created.
            if(existingSet.has(tableName)){
                // A refused ALTER now throws. Keep the sweep going over the
                // remaining tables so one bad table does not hide the rest, but record
                // it and rethrow after the sweep: the caller must not read a partial
                // schema convergence as a complete one.
                try {
                    await this.addMissingColumns(tableName, createSql);
                } catch(e){
                    columnFailures.push({ table: tableName, errno: e.errno || null, message: e.message });
                }
                continue;
            }

            logger.info('Creating table ' + tableName + '...');
            try {
                await this.doQuery(createSql);
                created++;
            } catch(e){
                // Table may reference another table not yet created; retry later.
                logger.info(util.format('Deferred: ' + tableName + ':', e));
            }
        }

        // Retry any deferred tables (handles foreign-key ordering)
        if(created < sourceTables.length - existingSet.size){
            let retryTables = await this.doQuery(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_type = 'BASE TABLE'",
                [this.dbName]
            );
            let retrySet = new Set(retryTables.map(r => r.table_name || r.TABLE_NAME));

            for(let row of sourceTables){
                let tableName = row.table_name || row.TABLE_NAME;
                if(retrySet.has(tableName)) continue;

                let idCheck = validation.validateIdentifier(tableName);
                if(!idCheck.valid) continue;

                let ddlRows = await sourceDb.doQuery("SHOW CREATE TABLE `" + tableName + "`");
                if(ddlRows.length === 0) continue;
                let createSql = ddlRows[0]['Create Table'];
                if(!createSql) continue;

                let ddlCheck = validation.validateDdl(createSql);
                if(!ddlCheck.valid){
                    logger.error('Rejected DDL for ' + tableName + ' (retry): ' + ddlCheck.reason);
                    continue;
                }

                try {
                    await this.doQuery(createSql);
                    logger.info('Created table ' + tableName + ' (retry)');
                } catch(e){
                    logger.error(util.format('Failed to create table ' + tableName + ':', e));
                }
            }
        }

        // replicateSchema only CREATEs missing tables. It never ALTERs an
        // existing table to add a column introduced after the replica was first
        // built. Run the column self-heal so replicas built before a column was
        // added pick it up on this pass.
        await this.ensureReplicatedColumns();

        // Similarly, addMissingColumns only propagates columns, not secondary
        // indexes. Ensure known secondary indexes that must exist on replicated
        // tables are present (idempotent; safe on snapshot-bootstrapped replicas).
        await this.ensureReplicaSecondaryIndexes();

        // Neither of those retypes an existing column, so a replica built before the
        // 2026-09-02 raw-wire-field widen keeps utf8mb3 on those columns and halts on the
        // first 4-byte character the widened origin accepts. Converge them here.
        await this.ensureReplicaUtf8mb4Columns();

        if(columnFailures.length){
            let err = new Error('Schema replication into ' + this.dbName + ' left columns missing: ' +
                columnFailures.map(f => f.table + ' (errno ' + f.errno + ')').join(', '));
            err.columnFailures = columnFailures;
            throw err;
        }

        logger.info('Schema replication complete for ' + this.dbName);
    }

    // Self-heal known column drift on already-existing replicated tables.
    // replicateSchema (and the server /schema fetch path) skip any table that
    // already exists locally, so a column added to an authoritative table after
    // the replica was first built never reaches the replica. The replica then
    // rejects every synced row referencing the new field with "Unknown column",
    // permanently stalling sync for that table until the column is added by hand.
    //
    // The token-ownership-trading feature added give_ownership/get_ownership to
    // the orders and swaps tables. Mirror the indexer's alterTableForDrift
    // contract: add the column from its authoritative definition only when
    // absent. Both columns are NOT NULL DEFAULT 0, so the ADD COLUMN backfills
    // existing rows safely. Scoped to indexer replicas (orders/swaps do not
    // exist in the decoder schema). Tables absent locally are skipped (fresh
    // replicas create them with the columns already present).
    //
    // Also relaxes NULLABILITY drift in the SAFE direction (NOT NULL -> NULL):
    // contract_emissions.action_index was declared NOT NULL, but internal SLASH
    // emissions carry action_index = NULL (they deduct stake / write a
    // slash_events row but mint no on-wire action). The old stream scoped
    // contract_emissions by action_index (an INNER JOIN that silently dropped
    // those NULL rows); the emissions fix streams by execution_index and so
    // delivers them. A NOT NULL replica column rejects the INSERT with
    // errno 1048 ("Column 'action_index' cannot be null"). That is NOT a
    // schema-gap the apply-time self-heal catches (it only heals errno
    // 1146 missing-table / 1054 missing-column), so the replica would HALT
    // permanently. Relaxing here, at startup before any row data is accepted,
    // heals replicas built before the column was relaxed. Relax-only and
    // idempotent (no-op once nullable; fresh replicas bootstrap from the already
    // -relaxed source DDL); never tightens (NULL -> NOT NULL could fail on
    // existing NULLs and is never required for forward schema evolution). See
    // xchain-indexer/migrations/20260531_contract_emissions_action_index_nullable.sql.
    async ensureReplicatedColumns(){
        if(this.dbType !== 'indexer') return;
        // state_tree_roots.contract_state_root is here for a DIFFERENT reason than
        // the four ownership columns, and the difference is worth stating because
        // it is what makes this entry non-obvious. state_tree_roots is
        // FOLLOWER-DERIVED, not replicated: verifySyncTables creates it from this
        // repo's own src/sql/state_tree_roots.sql, and creation only happens when
        // the table is ABSENT. So an aged replica that already has the table never
        // gains a column added to that file afterwards, and every recomputed block
        // would fail its INSERT with errno 1054 the moment the code writes the new
        // column. Not at an armed height: on the FIRST block after deploy, on every
        // follower at once. Fresh replicas are unaffected (they create the table
        // with the column), which is exactly what makes it easy to ship and only
        // discover in production. See the SPV sub-tree spec Stage A work list.
        let drift = [
            { table: 'orders', column: 'give_ownership', definition: 'TINYINT(1) NOT NULL DEFAULT 0' },
            { table: 'orders', column: 'get_ownership',  definition: 'TINYINT(1) NOT NULL DEFAULT 0' },
            { table: 'swaps',  column: 'give_ownership', definition: 'TINYINT(1) NOT NULL DEFAULT 0' },
            { table: 'swaps',  column: 'get_ownership',  definition: 'TINYINT(1) NOT NULL DEFAULT 0' },
            { table: 'state_tree_roots', column: 'contract_state_root',
              definition: 'CHAR(64) NULL AFTER `block_merkle_root`' },
            { table: 'state_tree_roots', column: 'contract_state_root_shadow',
              definition: 'CHAR(64) NULL AFTER `contract_state_root`' },
            // Stage B's shadow column ( B3), same reasoning as the two
            // above: state_tree_roots is follower-derived, so an aged replica
            // never gains it from the definition file and the first shadow-window
            // block would fail its INSERT with errno 1054.
            { table: 'state_tree_roots', column: 'balances_root_escrow_shadow',
              definition: 'CHAR(64) NULL AFTER `contract_state_root_shadow`' },
            // The key-rebuild preconditions ride the same ADD COLUMN loop, so the columns
            // land in this step and the rebuilds that name them run against a converged
            // table later in the SAME startup. Both are NOT NULL DEFAULT 0, so the add
            // backfills existing rows on the value their widened key expects.
            ...KEY_REBUILD_PRECONDITION_COLUMNS
        ];
        for(let { table, column, definition } of drift){
            let tableRows = await this.doQuery(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
                [this.dbName, table]
            );
            if(tableRows.length === 0) continue;

            let colRows = await this.doQuery(
                "SELECT COLUMN_NAME FROM information_schema.columns WHERE table_schema = ? AND table_name = ? AND COLUMN_NAME = ?",
                [this.dbName, table, column]
            );
            if(colRows.length > 0) continue;

            logger.info('Schema drift on ' + table + '.' + column + ': column missing on replica. Adding ' + definition + '.');
            await this.doQuery('ALTER TABLE `' + table + '` ADD COLUMN `' + column + '` ' + definition);
        }

        // Nullability relaxations: each entry's column must be nullable upstream;
        // we relax it on the replica iff it is currently NOT NULL. `type` is the
        // authoritative column type (sans NOT NULL) used for the MODIFY.
        let relax = [
            { table: 'contract_emissions', column: 'action_index', type: 'BIGINT UNSIGNED NULL' }
        ];
        for(let { table, column, type } of relax){
            let colRows = await this.doQuery(
                "SELECT IS_NULLABLE FROM information_schema.columns WHERE table_schema = ? AND table_name = ? AND COLUMN_NAME = ?",
                [this.dbName, table, column]
            );
            if(colRows.length === 0) continue;                       // table/column absent; skip
            let nullable = colRows[0].IS_NULLABLE || colRows[0].is_nullable;
            if(String(nullable).toUpperCase() !== 'NO') continue;    // already nullable; no-op

            logger.info('Schema drift on ' + table + '.' + column + ': NOT NULL on replica but nullable upstream. Relaxing to allow NULL.');
            await this.doQuery('ALTER TABLE `' + table + '` MODIFY COLUMN `' + column + '` ' + type);
        }

        // AUTO_INCREMENT repair for hub-mirror id cursors. The indexer reconciler
        // previously stripped AUTO_INCREMENT from the id column of these four tables
        // on every startup (migration 2026-06-10-mirror-id-autoincrement-repair).
        // Origins self-heal via that migration, but replicas bootstrapped from a
        // stripped-state origin cloned the stripped DDL and have no automated path.
        // Detect a missing AUTO_INCREMENT on the id column and restore it here.
        // Idempotent: MODIFY to the same definition is a no-op; table absent = skip.
        let autoIncTables = [
            'price_snapshots',
            'cross_chain_matches',
            'capability_snapshots',
            'state_checkpoints'
        ];
        for(let table of autoIncTables){
            let colRows = await this.doQuery(
                "SELECT EXTRA FROM information_schema.columns WHERE table_schema = ? AND table_name = ? AND COLUMN_NAME = 'id'",
                [this.dbName, table]
            );
            if(colRows.length === 0) continue;   // table absent or has no id column; skip
            let extra = colRows[0].EXTRA || colRows[0].extra || '';
            if(String(extra).toLowerCase().indexOf('auto_increment') !== -1) continue;  // already correct; no-op

            logger.info('Schema drift on ' + table + '.id: AUTO_INCREMENT missing on replica. Repairing.');
            // Re-key any id=0 rows before the ALTER rebuilds the index (avoids a
            // collision when the AUTO_INCREMENT attribute is restored). Mirrors the
            // indexer migration. The old scalar-subquery UPDATE assigned EVERY id=0
            // row the SAME MAX(id)+1 value, so with more than one id=0 row the
            // statement aborted mid-flight on a duplicate-key error (caught/logged,
            // leaving AUTO_INCREMENT unrestored). Re-key each id=0 row to a DISTINCT
            // value instead: a session-counter UPDATE assigns MAX(id)+1, MAX(id)+2,
            // ... in a single ordered pass (each row gets its own id, no collision).
            // Idempotent: with zero id=0 rows it is a no-op; fail-soft on race errors.
            try {
                let baseRows = await this.doQuery('SELECT COALESCE(MAX(id), 0) AS base FROM `' + table + '`');
                let base = (baseRows && baseRows.length) ? Number(baseRows[0].base || baseRows[0].BASE || 0) : 0;
                // Seed the counter at MAX(id); the per-row `@n := @n + 1` yields
                // base+1, base+2, ... for each id=0 row. The ORDER BY is a no-op
                // tiebreak here (every matched row has id=0); the requirement is only
                // that the rewritten ids are DISTINCT, not any particular assignment
                // order. These AUTO_INCREMENT re-key tables are replication-local and
                // never enter a hash preimage, so cross-replica order is immaterial.
                await this.doQuery('SET @n := ?', [base]);
                await this.doQuery('UPDATE `' + table + '` SET id = (@n := @n + 1) WHERE id = 0 ORDER BY id ASC');
                await this.doQuery('ALTER TABLE `' + table + '` MODIFY id BIGINT NOT NULL AUTO_INCREMENT');
                logger.info('Repaired AUTO_INCREMENT on ' + table + '.id');
            } catch(e){
                // errno 1146 (table absent) or 1054 (column absent) can race; log and continue.
                logger.error(util.format('Failed to repair AUTO_INCREMENT on ' + table + '.id:', e));
            }
        }

        // 5244: Widen attests.request_status ENUM to include 'rejected' on replicas
        // that bootstrapped before the v4 schema migration
        // (2026-06-13-attests-request-status-add-rejected). A v3-schema replica holds
        // ENUM('pending','fulfilled','expired','errored'); streaming a row with
        // request_status='rejected' hits errno 1265 (data truncated / rejected in
        // strict mode) and permanently halts replication for that block. Detect the
        // narrow ENUM via information_schema and MODIFY to the full canonical set
        // when 'rejected' is absent. Idempotent: once widened the probe finds the
        // full set and skips. indexer-only (attests does not exist in the decoder schema).
        if(this.dbType === 'indexer'){
            try {
                let enumRows = await this.doQuery(
                    "SELECT COLUMN_TYPE FROM information_schema.columns " +
                    "WHERE table_schema = ? AND table_name = 'attests' AND COLUMN_NAME = 'request_status'",
                    [this.dbName]
                );
                if(enumRows.length > 0){
                    let columnType = String(enumRows[0].COLUMN_TYPE || enumRows[0].column_type || '');
                    if(columnType.indexOf("'rejected'") === -1){
                        logger.info('Schema drift on attests.request_status: ENUM missing \'rejected\'. Widening to canonical set.');
                        await this.doQuery(
                            "ALTER TABLE `attests` MODIFY COLUMN `request_status` " +
                            "ENUM('pending','fulfilled','expired','errored','rejected') NOT NULL DEFAULT 'pending'"
                        );
                    }
                }
            } catch(e){
                // errno 1146 = table absent on an older replica that has not yet had
                // attests created; skip silently. Any other error is logged.
                if(e.errno !== 1146)
                    logger.error(util.format('Failed to widen attests.request_status ENUM:', e));
            }
        }
    }

    // Fail-closed schema contract for the columns the stake-weight snapshot ORDERS on
    // (stake_weight_collation_activation.js). Twin of xchain-indexer's
    // assertStakeWeightOrderingCollation, sharing that module's ONE definition of the
    // declared contract so the two services cannot disagree about what "undrifted"
    // means. The follower rebuilds stakes_root from the byte-mirrored
    // _cappedStakeWeightsSql; its window caps truncate on this order, so a drifted
    // collation selects different cap survivors and the replica halts on a root it
    // computed wrong. Once the collation gate is armed, a drifted CHARSET fails the
    // query outright (errno 1253), so halting at boot with the column named is the
    // cheap end of that.
    //
    // The comparison normalises the utf8 / utf8mb3 spelling on both sides: MariaDB 10.6
    // renamed the charset, so a correctly declared column reports utf8mb3_general_ci and
    // a literal name comparison would halt the whole fleet on a correct schema.
    //
    // Indexer replicas only (decoder replicas hold no stakes). An absent column and an
    // unreadable name both return early rather than halt.
    async assertStakeWeightOrderingCollation(){
        if(this.dbType !== 'indexer') return;
        for(const spec of stakeWeightCollation.STAKE_WEIGHT_ORDERING_COLUMNS){
            // rethrow, not the fail-soft default: outside a transaction doQuery logs a
            // driver fault and returns [], which the absent-table branch below would read
            // as "no such column" and skip. A fail-closed collation guard that a transient
            // fault turns into a pass is fail-open on the replica fleet.
            let rows = await this.doQuery(
                "SELECT CHARACTER_SET_NAME, COLLATION_NAME FROM information_schema.columns " +
                "WHERE table_schema = ? AND table_name = ? AND column_name = ?",
                [this.dbName, spec.table, spec.column],
                null,
                { rethrow: true }
            );
            if(!rows || rows.length === 0) continue;  // table absent; schema apply creates it
            let reason = stakeWeightCollation.collationDriftReason(spec, rows[0]);
            if(reason) throw new Error(reason);
        }
    }

    // Ensure known secondary indexes exist on replicated tables. addMissingColumns
    // propagates new columns from the source schema but does not carry secondary indexes
    // (SHOW CREATE TABLE returns index definitions inline with the CREATE TABLE DDL, but
    // the /schema fetch path's validateDdl rejects multi-statement DDL, so a separate
    // ALTER TABLE ADD INDEX is the only safe delivery path). Snapshot-bootstrapped
    // replicas that pre-date a migration that added a secondary index to the source never
    // receive it; this idempotent ensure step closes that gap at startup.
    //
    // Only additive, never drops existing indexes. Each ALTER TABLE ADD INDEX IF NOT
    // EXISTS is a no-op once the index exists, so repeated startups are safe. The InnoDB
    // online DDL builds the index INPLACE and does not block DML, but it does run
    // synchronously at startup; on a large already-populated table it takes time. On a
    // freshly snapshot-bootstrapped replica the tables are already correct (the source
    // shipped them in CREATE TABLE DDL), so this only incurs cost on replicas that were
    // bootstrapped before the index was added to the source.
    async ensureReplicaSecondaryIndexes(){
        if(this.dbType !== 'indexer') return;
        // index_tickers.block_index: added by xchain-indexer migration
        // 2026-06-21-index-tables-block-index-secondary-idx.sql. Used by ClientRollback
        // (DELETE WHERE block_index >= ?) and the index-map parity checksum
        // (WHERE block_index IS NOT NULL AND block_index <= ?). Without this index,
        // both paths degrade to a full table scan on multi-million-row replicas.
        // state_tree_roots.block_index: added by xchain-indexer migration
        // 2026-09-12-state-tree-roots-block-index-idx.sql, for the same reason. The
        // table's two existing keys both lead with (chain, network), so ClientRollback's
        // DELETE WHERE block_index >= ? scans the entire root history on a replica, which
        // holds one row per block for the life of the chain.
        let ensureIndexes = [
            { table: 'index_tickers',    indexName: 'block_index', columns: '(block_index)' },
            { table: 'index_addresses',  indexName: 'block_index', columns: '(block_index)' },
            { table: 'state_tree_roots', indexName: 'block_index', columns: '(block_index)' }
        ];
        for(let { table, indexName, columns } of ensureIndexes){
            let tableRows = await this.doQuery(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
                [this.dbName, table]
            );
            if(tableRows.length === 0) continue;  // table absent; schema apply will create it with indexes

            // Check if the index already exists before issuing the ALTER (avoids a
            // logged warning from MariaDB when IF NOT EXISTS is used on older versions).
            let idxRows = await this.doQuery(
                "SELECT index_name FROM information_schema.statistics WHERE table_schema = ? AND table_name = ? AND index_name = ?",
                [this.dbName, table, indexName]
            );
            if(idxRows.length > 0) continue;  // already present; no-op

            try {
                await this.doQuery('ALTER TABLE `' + table + '` ADD INDEX `' + indexName + '` ' + columns);
                logger.info('Added secondary index ' + indexName + ' to ' + table + ' on ' + this.dbName);
            } catch(e){
                // errno 1061 = duplicate key name (race with another startup); harmless.
                if(e.errno !== 1061)
                    logger.error(util.format('Failed to add secondary index ' + indexName + ' to ' + table + ':', e));
            }
        }

        // 5245: Relax the attests UNIQUE(request_id, version) index to non-unique
        // on replicas that bootstrapped before the v4 migration
        // (2026-06-17-attests-drop-unique-request-id-version). The v3 schema carried
        // a UNIQUE index; the v4 migration drops+recreates it non-unique so a request
        // can carry multiple v1 retry rows. A sync-only replica that bootstrapped
        // with the stale UNIQUE halts on the first second-v1 row (errno 1062
        // ER_DUP_ENTRY). Detect a UNIQUE index via information_schema.statistics
        // (NON_UNIQUE=0) and DROP+recreate as a plain index. Idempotent: already
        // non-unique (NON_UNIQUE=1) is skipped; index absent is skipped (fresh
        // replicas bootstrap from the already-correct source DDL). indexer-only.
        if(this.dbType === 'indexer'){
            try {
                let tableCheck = await this.doQuery(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_name = 'attests'",
                    [this.dbName]
                );
                if(tableCheck.length > 0){
                    let idxRows = await this.doQuery(
                        "SELECT NON_UNIQUE FROM information_schema.statistics " +
                        "WHERE table_schema = ? AND table_name = 'attests' AND index_name = 'request_id_version' LIMIT 1",
                        [this.dbName]
                    );
                    if(idxRows.length > 0){
                        let nonUnique = Number(idxRows[0].NON_UNIQUE || idxRows[0].non_unique || 0);
                        if(nonUnique === 0){
                            // Index is UNIQUE on this replica; relax it.
                            logger.info('Schema drift on attests: UNIQUE(request_id_version) detected. Relaxing to non-unique.');
                            await this.doQuery('ALTER TABLE `attests` DROP INDEX `request_id_version`');
                            await this.doQuery('CREATE INDEX `request_id_version` ON `attests` (request_id, version)');
                        }
                    }
                }
            } catch(e){
                if(e.errno !== 1146)
                    logger.error(util.format('Failed to relax attests request_id_version index:', e));
            }

            // votes append-only migration (indexer 219da33 /
            // 2026-07-03-votes-append-only-unique-idx). The pre-219da33 schema keyed
            // votes UNIQUE(poll_index, voter_address_id, choice): one live ballot per
            // voter, last-write-wins. Append-only re-balloting inserts a NEW
            // action_index set per re-vote, so the unique key gained action_index
            // (poll_voter_action_choice). A replica that bootstrapped with the stale
            // poll_voter_choice key wedges on the first re-ballot row (errno 1062
            // ER_DUP_ENTRY) because the applier's last-write-wins pre-delete was
            // removed when votes went append-only, so the collision is unhealable at
            // apply time. Mirror the indexer's auto-migration here: drop the stale
            // UNIQUE and add the widened one. Idempotent (drop skipped when absent,
            // create skipped when present) and safe under the old writer, which held
            // at most one action_index per (poll, voter) so the widened key cannot
            // fail on existing rows. indexer-only.
            try {
                let votesCheck = await this.doQuery(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_name = 'votes'",
                    [this.dbName]
                );
                if(votesCheck.length > 0){
                    let staleIdx = await this.doQuery(
                        "SELECT index_name FROM information_schema.statistics " +
                        "WHERE table_schema = ? AND table_name = 'votes' AND index_name = 'poll_voter_choice' LIMIT 1",
                        [this.dbName]
                    );
                    if(staleIdx.length > 0){
                        logger.info('Schema drift on votes: stale UNIQUE(poll_voter_choice) detected. Migrating to append-only poll_voter_action_choice.');
                        await this.doQuery('ALTER TABLE `votes` DROP INDEX `poll_voter_choice`');
                    }
                    let newIdx = await this.doQuery(
                        "SELECT index_name FROM information_schema.statistics " +
                        "WHERE table_schema = ? AND table_name = 'votes' AND index_name = 'poll_voter_action_choice' LIMIT 1",
                        [this.dbName]
                    );
                    if(newIdx.length === 0){
                        await this.doQuery('CREATE UNIQUE INDEX `poll_voter_action_choice` ON `votes` (poll_index, voter_address_id, action_index, choice)');
                    }
                }
            } catch(e){
                if(e.errno !== 1146)
                    logger.error(util.format('Failed to migrate votes append-only unique index:', e));
            }

            // anchor_actions bundle-section key (indexer migration
            // 2026-08-28-anchor-actions-section-index-pk). ANCHOR v7 bundles every
            // checkpointed chain into ONE action, stored as N rows sharing an
            // action_index and separated by section_index, so the source widened
            // PRIMARY KEY (action_index) to (action_index, section_index).
            //
            // The replica's own schema self-heal cannot reach that. addMissingColumns
            // ADDs section_index (NOT NULL DEFAULT 0) because it is a column gap, and
            // ensureReplicaSecondaryIndexes above only ever ADDs secondary indexes; a
            // PRIMARY KEY is neither. So a replica bootstrapped before the migration
            // ends up with the new column under the OLD single-column key, and the
            // first v7 bundle wedges it: anchor_actions is in neither ignoreTables nor
            // upsertFullDumpTables (ClientApplier), so its rows take a plain INSERT and
            // section 1 collides with section 0 on ER_DUP_ENTRY (1062). 1062 is not in
            // ClientSync.healSchemaIfStale's {1146, 1054} heal set, so the apply
            // transaction rolls back and re-fails on every retry, forever. Same
            // unhealable-at-apply-time shape as the votes append-only key above.
            //
            // Widening is the safe direction and cannot fail on existing rows: every
            // pre-v7 row is a single body at section_index 0 (DEFAULT), so the composite
            // key is a strict superset of the key it replaces. Detection reads the
            // PRIMARY's column list from information_schema.statistics; the swap runs as
            // ONE ALTER so the table is never briefly without a primary key. Idempotent:
            // a replica already on the composite key (fresh bootstrap from the source's
            // own DDL) matches neither predicate and is skipped. indexer-only.
            try {
                let anchorCheck = await this.doQuery(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_name = 'anchor_actions'",
                    [this.dbName]
                );
                if(anchorCheck.length > 0){
                    let pkCols = await this.doQuery(
                        "SELECT column_name FROM information_schema.statistics " +
                        "WHERE table_schema = ? AND table_name = 'anchor_actions' AND index_name = 'PRIMARY' " +
                        "ORDER BY seq_in_index ASC",
                        [this.dbName]
                    );
                    let cols = pkCols.map(r => String(r.column_name || r.COLUMN_NAME || ''));
                    if(cols.length === 1 && cols[0] === 'action_index'){
                        // The column must already be present or the ADD PRIMARY KEY names
                        // an unknown column (1072) and the ALTER is refused wholesale,
                        // leaving the stale key in place. ensureReplicatedColumns runs
                        // immediately before this in replicateSchema, but SyncService calls
                        // this method on its own, so re-check and close the gap here rather
                        // than hand it to a next startup that repeats this same order.
                        let colRows = await this.doQuery(
                            "SELECT column_name FROM information_schema.columns " +
                            "WHERE table_schema = ? AND table_name = 'anchor_actions' AND column_name = 'section_index'",
                            [this.dbName]
                        );
                        let haveColumn = colRows.length > 0 ||
                            await this.ensureKeyRebuildColumn('anchor_actions', 'section_index');
                        if(!haveColumn){
                            logger.warn('anchor_actions still on PRIMARY KEY (action_index) and section_index could not be added; ' +
                                'the widened key for ANCHOR v7 bundle sections cannot be built on this replica');
                        } else {
                            logger.info('Schema drift on anchor_actions: single-column PRIMARY KEY (action_index) detected. ' +
                                'Widening to (action_index, section_index) for ANCHOR v7 bundle sections.');
                            await this.doQueryStrict(
                                'ALTER TABLE `anchor_actions` DROP PRIMARY KEY, ADD PRIMARY KEY (`action_index`, `section_index`)');
                        }
                    }
                }
            } catch(e){
                // 1146 (table absent) is a schema-shape difference, not a fault. Anything
                // else leaves the replica on a key that WILL wedge on the first v7 bundle,
                // so it is logged loudly; the apply-time 1062 is the backstop signal.
                if(e.errno !== 1146)
                    logger.error(util.format('Failed to widen the anchor_actions primary key to (action_index, section_index):', e));
            }

            // validator_rewards reward_unique qualifier key (indexer migration
            // 2026-08-24-validator-rewards-round-qualifier). The reward identity gained
            // round_qualifier, which carries snapshot_block for 'anchor_archive' and 0 for
            // every other reward type, so the source's UNIQUE key became
            // (source_id, signing_pubkey_id, reward_type, round_reference, round_qualifier).
            //
            // Nothing else converges that key on a replica. sync runs no migrations, and
            // addMissingColumns ADDs round_qualifier (NOT NULL DEFAULT 0) because it is a
            // column gap while ensureReplicaSecondaryIndexes above only ever ADDs indexes
            // that are ABSENT; reward_unique is present under both schemas, just narrower.
            // A replica bootstrapped before the migration therefore ends up with the new
            // column under the OLD four-column key, and validator_rewards is in
            // ClientApplier.ignoreTables, so the apply is INSERT IGNORE: two genuinely
            // distinct archive rewards differing only in round_qualifier collapse into one
            // row, silently, with the second row ignored rather than erroring. The table
            // declares no hash class (tableLifecycle.js), so no ledger/state hash catches
            // it either; the only tell is the advisory TABLE_COUNT_MISMATCH. Latent while
            // ANCHOR_REWARD_DERIVE_ACTIVATION is inert, which is why it has to land before
            // that gate is ratified rather than after.
            //
            // Detection is by COLUMN LIST, never by name: the index keeps the name
            // `reward_unique` under both schemas, so a presence check always reports
            // "present" and would never heal. Only the exact stale four-column definition
            // is migrated; an already-correct key is a silent no-op, and any other shape is
            // left alone and reported rather than guessed at. Widening cannot fail on
            // existing rows (the new key is a strict superset and round_qualifier is NOT
            // NULL DEFAULT 0, so every pre-existing row keeps the key it had), and the drop
            // and the add ride ONE ALTER so the table is never briefly keyless. indexer-only.
            try {
                let rewardsCheck = await this.doQueryStrict(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_name = 'validator_rewards'",
                    [this.dbName]
                );
                if(rewardsCheck.length > 0){
                    let idxCols = await this.doQueryStrict(
                        "SELECT column_name FROM information_schema.statistics " +
                        "WHERE table_schema = ? AND table_name = 'validator_rewards' AND index_name = 'reward_unique' " +
                        "ORDER BY seq_in_index ASC",
                        [this.dbName]
                    );
                    let cols  = idxCols.map(r => String(r.column_name || r.COLUMN_NAME || ''));
                    let stale = ['source_id', 'signing_pubkey_id', 'reward_type', 'round_reference'];
                    if(cols.length === stale.length && cols.every((c, i) => c === stale[i])){
                        // The ADD names round_qualifier, so an absent column makes the whole
                        // ALTER errno 1072 and the stale key survives with no signal. The
                        // column self-heal (ensureReplicatedColumns / addMissingColumns) runs
                        // earlier in replicateSchema, but SyncService calls this method on its
                        // own, so re-check and close the gap here rather than hand it to a next
                        // startup that repeats this same order.
                        let colRows = await this.doQueryStrict(
                            "SELECT column_name FROM information_schema.columns " +
                            "WHERE table_schema = ? AND table_name = 'validator_rewards' AND column_name = 'round_qualifier'",
                            [this.dbName]
                        );
                        let haveColumn = colRows.length > 0 ||
                            await this.ensureKeyRebuildColumn('validator_rewards', 'round_qualifier');
                        if(!haveColumn){
                            logger.warn('validator_rewards still on the four-column reward_unique and round_qualifier could not be added; ' +
                                'the archive reward identity stays ambiguous on this replica');
                        } else {
                            logger.info('Schema drift on validator_rewards: four-column UNIQUE reward_unique detected. ' +
                                'Rebuilding with round_qualifier for the anchor_archive reward identity.');
                            await this.doQueryStrict(
                                'ALTER TABLE `validator_rewards` DROP INDEX `reward_unique`, ' +
                                'ADD UNIQUE INDEX `reward_unique` ' +
                                '(`source_id`, `signing_pubkey_id`, `reward_type`, `round_reference`, `round_qualifier`)');
                        }
                    }
                }
            } catch(e){
                // 1146 (table absent) is a schema-shape difference, not a fault. Anything
                // else leaves the replica on a key that silently DEDUPLICATES two distinct
                // archive rewards, which no hash and no halt would ever surface, so it is
                // logged loudly.
                if(e.errno !== 1146)
                    logger.error(util.format('Failed to rebuild the validator_rewards reward_unique key with round_qualifier:', e));
            }
        }
    }

    // Widen the raw-wire-field columns to utf8mb4 on an already-existing replica.
    //
    // The source indexer converges through a dated migration
    // (2026-09-02-utf8mb4-raw-wire-fields.sql and its NOT NULL pair). sync runs no
    // migrations: a replica's tables are copied from the source's SHOW CREATE TABLE at
    // bootstrap, and addMissingColumns only ever ADDs a column, never retypes one. So a
    // replica built before that migration keeps utf8mb3 on these columns forever - and the
    // moment the widened ORIGIN accepts a 4-byte character (a contract whose source carries
    // an emoji, an EXECUTE method name, a VOTE quorum), every aged follower halts applying
    // that block with errno 1366 while the source runs on. This is the replica half of that
    // migration, and it has to land with it: an origin that can hold the bytes and a
    // follower that cannot is a fleet-wide halt with no schema error anywhere upstream.
    //
    // src/schema/utf8mb4_columns.js is the byte-identical twin of the indexer's copy, so the two
    // sides cannot disagree about which columns are in the set or what shape they take.
    //
    // Idempotent and additive: a column already utf8mb4 is skipped (so a snapshot-bootstrapped
    // replica pays one information_schema read per table and nothing else), an absent table or
    // column is skipped, and the widen only ever grows the accepted byte domain - utf8mb3 is a
    // strict subset of utf8mb4, so no stored value is rewritten and no row is lost. The
    // per-table clauses ride ONE ALTER because each ALTER is a COPY rebuild under a metadata
    // lock. indexer replicas only (decoder replicas hold none of these tables).
    async ensureReplicaUtf8mb4Columns(){
        if(this.dbType !== 'indexer') return;
        for(const [table, entries] of utf8mb4Columns.byTable()){
            let rows;
            try {
                // rethrow, not the fail-soft default: outside a transaction doQuery logs a
                // driver fault and returns [], which the absent-table branch below would read
                // as "this replica does not carry the table" and skip silently, leaving the
                // follower narrow. A transient fault must look like a fault.
                rows = await this.doQuery(
                    "SELECT COLUMN_NAME, CHARACTER_SET_NAME FROM information_schema.columns " +
                    "WHERE table_schema = ? AND table_name = ?",
                    [this.dbName, table],
                    null,
                    { rethrow: true }
                );
            } catch(e){
                logger.error(util.format('Failed to read the column charsets of ' + table + ' while widening to utf8mb4:', e));
                continue;
            }
            if(!rows || rows.length === 0) continue;   // table absent on this replica

            let live = new Map();
            for(const row of rows)
                live.set(String(row.COLUMN_NAME || row.column_name || '').toLowerCase(), row);

            let pending = entries.filter(entry => {
                let row = live.get(entry.column.toLowerCase());
                return row !== undefined && !utf8mb4Columns.isAlreadyUtf8mb4(row);
            });
            if(pending.length === 0) continue;

            try {
                await this.doQuery('ALTER TABLE `' + table + '` ' +
                    pending.map(utf8mb4Columns.modifyClause).join(', '), [], null, { rethrow: true });
                logger.info('Widened ' + pending.length + ' raw-wire-field column(s) on ' + table +
                    ' to utf8mb4 in ' + this.dbName + ': ' + pending.map(e => e.column).join(', '));
            } catch(e){
                // Not fatal to startup: the replica is exactly as usable as it was a moment
                // ago, and every other table still converges. But it stays wedge-capable on
                // these columns, and nothing downstream would say so, so log it loudly.
                logger.error(util.format('Failed to widen ' + table + ' to utf8mb4 (errno ' + ((e && e.errno) || 'unknown') +
                    '); this replica still halts on a 4-byte character in ' +
                    pending.map(e => e.column).join(', '), e));
            }
        }
    }

    // Get a database connection (with exponential backoff + circuit breaker).
    // Returns the active shared transaction connection when one is open, so every
    // query on this Db instance funnels through that same transaction.
    async getConnection(){
        if(this.transactionConnection)
            return this.transactionConnection;
        return await this.acquirePoolConnection();
    }

    // Acquire a fresh connection straight from the pool, bypassing the shared
    // transactionConnection. Carries the same exponential-backoff retry + circuit
    // breaker as getConnection. Used by getConnection() and by beginReadSnapshot(),
    // which needs a DEDICATED connection: a snapshot read must never pin the shared
    // writer connection, or a concurrent /snapshot read and the live ServerPoller
    // writer would collide on one connection (and the snapshot's commit/release
    // would pull the connection out from under in-flight writes).
    async acquirePoolConnection(){
        // Circuit breaker: reject immediately if open
        if(this.circuitState === 'open'){
            if(Date.now() < this.circuitOpenUntil)
                this.util.throwError('Circuit breaker open: database connections rejected until cooldown expires');
            this.circuitState = 'half-open';
            logger.info('Circuit breaker half-open: attempting reconnection');
        }
        let connection  = null;
        let attempts    = 0;
        let maxAttempts = 30;
        let baseDelay   = 500;
        let maxDelay    = 15000;
        while(connection == null){
            try {
                connection = await this.pool.getConnection();
                if(this.circuitState === 'half-open'){
                    this.circuitState = 'closed';
                    this.circuitFailures = 0;
                    logger.info('Circuit breaker closed: database connection restored');
                }
                this.circuitFailures = 0;
            } catch (e){
                attempts++;
                this.circuitFailures = (this.circuitFailures || 0) + 1;
                if(this.circuitFailures >= this.circuitThreshold){
                    this.circuitState = 'open';
                    this.circuitOpenUntil = Date.now() + this.circuitCooldown;
                    this.util.throwError('Circuit breaker opened after ' + this.circuitFailures + ' consecutive failures');
                }
                if(attempts >= maxAttempts)
                    this.util.throwError('Could not connect to MariaDB after ' + maxAttempts + ' attempts');
                let delay = Math.min(baseDelay * Math.pow(2, attempts - 1), maxDelay);
                let jitter = Math.floor(Math.random() * delay * 0.3);
                logger.error(util.format('MariaDB connection attempt ' + attempts + '/' + maxAttempts + ' failed. Retrying in ' + (delay + jitter) + 'ms...', e))
                connection = null;
                await this.util.sleep(delay + jitter);
            }
        }
        return connection;
    }

    async releaseConnection(){
        if(this.transactionConnection != null){
            await this.transactionConnection.release();
            this.transactionConnection = null;
        }
    }

    async beginTransaction(){
        if(this.transactionConnection != null)
            await this.releaseConnection();
        this.transactionConnection = await this.getConnection();
        try {
            await this.transactionConnection.beginTransaction();
        } catch(e){
            await this.transactionConnection.release();
            this.transactionConnection = null;
            this.util.throwError('beginTransaction error=' + e);
        }
    }

    async rollbackTransaction(){
        if(this.transactionConnection != null){
            // Log the DB name and type so a rollback entry in the journal is
            // traceable to the specific replica/source DB that triggered it, rather
            // than appearing as an anonymous "rolling back" with no context.
            logger.info('Rolling back transaction on ' + this.dbName + ' (' + this.dbType + ')');
            try {
                await this.transactionConnection.rollback();
            } finally {
                await this.transactionConnection.release();
                this.transactionConnection = null;
            }
        }
    }

    async commitTransaction(){
        if(this.transactionConnection != null){
            try {
                await this.transactionConnection.commit();
                await this.transactionConnection.release();
                this.transactionConnection = null;
                return true;
            } catch (e){
                logger.error(util.format('Error committing transaction:', e))
                try {
                    await this.transactionConnection.rollback();
                } finally {
                    await this.transactionConnection.release();
                    this.transactionConnection = null;
                }
                this.util.throwError('commitTransaction error=' + e);
            }
        }
        return false;
    }

    // Begin a read-only REPEATABLE READ transaction with a consistent snapshot.
    // Used by snapshot reads so the block-height anchor, the hash headers, and
    // every paginated table read all observe the database at a single point in
    // time. Without this, a concurrent block commit mid-read can produce a
    // snapshot whose advertised hashes (captured first) disagree with the row
    // data (read later), failing hash verification on the consuming validator.
    // InnoDB MVCC means this read view does not block writers (the snapshot
    // source keeps committing new blocks while the read view stays pinned).
    // Isolation is set explicitly rather than relying on the server default so
    // the guarantee holds regardless of how the source DB is configured.
    //
    // Returns a DEDICATED connection (not the shared this.transactionConnection)
    // that the caller threads into its reads (getLastBlock/getBlockHashRow/
    // streamTableRows/... all accept an optional conn) and ends via
    // commitReadSnapshot/rollbackReadSnapshot. Using a dedicated connection lets
    // multiple snapshots run concurrently and keeps the live writer (ServerPoller,
    // TransparencyLog) off the snapshot's read view entirely.
    async beginReadSnapshot(){
        let conn = await this.acquirePoolConnection();
        try {
            await conn.query('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ');
            await conn.query('START TRANSACTION WITH CONSISTENT SNAPSHOT');
        } catch(e){
            try { await conn.release(); } catch(_e){ /* already gone */ }
            this.util.throwError('beginReadSnapshot error=' + e);
        }
        return conn;
    }

    // End a read snapshot opened by beginReadSnapshot and return its dedicated
    // connection to the pool. A read-only snapshot has nothing to persist, so the
    // commit just closes the transaction; the release returns the connection.
    async commitReadSnapshot(conn){
        if(conn == null) return;
        try {
            await conn.commit();
        } finally {
            await conn.release();
        }
    }

    // Abort a read snapshot and release its dedicated connection. Best-effort
    // rollback; the connection is released regardless.
    async rollbackReadSnapshot(conn){
        if(conn == null) return;
        try {
            await conn.rollback();
        } catch(e){
            /* best-effort; still release below */
        } finally {
            await conn.release();
        }
    }

    // Run a query and return results.
    // conn (optional): run on this explicit connection instead of acquiring one.
    // Used by read-snapshot reads (beginReadSnapshot). The caller owns that
    // connection's lifecycle (commit/rollback/release), so errors propagate.
    async doQuery(query, args, conn, opts){
        let results = [];
        if(!this.util.isNull(query)){
            if(Array.isArray(args)){
                for(let i = 0; i < args.length; i++){
                    // Buffers (binary/blob column values) must reach the driver intact.
                    // toString() would UTF-8-decode and corrupt them. Other objects keep
                    // the legacy stringify coercion.
                    if(args[i] !== null && args[i] !== undefined && typeof args[i] === 'object' && !Buffer.isBuffer(args[i]))
                        args[i] = args[i].toString();
                }
            }
            if(conn)
                return await conn.query(query, args);
            let tx = this.transactionConnection != null;
            let db = await this.getConnection();
            try {
                results = await db.query(query, args);
            } catch (error){
                this.util.logError('Error running database query:', error);
                // Inside a transaction the error always propagates (the caller owns the
                // rollback). Outside a transaction callers historically get [] on failure
                // (fail-soft), which is wrong for a fail-CLOSED reader that must not treat
                // a transient DB error as an authoritative empty result: those pass
                // opts.rethrow so the error surfaces (e.g. the durable halt check).
                if(tx || (opts && opts.rethrow)) throw error;
            } finally {
                if(!tx) await db.release();
            }
        }
        return results;
    }

    // Like doQuery, but a query error ALWAYS throws, transactional or not.
    // The indexer twin carries the same method (xchain-indexer/src/db.js), so a
    // byte-identical consensus module can call it on either side.
    //
    // For consensus-input reads: doQuery collapses a non-transactional query
    // error into [], indistinguishable from a genuinely empty result, so a
    // transient DB fault becomes "no data" on this node only and can fork the
    // ledger (M-17). Inside a transaction the two are equivalent; outside one
    // (snapshot seeding, tooling) they are not, which is exactly where the
    // sub-tree derivations run without a transaction.
    async doQueryStrict(query, args, conn){
        return await this.doQuery(query, args, conn, { rethrow: true });
    }

    async close(){
        try {
            await this.pool.end();
        } catch(e){
            logger.info(util.format('Error closing database pool:', e));
        }
    }

}

// Installed NON-ENUMERABLY, exactly as a class method is. An enumerable
// prototype property would show up in for...in over an instance and in any
// shallow clone of it, which a plain Object.assign install would have changed
// about a class that has never had one.
for(const file of MIXIN_FILES){
    const mixin = require(file);
    for(const name of Object.keys(mixin))
        Object.defineProperty(Database.prototype, name, {
            value: mixin[name], enumerable: false, writable: true, configurable: true
        });
}


// Exposed for the unit suite (and the indexer-twin drift check): the weightless-row
// guard is consensus-relevant, so it is tested directly, not only through a query.
Database.requireStakeWeight = requireStakeWeight;

module.exports = Database;
