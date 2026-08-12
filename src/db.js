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
 * XChain Sync - Database Class
 *
 * This file handles connecting to MariaDB and running SQL queries.
 * Simplified from xchain-indexer/src/db.js (no action processing,
 * just connection pool, circuit breaker, and query execution).
 *
 * Supports both 'indexer' and 'decoder' DBs via the dbType parameter.
 * Most queries are schema-agnostic; the indexer-specific block-hash join
 * (ledger_hash/actions_hash/contract_hash) only runs for dbType='indexer'.
 *
 ********************************************************************/

const mariadb    = require('mariadb');
const fs         = require('fs');
const path       = require('path');
const validation = require('./validation');
const { splitSqlStatements } = require('./sqlUtil');
const { canonicalizeHashAddress } = require('./protocolAddressRoles');
const poolSizing = require('./poolSizing');
const swqCap = require('./swq_source_cap_activation');
const { isStateKeyBinCollationActive } = require('./state_key_collation_activation');

// Guard for the few queries that must interpolate a table name into a
// backtick-quoted identifier (COUNT(*), pagination, TRUNCATE). Parameter
// binding cannot carry identifiers. Some callers pass server-supplied names
// (e.g. a sync source's `table_counts` keys), so a stray backtick or
// metacharacter here would break out of the quoting. Reject anything that
// isn't a plain [A-Za-z0-9_] identifier before it reaches the query string.
function assertValidIdentifier(table){
    const check = validation.validateIdentifier(table);
    if(!check.valid)
        throw new Error('Refusing to query unsafe table identifier: ' + check.reason);
}

// Delay between attempts in the infinite DB-connection retry loops
// (verifyDatabase / createDatabase). Named so the cadence lives in one place
// and is not confused with the unrelated connectTimeout in the pool config.
const DB_RETRY_DELAY_MS = 5000;

class Database {

    constructor(host, port, dbName, user, pass, util, dbType) {
        this.host   = host;
        this.port   = port;
        this.dbName = dbName;
        this.user   = user;
        this.pass   = pass;
        this.util   = util;
        this.dbType = dbType || 'indexer';  // 'indexer' (default) or 'decoder'

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
            // Return DATETIME columns as MariaDB-format strings rather than
            // JS Dates. JSON.stringify would otherwise emit Date as ISO
            // ('2023-11-15T06:13:21.000Z'), which MariaDB strict mode rejects
            // on re-insert. Affects the decoder DATETIME column events.time
            // (decoder dispensers.expiration is now a BIGINT unix timestamp,
            // replicated as a number via bigIntAsNumber); indexer schemas use
            // INTEGER timestamps, so this is a no-op there.
            dateStrings:        true,
            minDelayValidation: 3000,
            queryTimeout:       poolParams.queryTimeout
        };

        // Setup pool of connections
        this.pool = mariadb.createPool(this.connectionPoolParams);
        this.transactionConnection = null;

        // Circuit breaker state
        this.circuitState     = 'closed';
        this.circuitFailures  = 0;
        this.circuitThreshold = 10;
        this.circuitCooldown  = 30000;
        this.circuitOpenUntil = 0;
    }

    // Verify a database exists
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
                console.error('Error checking if database ' + this.dbName + ' exists:', e)
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

    // Create a database if it doesn't exist
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
        console.log("Creating " + this.dbName + " database!");
        while(true){
            try {
                let db = await mariadb.createConnection(connectionParams);
                await db.query("CREATE DATABASE IF NOT EXISTS `" + this.dbName + "`");
                await db.end();
                return true;
            } catch(e){
                console.error('Error creating database ' + this.dbName + ':', e)
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
        for(let file of files){
            if(file.indexOf('.sql') !== -1){
                let table = file.substring(0, file.indexOf('.sql'));
                if(this.dbType !== 'indexer' && table !== 'sync_halt') continue;
                console.log('Verifying ' + table + ' table exists...');
                try {
                    let results = await db.query("SELECT * FROM information_schema.tables WHERE table_schema = ? AND table_name = ?", [this.dbName, table]);
                    if(results.length === 0)
                        await this._createTableFromFile(file);
                } catch(e){
                    this.util.throwError('Error verifying ' + table + ' table: ' + e);
                    return false;
                }
            }
        }
        await db.release();
        return true;
    }

    // Create a table from a local SQL file (only used for sync-service-owned tables like sync_meta)
    async _createTableFromFile(file){
        let dir     = path.join(__dirname, 'sql');
        let data    = fs.readFileSync(dir + '/' + file, "utf8");
        let table   = file.substring(0, file.indexOf('.sql'));
        let queries = splitSqlStatements(data);
        console.log('Creating ' + table + ' table and indexes...');
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
    // when a generated ALTER was refused by the server : the column is
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
    // below stayed broken .
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
                console.error('Skipping invalid column name ' + col + ' on ' + tableName + ' (' + colCheck.reason + ')');
                continue;
            }

            let def = validation.extractColumnDefinition(sourceDdl, col);
            if(!def){
                console.warn('Could not extract definition for column ' + col + ' on ' + tableName + '; skipping (manual ALTER may be required)');
                continue;
            }

            let actions = ['ADD COLUMN ' + def];
            if(validation.isAutoIncrementDefinition(def)){
                let keyAction = await this._autoIncrementKeyAction(tableName, col, sourceDdl);
                actions.push(keyAction);
            }
            let alter = "ALTER TABLE `" + tableName + "` " + actions.join(', ');

            try {
                // doQueryStrict, not doQuery: outside a transaction doQuery logs the
                // error and returns [], so the success log below fired on a REFUSED
                // ALTER and reported "Added column" for a column that does not exist.
                await this.doQueryStrict(alter);
                console.log('Added column ' + col + ' to ' + tableName);
                added++;
            } catch(e){
                failed.push({ column: col, errno: (e && e.errno) || null, message: (e && e.message) || String(e) });
                console.error('FAILED to add column ' + col + ' to ' + tableName +
                    ' (errno ' + ((e && e.errno) || 'unknown') + '); the column is still missing. ALTER was: ' + alter, e);
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
    // which is what wedged the decoder replicas' pubkeys.id self-heal .
    //
    // The key is taken from the SOURCE DDL so the replica converges on the
    // source's own definition rather than a guess. A source PRIMARY KEY is only
    // reproducible when the replica has no primary key yet; otherwise (and when
    // the source's covering key is multi-column or absent) a UNIQUE key on the
    // column alone satisfies the auto-increment requirement without disturbing
    // the existing keys.
    async _autoIncrementKeyAction(tableName, col, sourceDdl){
        let key = validation.extractKeyForColumn(sourceDdl, col);

        if(key && key.type === 'primary' && !(await this._hasPrimaryKey(tableName)))
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
    async _hasPrimaryKey(tableName){
        try {
            let rows = await this.doQueryStrict(
                "SELECT index_name FROM information_schema.statistics WHERE table_schema = ? AND table_name = ? AND index_name = 'PRIMARY' LIMIT 1",
                [this.dbName, tableName]
            );
            return rows.length > 0;
        } catch(e){
            console.error('Could not read primary-key state for ' + tableName + '; assuming one exists:', e);
            return true;
        }
    }

    // Replicate schema from a source database into this database.
    // Reads all table DDLs from the source via SHOW CREATE TABLE and
    // creates any missing tables locally. For tables that already exist,
    // propagates any columns the source has added since the replica was
    // bootstrapped (see addMissingColumns). This ensures the replica always
    // matches the authoritative indexer schema (no copied SQL files needed).
    async replicateSchema(sourceDb){
        console.log('Replicating schema from ' + sourceDb.dbName + ' into ' + this.dbName + '...');

        // Get list of all tables in the source database
        let sourceTables = await sourceDb.doQuery(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_type = 'BASE TABLE' ORDER BY table_name",
            [sourceDb.dbName]
        );

        // Get list of existing tables in this (target) database
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
                console.error('Skipping invalid table name: ' + tableName + ' (' + idCheck.reason + ')');
                continue;
            }

            // Get the CREATE TABLE DDL from the source
            let ddlRows = await sourceDb.doQuery("SHOW CREATE TABLE `" + tableName + "`");
            if(ddlRows.length === 0) continue;

            let createSql = ddlRows[0]['Create Table'];
            if(!createSql) continue;

            // Validate DDL before executing
            let ddlCheck = validation.validateDdl(createSql);
            if(!ddlCheck.valid){
                console.error('Rejected DDL for ' + tableName + ': ' + ddlCheck.reason);
                continue;
            }

            // Table already exists on the replica: don't recreate it, but
            // propagate any columns the source has added since it was created.
            if(existingSet.has(tableName)){
                // A refused ALTER now throws . Keep the sweep going over the
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

            console.log('Creating table ' + tableName + '...');
            try {
                await this.doQuery(createSql);
                created++;
            } catch(e){
                // Table may reference another table not yet created; retry later.
                console.log('Deferred: ' + tableName + ':', e);
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
                    console.error('Rejected DDL for ' + tableName + ' (retry): ' + ddlCheck.reason);
                    continue;
                }

                try {
                    await this.doQuery(createSql);
                    console.log('Created table ' + tableName + ' (retry)');
                } catch(e){
                    console.error('Failed to create table ' + tableName + ':', e);
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

        if(columnFailures.length){
            let err = new Error('Schema replication into ' + this.dbName + ' left columns missing: ' +
                columnFailures.map(f => f.table + ' (errno ' + f.errno + ')').join(', '));
            err.columnFailures = columnFailures;
            throw err;
        }

        console.log('Schema replication complete for ' + this.dbName);
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
              definition: 'CHAR(64) NULL AFTER `contract_state_root_shadow`' }
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

            console.log('Schema drift on ' + table + '.' + column + ': column missing on replica. Adding ' + definition + '.');
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

            console.log('Schema drift on ' + table + '.' + column + ': NOT NULL on replica but nullable upstream. Relaxing to allow NULL.');
            await this.doQuery('ALTER TABLE `' + table + '` MODIFY COLUMN `' + column + '` ' + type);
        }

        // AUTO_INCREMENT repair for hub-mirror id cursors. The indexer reconciler
        // previously stripped AUTO_INCREMENT from the id column of these four tables
        // on every startup (migration 2026-06-10-mirror-id-autoincrement-repair).
        // Origins self-heal via that migration, but replicas bootstrapped from a
        // stripped-state origin cloned the stripped DDL and have no automated path.
        // Detect a missing AUTO_INCREMENT on the id column and restore it here.
        // Idempotent: MODIFY to the same definition is a no-op; table absent = skip.
        // #3713
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

            console.log('Schema drift on ' + table + '.id: AUTO_INCREMENT missing on replica. Repairing.');
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
                console.log('Repaired AUTO_INCREMENT on ' + table + '.id');
            } catch(e){
                // errno 1146 (table absent) or 1054 (column absent) can race; log and continue.
                console.error('Failed to repair AUTO_INCREMENT on ' + table + '.id:', e);
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
                        console.log('Schema drift on attests.request_status: ENUM missing \'rejected\'. Widening to canonical set.');
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
                    console.error('Failed to widen attests.request_status ENUM:', e);
            }
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
        let ensureIndexes = [
            { table: 'index_tickers',   indexName: 'block_index', columns: '(block_index)' },
            { table: 'index_addresses', indexName: 'block_index', columns: '(block_index)' }
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
                console.log('Added secondary index ' + indexName + ' to ' + table + ' on ' + this.dbName);
            } catch(e){
                // errno 1061 = duplicate key name (race with another startup); harmless.
                if(e.errno !== 1061)
                    console.error('Failed to add secondary index ' + indexName + ' to ' + table + ':', e);
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
                            console.log('Schema drift on attests: UNIQUE(request_id_version) detected. Relaxing to non-unique.');
                            await this.doQuery('ALTER TABLE `attests` DROP INDEX `request_id_version`');
                            await this.doQuery('CREATE INDEX `request_id_version` ON `attests` (request_id, version)');
                        }
                    }
                }
            } catch(e){
                if(e.errno !== 1146)
                    console.error('Failed to relax attests request_id_version index:', e);
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
                        console.log('Schema drift on votes: stale UNIQUE(poll_voter_choice) detected. Migrating to append-only poll_voter_action_choice.');
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
                    console.error('Failed to migrate votes append-only unique index:', e);
            }
        }
    }

    // Get a database connection (with exponential backoff + circuit breaker).
    // Returns the active shared transaction connection when one is open, so every
    // query on this Db instance funnels through that same transaction.
    async getConnection(){
        if(this.transactionConnection)
            return this.transactionConnection;
        return await this._acquirePoolConnection();
    }

    // Acquire a fresh connection straight from the pool, bypassing the shared
    // transactionConnection. Carries the same exponential-backoff retry + circuit
    // breaker as getConnection. Used by getConnection() and by beginReadSnapshot(),
    // which needs a DEDICATED connection: a snapshot read must never pin the shared
    // writer connection, or a concurrent /snapshot read and the live ServerPoller
    // writer would collide on one connection (and the snapshot's commit/release
    // would pull the connection out from under in-flight writes).
    async _acquirePoolConnection(){
        // Circuit breaker: reject immediately if open
        if(this.circuitState === 'open'){
            if(Date.now() < this.circuitOpenUntil)
                this.util.throwError('Circuit breaker open: database connections rejected until cooldown expires');
            this.circuitState = 'half-open';
            console.log('Circuit breaker half-open: attempting reconnection');
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
                    console.log('Circuit breaker closed: database connection restored');
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
                console.error('MariaDB connection attempt ' + attempts + '/' + maxAttempts + ' failed. Retrying in ' + (delay + jitter) + 'ms...', e)
                connection = null;
                await this.util.sleep(delay + jitter);
            }
        }
        return connection;
    }

    // Release a connection
    async releaseConnection(){
        if(this.transactionConnection != null){
            await this.transactionConnection.release();
            this.transactionConnection = null;
        }
    }

    // Begin a SQL transaction
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

    // Roll back a SQL transaction
    async rollbackTransaction(){
        if(this.transactionConnection != null){
            // Log the DB name and type so a rollback entry in the journal is
            // traceable to the specific replica/source DB that triggered it, rather
            // than appearing as an anonymous "rolling back" with no context.
            console.log('Rolling back transaction on ' + this.dbName + ' (' + this.dbType + ')');
            try {
                await this.transactionConnection.rollback();
            } finally {
                await this.transactionConnection.release();
                this.transactionConnection = null;
            }
        }
    }

    // Commit a SQL transaction
    async commitTransaction(){
        if(this.transactionConnection != null){
            try {
                await this.transactionConnection.commit();
                await this.transactionConnection.release();
                this.transactionConnection = null;
                return true;
            } catch (e){
                console.error('Error committing transaction:', e)
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
        let conn = await this._acquirePoolConnection();
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

    // Get the last block index from the blocks table
    async getLastBlock(conn){
        let query = "SELECT MAX(block_index) AS block_index FROM blocks";
        let rows  = await this.doQuery(query, null, conn);
        if(rows.length > 0 && rows[0].block_index !== null)
            return Number(rows[0].block_index);
        return null;
    }

    // Read the replication engine's own view of this node's freshness (#3904).
    // getLastBlock above reads the SERVED database, so on a node fronting a native
    // SQL replica the source and served heights are one failure domain: replication
    // stops applying, both freeze at the same number, and lag_blocks publishes 0
    // while the node is hours behind. Only the replication subsystem can tell those
    // apart. Returns { isReplica, running, secondsBehind }: isReplica false on a
    // primary/co-located source (empty result set), null when the read itself was
    // refused (missing REPLICATION CLIENT grant), which callers must treat as stale.
    async getReplicaStatus(conn){
        let rows;
        try {
            rows = await this.doQueryStrict("SHOW REPLICA STATUS", null, conn);
        } catch (error){
            // Pre-10.5 servers only know the SLAVE spelling; anything else is a
            // genuine failure and falls through to the unknown result below.
            try {
                rows = await this.doQueryStrict("SHOW SLAVE STATUS", null, conn);
            } catch (fallbackError){
                if(!this._replicaStatusWarned){
                    this._replicaStatusWarned = true;
                    this.util.logError('Cannot read replication status (needs REPLICATION CLIENT):', fallbackError);
                }
                return { isReplica: null, running: null, secondsBehind: null };
            }
        }
        if(!rows || rows.length === 0)
            return { isReplica: false, running: null, secondsBehind: null };
        let row = rows[0];
        let io  = row.Replica_IO_Running  != null ? row.Replica_IO_Running  : row.Slave_IO_Running;
        let sql = row.Replica_SQL_Running != null ? row.Replica_SQL_Running : row.Slave_SQL_Running;
        let behind = row.Seconds_Behind_Source != null ? row.Seconds_Behind_Source : row.Seconds_Behind_Master;
        return {
            isReplica: true,
            running: io === 'Yes' && sql === 'Yes',
            // NULL here means the SQL thread is not applying at all, never "0 behind".
            secondsBehind: behind == null ? null : Number(behind)
        };
    }

    // Get block hash data for a given block_index.
    // Indexer: joins to index_transactions for the synthetic ledger/actions/contract hashes.
    // Decoder: joins only to the blockchain block hash (via block_hash_id -> index_transactions).
    // Decoder DB has no synthetic chain-of-state hashes.
    // --- Divergence halt (sync_halt) ------------------------------------------
    // Durably record a confirmed cross-source consensus divergence so a halted
    // client does not silently resume onto a contested chain after a restart.

    async recordHalt(dbType, blockIndex, reason, mismatches, sources){
        // Idempotent: don't stack duplicate active halts for the same block. Tolerate a
        // transient read failure in this pre-check by falling through to the INSERT -
        // recording the halt IS the durability guarantee, so it must never be skipped
        // on a read blip (getActiveHalt now fails closed / throws).
        let existing = null;
        try { existing = await this.getActiveHalt(dbType); } catch(e){ existing = null; }
        if(existing && Number(existing.block_index) === Number(blockIndex)) return existing;
        await this.doQuery(
            "INSERT INTO sync_halt (db_type, block_index, reason, mismatches, sources) VALUES (?, ?, ?, ?, ?)",
            [dbType, blockIndex, String(reason || 'divergence').slice(0, 64),
             JSON.stringify(mismatches || []), JSON.stringify(sources || [])]
        );
        try { return await this.getActiveHalt(dbType); } catch(e){ return null; }
    }

    // Fail CLOSED: pass rethrow so a transient query error PROPAGATES instead of
    // returning [] (which the caller would read as "no active halt" and silently
    // resume a possibly-halted replica onto a contested chain). Callers that must
    // tolerate a read failure (recordHalt's pre-check) catch it explicitly.
    async getActiveHalt(dbType){
        let rows = await this.doQuery(
            "SELECT * FROM sync_halt WHERE db_type=? AND cleared_at IS NULL ORDER BY id DESC LIMIT 1",
            [dbType], null, { rethrow: true }
        );
        return (rows && rows.length) ? rows[0] : null;
    }

    // --- Durable client key/value markers (sync_state) ------------------------
    // Small durable store for per-client state that must survive a restart but is
    // not block data (mirrors the sync_halt durable-state pattern). Created on
    // demand so no schema file / migration is needed on existing replicas. Keys are
    // namespaced by the caller (e.g. 'bootstrap_base:indexer'). Used by ClientSync
    // to persist the truncated-replica join floor (_bootstrapBase): an in-memory-only
    // field is lost on restart, dropping the join-block recompute skip and the
    // truncation floor that protects against an in-window reorg below `base`.
    async _ensureSyncStateTable(){
        if(this._syncStateReady) return;
        await this.doQuery(
            "CREATE TABLE IF NOT EXISTS sync_state (" +
            "  state_key   VARCHAR(128) NOT NULL PRIMARY KEY," +
            "  state_value TEXT," +
            "  updated_at  DATETIME DEFAULT CURRENT_TIMESTAMP" +
            ") ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci"
        );
        this._syncStateReady = true;
    }

    // Read a durable client marker. Returns the stored string, or null if absent
    // (or if the table cannot be reached: fail-soft, the caller treats null as
    // "no persisted value" and falls back to its in-memory default).
    async getSyncState(key){
        try {
            await this._ensureSyncStateTable();
            let rows = await this.doQuery("SELECT state_value FROM sync_state WHERE state_key=? LIMIT 1", [key]);
            return (rows && rows.length) ? rows[0].state_value : null;
        } catch(e){
            console.error('getSyncState(' + key + ') failed (treating as unset):', e);
            return null;
        }
    }

    // Write a durable client marker (upsert). Fail-soft: a persistence failure is
    // logged, never thrown, so it cannot abort a bootstrap/catch-up.
    async setSyncState(key, value){
        try {
            await this._ensureSyncStateTable();
            await this.doQuery(
                "INSERT INTO sync_state (state_key, state_value) VALUES (?, ?) " +
                "ON DUPLICATE KEY UPDATE state_value=VALUES(state_value), updated_at=CURRENT_TIMESTAMP",
                [key, value == null ? null : String(value)]
            );
            return true;
        } catch(e){
            console.error('setSyncState(' + key + ') failed (continuing):', e);
            return false;
        }
    }

    // Delete a durable client marker. Fail-soft: a persistence failure is logged,
    // never thrown, so it cannot abort a bootstrap/catch-up.
    async deleteSyncState(key){
        try {
            await this._ensureSyncStateTable();
            await this.doQuery("DELETE FROM sync_state WHERE state_key=?", [key]);
            return true;
        } catch(e){
            console.error('deleteSyncState(' + key + ') failed (continuing):', e);
            return false;
        }
    }

    async clearHalt(dbType){
        let res = await this.doQuery(
            "UPDATE sync_halt SET cleared_at=NOW() WHERE db_type=? AND cleared_at IS NULL",
            [dbType]
        );
        return res ? res.affectedRows : 0;
    }

    async getBlockHashRow(block_index, conn){
        let query;
        if(this.dbType === 'decoder'){
            query = `SELECT
                    b.block_index,
                    b.block_time,
                    t1.hash as block_hash
                FROM
                    blocks b
                    LEFT JOIN index_transactions t1 ON (t1.id=b.block_hash_id)
                WHERE
                    b.block_index=?`;
        } else {
            query = `SELECT
                    b.block_index,
                    b.block_time,
                    t1.hash as ledger_hash,
                    t2.hash as actions_hash,
                    t3.hash as contract_hash,
                    t4.hash as state_hash
                FROM
                    blocks b
                    LEFT JOIN index_transactions t1 ON (t1.id=b.ledger_hash_id)
                    LEFT JOIN index_transactions t2 ON (t2.id=b.actions_hash_id)
                    LEFT JOIN index_transactions t3 ON (t3.id=b.contract_hash_id)
                    LEFT JOIN index_transactions t4 ON (t4.id=b.state_hash_id)
                WHERE
                    b.block_index=?`;
        }
        let rows = await this.doQuery(query, [block_index], conn);
        if(rows.length > 0)
            return rows[0];
        return null;
    }

    // Light-client state-commitment roots for a block (SPV spec sec.4-5). Returns
    // { balances_root, stakes_root, state_root, block_merkle_root } or null. Used by
    // the follower's incremental SMT (reads block-1's balances_root) and by
    // ServerPoller to attach the committed roots to the outgoing block payload.
    async getStateRootsRow(chain, network, block_index, conn){
        let rows = await this.doQuery(
            `SELECT balances_root, stakes_root, state_root, block_merkle_root
             FROM state_tree_roots
             WHERE chain=? AND network=? AND block_index=? LIMIT 1`,
            [chain, network, block_index], conn);
        return rows.length ? rows[0] : null;
    }

    // Gather a block's content rows for the block_merkle_root (SPV spec sec.5), in
    // the frozen cross-kind order stateCommitment.computeBlockMerkleRoot expects:
    // { ledger:{credits,debits,escrows}, actions, contracts:{contracts,state,
    //   executions,emissions,deposits,withdrawals} }.
    //
    // !!! CONFORMANCE: the SELECT column sets + ORDER BY below MUST stay
    // byte-identical to BlockHasher.computeBlockHashes (and the indexer's
    // getBlockHashes), since block_merkle_root covers the same rows the consensus
    // ledger/actions/contract hashes do. The xchain-e2e consensusHashConformance
    // test is the drift guard. !!!
    //
    // `network`/`coin` drive the state_key collation flag-day
    // (state_key_collation_activation.js), mirroring BlockHasher; omitted ->
    // legacy folding collation (pre-activation behavior).
    async getBlockLeafRows(block_index, conn, network, coin){
        let q;
        let ledger = { credits: [], debits: [], escrows: [] };
        // credits
        q = `SELECT c.action_index, a1.address AS address, t1.tick AS tick, c.amount
             FROM credits c
                INNER JOIN actions        a  ON (a.action_index=c.action_index)
                LEFT  JOIN index_addresses a1 ON (a1.id=c.address_id)
                LEFT  JOIN index_tickers   t1 ON (t1.id=c.tick_id)
             WHERE a.block_index=?
             ORDER BY c.action_index ASC, a1.address COLLATE utf8_bin ASC, t1.tick COLLATE utf8mb4_bin ASC, c.amount ASC`;
        ledger.credits = await this.doQuery(q, [block_index], conn);
        // debits
        q = `SELECT d.action_index, a1.address AS address, t1.tick AS tick, d.amount
             FROM debits d
                INNER JOIN actions        a  ON (a.action_index=d.action_index)
                LEFT  JOIN index_addresses a1 ON (a1.id=d.address_id)
                LEFT  JOIN index_tickers   t1 ON (t1.id=d.tick_id)
             WHERE a.block_index=?
             ORDER BY d.action_index ASC, a1.address COLLATE utf8_bin ASC, t1.tick COLLATE utf8mb4_bin ASC, d.amount ASC`;
        ledger.debits = await this.doQuery(q, [block_index], conn);
        // escrows
        q = `SELECT e.action_index, a1.address AS address, t1.tick AS tick, e.amount
             FROM escrows e
                INNER JOIN actions        a  ON (a.action_index=e.action_index)
                LEFT  JOIN index_addresses a1 ON (a1.id=e.address_id)
                LEFT  JOIN index_tickers   t1 ON (t1.id=e.tick_id)
             WHERE a.block_index=?
             ORDER BY e.action_index ASC, a1.address COLLATE utf8_bin ASC, t1.tick COLLATE utf8mb4_bin ASC, e.amount ASC`;
        ledger.escrows = await this.doQuery(q, [block_index], conn);
        // CONSENSUS: canonicalize protocol special addresses (BURN/GAS/DONATE/REWARD)
        // to their chain-independent role token, byte-for-byte mirror of BlockHasher
        // and the indexer's getBlockHashes. block_merkle_root covers the same ledger
        // rows the flat ledger_hash does, so without this the follower recomputes a
        // raw-address merkle root that diverges from the source's canonicalized root on
        // any special-address block and halts. Ordered AFTER the SQL sort (which keys
        // on the raw stored address) so the leaf sequence matches the source exactly.
        for (const row of ledger.credits) row.address = canonicalizeHashAddress(row.address);
        for (const row of ledger.debits)  row.address = canonicalizeHashAddress(row.address);
        for (const row of ledger.escrows) row.address = canonicalizeHashAddress(row.address);
        // actions
        q = `SELECT a.action_index, a.tx_index, ia.action AS action
             FROM actions a
                LEFT JOIN index_actions ia ON (ia.id=a.action_id)
             WHERE a.block_index=?
             ORDER BY a.action_index ASC`;
        let actions = await this.doQuery(q, [block_index], conn);
        let contracts = { contracts: [], state: [], executions: [], emissions: [], deposits: [], withdrawals: [] };
        // new deployments
        q = `SELECT c.action_index, a1.address AS source_address, c.code_hash, s1.status AS status
             FROM contracts c
                INNER JOIN actions a ON (a.action_index=c.action_index)
                LEFT  JOIN index_addresses a1 ON (a1.id=c.source_id)
                LEFT  JOIN index_statuses  s1 ON (s1.id=c.status_id)
             WHERE a.block_index=?
             ORDER BY c.action_index ASC`;
        contracts.contracts = await this.doQuery(q, [block_index], conn);
        // contract state (latest value per key written in this block).
        // state_key collation is flag-day gated, mirroring BlockHasher and the
        // indexer's getBlockHashes: legacy folding (utf8_general_ci) below the
        // activation height, COLLATE utf8_bin pinned at/after it
        // (see state_key_collation_activation.js).
        let stateKeyCollate = isStateKeyBinCollationActive(block_index, network, coin) ? ' COLLATE utf8_bin' : '';
        q = `SELECT cs.contract_index, cs.state_key, cs.state_value
             FROM contract_state cs
                INNER JOIN (
                    SELECT MAX(id) as max_id FROM contract_state
                    WHERE block_index=? GROUP BY contract_index, state_key` + stateKeyCollate + `
                ) latest ON cs.id = latest.max_id
             ORDER BY cs.contract_index ASC, cs.state_key` + stateKeyCollate + ` ASC`;
        contracts.state = await this.doQuery(q, [block_index], conn);
        // executions
        q = `SELECT ce.action_index, ce.contract_index, a1.address AS caller_address, ce.gas_used, s1.status AS status, ce.emitted_count
             FROM contract_executions ce
                INNER JOIN actions a ON (a.action_index=ce.action_index)
                LEFT  JOIN index_addresses a1 ON (a1.id=ce.caller_id)
                LEFT  JOIN index_statuses  s1 ON (s1.id=ce.status_id)
             WHERE a.block_index=?
             ORDER BY ce.action_index ASC`;
        contracts.executions = await this.doQuery(q, [block_index], conn);
        // emissions (join through executions to get block scope)
        q = `SELECT em.execution_index, em.emitted_action, em.action_index, em.position
             FROM contract_emissions em
                INNER JOIN contract_executions ce ON (ce.action_index=em.execution_index)
                INNER JOIN actions a ON (a.action_index=ce.action_index)
             WHERE a.block_index=?
             ORDER BY em.execution_index ASC, em.position ASC`;
        contracts.emissions = await this.doQuery(q, [block_index], conn);
        // deposits
        q = `SELECT d.action_index, d.contract_index, a1.address AS source_address, t1.tick AS tick, d.amount, s1.status AS status
             FROM deposits d
                INNER JOIN actions a ON (a.action_index=d.action_index)
                LEFT  JOIN index_addresses a1 ON (a1.id=d.source_id)
                LEFT  JOIN index_tickers   t1 ON (t1.id=d.tick_id)
                LEFT  JOIN index_statuses  s1 ON (s1.id=d.status_id)
             WHERE a.block_index=?
             ORDER BY d.action_index ASC, d.contract_index ASC, a1.address COLLATE utf8_bin ASC, t1.tick COLLATE utf8mb4_bin ASC, d.amount ASC, s1.status COLLATE utf8_bin ASC`;
        contracts.deposits = await this.doQuery(q, [block_index], conn);
        // withdrawals
        q = `SELECT w.action_index, w.contract_index, a1.address AS source_address, t1.tick AS tick, w.amount, s1.status AS status
             FROM withdrawals w
                INNER JOIN actions a ON (a.action_index=w.action_index)
                LEFT  JOIN index_addresses a1 ON (a1.id=w.source_id)
                LEFT  JOIN index_tickers   t1 ON (t1.id=w.tick_id)
                LEFT  JOIN index_statuses  s1 ON (s1.id=w.status_id)
             WHERE a.block_index=?
             ORDER BY w.action_index ASC, w.contract_index ASC, a1.address COLLATE utf8_bin ASC, t1.tick COLLATE utf8mb4_bin ASC, w.amount ASC, s1.status COLLATE utf8_bin ASC`;
        contracts.withdrawals = await this.doQuery(q, [block_index], conn);
        return { ledger, actions, contracts };
    }

    // Get block data for a range of blocks (for building payloads).
    // Same indexer-vs-decoder branching as getBlockHashRow.
    async getBlockRows(startBlock, endBlock){
        let query;
        if(this.dbType === 'decoder'){
            query = `SELECT
                    b.block_index,
                    b.block_time,
                    t1.hash as block_hash
                FROM
                    blocks b
                    LEFT JOIN index_transactions t1 ON (t1.id=b.block_hash_id)
                WHERE
                    b.block_index >= ? AND b.block_index <= ?
                ORDER BY b.block_index ASC`;
        } else {
            query = `SELECT
                    b.block_index,
                    b.block_time,
                    t1.hash as ledger_hash,
                    t2.hash as actions_hash,
                    t3.hash as contract_hash
                FROM
                    blocks b
                    LEFT JOIN index_transactions t1 ON (t1.id=b.ledger_hash_id)
                    LEFT JOIN index_transactions t2 ON (t2.id=b.actions_hash_id)
                    LEFT JOIN index_transactions t3 ON (t3.id=b.contract_hash_id)
                WHERE
                    b.block_index >= ? AND b.block_index <= ?
                ORDER BY b.block_index ASC`;
        }
        return await this.doQuery(query, [startBlock, endBlock]);
    }

    // Get the first action_index at or after a given block
    async getFirstActionIndex(block_index, conn){
        let query = `SELECT action_index FROM actions a WHERE a.block_index >= ? ORDER BY a.action_index ASC LIMIT 1`;
        let rows  = await this.doQuery(query, [block_index], conn);
        if(rows.length > 0)
            return Number(rows[0].action_index);
        return null;
    }

    // Resolve a status name to its local index_statuses id. index_statuses is replicated, so the
    // id resolves consistently against the replica's own *.status_id values. Used by
    // ClientRollback's cooldown-maturity reversal mirror. Returns null if the status is absent
    // (e.g. 'completed' never created because no cooldown has matured); the caller then skips.
    async getStatusId(status){
        let rows = await this.doQuery("SELECT id FROM index_statuses WHERE status = ? LIMIT 1", [status]);
        return rows.length > 0 ? Number(rows[0].id) : null;
    }

    // Light-client stakes_root support (SPV spec sec.4.1, BTC-only). Source-deduped
    // capability stake-weight query, ported VERBATIM from xchain-indexer/src/db.js
    // _stakeWeightsSql: it MUST produce a byte-identical SQL string + arg order or
    // the follower's stakes_root diverges from the indexer's committed root and the
    // state-commitment check false-halts. The cross-repo drift guard in
    // test/unit/rollback-coverage.test.js locks the two together. Reads only tables
    // xchain-sync replicates (stakes, delegations, stake_key_revocations,
    // capability_slash_events, index_addresses, index_pubkeys).
    _stakeWeightsSql(valid_id, blockIndex, minStake){
        // Precision: DECIMAL(30,8) (22 integer digits, 8 fractional) is sufficient because the
        // staking tick is XCHAIN at 8 decimals and total supply stays far below 10^22; every
        // same-version node truncates identically, so the stake-weight tally is deterministic.
        // If a >8-decimal staking tick is ever introduced, widen these casts to
        // DECIMAL(60, <tick-decimals>) AND pin a consistent sql_mode fleet-wide (an overflow at
        // >22 integer digits is otherwise sql_mode-dependent) before that tick can stake.
        // Permanent disqualification - see _effectiveCapabilitySetSql. Excludes equivocation-
        // slashed keys from the effective-key set (both stake-key and delegated-key branches)
        // so the source-deduped stake-weight tally matches the count-quorum set exactly.
        const slashExcl = (keyCol) =>
            `AND NOT EXISTS (SELECT 1 FROM capability_slash_events cse
                             WHERE cse.signing_pubkey_id = ${keyCol} AND cse.block_index <= ?)`;
        let sql = `SELECT ip.pubkey AS pubkey,
                          sa.address AS source,
                          q.total    AS weight
                   FROM (
                       SELECT s.source_id AS source_id,
                              SUM(CAST(s.amount AS DECIMAL(30,8))) AS total
                       FROM stakes s
                       WHERE s.status_id = ?
                         AND s.activation_block <= ?
                         AND (s.deactivation_block IS NULL OR s.deactivation_block > ?)
                       GROUP BY s.source_id
                       HAVING total >= CAST(? AS DECIMAL(30,8))
                   ) q
                   JOIN index_addresses sa ON sa.id = q.source_id
                   JOIN (
                       SELECT s2.source_id AS source_id, s2.signing_pubkey_id AS pubkey_id
                       FROM stakes s2
                       WHERE s2.status_id = ?
                         AND s2.activation_block <= ?
                         AND (s2.deactivation_block IS NULL OR s2.deactivation_block > ?)
                         AND NOT EXISTS (
                             SELECT 1 FROM stake_key_revocations r
                             WHERE r.source_id = s2.source_id
                               AND r.signing_pubkey_id = s2.signing_pubkey_id
                               AND r.status_id = ?
                               AND r.deactivation_block <= ?
                               AND r.action_index > s2.action_index)
                         ${slashExcl('s2.signing_pubkey_id')}
                       GROUP BY s2.source_id, s2.signing_pubkey_id
                       UNION
                       SELECT d.source_id AS source_id, d.signing_pubkey_id AS pubkey_id
                       FROM delegations d
                       WHERE d.status_id = ?
                         AND d.activation_block <= ?
                         AND (d.deactivation_block IS NULL OR d.deactivation_block > ?)
                         ${slashExcl('d.signing_pubkey_id')}
                   ) ek ON ek.source_id = q.source_id
                   JOIN index_pubkeys ip ON ip.id = ek.pubkey_id`;
        let args = [valid_id, blockIndex, blockIndex, minStake,
                    valid_id, blockIndex, blockIndex, valid_id, blockIndex, blockIndex,
                    valid_id, blockIndex, blockIndex, blockIndex];
        return { sql, args };
    }

    // SWQ source-cap wrapper (SWQ-TRUNC-1 liveness half). Wraps an inner source-keyed
    // stake-weight builder ({sql,args} from _stakeWeightsSql or the sync AsOf variant)
    // and replaces the raw key-row LIMIT with a windowed cap on the consensus UNIT:
    // DISTINCT staking SOURCES (DENSE_RANK over source) plus a per-source key bound
    // (ROW_NUMBER per source). One source can no longer fill the window and evict
    // honest sources. Over-fetches one extra source (_sr <= maxSources + 1) so the
    // caller can flag a genuinely >maxSources federation as truncated (the primitive
    // then fails closed); the per-source key cap only bounds the row/leaf count and
    // never sets truncated (dropping a source's excess keys does not change its
    // weight). Row order is consensus-irrelevant (the stakes_root SMT keys on
    // pubkey+capability); only the returned SET is. CONSENSUS-CRITICAL: feeds the
    // hashed stakes_root at/after SWQ_SOURCE_CAP_ACTIVATION and MUST stay byte-identical
    // to the xchain-indexer twin (cross-repo drift guard in rollback-coverage.test.js).
    _cappedStakeWeightsSql(inner, maxSources, maxKeys){
        let sql = `SELECT r.pubkey AS pubkey, r.source AS source, r.weight AS weight, r._sr AS _sr
                   FROM (
                       SELECT b.pubkey AS pubkey, b.source AS source, b.weight AS weight,
                              DENSE_RANK() OVER (ORDER BY b.source)                        AS _sr,
                              ROW_NUMBER() OVER (PARTITION BY b.source ORDER BY b.pubkey)  AS _kr
                       FROM (${inner.sql}) b
                   ) r
                   WHERE r._sr <= ? AND r._kr <= ?
                   ORDER BY r.source, r.pubkey`;
        let args = [...inner.args, maxSources + 1, maxKeys];
        return { sql, args };
    }

    // Apply the cap regime in force for `coin`/`network` at `blockIndex` to an inner
    // source-keyed stake-weight builder, returning { rows:[{pubkey,source,weight}],
    // truncated }. Twin of the indexer's _stakeWeightsWithCap gate: at/after
    // SWQ_SOURCE_CAP_ACTIVATION the windowed source-cap (_cappedStakeWeightsSql);
    // below it the legacy uncapped key-row LIMIT. The gate + caps + _cappedStakeWeightsSql
    // are byte-mirrored to the indexer so the follower's stakes_root set is identical on
    // both sides of the height. Sync reads coin/network from the caller (it has no
    // per-chain config); a null coin/network stays inert (legacy uncapped path).
    async _applyStakeWeightCap(inner, blockIndex, limit, coin, network, label){
        if(swqCap.isSwqSourceCapActive(blockIndex, network, coin)){
            let maxSources = swqCap.STAKE_WEIGHT_MAX_SOURCES;
            let maxKeys    = swqCap.STAKE_WEIGHT_MAX_KEYS_PER_SOURCE;
            let capped = this._cappedStakeWeightsSql(inner, maxSources, maxKeys);
            let raw = await this.doQuery(capped.sql, capped.args);
            let truncated = raw.some(r => Number(r._sr) > maxSources);
            if(truncated)
                console.warn(label + ' saw more than ' + maxSources + ' distinct staking sources at block ' + blockIndex + ' - stakes_root snapshot truncated; stake-weighted quorum fails closed. Raise STAKE_WEIGHT_MAX_SOURCES (coordinated flag-day upgrade) if the federation has grown.');
            let rows = (truncated ? raw.filter(r => Number(r._sr) <= maxSources) : raw).map(r => ({
                pubkey: String(r.pubkey),
                source: String(r.source),
                weight: (r.weight === null || r.weight === undefined) ? '0' : String(r.weight)
            }));
            return { rows, truncated };
        }
        let query = `${inner.sql} ORDER BY source, pubkey LIMIT ?`;
        let raw = await this.doQuery(query, [...inner.args, limit]);
        let truncated = raw.length >= limit;
        if(truncated)
            console.warn(label + ' hit the result cap of ' + limit + ' rows at block ' + blockIndex + ' - stakes_root set may be truncated vs the source. Raise the frozen VALIDATOR_QUERY_LIMIT (coordinated fleet upgrade) if the federation has grown.');
        let rows = raw.map(r => ({
            pubkey: String(r.pubkey),
            source: String(r.source),
            weight: (r.weight === null || r.weight === undefined) ? '0' : String(r.weight)
        }));
        return { rows, truncated };
    }

    // Source-keyed capability stake weights at a block, mirroring the indexer's
    // getStakeWeightsByCapability BTC path (the follower only builds the BTC
    // stakes_root, so the off-BTC hub-mirror branch is not needed here). minStake
    // is the capability's frozen MIN_STAKE floor; limit is the frozen
    // VALIDATOR_QUERY_LIMIT. Same ORDER BY + cap as the source so the selected set
    // is identical even on truncation.
    async getStakeWeightsByCapability(capability, blockIndex, minStake, limit, coin, network){
        let valid_id = await this.getStatusId('valid');
        if(valid_id === null) return [];
        let sw = this._stakeWeightsSql(valid_id, blockIndex, String(minStake));
        let { rows } = await this._applyStakeWeightCap(sw, blockIndex, limit, coin, network, 'getStakeWeightsByCapability(' + capability + ')');
        return rows;
    }

    // HISTORICAL stake weights at snapshotBlock S, reconstructing the amount that
    // stakes_root[S] committed IN ORDER, for the SPV checkpoint forward-follow
    // (ClientSync._oraclePublishSetAt / _followCheckpointForward). getStakeWeightsByCapability
    // reads live SUM(stakes.amount), but a SLASH zeroes stakes.amount IN PLACE, so a
    // query for a past S run at the current tip understates the weight committed at S
    // (and via HAVING can false-drop a source below the floor -> false-halt on a
    // legitimate rotation). This method adds back capability_slash_debits whose slash
    // block_index > S (target_table='stakes' only; unstakes debits do not feed the
    // stakes weight), restoring each row's pre-slash amount: 0 + prev_amount = prev_amount.
    //
    // SYNC-ONLY / forward-follow-only. MUST NOT be added to or called from xchain-indexer:
    // the indexer's action handlers (anchor/xcall/xexec/cross_settle) and the in-order
    // stakes_root build call getStakeWeightsByCapability while S is the tip, BEFORE any
    // later slash mutates the row, so they already read the correct value; adding the
    // add-back there would double-count and fork the committed ledger. This is a NO-OP
    // when no slash with block_index > S exists (addback is NULL -> COALESCE 0), so it
    // returns a result byte-identical to getStakeWeightsByCapability(cap, S) computed
    // in order at S. _stakeWeightsSql (the cross-repo byte-identical twin) is deliberately
    // NOT reused/modified here so the drift guard and the consensus query stay untouched.
    async getStakeWeightsByCapabilityAsOf(capability, snapshotBlock, minStake, limit, coin, network){
        let valid_id = await this.getStatusId('valid');
        if(valid_id === null) return [];
        // Membership exclusion is identical to _stakeWeightsSql: a key slashed at
        // block > S has cse.block_index > S, so NOT EXISTS is TRUE and the key is
        // correctly KEPT in the set at S. Only the q-subquery AMOUNT is reconstructed.
        const slashExcl = (keyCol) =>
            `AND NOT EXISTS (SELECT 1 FROM capability_slash_events cse
                             WHERE cse.signing_pubkey_id = ${keyCol} AND cse.block_index <= ?)`;
        let sql = `SELECT ip.pubkey AS pubkey,
                          sa.address AS source,
                          q.total    AS weight
                   FROM (
                       SELECT s.source_id AS source_id,
                              SUM(CAST(s.amount AS DECIMAL(30,8))
                                  + COALESCE(CAST(addback.amt AS DECIMAL(30,8)), 0)) AS total
                       FROM stakes s
                       LEFT JOIN (
                           SELECT csd.stake_action_index AS stake_action_index,
                                  SUM(CAST(csd.amount AS DECIMAL(30,8))) AS amt
                           FROM capability_slash_debits csd
                           WHERE csd.target_table = 'stakes' AND csd.block_index > ?
                           GROUP BY csd.stake_action_index
                       ) addback ON addback.stake_action_index = s.action_index
                       WHERE s.status_id = ?
                         AND s.activation_block <= ?
                         AND (s.deactivation_block IS NULL OR s.deactivation_block > ?)
                       GROUP BY s.source_id
                       HAVING total >= CAST(? AS DECIMAL(30,8))
                   ) q
                   JOIN index_addresses sa ON sa.id = q.source_id
                   JOIN (
                       SELECT s2.source_id AS source_id, s2.signing_pubkey_id AS pubkey_id
                       FROM stakes s2
                       WHERE s2.status_id = ?
                         AND s2.activation_block <= ?
                         AND (s2.deactivation_block IS NULL OR s2.deactivation_block > ?)
                         AND NOT EXISTS (
                             SELECT 1 FROM stake_key_revocations r
                             WHERE r.source_id = s2.source_id
                               AND r.signing_pubkey_id = s2.signing_pubkey_id
                               AND r.status_id = ?
                               AND r.deactivation_block <= ?
                               AND r.action_index > s2.action_index)
                         ${slashExcl('s2.signing_pubkey_id')}
                       GROUP BY s2.source_id, s2.signing_pubkey_id
                       UNION
                       SELECT d.source_id AS source_id, d.signing_pubkey_id AS pubkey_id
                       FROM delegations d
                       WHERE d.status_id = ?
                         AND d.activation_block <= ?
                         AND (d.deactivation_block IS NULL OR d.deactivation_block > ?)
                         ${slashExcl('d.signing_pubkey_id')}
                   ) ek ON ek.source_id = q.source_id
                   JOIN index_pubkeys ip ON ip.id = ek.pubkey_id`;
        // Arg order tracks the placeholders left-to-right: the addback block bound
        // first, then the same sequence _stakeWeightsSql uses (all historical-block
        // args bound to snapshotBlock), then the LIMIT.
        let args = [snapshotBlock,
                    valid_id, snapshotBlock, snapshotBlock, String(minStake),
                    valid_id, snapshotBlock, snapshotBlock, valid_id, snapshotBlock, snapshotBlock,
                    valid_id, snapshotBlock, snapshotBlock, snapshotBlock];
        let { rows } = await this._applyStakeWeightCap({ sql, args }, snapshotBlock, limit, coin, network, 'getStakeWeightsByCapabilityAsOf(' + capability + ')');
        return rows;
    }

    // Get all rows from a table for a given block (block_index-scoped tables).
    // ORDER BY block_index, then by the first column for deterministic ordering
    // across sources with differing insert histories (matches the snapshot path).
    async getBlockScopedRows(table, block_index, conn){
        let query = "SELECT * FROM `" + table + "` WHERE block_index = ? ORDER BY block_index ASC, 1 ASC";
        return await this.doQuery(query, [block_index], conn);
    }

    // Get all rows from a table for actions in a given block (action_index-scoped tables).
    // Indexer-only: decoder DB has no actions table.
    // Scope by the ACTION's own block_index, NOT a transactions join: protocol-generated
    // actions (ORDER_MATCH / SWAP_MATCH / *_EXPIRE) carry tx_index = NULL with no transactions
    // row, so the old tx-join dropped their ledger rows (match settlements, expiry refunds) from
    // the payload while the consensus hash now includes them. A follower would then recompute a
    // divergent hash and halt. a.block_index is set for every action, so this streams them and
    // matches BlockHasher.
    async getActionScopedRows(table, block_index, conn){
        let query = `SELECT t.* FROM \`${table}\` t
            INNER JOIN actions a ON (a.action_index = t.action_index)
            WHERE a.block_index = ?
            ORDER BY t.action_index ASC`;
        return await this.doQuery(query, [block_index], conn);
    }

    // Discover in ONE round-trip which action-scoped tables actually carry rows in a
    // block, so the payload builder can fetch only those. Without it _buildBlockPayload
    // issues getActionScopedRows once per table in the lifecycle registry (86 today),
    // empty ones included, and that count rises with every replicated table added
    // ().
    //
    // The existence predicate is getActionScopedRows' predicate verbatim (same INNER
    // JOIN on action_index, same a.block_index = ?), so "absent from this Set" means
    // exactly "getActionScopedRows would have returned zero rows". That equivalence is
    // the whole safety argument: the payload feeds a consensus hash followers recompute,
    // so a skip that is not provably empty would halt them.
    //
    // Candidates are filtered through listExistingTables first. A source legitimately
    // predates a table family, and where the per-table loop absorbs that as a skippable
    // schema gap (errno 1146), one missing table would fail the whole UNION. Callers
    // still fall back to the full loop when this throws, so a probe fault costs
    // round-trips, never rows.
    //
    // doQueryStrict, not doQuery: outside a transaction doQuery is fail-soft and returns
    // [] on error, which here would read as "every table is empty" and silently empty the
    // block.
    async getNonEmptyActionScopedTables(tables, block_index, conn){
        let present = await this.listExistingTables(conn);
        let candidates = [];
        for(let table of tables){
            assertValidIdentifier(table);
            if(present.has(table)) candidates.push(table);
        }
        if(candidates.length === 0) return new Set();
        // Table name is safe to inline as a literal: assertValidIdentifier above admits
        // only [A-Za-z0-9_].
        let branches = candidates.map(table =>
            "(SELECT '" + table + "' AS tbl FROM `" + table + "` t" +
            " INNER JOIN actions a ON (a.action_index = t.action_index)" +
            " WHERE a.block_index = ? LIMIT 1)");
        let rows = await this.doQueryStrict(branches.join(' UNION ALL '),
                                            candidates.map(() => block_index), conn);
        return new Set(rows.map(r => r.tbl));
    }

    // Get all contract_emissions rows for a block, including INTERNAL emissions whose
    // action_index IS NULL (e.g. a SLASH). The generic getActionScopedRows() above joins
    // on t.action_index, so its INNER JOIN drops NULL-action_index rows. The consensus
    // contract_hash (BlockHasher) includes them via the execution_index -> contract_executions
    // chain. Streaming via that same chain keeps the server payload and the hash in agreement,
    // so a follower's recompute can't diverge. Query is kept byte-aligned with BlockHasher's
    // emissions query (same joins, columns, and ORDER BY). Select the four protocol columns
    // explicitly (not em.*, which would carry the AUTO_INCREMENT `id` and break idempotent
    // re-apply after a reorg).
    async getEmissionRowsForBlock(block_index, conn){
        let query = `SELECT em.execution_index, em.emitted_action, em.action_index, em.position
            FROM contract_emissions em
            INNER JOIN contract_executions ce ON (ce.action_index = em.execution_index)
            INNER JOIN actions a ON (a.action_index = ce.action_index)
            WHERE a.block_index = ?
            ORDER BY em.execution_index ASC, em.position ASC`;
        return await this.doQuery(query, [block_index], conn);
    }

    // Get all rows from a table for transactions in a given block (tx_index-scoped tables).
    // Used for decoder DB tables like transaction_outputs, which key off tx_index and
    // join to the transactions table to recover the block scope.
    async getTxScopedRows(table, block_index, conn){
        let query = `SELECT t.* FROM \`${table}\` t
            INNER JOIN transactions tx ON (tx.tx_index = t.tx_index)
            WHERE tx.block_index = ?
            ORDER BY t.tx_index ASC, 1 ASC`;
        return await this.doQuery(query, [block_index], conn);
    }

    // Get transactions for a given block
    async getTransactions(block_index, conn){
        let query = "SELECT * FROM transactions WHERE block_index = ? ORDER BY tx_index ASC";
        return await this.doQuery(query, [block_index], conn);
    }

    // Get actions for a given block. Scope by the action's own block_index (not a transactions
    // join): protocol-generated actions (ORDER_MATCH / SWAP_MATCH / *_EXPIRE) have tx_index = NULL
    // and must still stream to followers (and are now in the consensus hash).
    async getActions(block_index, conn){
        let query = `SELECT a.* FROM actions a
            WHERE a.block_index = ?
            ORDER BY a.action_index ASC`;
        return await this.doQuery(query, [block_index], conn);
    }

    // Stream every row of a table in ONE ordered pass on the given (dedicated
    // snapshot) connection. Returns the driver's row Readable (async-iterable);
    // the caller must consume it fully or destroy() it.
    //
    // This deliberately replaces LIMIT/OFFSET paging (`ORDER BY 1 LIMIT ? OFFSET ?`):
    // most replicated ledger tables have NO primary key and a non-unique first
    // column (e.g. credits/debits share one action_index across a match's rows), so
    // `ORDER BY 1` is not a total order and SQL does not guarantee a stable tie
    // order across separate OFFSET executions. A boundary tie could then be emitted
    // in two adjacent pages (duplicate -> plain-INSERT apply aborts, or silently
    // doubles a keyless row) or in neither (skip -> replica short, caught only by
    // the advisory count check). A single query execution reads each row exactly
    // once, so no cross-execution tie order exists to disagree. Keyset paging is
    // not an option here: the keyless tables have nothing unique to key on.
    // ORDER BY 1 is kept so the emitted row order matches the historical snapshot
    // shape (and getBlockScopedRows' "matches the snapshot path" comment).
    streamTableRows(table, conn){
        assertValidIdentifier(table);
        return conn.queryStream("SELECT * FROM `" + table + "` ORDER BY 1");
    }

    // The set of BASE TABLEs that actually exist in this database.
    //
    // Exists so a caller can skip a table instead of discovering its absence by
    // failing a query against it. That distinction is not cosmetic: the /status
    // handlers enumerate the STATIC replicated-table list, which grows whenever a
    // new family lands in this repo, so on any replica whose source predates that
    // family every poll used to raise ER_NO_SUCH_TABLE, log a multi-line SqlError,
    // and swallow it. Expected, tolerated, and indistinguishable in the journal
    // from a real fault: 11,542 of them on the origin-host BTC regtest replica over
    // two days, for tables the SOURCE does not have either .
    //
    // doQueryStrict, so a failure to LIST is never silently read as "nothing
    // exists": that would empty table_counts and make an incomplete replica look
    // complete to _verifyTableCounts. Callers fall back to probing instead.
    async listExistingTables(conn){
        let rows = await this.doQueryStrict(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_type = 'BASE TABLE'",
            [this.dbName], conn);
        return new Set(rows.map(r => r.table_name || r.TABLE_NAME));
    }

    // Get total row count for a table.
    //
    // doQueryStrict, NOT doQuery, and the reason is a live defect rather than
    // tidiness . Outside a transaction doQuery is fail-soft: it logs the
    // SqlError and returns [], so `rows[0].cnt` then threw a TypeError with NO
    // errno. Every caller that classifies the failure by errno was therefore
    // reading a different error than the one the database raised, and the one
    // that matters is ClientSync._verifyTableCounts: its catch routes errno 1146
    // ("the source's schema moved ahead of this replica") into the debounced
    // schema heal that CREATEs the missing table. A TypeError carries no errno,
    // so that heal could never fire from here, and the replica stayed missing the
    // table forever while logging the same SqlError on every status poll.
    // Observed on the origin-host BTC regtest replica: `bet_resolves` absent and
    // erroring 11,542 times over two days with the heal wired and unreachable.
    //
    // Strict here is safe for the tolerant callers too: /status wraps this in a
    // try/catch and omits the table, which is unchanged. What changes is that the
    // error they swallow, and the one ClientSync inspects, is now the database's
    // own errno-bearing SqlError.
    async getTableCount(table, conn){
        assertValidIdentifier(table);
        let query = "SELECT COUNT(*) as cnt FROM `" + table + "`";
        let rows = await this.doQueryStrict(query, null, conn);
        return Number(rows[0].cnt);
    }

    // Per-database size + table-count stats for every replicated XChain_* DB on this
    // server. information_schema is server-wide, so one connection reports them all.
    // Used by the /catalog endpoint. Rows: {db_name, tables, data_bytes, index_bytes}.
    async getDatabaseStats(){
        let query = "SELECT table_schema AS db_name, COUNT(*) AS tables, " +
                    "COALESCE(SUM(data_length),0) AS data_bytes, " +
                    "COALESCE(SUM(index_length),0) AS index_bytes " +
                    "FROM information_schema.tables " +
                    "WHERE table_type='BASE TABLE' AND table_schema LIKE 'XChain\\_%' " +
                    "GROUP BY table_schema";
        return await this.doQuery(query);
    }

    // Truncate a table
    async truncateTable(table){
        assertValidIdentifier(table);
        await this.doQuery("TRUNCATE TABLE `" + table + "`");
    }

    // Close all connections in the pool
    async close(){
        try {
            await this.pool.end();
        } catch(e){
            console.log('Error closing database pool:', e);
        }
    }
}

module.exports = Database;
