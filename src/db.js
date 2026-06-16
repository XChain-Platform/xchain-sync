/*********************************************************************
 *
 * Copyright © 2025–2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC – https://dankest.llc
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This file is part of XChain Platform. Licensed under the GNU Affero
 * General Public License v3.0 or later; see LICENSE.md. A commercial
 * license (without AGPL source-disclosure terms) is available —
 * contact legal@dankest.llc.
 *
 **********************************************************************
 *
 * XChain Sync - Database Class
 *
 * This file handles connecting to MariaDB and running SQL queries.
 * Simplified from xchain-indexer/src/db.js — no action processing,
 * just connection pool, circuit breaker, and query execution.
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

// Guard for the few queries that must interpolate a table name into a
// backtick-quoted identifier (COUNT(*), pagination, TRUNCATE) — parameter
// binding can't carry identifiers. Some callers pass server-supplied names
// (e.g. a sync source's `table_counts` keys), so a stray backtick or
// metacharacter here would break out of the quoting. Reject anything that
// isn't a plain [A-Za-z0-9_] identifier before it reaches the query string.
function assertValidIdentifier(table){
    const check = validation.validateIdentifier(table);
    if(!check.valid)
        throw new Error('Refusing to query unsafe table identifier: ' + check.reason);
}

class Database {

    constructor(host, port, dbName, user, pass, util, dbType) {
        this.host   = host;
        this.port   = port;
        this.dbName = dbName;
        this.user   = user;
        this.pass   = pass;
        this.util   = util;
        this.dbType = dbType || 'indexer';  // 'indexer' (default) or 'decoder'

        // Connection pool parameters
        this.connectionPoolParams = {
            host:               this.host,
            user:               this.user,
            password:           this.pass,
            database:           this.dbName,
            port:               this.port,
            connectionLimit:    10,
            connectTimeout:     10000,
            acquireTimeout:     10000,
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
            queryTimeout:       parseInt(process.env.DB_QUERY_TIMEOUT) || 30000
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
                await this.util.sleep(5000);
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
                await this.util.sleep(5000);
            }
        }
    }

    // Verify sync-service-owned tables exist (replicated tables are created
    // dynamically via replicateSchema / the /schema fetch, not from local SQL
    // files). Which sync-owned tables apply depends on the DB shape: the
    // transparency log (sync_meta + merkle_epochs) is indexer-only, but the
    // durable divergence halt (sync_halt) applies to BOTH db types —
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
    // with a best-effort ALTER TABLE ADD COLUMN — a column whose definition
    // cannot be cleanly parsed is logged and skipped rather than aborting the
    // whole sync. DDL auto-commits, so this must run before any snapshot
    // transaction is opened. Returns the number of columns added.
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

        let added = 0;
        for(let col of sourceColumns){
            if(destSet.has(col)) continue;

            let colCheck = validation.validateIdentifier(col);
            if(!colCheck.valid){
                console.error('Skipping invalid column name ' + col + ' on ' + tableName + ' (' + colCheck.reason + ')');
                continue;
            }

            let def = validation.extractColumnDefinition(sourceDdl, col);
            if(!def){
                console.warn('Could not extract definition for column ' + col + ' on ' + tableName + ' — skipping (manual ALTER may be required)');
                continue;
            }

            try {
                await this.doQuery("ALTER TABLE `" + tableName + "` ADD COLUMN " + def);
                console.log('Added column ' + col + ' to ' + tableName);
                added++;
            } catch(e){
                console.error('Failed to add column ' + col + ' to ' + tableName + ':', e);
            }
        }
        return added;
    }

    // Replicate schema from a source database into this database.
    // Reads all table DDLs from the source via SHOW CREATE TABLE and
    // creates any missing tables locally. For tables that already exist,
    // propagates any columns the source has added since the replica was
    // bootstrapped (see addMissingColumns). This ensures the replica always
    // matches the authoritative indexer schema — no copied SQL files needed.
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

            // Table already exists on the replica — don't recreate it, but
            // propagate any columns the source has added since it was created.
            if(existingSet.has(tableName)){
                await this.addMissingColumns(tableName, createSql);
                continue;
            }

            console.log('Creating table ' + tableName + '...');
            try {
                await this.doQuery(createSql);
                created++;
            } catch(e){
                // Table may reference another table not yet created — retry later
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

        // replicateSchema only CREATEs missing tables — it never ALTERs an
        // existing table to add a column introduced after the replica was first
        // built. Run the column self-heal so replicas built before a column was
        // added pick it up on this pass.
        await this.ensureReplicatedColumns();

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
    // existing rows safely. Scoped to indexer replicas — orders/swaps do not
    // exist in the decoder schema. Tables absent locally are skipped (fresh
    // replicas create them with the columns already present).
    //
    // Also relaxes NULLABILITY drift in the SAFE direction (NOT NULL -> NULL):
    // contract_emissions.action_index was declared NOT NULL, but internal SLASH
    // emissions carry action_index = NULL (they deduct stake / write a
    // slash_events row but mint no on-wire action). The old stream scoped
    // contract_emissions by action_index (an INNER JOIN that silently dropped
    // those NULL rows); the emissions fix streams by execution_index and so
    // delivers them — and a NOT NULL replica column rejects the INSERT with
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
        let drift = [
            { table: 'orders', column: 'give_ownership' },
            { table: 'orders', column: 'get_ownership'  },
            { table: 'swaps',  column: 'give_ownership' },
            { table: 'swaps',  column: 'get_ownership'  }
        ];
        for(let { table, column } of drift){
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

            console.log('Schema drift on ' + table + '.' + column + ': column missing on replica. Adding TINYINT(1) NOT NULL DEFAULT 0.');
            await this.doQuery('ALTER TABLE `' + table + '` ADD COLUMN `' + column + '` TINYINT(1) NOT NULL DEFAULT 0');
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
            if(colRows.length === 0) continue;                       // table/column absent — skip
            let nullable = colRows[0].IS_NULLABLE || colRows[0].is_nullable;
            if(String(nullable).toUpperCase() !== 'NO') continue;    // already nullable — no-op

            console.log('Schema drift on ' + table + '.' + column + ': NOT NULL on replica but nullable upstream. Relaxing to allow NULL.');
            await this.doQuery('ALTER TABLE `' + table + '` MODIFY COLUMN `' + column + '` ' + type);
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
                this.util.throwError('Circuit breaker open — database connections rejected until cooldown expires');
            this.circuitState = 'half-open';
            console.log('Circuit breaker half-open — attempting reconnection');
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
                    console.log('Circuit breaker closed — database connection restored');
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
            console.log("rolling back");
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
    // InnoDB MVCC means this read view does not block writers — the snapshot
    // source keeps committing new blocks while the read view stays pinned.
    // Isolation is set explicitly rather than relying on the server default so
    // the guarantee holds regardless of how the source DB is configured.
    //
    // Returns a DEDICATED connection (not the shared this.transactionConnection)
    // that the caller threads into its reads (getLastBlock/getBlockHashRow/
    // getTablePage/... all accept an optional conn) and ends via
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
    // rollback — the connection is released regardless.
    async rollbackReadSnapshot(conn){
        if(conn == null) return;
        try {
            await conn.rollback();
        } catch(e){
            /* best-effort — still release below */
        } finally {
            await conn.release();
        }
    }

    // Run a query and return results.
    // conn (optional): run on this explicit connection instead of acquiring one —
    // used by read-snapshot reads (beginReadSnapshot). The caller owns that
    // connection's lifecycle (commit/rollback/release), so errors propagate.
    async doQuery(query, args, conn){
        let results = [];
        if(!this.util.isNull(query)){
            if(Array.isArray(args)){
                for(let i = 0; i < args.length; i++){
                    // Buffers (binary/blob column values) must reach the driver intact —
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
                if(tx) throw error;
            }
            if(!tx) await db.release();
        }
        return results;
    }

    // Get the last block index from the blocks table
    async getLastBlock(conn){
        let query = "SELECT MAX(block_index) AS block_index FROM blocks";
        let rows  = await this.doQuery(query, null, conn);
        if(rows.length > 0 && rows[0].block_index !== null)
            return Number(rows[0].block_index);
        return null;
    }

    // Get block hash data for a given block_index.
    // Indexer: joins to index_transactions for the synthetic ledger/actions/contract hashes.
    // Decoder: simpler — decoder DB has no synthetic chain-of-state hashes; only the
    // blockchain block hash itself (via block_hash_id → index_transactions).
    // --- Divergence halt (sync_halt) ------------------------------------------
    // Durably record a confirmed cross-source consensus divergence so a halted
    // client does not silently resume onto a contested chain after a restart.

    async recordHalt(dbType, blockIndex, reason, mismatches, sources){
        // Idempotent: don't stack duplicate active halts for the same block.
        let existing = await this.getActiveHalt(dbType);
        if(existing && Number(existing.block_index) === Number(blockIndex)) return existing;
        await this.doQuery(
            "INSERT INTO sync_halt (db_type, block_index, reason, mismatches, sources) VALUES (?, ?, ?, ?, ?)",
            [dbType, blockIndex, String(reason || 'divergence').slice(0, 64),
             JSON.stringify(mismatches || []), JSON.stringify(sources || [])]
        );
        return await this.getActiveHalt(dbType);
    }

    async getActiveHalt(dbType){
        let rows = await this.doQuery(
            "SELECT * FROM sync_halt WHERE db_type=? AND cleared_at IS NULL ORDER BY id DESC LIMIT 1",
            [dbType]
        );
        return (rows && rows.length) ? rows[0] : null;
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
    // (e.g. 'completed' never created because no cooldown has matured) — the caller then skips.
    async getStatusId(status){
        let rows = await this.doQuery("SELECT id FROM index_statuses WHERE status = ? LIMIT 1", [status]);
        return rows.length > 0 ? Number(rows[0].id) : null;
    }

    // Get all rows from a table for a given block (block_index-scoped tables)
    async getBlockScopedRows(table, block_index){
        let query = "SELECT * FROM `" + table + "` WHERE block_index = ?";
        return await this.doQuery(query, [block_index]);
    }

    // Get all rows from a table for actions in a given block (action_index-scoped tables).
    // Indexer-only: decoder DB has no actions table.
    // Scope by the ACTION's own block_index, NOT a transactions join: protocol-generated
    // actions (ORDER_MATCH / SWAP_MATCH / *_EXPIRE) carry tx_index = NULL with no transactions
    // row, so the old tx-join dropped their ledger rows (match settlements, expiry refunds) from
    // the payload while the consensus hash now includes them — a follower would then recompute a
    // divergent hash and halt. a.block_index is set for every action, so this streams them and
    // matches BlockHasher.
    async getActionScopedRows(table, block_index){
        let query = `SELECT t.* FROM \`${table}\` t
            INNER JOIN actions a ON (a.action_index = t.action_index)
            WHERE a.block_index = ?`;
        return await this.doQuery(query, [block_index]);
    }

    // Get all contract_emissions rows for a block, including INTERNAL emissions whose
    // action_index IS NULL (e.g. a SLASH). The generic getActionScopedRows() above joins
    // on t.action_index, so its INNER JOIN drops NULL-action_index rows — but the consensus
    // contract_hash (BlockHasher) includes them via the execution_index -> contract_executions
    // chain. Streaming via that same chain keeps the server payload and the hash in agreement,
    // so a follower's recompute can't diverge. Query is kept byte-aligned with BlockHasher's
    // emissions query (same joins, columns, and ORDER BY). Select the four protocol columns
    // explicitly (NOT em.* — that would carry the AUTO_INCREMENT `id` and break idempotent
    // re-apply after a reorg).
    async getEmissionRowsForBlock(block_index){
        let query = `SELECT em.execution_index, em.emitted_action, em.action_index, em.position
            FROM contract_emissions em
            INNER JOIN contract_executions ce ON (ce.action_index = em.execution_index)
            INNER JOIN actions a ON (a.action_index = ce.action_index)
            WHERE a.block_index = ?
            ORDER BY em.execution_index ASC, em.position ASC`;
        return await this.doQuery(query, [block_index]);
    }

    // Get all rows from a table for transactions in a given block (tx_index-scoped tables).
    // Used for decoder DB tables like transaction_outputs and dispensers, which key off
    // tx_index and join to the transactions table to recover the block scope.
    async getTxScopedRows(table, block_index){
        let query = `SELECT t.* FROM \`${table}\` t
            INNER JOIN transactions tx ON (tx.tx_index = t.tx_index)
            WHERE tx.block_index = ?`;
        return await this.doQuery(query, [block_index]);
    }

    // Get transactions for a given block
    async getTransactions(block_index){
        let query = "SELECT * FROM transactions WHERE block_index = ?";
        return await this.doQuery(query, [block_index]);
    }

    // Get actions for a given block. Scope by the action's own block_index (not a transactions
    // join): protocol-generated actions (ORDER_MATCH / SWAP_MATCH / *_EXPIRE) have tx_index = NULL
    // and must still stream to followers (and are now in the consensus hash).
    async getActions(block_index){
        let query = `SELECT a.* FROM actions a
            WHERE a.block_index = ?`;
        return await this.doQuery(query, [block_index]);
    }

    // Get all rows from a table (paginated)
    async getTablePage(table, limit, offset, conn){
        assertValidIdentifier(table);
        let query = "SELECT * FROM `" + table + "` ORDER BY 1 LIMIT ? OFFSET ?";
        return await this.doQuery(query, [limit, offset], conn);
    }

    // Get total row count for a table
    async getTableCount(table, conn){
        assertValidIdentifier(table);
        let query = "SELECT COUNT(*) as cnt FROM `" + table + "`";
        let rows = await this.doQuery(query, null, conn);
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
