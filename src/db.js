/*********************************************************************
 *
 * Copyright © 2025 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC – https://dankest.llc
 *
 * Licensed under the Dankest Community License (Apache License 2.0 + Additional Terms).
 * You may not use this file except in compliance with that License.
 *
 * A copy of the License is available at:
 *     https://dankest.llc/license
 *
 * This software is provided "AS IS", without warranties or conditions of any kind.
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
            // on re-insert. Affects decoder columns events.time and
            // dispensers.expiration; indexer schemas use INTEGER timestamps,
            // so this is a no-op there.
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

    // Verify sync-service-owned tables exist (only sync_meta — indexer tables
    // are replicated dynamically via replicateSchema, not from local SQL files)
    async verifySyncTables(){
        let dir  = path.join(__dirname, 'sql');
        let files = fs.readdirSync(dir);
        let db    = await this.getConnection();
        for(let file of files){
            if(file.indexOf('.sql') !== -1){
                let table = file.substring(0, file.indexOf('.sql'));
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
        let queries = data.split(';');
        console.log('Creating ' + table + ' table and indexes...');
        for(let query of queries){
            query = query.trim();
            if(query === '') continue;
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
    }

    // Get a database connection (with exponential backoff + circuit breaker)
    async getConnection(){
        if(this.transactionConnection)
            return this.transactionConnection;
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
    async beginReadSnapshot(){
        if(this.transactionConnection != null)
            await this.releaseConnection();
        this.transactionConnection = await this.getConnection();
        try {
            await this.transactionConnection.query('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ');
            await this.transactionConnection.query('START TRANSACTION WITH CONSISTENT SNAPSHOT');
        } catch(e){
            await this.transactionConnection.release();
            this.transactionConnection = null;
            this.util.throwError('beginReadSnapshot error=' + e);
        }
    }

    // Run a query and return results
    async doQuery(query, args){
        let results = [];
        if(!this.util.isNull(query)){
            if(Array.isArray(args)){
                for(let i = 0; i < args.length; i++){
                    if(args[i] !== null && args[i] !== undefined && typeof args[i] === 'object')
                        args[i] = args[i].toString();
                }
            }
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
    async getLastBlock(){
        let query = "SELECT MAX(block_index) AS block_index FROM blocks";
        let rows  = await this.doQuery(query);
        if(rows.length > 0 && rows[0].block_index !== null)
            return Number(rows[0].block_index);
        return null;
    }

    // Get block hash data for a given block_index.
    // Indexer: joins to index_transactions for the synthetic ledger/actions/contract hashes.
    // Decoder: simpler — decoder DB has no synthetic chain-of-state hashes; only the
    // blockchain block hash itself (via block_hash_id → index_transactions).
    async getBlockHashRow(block_index){
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
                    t3.hash as contract_hash
                FROM
                    blocks b
                    LEFT JOIN index_transactions t1 ON (t1.id=b.ledger_hash_id)
                    LEFT JOIN index_transactions t2 ON (t2.id=b.actions_hash_id)
                    LEFT JOIN index_transactions t3 ON (t3.id=b.contract_hash_id)
                WHERE
                    b.block_index=?`;
        }
        let rows = await this.doQuery(query, [block_index]);
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
    async getFirstActionIndex(block_index){
        let query = `SELECT action_index FROM actions a WHERE a.block_index >= ? ORDER BY a.action_index ASC LIMIT 1`;
        let rows  = await this.doQuery(query, [block_index]);
        if(rows.length > 0)
            return Number(rows[0].action_index);
        return null;
    }

    // Get all rows from a table for a given block (block_index-scoped tables)
    async getBlockScopedRows(table, block_index){
        let query = "SELECT * FROM `" + table + "` WHERE block_index = ?";
        return await this.doQuery(query, [block_index]);
    }

    // Get all rows from a table for actions in a given block (action_index-scoped tables).
    // Indexer-only: decoder DB has no actions table.
    async getActionScopedRows(table, block_index){
        let query = `SELECT t.* FROM \`${table}\` t
            INNER JOIN actions a ON (a.action_index = t.action_index)
            INNER JOIN transactions tx ON (tx.tx_index = a.tx_index)
            WHERE tx.block_index = ?`;
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

    // Get actions for a given block
    async getActions(block_index){
        let query = `SELECT a.* FROM actions a
            INNER JOIN transactions t ON (t.tx_index = a.tx_index)
            WHERE t.block_index = ?`;
        return await this.doQuery(query, [block_index]);
    }

    // Get all rows from a table (paginated)
    async getTablePage(table, limit, offset){
        let query = "SELECT * FROM `" + table + "` ORDER BY 1 LIMIT ? OFFSET ?";
        return await this.doQuery(query, [limit, offset]);
    }

    // Get total row count for a table
    async getTableCount(table){
        let query = "SELECT COUNT(*) as cnt FROM `" + table + "`";
        let rows = await this.doQuery(query);
        return Number(rows[0].cnt);
    }

    // Truncate a table
    async truncateTable(table){
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
