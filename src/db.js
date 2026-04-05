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
 * XChain Indexer Sync - Database Class
 *
 * This file handles connecting to MariaDB and running SQL queries.
 * Simplified from xchain-indexer/src/db.js — no action processing,
 * just connection pool, circuit breaker, and query execution.
 *
 ********************************************************************/

const mariadb = require('mariadb');
const fs      = require('fs');
const path    = require('path');

class Database {

    constructor(host, port, dbName, user, pass, util) {
        this.host   = host;
        this.port   = port;
        this.dbName = dbName;
        this.user   = user;
        this.pass   = pass;
        this.util   = util;

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
            minDelayValidation: 3000
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
                console.log('Database connection error:', e.code || 'unknown');
                console.log("Error checking if " + this.dbName + " exists. Trying again in 5 seconds...");
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
        if(!/^[A-Za-z0-9_]+$/.test(this.dbName))
            throw new Error('Invalid database name: ' + this.dbName);
        console.log("Creating " + this.dbName + " database!");
        while(true){
            try {
                let db = await mariadb.createConnection(connectionParams);
                await db.query("CREATE DATABASE IF NOT EXISTS `" + this.dbName + "`");
                await db.end();
                return true;
            } catch(e){
                console.log("Database creation error:", e.code || 'unknown');
                console.log("Error creating " + this.dbName + ". Trying again in 5 seconds...");
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

    // Replicate schema from a source database into this database.
    // Reads all table DDLs from the source via SHOW CREATE TABLE and
    // creates any missing tables locally. This ensures the replica always
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
            if(existingSet.has(tableName)) continue;

            // Get the CREATE TABLE DDL from the source
            let ddlRows = await sourceDb.doQuery("SHOW CREATE TABLE `" + tableName + "`");
            if(ddlRows.length === 0) continue;

            let createSql = ddlRows[0]['Create Table'];
            if(!createSql) continue;

            console.log('Creating table ' + tableName + '...');
            try {
                await this.doQuery(createSql);
                created++;
            } catch(e){
                // Table may reference another table not yet created — retry later
                console.log('Deferred: ' + tableName + ' (' + (e.message || e) + ')');
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

                let ddlRows = await sourceDb.doQuery("SHOW CREATE TABLE `" + tableName + "`");
                if(ddlRows.length === 0) continue;
                let createSql = ddlRows[0]['Create Table'];
                if(!createSql) continue;

                try {
                    await this.doQuery(createSql);
                    console.log('Created table ' + tableName + ' (retry)');
                } catch(e){
                    console.error('Failed to create table ' + tableName + ':', e.message || e);
                }
            }
        }

        console.log('Schema replication complete for ' + this.dbName);
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
                console.log("Can't connect to mariadb. Retrying in " + (delay + jitter) + 'ms... (' + attempts + '/' + maxAttempts + ')');
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
                console.log("Error committing transaction");
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

    // Get block hash data for a given block_index
    async getBlockHashRow(block_index){
        let query = `SELECT
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
        let rows = await this.doQuery(query, [block_index]);
        if(rows.length > 0)
            return rows[0];
        return null;
    }

    // Get block data for a range of blocks (for building payloads)
    async getBlockRows(startBlock, endBlock){
        let query = `SELECT
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

    // Get all rows from a table for actions in a given block (action_index-scoped tables)
    async getActionScopedRows(table, block_index){
        let query = `SELECT t.* FROM \`${table}\` t
            INNER JOIN actions a ON (a.action_index = t.action_index)
            INNER JOIN transactions tx ON (tx.tx_index = a.tx_index)
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
        let query = "SELECT * FROM `" + table + "` LIMIT ? OFFSET ?";
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
            console.log('Error closing database pool:', e.message);
        }
    }
}

module.exports = Database;
