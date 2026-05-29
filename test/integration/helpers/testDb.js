const path = require('path');
const fs   = require('fs');
const { getMariadb } = require('./mariadbLoader');
const Utility = require('../../../src/utility');

const TEST_DB_HOST = process.env.TEST_DB_HOST || '192.168.2.5';
const TEST_DB_PORT = parseInt(process.env.TEST_DB_PORT) || 3306;
const TEST_DB_USER = process.env.TEST_DB_USER || 'xchain-node';
const TEST_DB_PASS = process.env.TEST_DB_PASS || 'XChain4life';

const SOURCE_DB_NAME  = 'xchain_sync_test_source';
const REPLICA_DB_NAME = 'xchain_sync_test_replica';

const util = new Utility();

// Lightweight DB wrapper using raw mariadb pool (no circuit breaker)
class TestDatabase {
    constructor(pool, dbName) {
        this.pool   = pool;
        this.dbName = dbName;
        this.transactionConnection = null;
    }

    async doQuery(query, args) {
        if (Array.isArray(args)) {
            for (let i = 0; i < args.length; i++) {
                if (args[i] !== null && args[i] !== undefined && typeof args[i] === 'object')
                    args[i] = args[i].toString();
            }
        }
        let conn = this.transactionConnection || await this.pool.getConnection();
        try {
            return await conn.query(query, args);
        } finally {
            if (!this.transactionConnection) conn.release();
        }
    }

    async getLastBlock() {
        let rows = await this.doQuery("SELECT MAX(block_index) AS block_index FROM blocks");
        if (rows.length > 0 && rows[0].block_index !== null)
            return Number(rows[0].block_index);
        return null;
    }

    async getBlockHashRow(block_index) {
        let rows = await this.doQuery(`SELECT
            b.block_index, b.block_time,
            t1.hash as ledger_hash, t2.hash as actions_hash, t3.hash as contract_hash
            FROM blocks b
            LEFT JOIN index_transactions t1 ON (t1.id=b.ledger_hash_id)
            LEFT JOIN index_transactions t2 ON (t2.id=b.actions_hash_id)
            LEFT JOIN index_transactions t3 ON (t3.id=b.contract_hash_id)
            WHERE b.block_index=?`, [block_index]);
        return rows.length > 0 ? rows[0] : null;
    }

    async getFirstActionIndex(block_index) {
        let rows = await this.doQuery(
            "SELECT action_index FROM actions WHERE block_index >= ? ORDER BY action_index ASC LIMIT 1",
            [block_index]
        );
        return rows.length > 0 ? Number(rows[0].action_index) : null;
    }

    async getBlockScopedRows(table, block_index) {
        return await this.doQuery("SELECT * FROM `" + table + "` WHERE block_index = ?", [block_index]);
    }

    async getActionScopedRows(table, block_index) {
        return await this.doQuery(`SELECT t.* FROM \`${table}\` t
            INNER JOIN actions a ON (a.action_index = t.action_index)
            INNER JOIN transactions tx ON (tx.tx_index = a.tx_index)
            WHERE tx.block_index = ?`, [block_index]);
    }

    async getTransactions(block_index) {
        return await this.doQuery("SELECT * FROM transactions WHERE block_index = ?", [block_index]);
    }

    async getActions(block_index) {
        return await this.doQuery(`SELECT a.* FROM actions a
            INNER JOIN transactions t ON (t.tx_index = a.tx_index)
            WHERE t.block_index = ?`, [block_index]);
    }

    async getTablePage(table, limit, offset) {
        return await this.doQuery("SELECT * FROM `" + table + "` LIMIT ? OFFSET ?", [limit, offset]);
    }

    async getTableCount(table) {
        let rows = await this.doQuery("SELECT COUNT(*) as cnt FROM `" + table + "`");
        return Number(rows[0].cnt);
    }

    async truncateTable(table) {
        await this.doQuery("TRUNCATE TABLE `" + table + "`");
    }

    async beginTransaction() {
        if (this.transactionConnection) await this.releaseConnection();
        this.transactionConnection = await this.pool.getConnection();
        await this.transactionConnection.beginTransaction();
    }

    async beginReadSnapshot() {
        if (this.transactionConnection) await this.releaseConnection();
        this.transactionConnection = await this.pool.getConnection();
        await this.transactionConnection.query('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ');
        await this.transactionConnection.query('START TRANSACTION WITH CONSISTENT SNAPSHOT');
    }

    async commitTransaction() {
        if (this.transactionConnection) {
            await this.transactionConnection.commit();
            this.transactionConnection.release();
            this.transactionConnection = null;
        }
    }

    async rollbackTransaction() {
        if (this.transactionConnection) {
            await this.transactionConnection.rollback();
            this.transactionConnection.release();
            this.transactionConnection = null;
        }
    }

    async releaseConnection() {
        if (this.transactionConnection) {
            this.transactionConnection.release();
            this.transactionConnection = null;
        }
    }

    async getConnection() {
        if (this.transactionConnection) return this.transactionConnection;
        return await this.pool.getConnection();
    }

    async close() {
        try { await this.pool.end(); } catch (e) {}
    }
}

// Create a TestDatabase for a given db name
async function createDb(dbName) {
    let mariadb = await getMariadb();
    let pool = mariadb.createPool({
        host: TEST_DB_HOST, port: TEST_DB_PORT,
        user: TEST_DB_USER, password: TEST_DB_PASS,
        database: dbName,
        connectionLimit: 10,
        insertIdAsNumber: true,
        bigIntAsNumber: true
    });
    return new TestDatabase(pool, dbName);
}

// Create database if it doesn't exist, then return a TestDatabase for it
async function createDatabase(dbName) {
    let mariadb = await getMariadb();
    let conn = await mariadb.createConnection({
        host: TEST_DB_HOST, port: TEST_DB_PORT,
        user: TEST_DB_USER, password: TEST_DB_PASS
    });
    await conn.query("CREATE DATABASE IF NOT EXISTS `" + dbName + "`");
    await conn.end();
    return await createDb(dbName);
}

// Load indexer schema SQL files into a database
async function seedSchema(db) {
    let sqlDir = path.join(__dirname, '..', '..', '..', '..', 'xchain-indexer', 'src', 'sql');
    let files = fs.readdirSync(sqlDir).filter(f => f.endsWith('.sql')).sort();

    let indexFiles = files.filter(f => f.startsWith('index_'));
    let otherFiles = files.filter(f => !f.startsWith('index_'));
    let ordered = [...indexFiles, ...otherFiles];

    for (let file of ordered) {
        let data = fs.readFileSync(path.join(sqlDir, file), 'utf8');
        let queries = data.split(';').map(q => q.trim()).filter(q => q);
        for (let query of queries) {
            try { await db.doQuery(query); } catch (e) {}
        }
    }

    // Also create sync_meta
    let syncMetaSql = fs.readFileSync(path.join(__dirname, '..', '..', '..', 'src', 'sql', 'sync_meta.sql'), 'utf8');
    let queries = syncMetaSql.split(';').map(q => q.trim()).filter(q => q);
    for (let query of queries) {
        try { await db.doQuery(query); } catch (e) {}
    }
}

// Get list of all tables in a database
async function getTables(db) {
    let rows = await db.doQuery(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_type = 'BASE TABLE' ORDER BY table_name",
        [db.dbName]
    );
    return rows.map(r => r.table_name || r.TABLE_NAME);
}

// Truncate all tables in a database
async function truncateAll(db) {
    let tables = await getTables(db);
    await db.doQuery("SET FOREIGN_KEY_CHECKS = 0");
    for (let table of tables) {
        await db.doQuery("TRUNCATE TABLE `" + table + "`");
    }
    await db.doQuery("SET FOREIGN_KEY_CHECKS = 1");
}

// Get row count for a table
async function getRowCount(db, table) {
    let rows = await db.doQuery("SELECT COUNT(*) as cnt FROM `" + table + "`");
    return Number(rows[0].cnt);
}

// Drop a database
async function dropDatabase(dbName) {
    let mariadb = await getMariadb();
    try {
        let conn = await mariadb.createConnection({
            host: TEST_DB_HOST, port: TEST_DB_PORT,
            user: TEST_DB_USER, password: TEST_DB_PASS
        });
        await conn.query("DROP DATABASE IF EXISTS `" + dbName + "`");
        await conn.end();
    } catch (e) {}
}

module.exports = {
    TEST_DB_HOST, TEST_DB_PORT, TEST_DB_USER, TEST_DB_PASS,
    SOURCE_DB_NAME, REPLICA_DB_NAME,
    util,
    createDb, createDatabase, seedSchema,
    getTables, truncateAll, getRowCount, dropDatabase,
    TestDatabase
};
