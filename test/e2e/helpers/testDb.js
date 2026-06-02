const path = require('path');
const fs   = require('fs');
const { getMariadb } = require('./mariadbLoader');
const Utility = require('../../../src/utility');
const { splitSqlStatements } = require('../../../src/sqlUtil');

const TEST_DB_HOST = process.env.E2E_DB_HOST || '127.0.0.1';
const TEST_DB_PORT = parseInt(process.env.E2E_DB_PORT) || 23306;
const TEST_DB_USER = process.env.E2E_DB_USER || 'xchain-node';
const TEST_DB_PASS = process.env.E2E_DB_PASS || 'XChain4life';

const REPLICA_DB_HOST = process.env.E2E_REPLICA_DB_HOST || '127.0.0.1';
const REPLICA_DB_PORT = parseInt(process.env.E2E_REPLICA_DB_PORT) || 23307;
const REPLICA_DB_USER = process.env.E2E_REPLICA_DB_USER || 'xchain-node';
const REPLICA_DB_PASS = process.env.E2E_REPLICA_DB_PASS || 'XChain4life';

const SOURCE2_DB_HOST = process.env.E2E_SOURCE2_DB_HOST || '127.0.0.1';
const SOURCE2_DB_PORT = parseInt(process.env.E2E_SOURCE2_DB_PORT) || 23308;
const SOURCE2_DB_USER = process.env.E2E_SOURCE2_DB_USER || 'xchain-node';
const SOURCE2_DB_PASS = process.env.E2E_SOURCE2_DB_PASS || 'XChain4life';

const SOURCE_DB_NAME  = 'xchain_e2e_source';
const REPLICA_DB_NAME = 'xchain_e2e_replica';
const SOURCE2_DB_NAME = 'xchain_e2e_source2';

const util = new Utility();

class TestDatabase {
    constructor(pool, dbName) {
        this.pool   = pool;
        this.dbName = dbName;
        this.transactionConnection = null;
    }

    async doQuery(query, args) {
        if (Array.isArray(args)) {
            for (let i = 0; i < args.length; i++) {
                // Mirror src/db.js: Buffers (binary columns) must reach the driver
                // intact — toString() would UTF-8-decode and corrupt them.
                if (args[i] !== null && args[i] !== undefined && typeof args[i] === 'object' && !Buffer.isBuffer(args[i]))
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
        return await this.doQuery("SELECT * FROM `" + table + "` ORDER BY 1 LIMIT ? OFFSET ?", [limit, offset]);
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

    async close() {
        try { await this.pool.end(); } catch (e) {}
    }
}

async function createDb(dbName, host, port, user, pass) {
    let mariadb = await getMariadb();
    let pool = mariadb.createPool({
        host: host || TEST_DB_HOST,
        port: port || TEST_DB_PORT,
        user: user || TEST_DB_USER,
        password: pass || TEST_DB_PASS,
        database: dbName,
        connectionLimit: 10,
        insertIdAsNumber: true,
        bigIntAsNumber: true,
        // Match src/db.js — without this, DATETIME/TIMESTAMP columns come back
        // as JS Dates, JSON.stringify emits ISO format, and MariaDB rejects
        // them on re-insert. Affects sync_meta.logged_at (which the poller
        // writes in the background during e2e tests, so the column shows up
        // in every snapshot).
        dateStrings: true
    });
    return new TestDatabase(pool, dbName);
}

async function createDatabase(dbName, host, port, user, pass) {
    let mariadb = await getMariadb();
    let h = host || TEST_DB_HOST;
    let p = port || TEST_DB_PORT;
    let u = user || TEST_DB_USER;
    let pw = pass || TEST_DB_PASS;
    // CREATE DATABASE needs a privileged user. The MARIADB_USER set up by the
    // docker-compose only has rights on databases that already exist (no global
    // CREATE), so use root for the admin step and then GRANT the test user.
    // Compose declares MARIADB_ROOT_PASSWORD=test for both source-db and replica-db.
    let admin = await mariadb.createConnection({ host: h, port: p, user: 'root', password: 'test' });
    await admin.query("CREATE DATABASE IF NOT EXISTS `" + dbName + "`");
    await admin.query("GRANT ALL PRIVILEGES ON `" + dbName + "`.* TO `" + u + "`@'%'");
    await admin.query("FLUSH PRIVILEGES");
    await admin.end();
    return await createDb(dbName, h, p, u, pw);
}

// The schema-seed SQL lives on the REDACTED-LOCAL-PATH Parallels share, which
// intermittently blips ENOENT on an existing file/dir between calls (see the
// project's parallels-fs-enoent-race note). A bare readdirSync/readFileSync here
// would crash a whole chaos/e2e suite's `before all` hook and cascade-fail every
// test under it. Retry the read a few times on ENOENT only; any other error (or a
// genuinely missing path after retries) still throws.
async function _fsReadRetry(fn) {
    let lastErr;
    for (let attempt = 0; attempt < 5; attempt++) {
        try { return fn(); }
        catch (e) {
            if (e.code !== 'ENOENT') throw e;
            lastErr = e;
            await new Promise(r => setTimeout(r, 300));
        }
    }
    throw lastErr;
}

async function seedSchema(db) {
    let sqlDir = path.join(__dirname, '..', '..', '..', '..', 'xchain-indexer', 'src', 'sql');
    let files = (await _fsReadRetry(() => fs.readdirSync(sqlDir))).filter(f => f.endsWith('.sql')).sort();

    let indexFiles = files.filter(f => f.startsWith('index_'));
    let otherFiles = files.filter(f => !f.startsWith('index_'));
    let ordered = [...indexFiles, ...otherFiles];

    for (let file of ordered) {
        let data = await _fsReadRetry(() => fs.readFileSync(path.join(sqlDir, file), 'utf8'));
        let queries = splitSqlStatements(data);
        for (let query of queries) {
            try { await db.doQuery(query); } catch (e) {}
        }
    }

    // Sync-service-owned tables (sync_meta, merkle_epochs). Idempotent: in e2e
    // the spawned server also creates these via db.js#verifyTables at startup.
    let syncSqlDir = path.join(__dirname, '..', '..', '..', 'src', 'sql');
    let syncFiles = (await _fsReadRetry(() => fs.readdirSync(syncSqlDir))).filter(f => f.endsWith('.sql')).sort();
    for (let file of syncFiles) {
        let sql = await _fsReadRetry(() => fs.readFileSync(path.join(syncSqlDir, file), 'utf8'));
        for (let query of splitSqlStatements(sql)) {
            try { await db.doQuery(query); } catch (e) {}
        }
    }
}

async function getTables(db) {
    let rows = await db.doQuery(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_type = 'BASE TABLE' ORDER BY table_name",
        [db.dbName]
    );
    return rows.map(r => r.table_name || r.TABLE_NAME);
}

async function truncateAll(db) {
    let tables = await getTables(db);
    await db.doQuery("SET FOREIGN_KEY_CHECKS = 0");
    for (let table of tables) {
        await db.doQuery("TRUNCATE TABLE `" + table + "`");
    }
    await db.doQuery("SET FOREIGN_KEY_CHECKS = 1");
}

async function getRowCount(db, table) {
    let rows = await db.doQuery("SELECT COUNT(*) as cnt FROM `" + table + "`");
    return Number(rows[0].cnt);
}

async function dropDatabase(dbName, host, port, user, pass) {
    let mariadb = await getMariadb();
    try {
        let conn = await mariadb.createConnection({
            host: host || TEST_DB_HOST,
            port: port || TEST_DB_PORT,
            user: user || TEST_DB_USER,
            password: pass || TEST_DB_PASS
        });
        await conn.query("DROP DATABASE IF EXISTS `" + dbName + "`");
        await conn.end();
    } catch (e) {}
}

module.exports = {
    TEST_DB_HOST, TEST_DB_PORT, TEST_DB_USER, TEST_DB_PASS,
    REPLICA_DB_HOST, REPLICA_DB_PORT, REPLICA_DB_USER, REPLICA_DB_PASS,
    SOURCE2_DB_HOST, SOURCE2_DB_PORT, SOURCE2_DB_USER, SOURCE2_DB_PASS,
    SOURCE_DB_NAME, REPLICA_DB_NAME, SOURCE2_DB_NAME,
    util,
    createDb, createDatabase, seedSchema,
    getTables, truncateAll, getRowCount, dropDatabase,
    TestDatabase
};
