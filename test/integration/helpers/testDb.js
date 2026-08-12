// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

const path = require('path');
const fs   = require('fs');
const { getMariadb } = require('./mariadbLoader');
const Utility = require('../../../src/utility');
const { splitSqlStatements } = require('../../../src/sqlUtil');

// These suites connect to a REAL MariaDB; credentials come from the
// environment only. No fallback value: a committed default is a published
// password the moment the repo is public.
const TEST_DB_HOST = process.env.TEST_DB_HOST || '127.0.0.1';
const TEST_DB_PORT = parseInt(process.env.TEST_DB_PORT) || 3306;
const TEST_DB_USER = process.env.TEST_DB_USER || 'root';
const TEST_DB_PASS = process.env.TEST_DB_PASS;
if (TEST_DB_PASS === undefined)
    throw new Error('TEST_DB_PASS must be set (plus TEST_DB_HOST/PORT/USER as needed) to run the DB-backed suites');

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

    // conn (optional): run on an explicit connection (read-snapshot path) instead
    // of acquiring one (mirrors src/db.js). The caller owns that connection's
    // lifecycle (commit/rollback/release).
    async doQuery(query, args, conn) {
        if (Array.isArray(args)) {
            for (let i = 0; i < args.length; i++) {
                // Mirror src/db.js: Buffers (binary columns) must reach the driver
                // intact; toString() would UTF-8-decode and corrupt them.
                if (args[i] !== null && args[i] !== undefined && typeof args[i] === 'object' && !Buffer.isBuffer(args[i]))
                    args[i] = args[i].toString();
            }
        }
        if (conn) return await conn.query(query, args);
        let c = this.transactionConnection || await this.pool.getConnection();
        try {
            return await c.query(query, args);
        } finally {
            if (!this.transactionConnection) c.release();
        }
    }

    // M-17 strict read. Production's doQuery collapses a NON-transactional query
    // error into [], which is why the sub-tree derivations read through
    // doQueryStrict; this harness's doQuery already throws on every error, so the
    // strict contract is what it has always offered and the alias just lets those
    // derivations resolve here. Without it, every DB-backed test that turns the
    // state-commitment path ON dies on `doQueryStrict is not a function`, which is
    // one structural reason no integration test had ever exercised it.
    async doQueryStrict(query, args, conn) {
        return await this.doQuery(query, args, conn);
    }

    async getLastBlock(conn) {
        let rows = await this.doQuery("SELECT MAX(block_index) AS block_index FROM blocks", null, conn);
        if (rows.length > 0 && rows[0].block_index !== null)
            return Number(rows[0].block_index);
        return null;
    }

    async getBlockHashRow(block_index, conn) {
        let rows = await this.doQuery(`SELECT
            b.block_index, b.block_time,
            t1.hash as ledger_hash, t2.hash as actions_hash, t3.hash as contract_hash
            FROM blocks b
            LEFT JOIN index_transactions t1 ON (t1.id=b.ledger_hash_id)
            LEFT JOIN index_transactions t2 ON (t2.id=b.actions_hash_id)
            LEFT JOIN index_transactions t3 ON (t3.id=b.contract_hash_id)
            WHERE b.block_index=?`, [block_index], conn);
        return rows.length > 0 ? rows[0] : null;
    }

    async getFirstActionIndex(block_index, conn) {
        let rows = await this.doQuery(
            "SELECT action_index FROM actions WHERE block_index >= ? ORDER BY action_index ASC LIMIT 1",
            [block_index], conn
        );
        return rows.length > 0 ? Number(rows[0].action_index) : null;
    }

    // Mirror of src/db.js getStatusId(): resolves a status string to its
    // index_statuses id (or null). ClientRollback.js calls this during the
    // cooldown-maturity re-derive; the integration TestDatabase needs it so the
    // S-5 client-rollback suite runs against a real DB instead of erroring on a
    // missing method. SQL is byte-identical to production.
    async getStatusId(status) {
        let rows = await this.doQuery("SELECT id FROM index_statuses WHERE status = ? LIMIT 1", [status]);
        return rows.length > 0 ? Number(rows[0].id) : null;
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

    // Mirror of src/db.js getNonEmptyActionScopedTables(): a UNION-ALL probe naming
    // which action-scoped tables carry rows in this block, so ServerPoller queries
    // content instead of the whole registry. The existence predicate must match this
    // class's getActionScopedRows predicate exactly, or "probe says empty" would stop
    // meaning "fetch returns []" and the payload would silently drift from production.
    // Missing tables are filtered out first since one absent table fails the whole UNION.
    async getNonEmptyActionScopedTables(tables, block_index) {
        const present = new Set(await getTables(this));
        const candidates = tables.filter(t => /^[A-Za-z0-9_]+$/.test(t) && present.has(t));
        if (candidates.length === 0) return new Set();
        const branches = candidates.map(table =>
            `(SELECT '${table}' AS tbl FROM \`${table}\` t
                INNER JOIN actions a ON (a.action_index = t.action_index)
                INNER JOIN transactions tx ON (tx.tx_index = a.tx_index)
                WHERE tx.block_index = ? LIMIT 1)`);
        const rows = await this.doQuery(branches.join(' UNION ALL '),
                                        candidates.map(() => block_index));
        return new Set(rows.map(r => r.tbl));
    }

    // Mirror of src/db.js getEmissionRowsForBlock(): block-scopes contract_emissions
    // through execution_index -> contract_executions -> actions (NOT em.action_index),
    // so NULL-action_index internal emissions (e.g. SLASH) are included. These are the rows the
    // action-scoped INNER JOIN drops. Must stay byte-identical to the production query.
    async getEmissionRowsForBlock(block_index) {
        return await this.doQuery(`SELECT em.execution_index, em.emitted_action, em.action_index, em.position
            FROM contract_emissions em
            INNER JOIN contract_executions ce ON (ce.action_index = em.execution_index)
            INNER JOIN actions a ON (a.action_index = ce.action_index)
            INNER JOIN transactions tx ON (tx.tx_index = a.tx_index)
            WHERE tx.block_index = ?
            ORDER BY em.execution_index ASC, em.position ASC`, [block_index]);
    }

    async getTransactions(block_index) {
        return await this.doQuery("SELECT * FROM transactions WHERE block_index = ?", [block_index]);
    }

    async getActions(block_index) {
        return await this.doQuery(`SELECT a.* FROM actions a
            INNER JOIN transactions t ON (t.tx_index = a.tx_index)
            WHERE t.block_index = ?`, [block_index]);
    }

    // Mirrors src/db.js: one un-paged ordered streaming pass per table (LIMIT/OFFSET
    // re-paging had no total order on keyless tables and could dup/skip a boundary tie).
    streamTableRows(table, conn) {
        return conn.queryStream("SELECT * FROM `" + table + "` ORDER BY 1");
    }

    async getTableCount(table, conn) {
        let rows = await this.doQuery("SELECT COUNT(*) as cnt FROM `" + table + "`", null, conn);
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

    // Mirrors src/db.js post-#3732: a read snapshot uses a DEDICATED connection
    // (returned to the caller), independent of the shared transactionConnection,
    // so a concurrent writer never collides with the snapshot read.
    async beginReadSnapshot() {
        let conn = await this.pool.getConnection();
        try {
            await conn.query('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ');
            await conn.query('START TRANSACTION WITH CONSISTENT SNAPSHOT');
        } catch (e) {
            try { conn.release(); } catch (_e) {}
            throw e;
        }
        return conn;
    }

    async commitReadSnapshot(conn) {
        if (!conn) return;
        try { await conn.commit(); } finally { conn.release(); }
    }

    async rollbackReadSnapshot(conn) {
        if (!conn) return;
        try { await conn.rollback(); } catch (e) { /* best-effort */ } finally { conn.release(); }
    }

    // Run fn(conn) inside a single transaction on a DEDICATED connection (see
    // the e2e TestDatabase twin): the shared fixtures commit each seeded block
    // atomically so no snapshot ever observes a half-written block.
    async withTransaction(fn) {
        let conn = await this.pool.getConnection();
        try {
            await conn.beginTransaction();
            let result = await fn(conn);
            await conn.commit();
            return result;
        } catch (e) {
            try { await conn.rollback(); } catch (_) {}
            throw e;
        } finally {
            try { conn.release(); } catch (_) {}
        }
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

// Adopt selected REAL Database methods by reference instead of reimplementing them:
// each is pure SQL over this.doQuery/getStatusId (which this wrapper already
// provides), so borrowing them means the harness can't drift from the queries
// production actually runs. These three back the state-commitment path (prior
// block roots, block_merkle_root, and stakes_root) plus the private helpers they compose.
const RealDatabase = require('../../../src/db');
for (const method of ['getStateRootsRow', 'getBlockLeafRows',
                      'getStakeWeightsByCapability', '_stakeWeightsSql',
                      '_applyStakeWeightCap', '_cappedStakeWeightsSql'])
    TestDatabase.prototype[method] = RealDatabase.prototype[method];

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
    // Prefer the sibling-schema path the CI e2e job exports (XCHAIN_INDEXER_SQL_PATH
    // points at the checked-out xchain-indexer/src/sql); fall back to the monorepo
    // sibling layout for local runs. Mirrors the convention in rollback-coverage.test.js
    // so these DB-backed suites can run in CI without a sibling working copy.
    let sqlDir = process.env.XCHAIN_INDEXER_SQL_PATH
        || path.join(__dirname, '..', '..', '..', '..', 'xchain-indexer', 'src', 'sql');
    let files = fs.readdirSync(sqlDir).filter(f => f.endsWith('.sql')).sort();

    let indexFiles = files.filter(f => f.startsWith('index_'));
    let otherFiles = files.filter(f => !f.startsWith('index_'));
    let ordered = [...indexFiles, ...otherFiles];

    for (let file of ordered) {
        let data = fs.readFileSync(path.join(sqlDir, file), 'utf8');
        let queries = splitSqlStatements(data);
        for (let query of queries) {
            try { await db.doQuery(query); } catch (e) {}
        }
    }

    // Also create the sync-service-owned tables (sync_meta, merkle_epochs).
    // In production these are created by db.js#verifyTables at startup; the
    // integration harness builds the app manually, so seed them here.
    let syncSqlDir = path.join(__dirname, '..', '..', '..', 'src', 'sql');
    for (let file of fs.readdirSync(syncSqlDir).filter(f => f.endsWith('.sql')).sort()) {
        let sql = fs.readFileSync(path.join(syncSqlDir, file), 'utf8');
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
