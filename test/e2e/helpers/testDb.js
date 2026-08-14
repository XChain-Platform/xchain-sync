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
const validation = require('../../../src/validation');

const TEST_DB_HOST = process.env.E2E_DB_HOST || '127.0.0.1';
const TEST_DB_PORT = parseInt(process.env.E2E_DB_PORT) || 23306;
const TEST_DB_USER = process.env.E2E_DB_USER || 'xchain-node';
const TEST_DB_PASS = process.env.E2E_DB_PASS || 'xchain-fixture-throwaway';

const REPLICA_DB_HOST = process.env.E2E_REPLICA_DB_HOST || '127.0.0.1';
const REPLICA_DB_PORT = parseInt(process.env.E2E_REPLICA_DB_PORT) || 23307;
const REPLICA_DB_USER = process.env.E2E_REPLICA_DB_USER || 'xchain-node';
const REPLICA_DB_PASS = process.env.E2E_REPLICA_DB_PASS || 'xchain-fixture-throwaway';

const SOURCE2_DB_HOST = process.env.E2E_SOURCE2_DB_HOST || '127.0.0.1';
const SOURCE2_DB_PORT = parseInt(process.env.E2E_SOURCE2_DB_PORT) || 23308;
const SOURCE2_DB_USER = process.env.E2E_SOURCE2_DB_USER || 'xchain-node';
const SOURCE2_DB_PASS = process.env.E2E_SOURCE2_DB_PASS || 'xchain-fixture-throwaway';

const SOURCE_DB_NAME  = 'xchain_e2e_source';
const REPLICA_DB_NAME = 'xchain_e2e_replica';
const SOURCE2_DB_NAME = 'xchain_e2e_source2';

const util = new Utility();

class TestDatabase {
    constructor(pool, dbName, connCfg) {
        this.pool   = pool;
        this.dbName = dbName;
        this.transactionConnection = null;
        // Connection params (sans pool options) so the schema seeder can open a
        // one-off multipleStatements connection without re-deriving host/port/user.
        this.connCfg = connCfg || null;
    }

    async doQuery(query, args, conn) {
        if (Array.isArray(args)) {
            for (let i = 0; i < args.length; i++) {
                // Mirror src/db.js: Buffers (binary columns) must reach the driver
                // intact; toString() would UTF-8-decode and corrupt them.
                if (args[i] !== null && args[i] !== undefined && typeof args[i] === 'object' && !Buffer.isBuffer(args[i]))
                    args[i] = args[i].toString();
            }
        }
        // Explicit connection (read snapshots, fixture block transactions): the
        // caller owns its lifecycle, errors propagate, no retry. Mirrors
        // src/db.js doQuery(query, args, conn).
        if (conn) return await conn.query(query, args);
        // Non-transaction queries retry once on a fatal connection error. The chaos
        // suite disables/re-enables the DB proxy (toxiproxy) to simulate outages,
        // which leaves dead connections in the pool; a stale one handed out here
        // throws "Cannot execute new commands: connection closed" and would otherwise
        // spuriously fail a test (frequently the NEXT test's setup, since the pool is
        // reused across the describe). Re-acquiring discards the dead connection and
        // gets a fresh one. Transaction connections are not retried (a broken
        // transaction can't be resumed).
        // Retry enough to drain a whole pool of dead connections (connectionLimit is
        // 10): each stale connection is destroyed so the pool replaces it with a fresh
        // one, and only a query-level connection error is retried. A real outage makes
        // getConnection() itself throw (outside the try) and propagates immediately, so
        // this never spins on a genuinely-down DB.
        let maxAttempts = this.transactionConnection ? 1 : 12;
        let lastErr;
        for (let attempt = 1; attempt <= maxAttempts; attempt++) {
            let conn = this.transactionConnection || await this.pool.getConnection();
            let deadConn = false;
            try {
                return await conn.query(query, args);
            } catch (e) {
                lastErr = e;
                let isConnErr = e.fatal === true || /connection (closed|reset|lost)|ECONNRESET|socket/i.test(e.message || '');
                deadConn = !this.transactionConnection && isConnErr;
                if (!deadConn || attempt >= maxAttempts) throw e;
            } finally {
                if (!this.transactionConnection) {
                    try { if (deadConn && conn.destroy) conn.destroy(); else conn.release(); } catch (_) {}
                }
            }
        }
        throw lastErr;
    }

    // M-17 strict read, the same alias test/integration/helpers/testDb.js already
    // carries. Production's doQuery collapses a NON-transactional query error into
    // [], so the sub-tree derivations read through doQueryStrict; this harness's
    // doQuery already throws on every error, so strict is the contract it has
    // always offered and the alias just lets those derivations resolve here.
    // Without it the state-commitment conformance suite dies on
    // `db.doQueryStrict is not a function`, which is a missing stand-in method
    // reported as if the read itself were broken. The integration harness was
    // given this and the e2e one was not, which is why the same suite passes at
    // one tier and fails at the other.
    async doQueryStrict(query, args, conn) {
        return await this.doQuery(query, args, conn);
    }

    // Committed-state read, bypassing transactionConnection. The client under
    // test applies blocks inside a transaction ON THIS OBJECT; a wait/assert
    // poll issued through doQuery rides that same connection and interleaves
    // INSIDE the open apply transaction, observing the uncommitted blocks row
    // before the rest of the apply (e.g. the balances rebuild) has run. Tests
    // that then assert on "the replica reached block N" race half-applied state.
    async doQueryCommitted(query, args) {
        let conn = await this.pool.getConnection();
        try { return await conn.query(query, args); }
        finally { try { conn.release(); } catch (_) {} }
    }

    async getLastBlock(conn) {
        let q = "SELECT MAX(block_index) AS block_index FROM blocks";
        let rows = conn ? await this.doQuery(q, [], conn) : await this.doQueryCommitted(q);
        if (rows.length > 0 && rows[0].block_index !== null)
            return Number(rows[0].block_index);
        return null;
    }

    async getBlockHashRow(block_index, conn) {
        // Mirror src/db.js: the fourth (replication-integrity) state_hash is
        // surfaced via the state_hash_id join so ServerPoller._buildBlockPayload can
        // ship it and a follower with VERIFY_STATE_HASH can recompute + compare.
        // NULL for blocks that never stored one (the common fixture case), which the
        // follower skips exactly as it does for pre-feature blocks.
        let rows = await this.doQuery(`SELECT
            b.block_index, b.block_time,
            t1.hash as ledger_hash, t2.hash as actions_hash, t3.hash as contract_hash,
            t4.hash as state_hash
            FROM blocks b
            LEFT JOIN index_transactions t1 ON (t1.id=b.ledger_hash_id)
            LEFT JOIN index_transactions t2 ON (t2.id=b.actions_hash_id)
            LEFT JOIN index_transactions t3 ON (t3.id=b.contract_hash_id)
            LEFT JOIN index_transactions t4 ON (t4.id=b.state_hash_id)
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

    async getBlockScopedRows(table, block_index, conn) {
        return await this.doQuery("SELECT * FROM `" + table + "` WHERE block_index = ?", [block_index], conn);
    }

    // Mirror src/db.js getEmissionRowsForBlock (568c800): ServerPoller streams
    // contract_emissions via the execution_index -> contract_executions chain
    // (byte-aligned with BlockHasher), not the generic action-scoped join.
    async getEmissionRowsForBlock(block_index, conn) {
        return await this.doQuery(`SELECT em.execution_index, em.emitted_action, em.action_index, em.position
            FROM contract_emissions em
            INNER JOIN contract_executions ce ON (ce.action_index = em.execution_index)
            INNER JOIN actions a ON (a.action_index = ce.action_index)
            WHERE a.block_index = ?
            ORDER BY em.execution_index ASC, em.position ASC`, [block_index], conn);
    }

    async getActionScopedRows(table, block_index, conn) {
        return await this.doQuery(`SELECT t.* FROM \`${table}\` t
            INNER JOIN actions a ON (a.action_index = t.action_index)
            INNER JOIN transactions tx ON (tx.tx_index = a.tx_index)
            WHERE tx.block_index = ?`, [block_index], conn);
    }

    async getTransactions(block_index, conn) {
        return await this.doQuery("SELECT * FROM transactions WHERE block_index = ?", [block_index], conn);
    }

    async getActions(block_index, conn) {
        return await this.doQuery(`SELECT a.* FROM actions a
            INNER JOIN transactions t ON (t.tx_index = a.tx_index)
            WHERE t.block_index = ?`, [block_index], conn);
    }

    // Mirrors src/db.js: one un-paged ordered streaming pass per table (LIMIT/OFFSET
    // re-paging had no total order on keyless tables and could dup/skip a boundary tie).
    // This harness's beginReadSnapshot tracks the connection on the instance and
    // returns undefined, so fall back to the tracked transactionConnection.
    streamTableRows(table, conn) {
        let c = conn || this.transactionConnection;
        return c.queryStream("SELECT * FROM `" + table + "` ORDER BY 1");
    }

    async getTableCount(table) {
        let rows = await this.doQuery("SELECT COUNT(*) as cnt FROM `" + table + "`");
        return Number(rows[0].cnt);
    }

    async truncateTable(table) {
        await this.doQuery("TRUNCATE TABLE `" + table + "`");
    }

    // Mirror src/db.js: ClientSync's schema-apply calls this on a table that
    // already exists (the e2e pre-seeds the replica schema), to propagate any
    // columns the source has added since bootstrap. Without it the bootstrap
    // hits "this.db.addMissingColumns is not a function", every table lands in
    // the pending set, and the run halts with schema-apply-failed.
    async addMissingColumns(tableName, sourceDdl) {
        let sourceColumns = validation.extractColumnNames(sourceDdl);
        if (sourceColumns.length === 0) return 0;
        let destRows = await this.doQuery(
            "SELECT column_name FROM information_schema.columns WHERE table_schema = ? AND table_name = ?",
            [this.dbName, tableName]
        );
        let destSet = new Set(destRows.map(r => r.column_name || r.COLUMN_NAME));
        let added = 0;
        for (let col of sourceColumns) {
            if (destSet.has(col)) continue;
            if (!validation.validateIdentifier(col).valid) continue;
            let def = validation.extractColumnDefinition(sourceDdl, col);
            if (!def) continue;
            try {
                await this.doQuery("ALTER TABLE `" + tableName + "` ADD COLUMN " + def);
                added++;
            } catch (e) { /* mirror src/db.js: per-column failure is logged, not fatal */ }
        }
        return added;
    }

    // Durable divergence-halt records (mirrors src/db.js so ClientSync's halt
    // machinery (recordHalt at divergence, getActiveHalt at startup, clearHalt
    // by the operator) runs for real against the test replica instead of
    // failing into the in-memory-only fallback.
    async recordHalt(dbType, blockIndex, reason, mismatches, sources) {
        let existing = await this.getActiveHalt(dbType);
        if (existing && Number(existing.block_index) === Number(blockIndex)) return existing;
        await this.doQuery(
            "INSERT INTO sync_halt (db_type, block_index, reason, mismatches, sources) VALUES (?, ?, ?, ?, ?)",
            [dbType, blockIndex, String(reason || 'divergence').slice(0, 64),
             JSON.stringify(mismatches || []), JSON.stringify(sources || [])]
        );
        return await this.getActiveHalt(dbType);
    }

    async getActiveHalt(dbType) {
        let rows = await this.doQuery(
            "SELECT * FROM sync_halt WHERE db_type=? AND cleared_at IS NULL ORDER BY id DESC LIMIT 1",
            [dbType]
        );
        return (rows && rows.length) ? rows[0] : null;
    }

    async clearHalt(dbType) {
        let res = await this.doQuery(
            "UPDATE sync_halt SET cleared_at=NOW() WHERE db_type=? AND cleared_at IS NULL",
            [dbType]
        );
        return res ? res.affectedRows : 0;
    }

    async beginTransaction() {
        if (this.transactionConnection) await this.releaseConnection();
        this.transactionConnection = await this.pool.getConnection();
        await this.transactionConnection.beginTransaction();
    }

    // DEDICATED-connection read snapshot, mirroring src/db.js exactly. This used
    // to park the snapshot on the shared instance-level transactionConnection,
    // which src/db.js explicitly forbids: the source TestDatabase is shared by
    // the fixtures (writes), the ServerPoller's per-batch snapshot, concurrent
    // /snapshot requests, and (in cross-source tests) a SECOND server's poller.
    // A second beginReadSnapshot released the first's connection with its
    // transaction still open, implicitly ROLLING BACK whatever had ridden it
    // (fixture block seeds, TransparencyLog sync_meta records), which surfaced
    // as flakes: a seeded tip that never appeared (waitFor timeout) and
    // replica sync_meta rows the source no longer had (parity divergence).
    async beginReadSnapshot() {
        let conn = await this.pool.getConnection();
        try {
            await conn.query('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ');
            await conn.query('START TRANSACTION WITH CONSISTENT SNAPSHOT');
        } catch (e) {
            try { conn.release(); } catch (_) {}
            throw e;
        }
        return conn;
    }

    // End / abort a read snapshot opened by beginReadSnapshot. SnapshotBuilder
    // and ServerPoller pass back the dedicated connection they were returned.
    async commitReadSnapshot(conn) {
        if (!conn) return;
        try { await conn.commit(); }
        finally { try { conn.release(); } catch (_) {} }
    }

    async rollbackReadSnapshot(conn) {
        if (!conn) return;
        try { await conn.rollback(); }
        catch (e) { /* best-effort; release regardless */ }
        finally { try { conn.release(); } catch (_) {} }
    }

    // Run fn(conn) inside a single transaction on a DEDICATED connection.
    // Used by the fixtures to commit each seeded block atomically, modelling
    // the production indexer's per-block transaction: no reader (poller
    // snapshot, /snapshot stream) may ever observe a half-written block.
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

    // Resolve a status name to its index_statuses id (matches src/db.js). The
    // reorg-rollback path (ClientRollback) and stateHash look up 'completed' /
    // 'valid' here; without it a reorg rollback threw "getStatusId is not a
    // function" and halted the client (reorg-rollback-failed).
    async getStatusId(status) {
        let rows = await this.doQuery("SELECT id FROM index_statuses WHERE status = ? LIMIT 1", [status]);
        return rows.length > 0 ? Number(rows[0].id) : null;
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
        // Match src/db.js: without this, DATETIME/TIMESTAMP columns come back
        // as JS Dates, JSON.stringify emits ISO format, and MariaDB rejects
        // them on re-insert. Affects sync_meta.logged_at (which the poller
        // writes in the background during e2e tests, so the column shows up
        // in every snapshot).
        dateStrings: true
    });
    return new TestDatabase(pool, dbName, {
        host: host || TEST_DB_HOST,
        port: port || TEST_DB_PORT,
        user: user || TEST_DB_USER,
        password: pass || TEST_DB_PASS,
        database: dbName
    });
}

async function createDatabase(dbName, host, port, user, pass) {
    let mariadb = await getMariadb();
    let h = host || TEST_DB_HOST;
    let p = port || TEST_DB_PORT;
    let u = user || TEST_DB_USER;
    let pw = pass || TEST_DB_PASS;
    // CREATE DATABASE needs a privileged user. The MARIADB_USER set up by the
    // docker-compose only has rights on databases that already exist (no global
    // CREATE), so an admin account does the create + grant. Defaults match the
    // compose containers (root / MARIADB_ROOT_PASSWORD=test); override with
    // E2E_DB_ADMIN_USER / E2E_DB_ADMIN_PASS to run the suites against any other
    // MariaDB (e.g. one shared instance hosting all three databases by name).
    let adminUser = process.env.E2E_DB_ADMIN_USER || 'root';
    let adminPass = process.env.E2E_DB_ADMIN_PASS !== undefined ? process.env.E2E_DB_ADMIN_PASS : 'test';
    let admin = await mariadb.createConnection({ host: h, port: p, user: adminUser, password: adminPass });
    await admin.query("CREATE DATABASE IF NOT EXISTS `" + dbName + "`");
    // Granting to yourself is a no-op that needs GRANT OPTION; skip it when the
    // test user IS the admin user.
    if (u !== adminUser) {
        await admin.query("GRANT ALL PRIVILEGES ON `" + dbName + "`.* TO `" + u + "`@'%'");
        await admin.query("FLUSH PRIVILEGES");
    }
    await admin.end();
    return await createDb(dbName, h, p, u, pw);
}

// The schema-seed SQL may live on a shared network filesystem, which
// intermittently blips ENOENT on an existing file/dir between calls. A bare
// readdirSync/readFileSync here would crash a whole chaos/e2e suite's
// `before all` hook and cascade-fail every test under it. Retry the read a
// few times on ENOENT only; any other error (or a genuinely missing path
// after retries) still throws.
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

// Apply one schema file's statements over a multipleStatements connection. The
// indexer schema is ~650 statements across ~110 files; running each statement as
// its own pooled round-trip made the e2e `before all` seed take ~2 minutes per
// database (source + replica), blowing past mocha's hook timeout and presenting
// as a "hang" on indexer-dbType suites. Batching each file into a single
// multi-statement query collapses the per-statement round-trips. We split first
// (splitSqlStatements strips `--` comments so a ';' inside prose never reaches
// the driver's own splitter, the attests.sql comment-split bug) then re-join, so
// the driver only ever sees clean `stmt; stmt`. On any batch error we fall back
// to per-statement execution with the same swallow-and-continue tolerance the
// original loop had (DROP-then-CREATE on a fresh DB, re-seed idempotency).
async function _applySqlBatch(conn, sqlText) {
    let queries = splitSqlStatements(sqlText);
    if (queries.length === 0) return;
    try {
        await conn.query(queries.join(';\n'));
    } catch (e) {
        for (let query of queries) {
            try { await conn.query(query); } catch (_) {}
        }
    }
}

async function seedSchema(db) {
    // The canonical indexer schema lives in the sibling repo checkout. CI checks
    // xchain-indexer out elsewhere and points here via XCHAIN_INDEXER_SQL_PATH.
    let sqlDir = process.env.XCHAIN_INDEXER_SQL_PATH
        || path.join(__dirname, '..', '..', '..', '..', 'xchain-indexer', 'src', 'sql');
    let files = (await _fsReadRetry(() => fs.readdirSync(sqlDir))).filter(f => f.endsWith('.sql')).sort();

    let indexFiles = files.filter(f => f.startsWith('index_'));
    let otherFiles = files.filter(f => !f.startsWith('index_'));
    let ordered = [...indexFiles, ...otherFiles];

    let syncSqlDir = path.join(__dirname, '..', '..', '..', 'src', 'sql');
    let syncFiles = (await _fsReadRetry(() => fs.readdirSync(syncSqlDir))).filter(f => f.endsWith('.sql')).sort();

    // One dedicated multipleStatements connection for the whole seed. Falls back
    // to the pooled per-statement path if a raw connection can't be opened.
    let mariadb = await getMariadb();
    let conn = null;
    if (db.connCfg) {
        try { conn = await mariadb.createConnection({ ...db.connCfg, multipleStatements: true }); }
        catch (e) { conn = null; }
    }

    if (conn) {
        try {
            for (let file of ordered) {
                let data = await _fsReadRetry(() => fs.readFileSync(path.join(sqlDir, file), 'utf8'));
                await _applySqlBatch(conn, data);
            }
            // Sync-service-owned tables (sync_meta, merkle_epochs). Idempotent: in
            // e2e the spawned server also creates these via db.js#verifyTables.
            for (let file of syncFiles) {
                let sql = await _fsReadRetry(() => fs.readFileSync(path.join(syncSqlDir, file), 'utf8'));
                await _applySqlBatch(conn, sql);
            }
        } finally {
            try { await conn.end(); } catch (_) {}
        }
        return;
    }

    // Fallback: original per-statement pooled path.
    for (let file of ordered) {
        let data = await _fsReadRetry(() => fs.readFileSync(path.join(sqlDir, file), 'utf8'));
        for (let query of splitSqlStatements(data)) {
            try { await db.doQuery(query); } catch (e) {}
        }
    }
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
