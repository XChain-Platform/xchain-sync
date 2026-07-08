// Copyright © 2025–2026 Dankest, LLC
// SPDX-License-Identifier: AGPL-3.0-or-later

'use strict';

const assert      = require('assert');
const sinon       = require('sinon');
const fs          = require('fs');
const proxyquire  = require('proxyquire').noCallThru();
const Database    = require('../../src/db');

// ─── proxyquire-based Database factory ──────────────────────────────────────
// mariadb is an ES module: sinon.stub(mariadb, 'createConnection') throws
// "ES Modules cannot be stubbed".  For tests that need to control
// createConnection we build a fresh Database class with a fake mariadb
// injected via proxyquire.

function makeDbWithFakeMariadb(fakeMariadb, dbType = 'indexer', dbName = 'replica_db', util = null) {
    let FakeDatabase = proxyquire('../../src/db', {
        mariadb: fakeMariadb
    });
    return new FakeDatabase('localhost', 3306, dbName, 'u', 'p', util || makeUtil(), dbType);
}

/** Build a minimal fake mariadb module */
function fakeMariadbWith(createConnectionFn) {
    return {
        createPool:        () => ({ end: sinon.stub().resolves(), getConnection: sinon.stub().resolves(fakeConn()) }),
        createConnection:  createConnectionFn,
        '@noCallThru':     true,
    };
}

// ─── helpers ────────────────────────────────────────────────────────────────

/**
 * Build a util stub.  `throwError` behaves like the real one (throws),
 * `sleep` resolves immediately (no real delays), `logError` / `isNull` are
 * enough for the tested paths.
 */
function makeUtil(overrides = {}) {
    return Object.assign({
        isNull:     (v) => v === null || v === undefined,
        throwError: (msg) => { throw new Error(msg); },
        sleep:      sinon.stub().resolves(),
        logError:   sinon.stub(),
    }, overrides);
}

/** Construct a Database with a given dbType (default 'indexer'). */
function makeDb(dbType = 'indexer', dbName = 'replica_db', util = null) {
    return new Database('localhost', 3306, dbName, 'u', 'p', util || makeUtil(), dbType);
}

/** Fake pool.getConnection → returns a fake connection */
function fakeConn(queryResult = []) {
    return {
        query:            sinon.stub().resolves(queryResult),
        release:          sinon.stub().resolves(),
        beginTransaction: sinon.stub().resolves(),
        rollback:         sinon.stub().resolves(),
        commit:           sinon.stub().resolves(),
    };
}

/** Stub console.log/error/warn to silence noise in all tests. */
function silenceConsole() {
    sinon.stub(console, 'log');
    sinon.stub(console, 'error');
    sinon.stub(console, 'warn');
}

// ═══════════════════════════════════════════════════════════════════════════
// 1. close()
// ═══════════════════════════════════════════════════════════════════════════
describe('Database.close()', function () {
    let db;
    beforeEach(function () { silenceConsole(); db = makeDb(); });
    afterEach(async function () { sinon.restore(); });

    it('calls pool.end() and resolves', async function () {
        let ended = false;
        sinon.stub(db.pool, 'end').callsFake(async () => { ended = true; });
        await db.close();
        assert.ok(ended);
    });

    it('swallows a pool.end() error', async function () {
        sinon.stub(db.pool, 'end').rejects(new Error('boom'));
        // should not throw
        await db.close();
    });
});

// ═══════════════════════════════════════════════════════════════════════════
// 1b. constructor: dbType default
// ═══════════════════════════════════════════════════════════════════════════
describe('Database constructor: dbType default', function () {
    afterEach(async function () { sinon.restore(); });

    it('defaults dbType to "indexer" when not provided', async function () {
        silenceConsole();
        let db = new Database('localhost', 3306, 'db', 'u', 'p', makeUtil());
        assert.strictEqual(db.dbType, 'indexer');
        await db.close();
    });
});

// ═══════════════════════════════════════════════════════════════════════════
// 2. doQuery()
// ═══════════════════════════════════════════════════════════════════════════
describe('Database.doQuery()', function () {
    let db;
    beforeEach(function () { silenceConsole(); db = makeDb(); });
    afterEach(async function () { sinon.restore(); await db.close(); });

    it('returns [] and does NOT call getConnection when query is null', async function () {
        let spy = sinon.stub(db, 'getConnection');
        let result = await db.doQuery(null);
        assert.deepStrictEqual(result, []);
        assert.ok(spy.notCalled);
    });

    it('returns [] when query is undefined', async function () {
        let spy = sinon.stub(db, 'getConnection');
        let result = await db.doQuery(undefined);
        assert.deepStrictEqual(result, []);
        assert.ok(spy.notCalled);
    });

    it('runs a query via a connection and releases it (non-tx path)', async function () {
        let conn = fakeConn([{ id: 1 }]);
        sinon.stub(db, 'getConnection').resolves(conn);
        let result = await db.doQuery('SELECT 1', []);
        assert.deepStrictEqual(result, [{ id: 1 }]);
        assert.ok(conn.release.calledOnce);
    });

    it('coerces plain-object args to string, leaves Buffer intact', async function () {
        let conn = fakeConn([]);
        sinon.stub(db, 'getConnection').resolves(conn);
        let buf = Buffer.from('hello');
        let obj = { toString: () => 'myobj' };
        await db.doQuery('SELECT ?', [obj, buf, null]);
        let passedArgs = conn.query.firstCall.args[1];
        assert.strictEqual(passedArgs[0], 'myobj');
        assert.ok(Buffer.isBuffer(passedArgs[1]));
        assert.strictEqual(passedArgs[2], null);
    });

    it('does NOT release conn when inside a transaction', async function () {
        let conn = fakeConn([{ ok: true }]);
        // Simulate an active transaction connection
        db.transactionConnection = conn;
        sinon.stub(db, 'getConnection').resolves(conn);
        await db.doQuery('SELECT 1');
        assert.ok(conn.release.notCalled);
    });

    it('on query error in non-tx path: logs error, does NOT throw, returns []', async function () {
        let conn = fakeConn();
        conn.query.rejects(new Error('query fail'));
        sinon.stub(db, 'getConnection').resolves(conn);
        let result = await db.doQuery('SELECT 1');
        assert.deepStrictEqual(result, []);
        assert.ok(db.util.logError.calledOnce);
        assert.ok(conn.release.calledOnce);
    });

    it('on query error inside a transaction: logs AND re-throws', async function () {
        let conn = fakeConn();
        conn.query.rejects(new Error('tx fail'));
        db.transactionConnection = conn;
        sinon.stub(db, 'getConnection').resolves(conn);
        await assert.rejects(
            () => db.doQuery('SELECT 1'),
            /tx fail/
        );
        // no release in tx path on error
        assert.ok(conn.release.notCalled);
    });

    it('explicit conn arg: runs on that connection, never acquires/releases one', async function () {
        let snapConn = fakeConn([{ id: 7 }]);
        let getSpy   = sinon.stub(db, 'getConnection');   // must NOT be called
        let result   = await db.doQuery('SELECT 1', ['a'], snapConn);
        assert.deepStrictEqual(result, [{ id: 7 }]);
        assert.ok(snapConn.query.calledOnceWith('SELECT 1', ['a']));
        assert.ok(getSpy.notCalled, 'explicit conn bypasses getConnection');
        assert.ok(snapConn.release.notCalled, 'caller owns the connection lifecycle');
    });

    it('explicit conn arg: query errors propagate (caller rolls back the snapshot)', async function () {
        let snapConn = fakeConn();
        snapConn.query.rejects(new Error('snap read fail'));
        sinon.stub(db, 'getConnection');
        await assert.rejects(
            () => db.doQuery('SELECT 1', [], snapConn),
            /snap read fail/
        );
        assert.ok(snapConn.release.notCalled);
    });
});

// ═══════════════════════════════════════════════════════════════════════════
// 3. getConnection(): circuit breaker
// ═══════════════════════════════════════════════════════════════════════════
describe('Database.getConnection(): circuit breaker', function () {
    let db;
    beforeEach(function () { silenceConsole(); db = makeDb(); });
    afterEach(async function () { sinon.restore(); await db.close(); });

    it('happy path: returns connection and resets failures', async function () {
        let conn = fakeConn();
        sinon.stub(db.pool, 'getConnection').resolves(conn);
        let result = await db.getConnection();
        assert.strictEqual(result, conn);
        assert.strictEqual(db.circuitFailures, 0);
    });

    it('returns transactionConnection directly when one is active', async function () {
        let conn = fakeConn();
        db.transactionConnection = conn;
        let spy = sinon.stub(db.pool, 'getConnection');
        let result = await db.getConnection();
        assert.strictEqual(result, conn);
        assert.ok(spy.notCalled);
    });

    it('circuit open + cooldown not expired → throws immediately', async function () {
        db.circuitState     = 'open';
        db.circuitOpenUntil = Date.now() + 60000;
        await assert.rejects(
            () => db.getConnection(),
            /Circuit breaker open/
        );
    });

    it('circuit open + cooldown expired → transitions to half-open, succeeds', async function () {
        let conn = fakeConn();
        db.circuitState     = 'open';
        db.circuitOpenUntil = Date.now() - 1;     // already expired
        sinon.stub(db.pool, 'getConnection').resolves(conn);
        let result = await db.getConnection();
        assert.strictEqual(result, conn);
        assert.strictEqual(db.circuitState, 'closed');
        assert.strictEqual(db.circuitFailures, 0);
    });

    it('half-open → success → closes circuit', async function () {
        let conn = fakeConn();
        db.circuitState = 'half-open';
        sinon.stub(db.pool, 'getConnection').resolves(conn);
        await db.getConnection();
        assert.strictEqual(db.circuitState, 'closed');
        assert.strictEqual(db.circuitFailures, 0);
    });

    it('retry-with-backoff: fails once then succeeds', async function () {
        let conn = fakeConn();
        let stub = sinon.stub(db.pool, 'getConnection');
        stub.onFirstCall().rejects(new Error('transient'));
        stub.resolves(conn);
        let result = await db.getConnection();
        assert.strictEqual(result, conn);
        assert.ok(db.util.sleep.calledOnce);
    });

    it('threshold exceeded: circuit opens and throws', async function () {
        db.circuitThreshold = 2;
        sinon.stub(db.pool, 'getConnection').rejects(new Error('down'));
        await assert.rejects(
            () => db.getConnection(),
            /Circuit breaker opened/
        );
        assert.strictEqual(db.circuitState, 'open');
    });

    it('maxAttempts exhaustion: throws when failures < threshold', async function () {
        // Set threshold above maxAttempts so the attempts cap fires first.
        db.circuitThreshold = 9999;
        // Override maxAttempts by patching (we can't easily do that), so instead
        // set circuitThreshold just above maxAttempts (30). On each failure
        // circuitFailures increments; we need it to stay < threshold.
        // Simple approach: fail 30 times (maxAttempts), each time circuitFailures
        // stays below 9999.  util.throwError should fire "Could not connect…".
        sinon.stub(db.pool, 'getConnection').rejects(new Error('down'));
        await assert.rejects(
            () => db.getConnection(),
            /Could not connect to MariaDB after/
        );
    });
});

// ═══════════════════════════════════════════════════════════════════════════
// 4. releaseConnection()
// ═══════════════════════════════════════════════════════════════════════════
describe('Database.releaseConnection()', function () {
    let db;
    beforeEach(function () { silenceConsole(); db = makeDb(); });
    afterEach(async function () { sinon.restore(); await db.close(); });

    it('releases and nulls transactionConnection when present', async function () {
        let conn = fakeConn();
        db.transactionConnection = conn;
        await db.releaseConnection();
        assert.ok(conn.release.calledOnce);
        assert.strictEqual(db.transactionConnection, null);
    });

    it('is a no-op when transactionConnection is null', async function () {
        // Should not throw
        await db.releaseConnection();
        assert.strictEqual(db.transactionConnection, null);
    });
});

// ═══════════════════════════════════════════════════════════════════════════
// 5. beginTransaction()
// ═══════════════════════════════════════════════════════════════════════════
describe('Database.beginTransaction()', function () {
    let db;
    beforeEach(function () { silenceConsole(); db = makeDb(); });
    afterEach(async function () { sinon.restore(); await db.close(); });

    it('acquires a connection and calls beginTransaction on it', async function () {
        let conn = fakeConn();
        sinon.stub(db, 'getConnection').resolves(conn);
        await db.beginTransaction();
        assert.strictEqual(db.transactionConnection, conn);
        assert.ok(conn.beginTransaction.calledOnce);
    });

    it('if transactionConnection already exists: releases it first, then opens new one', async function () {
        let oldConn = fakeConn();
        let newConn = fakeConn();
        db.transactionConnection = oldConn;
        // getConnection will be called after releaseConnection (which nulls it)
        sinon.stub(db, 'getConnection').resolves(newConn);
        await db.beginTransaction();
        assert.ok(oldConn.release.calledOnce);
        assert.strictEqual(db.transactionConnection, newConn);
    });

    it('releases conn and nulls transactionConnection when beginTransaction throws', async function () {
        let conn = fakeConn();
        conn.beginTransaction.rejects(new Error('begin fail'));
        sinon.stub(db, 'getConnection').resolves(conn);
        await assert.rejects(
            () => db.beginTransaction(),
            /beginTransaction error/
        );
        assert.ok(conn.release.calledOnce);
        assert.strictEqual(db.transactionConnection, null);
    });
});

// ═══════════════════════════════════════════════════════════════════════════
// 6. rollbackTransaction()
// ═══════════════════════════════════════════════════════════════════════════
describe('Database.rollbackTransaction()', function () {
    let db;
    beforeEach(function () { silenceConsole(); db = makeDb(); });
    afterEach(async function () { sinon.restore(); await db.close(); });

    it('rolls back and releases when transactionConnection is active', async function () {
        let conn = fakeConn();
        db.transactionConnection = conn;
        await db.rollbackTransaction();
        assert.ok(conn.rollback.calledOnce);
        assert.ok(conn.release.calledOnce);
        assert.strictEqual(db.transactionConnection, null);
    });

    it('is a no-op when no active transaction', async function () {
        // Should not throw
        await db.rollbackTransaction();
    });

    it('releases in finally even when rollback throws', async function () {
        let conn = fakeConn();
        conn.rollback.rejects(new Error('rollback fail'));
        db.transactionConnection = conn;
        // rollback error propagates, but finally always releases
        await assert.rejects(() => db.rollbackTransaction(), /rollback fail/);
        assert.ok(conn.release.calledOnce);
        assert.strictEqual(db.transactionConnection, null);
    });
});

// ═══════════════════════════════════════════════════════════════════════════
// 7. commitTransaction()
// ═══════════════════════════════════════════════════════════════════════════
describe('Database.commitTransaction()', function () {
    let db;
    beforeEach(function () { silenceConsole(); db = makeDb(); });
    afterEach(async function () { sinon.restore(); await db.close(); });

    it('returns false when no active transactionConnection', async function () {
        let result = await db.commitTransaction();
        assert.strictEqual(result, false);
    });

    it('commits, releases, nulls, returns true on success', async function () {
        let conn = fakeConn();
        db.transactionConnection = conn;
        let result = await db.commitTransaction();
        assert.ok(conn.commit.calledOnce);
        assert.ok(conn.release.calledOnce);
        assert.strictEqual(db.transactionConnection, null);
        assert.strictEqual(result, true);
    });

    it('on commit error: rolls back, releases, nulls, throws', async function () {
        let conn = fakeConn();
        conn.commit.rejects(new Error('commit fail'));
        db.transactionConnection = conn;
        await assert.rejects(
            () => db.commitTransaction(),
            /commitTransaction error/
        );
        assert.ok(conn.rollback.calledOnce);
        assert.ok(conn.release.calledOnce);
        assert.strictEqual(db.transactionConnection, null);
    });
});

// ═══════════════════════════════════════════════════════════════════════════
// 8. beginReadSnapshot()
// ═══════════════════════════════════════════════════════════════════════════
describe('Database.beginReadSnapshot()', function () {
    let db;
    beforeEach(function () { silenceConsole(); db = makeDb(); });
    afterEach(async function () { sinon.restore(); await db.close(); });

    it('acquires a DEDICATED connection and runs SET / START TRANSACTION on it', async function () {
        let conn = fakeConn();
        sinon.stub(db, '_acquirePoolConnection').resolves(conn);
        let returned = await db.beginReadSnapshot();
        // The dedicated connection is RETURNED (not stashed on transactionConnection).
        assert.strictEqual(returned, conn);
        assert.ok(conn.query.calledTwice);
        let calls = conn.query.args.map(a => a[0]);
        assert.ok(calls[0].includes('REPEATABLE READ'));
        assert.ok(calls[1].includes('CONSISTENT SNAPSHOT'));
    });

    it('does NOT touch the shared transactionConnection (decoupled from the writer)', async function () {
        let shared = fakeConn();
        db.transactionConnection = shared;     // a writer transaction is in flight
        let conn = fakeConn();
        sinon.stub(db, '_acquirePoolConnection').resolves(conn);
        await db.beginReadSnapshot();
        assert.strictEqual(db.transactionConnection, shared, 'writer connection left intact');
        assert.ok(shared.release.notCalled, 'snapshot does not release the writer connection');
    });

    it('on query error: releases the dedicated connection and throws (shared field untouched)', async function () {
        let conn = fakeConn();
        conn.query.rejects(new Error('snap fail'));
        sinon.stub(db, '_acquirePoolConnection').resolves(conn);
        await assert.rejects(
            () => db.beginReadSnapshot(),
            /beginReadSnapshot error/
        );
        assert.ok(conn.release.calledOnce);
        assert.strictEqual(db.transactionConnection, null);
    });
});

// ═══════════════════════════════════════════════════════════════════════════
// 8b. commitReadSnapshot() / rollbackReadSnapshot()
// ═══════════════════════════════════════════════════════════════════════════
describe('Database.commitReadSnapshot() / rollbackReadSnapshot()', function () {
    let db;
    beforeEach(function () { silenceConsole(); db = makeDb(); });
    afterEach(async function () { sinon.restore(); await db.close(); });

    it('commitReadSnapshot commits then releases the connection', async function () {
        let conn = fakeConn();
        await db.commitReadSnapshot(conn);
        assert.ok(conn.commit.calledOnce);
        assert.ok(conn.release.calledOnce);
        assert.ok(conn.commit.calledBefore(conn.release));
    });

    it('commitReadSnapshot still releases when commit throws', async function () {
        let conn = fakeConn();
        conn.commit.rejects(new Error('commit fail'));
        await assert.rejects(() => db.commitReadSnapshot(conn), /commit fail/);
        assert.ok(conn.release.calledOnce);
    });

    it('rollbackReadSnapshot rolls back then releases the connection', async function () {
        let conn = fakeConn();
        await db.rollbackReadSnapshot(conn);
        assert.ok(conn.rollback.calledOnce);
        assert.ok(conn.release.calledOnce);
        assert.ok(conn.rollback.calledBefore(conn.release));
    });

    it('rollbackReadSnapshot swallows a rollback error but still releases (best-effort)', async function () {
        let conn = fakeConn();
        conn.rollback.rejects(new Error('rb fail'));
        await db.rollbackReadSnapshot(conn);   // must NOT throw
        assert.ok(conn.release.calledOnce);
    });

    it('both are no-ops on a null connection', async function () {
        await db.commitReadSnapshot(null);
        await db.rollbackReadSnapshot(null);
    });
});

// ═══════════════════════════════════════════════════════════════════════════
// 9. verifyDatabase(): retry loop
// (Uses proxyquire because mariadb is an ES module; sinon can't stub it directly)
// ═══════════════════════════════════════════════════════════════════════════
describe('Database.verifyDatabase()', function () {
    afterEach(async function () { sinon.restore(); });

    it('returns true when DB exists', async function () {
        silenceConsole();
        let conn = { query: sinon.stub().resolves([{ schema_name: 'replica_db' }]), end: sinon.stub().resolves() };
        let db = makeDbWithFakeMariadb(fakeMariadbWith(sinon.stub().resolves(conn)));
        let result = await db.verifyDatabase();
        assert.strictEqual(result, true);
        assert.ok(conn.end.calledOnce);
        await db.close();
    });

    it('returns false when DB is not found', async function () {
        silenceConsole();
        let conn = { query: sinon.stub().resolves([]), end: sinon.stub().resolves() };
        let db = makeDbWithFakeMariadb(fakeMariadbWith(sinon.stub().resolves(conn)));
        let result = await db.verifyDatabase();
        assert.strictEqual(result, false);
        await db.close();
    });

    it('retries once on error then succeeds', async function () {
        silenceConsole();
        let conn = { query: sinon.stub().resolves([{ schema_name: 'replica_db' }]), end: sinon.stub().resolves() };
        let createStub = sinon.stub();
        createStub.onFirstCall().rejects(new Error('transient'));
        createStub.resolves(conn);
        let db = makeDbWithFakeMariadb(fakeMariadbWith(createStub));
        let result = await db.verifyDatabase();
        assert.strictEqual(result, true);
        assert.ok(db.util.sleep.calledOnce);
        await db.close();
    });
});

// ═══════════════════════════════════════════════════════════════════════════
// 10. verifyDatabaseOnce()
// ═══════════════════════════════════════════════════════════════════════════
describe('Database.verifyDatabaseOnce()', function () {
    afterEach(async function () { sinon.restore(); });

    it('returns true when DB exists and always ends the connection', async function () {
        silenceConsole();
        let conn = { query: sinon.stub().resolves([{ schema_name: 'replica_db' }]), end: sinon.stub().resolves() };
        let db = makeDbWithFakeMariadb(fakeMariadbWith(sinon.stub().resolves(conn)));
        let result = await db.verifyDatabaseOnce();
        assert.strictEqual(result, true);
        assert.ok(conn.end.calledOnce);
        await db.close();
    });

    it('returns false when DB not found, still ends connection', async function () {
        silenceConsole();
        let conn = { query: sinon.stub().resolves([]), end: sinon.stub().resolves() };
        let db = makeDbWithFakeMariadb(fakeMariadbWith(sinon.stub().resolves(conn)));
        let result = await db.verifyDatabaseOnce();
        assert.strictEqual(result, false);
        assert.ok(conn.end.calledOnce);
        await db.close();
    });

    it('throws (no retry) when createConnection rejects', async function () {
        silenceConsole();
        let db = makeDbWithFakeMariadb(fakeMariadbWith(sinon.stub().rejects(new Error('connect fail'))));
        await assert.rejects(
            () => db.verifyDatabaseOnce(),
            /connect fail/
        );
        await db.close();
    });

    it('ends connection in finally even when query throws', async function () {
        silenceConsole();
        let conn = { query: sinon.stub().rejects(new Error('q fail')), end: sinon.stub().resolves() };
        let db = makeDbWithFakeMariadb(fakeMariadbWith(sinon.stub().resolves(conn)));
        await assert.rejects(() => db.verifyDatabaseOnce(), /q fail/);
        assert.ok(conn.end.calledOnce);
        await db.close();
    });
});

// ═══════════════════════════════════════════════════════════════════════════
// 11. createDatabase()
// ═══════════════════════════════════════════════════════════════════════════
describe('Database.createDatabase()', function () {
    afterEach(async function () { sinon.restore(); });

    it('throws for an invalid dbName without connecting', async function () {
        silenceConsole();
        let db = makeDb('indexer', 'bad-name');
        await assert.rejects(
            () => db.createDatabase(),
            /Invalid database name/
        );
        await db.close();
    });

    it('creates the DB and returns true on success', async function () {
        silenceConsole();
        let conn = { query: sinon.stub().resolves(), end: sinon.stub().resolves() };
        let db = makeDbWithFakeMariadb(fakeMariadbWith(sinon.stub().resolves(conn)), 'indexer', 'valid_db');
        let result = await db.createDatabase();
        assert.strictEqual(result, true);
        assert.ok(conn.end.calledOnce);
        await db.close();
    });

    it('retries once on error then succeeds', async function () {
        silenceConsole();
        let conn = { query: sinon.stub().resolves(), end: sinon.stub().resolves() };
        let createStub = sinon.stub();
        createStub.onFirstCall().rejects(new Error('transient'));
        createStub.resolves(conn);
        let db = makeDbWithFakeMariadb(fakeMariadbWith(createStub), 'indexer', 'valid_db');
        let result = await db.createDatabase();
        assert.strictEqual(result, true);
        assert.ok(db.util.sleep.calledOnce);
        await db.close();
    });
});

// ═══════════════════════════════════════════════════════════════════════════
// 12. verifySyncTables() + _createTableFromFile()
// ═══════════════════════════════════════════════════════════════════════════
describe('Database.verifySyncTables()', function () {
    let db;
    beforeEach(function () { silenceConsole(); db = makeDb(); });
    afterEach(async function () { sinon.restore(); await db.close(); });

    it('skips non-.sql files', async function () {
        sinon.stub(fs, 'readdirSync').returns(['readme.txt']);
        let conn = fakeConn([]);
        sinon.stub(db, 'getConnection').resolves(conn);
        let result = await db.verifySyncTables();
        assert.strictEqual(result, true);
        assert.ok(conn.query.notCalled);
    });

    it('does not create table when it already exists', async function () {
        sinon.stub(fs, 'readdirSync').returns(['sync_meta.sql']);
        let conn = fakeConn([{ TABLE_NAME: 'sync_meta' }]);
        sinon.stub(db, 'getConnection').resolves(conn);
        sinon.stub(db, 'doQuery').resolves([]);
        let result = await db.verifySyncTables();
        assert.strictEqual(result, true);
        // doQuery should NOT have been called (table exists, no create needed)
        assert.ok(db.doQuery.notCalled);
    });

    it('creates table when it does not exist (calls _createTableFromFile)', async function () {
        sinon.stub(fs, 'readdirSync').returns(['sync_meta.sql']);
        // First call (information_schema check) returns empty → table missing
        let conn = { query: sinon.stub().resolves([]), release: sinon.stub().resolves() };
        sinon.stub(db, 'getConnection').resolves(conn);
        sinon.stub(fs, 'readFileSync').returns('CREATE TABLE sync_meta (id INT);');
        let doQueryStub = sinon.stub(db, 'doQuery').resolves([]);
        let result = await db.verifySyncTables();
        assert.strictEqual(result, true);
        // doQuery should have been called for the CREATE TABLE statement
        assert.ok(doQueryStub.calledOnce);
    });

    it('decoder dbType: creates ONLY sync_halt (transparency log is indexer-only)', async function () {
        let dec = makeDb('decoder');
        sinon.stub(fs, 'readdirSync').returns(['merkle_epochs.sql', 'sync_halt.sql', 'sync_meta.sql']);
        let conn = { query: sinon.stub().resolves([]), release: sinon.stub().resolves() };
        sinon.stub(dec, 'getConnection').resolves(conn);
        sinon.stub(fs, 'readFileSync').returns('CREATE TABLE sync_halt (id INT);');
        let doQueryStub = sinon.stub(dec, 'doQuery').resolves([]);
        let result = await dec.verifySyncTables();
        assert.strictEqual(result, true);
        // Only sync_halt is probed and created: one information_schema check,
        // one CREATE; sync_meta/merkle_epochs never touched on a decoder DB.
        assert.strictEqual(conn.query.callCount, 1);
        assert.strictEqual(doQueryStub.callCount, 1);
        await dec.close();
    });

    it('indexer dbType: applies the full sync-owned set including sync_halt', async function () {
        sinon.stub(fs, 'readdirSync').returns(['merkle_epochs.sql', 'sync_halt.sql', 'sync_meta.sql']);
        let conn = fakeConn([{ TABLE_NAME: 'x' }]);
        sinon.stub(db, 'getConnection').resolves(conn);
        let result = await db.verifySyncTables();
        assert.strictEqual(result, true);
        assert.strictEqual(conn.query.callCount, 3, 'all three sync-owned tables probed');
    });

    it('throws (via util.throwError) when query fails', async function () {
        sinon.stub(fs, 'readdirSync').returns(['sync_meta.sql']);
        let conn = {
            query:   sinon.stub().rejects(new Error('query fail')),
            release: sinon.stub().resolves()
        };
        sinon.stub(db, 'getConnection').resolves(conn);
        // util.throwError throws, propagating out of verifySyncTables
        await assert.rejects(
            () => db.verifySyncTables(),
            /Error verifying sync_meta table/
        );
    });

    it('_createTableFromFile: executes all statements from file', async function () {
        sinon.stub(fs, 'readFileSync').returns('CREATE TABLE foo (id INT); CREATE INDEX idx ON foo(id);');
        let doQueryStub = sinon.stub(db, 'doQuery').resolves([]);
        await db._createTableFromFile('foo.sql');
        assert.strictEqual(doQueryStub.callCount, 2);
    });
});

// ═══════════════════════════════════════════════════════════════════════════
// 13. ensureReplicatedColumns()
// ═══════════════════════════════════════════════════════════════════════════
describe('Database.ensureReplicatedColumns()', function () {
    afterEach(async function () { sinon.restore(); });

    it('returns immediately for decoder dbType (no-op)', async function () {
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
        sinon.stub(console, 'warn');
        let db = makeDb('decoder');
        let spy = sinon.stub(db, 'doQuery').resolves([]);
        await db.ensureReplicatedColumns();
        assert.ok(spy.notCalled);
        await db.close();
    });

    it('adds missing columns for indexer dbType', async function () {
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
        sinon.stub(console, 'warn');
        let db = makeDb('indexer');
        let calls = [];
        sinon.stub(db, 'doQuery').callsFake(async (sql, args) => {
            calls.push(sql);
            // Table exists check → return a row (table present)
            if (/information_schema\.tables/.test(sql)) return [{ table_name: args[1] }];
            // Column check → return empty (column absent) so ALTER fires
            if (/information_schema\.columns/.test(sql)) return [];
            return [];
        });
        await db.ensureReplicatedColumns();
        let alters = calls.filter(s => /ALTER TABLE/.test(s));
        // 4 drift entries → 4 ALTERs
        assert.strictEqual(alters.length, 4);
        await db.close();
    });

    it('skips column when table does not exist on replica', async function () {
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
        sinon.stub(console, 'warn');
        let db = makeDb('indexer');
        let calls = [];
        sinon.stub(db, 'doQuery').callsFake(async (sql) => {
            calls.push(sql);
            // Table does not exist
            if (/information_schema\.tables/.test(sql)) return [];
            return [];
        });
        await db.ensureReplicatedColumns();
        let alters = calls.filter(s => /ALTER TABLE/.test(s));
        assert.strictEqual(alters.length, 0);
        await db.close();
    });

    it('skips column when column already exists on replica', async function () {
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
        sinon.stub(console, 'warn');
        let db = makeDb('indexer');
        let calls = [];
        sinon.stub(db, 'doQuery').callsFake(async (sql, args) => {
            calls.push(sql);
            if (/information_schema\.tables/.test(sql)) return [{ table_name: args[1] }];
            // AUTO_INCREMENT-check reads (SELECT EXTRA ...): return 'auto_increment' so the
            // repair is correctly skipped when the column is already in the right state.
            // These reads are intentional new behavior added by the #3713 AUTO_INCREMENT
            // self-heal; the real intent of this test is that no ADD COLUMN or MODIFY is
            // issued when all columns are already present.
            if (/SELECT EXTRA FROM information_schema\.columns/.test(sql)) return [{ EXTRA: 'auto_increment' }];
            // ENUM-widen check (5244): return a COLUMN_TYPE that already includes 'rejected'
            // so the heal is correctly skipped when the ENUM is already up to date.
            if (/SELECT COLUMN_TYPE FROM information_schema\.columns/.test(sql))
                return [{ COLUMN_TYPE: "enum('pending','fulfilled','expired','errored','rejected')" }];
            // Ownership/nullability column-presence checks: column exists on replica
            if (/information_schema\.columns/.test(sql)) return [{ COLUMN_NAME: args[2] }];
            return [];
        });
        await db.ensureReplicatedColumns();
        let alters = calls.filter(s => /ALTER TABLE/.test(s));
        assert.strictEqual(alters.length, 0);
        await db.close();
    });
});

// ═══════════════════════════════════════════════════════════════════════════
// 14. getLastBlock()
// ═══════════════════════════════════════════════════════════════════════════
describe('Database.getLastBlock()', function () {
    let db;
    beforeEach(function () { silenceConsole(); db = makeDb(); });
    afterEach(async function () { sinon.restore(); await db.close(); });

    it('returns Number when rows contain a block_index', async function () {
        sinon.stub(db, 'doQuery').resolves([{ block_index: 500 }]);
        let result = await db.getLastBlock();
        assert.strictEqual(result, 500);
    });

    it('returns null when row.block_index is null', async function () {
        sinon.stub(db, 'doQuery').resolves([{ block_index: null }]);
        let result = await db.getLastBlock();
        assert.strictEqual(result, null);
    });

    it('returns null when rows is empty', async function () {
        sinon.stub(db, 'doQuery').resolves([]);
        let result = await db.getLastBlock();
        assert.strictEqual(result, null);
    });
});

// ═══════════════════════════════════════════════════════════════════════════
// 14b. addMissingColumns(): remaining branch coverage
// (The basic happy-path cases live in db-schema-evolution.test.js)
// ═══════════════════════════════════════════════════════════════════════════
describe('Database.addMissingColumns(): edge branches', function () {
    let db;
    beforeEach(function () { silenceConsole(); db = makeDb(); });
    afterEach(async function () { sinon.restore(); await db.close(); });

    // Lines 218-220: column name that fails validateIdentifier
    it('skips (logs error) a column with an invalid identifier name', async function () {
        // Build a DDL with a column line that has a name containing a dash (invalid)
        // extractColumnNames returns names from backtick lines; validateIdentifier
        // rejects non-[A-Za-z0-9_] chars.  We can't directly forge extractColumnNames
        // output, so we construct a DDL where the column name passes extraction but
        // fails the identifier check.  A column named "bad-col" passes the backtick
        // extraction (extractColumnNames just slices between backticks) but fails
        // validateIdentifier (contains '-').
        let ddl = [
            'CREATE TABLE `t` (',
            '  `bad-col` int(11) NOT NULL,',
            '  PRIMARY KEY (`id`)',
            ') ENGINE=InnoDB'
        ].join('\n');
        sinon.stub(db, 'doQuery').callsFake(async (sql) => {
            // Replica has no columns for this table
            if (/information_schema\.columns/.test(sql)) return [];
            return [];
        });
        let added = await db.addMissingColumns('t', ddl);
        // bad-col extracted but rejected; nothing added
        assert.strictEqual(added, 0);
        // console.error should have been called for the invalid column
        assert.ok(console.error.called);
    });

    // Lines 224-226: column whose definition can't be extracted (null def)
    it('warns and skips a column when extractColumnDefinition returns null', async function () {
        // DDL has column `new_col` in the name list but its definition line contains
        // a semicolon, which causes extractColumnDefinition to return null.
        let ddl = [
            'CREATE TABLE `t` (',
            '  `new_col` int(11) DEFAULT NULL; /* injected */',
            '  PRIMARY KEY (`id`)',
            ') ENGINE=InnoDB'
        ].join('\n');
        sinon.stub(db, 'doQuery').callsFake(async (sql) => {
            if (/information_schema\.columns/.test(sql)) return [];  // replica has nothing
            return [];
        });
        let added = await db.addMissingColumns('t', ddl);
        assert.strictEqual(added, 0);
        assert.ok(console.warn.called);
    });

    // Line 210: r.COLUMN_NAME fallback (uppercase key)
    it('reads COLUMN_NAME (uppercase) from information_schema rows', async function () {
        let ddl = [
            'CREATE TABLE `t` (',
            '  `myfield` int(11) NOT NULL,',
            '  PRIMARY KEY (`id`)',
            ') ENGINE=InnoDB'
        ].join('\n');
        sinon.stub(db, 'doQuery').callsFake(async (sql) => {
            if (/information_schema\.columns/.test(sql))
                return [{ COLUMN_NAME: 'myfield' }];  // uppercase key
            return [];
        });
        // myfield is already present (via COLUMN_NAME) so nothing added
        let added = await db.addMissingColumns('t', ddl);
        assert.strictEqual(added, 0);
    });

    // Lines 233-234: ALTER TABLE throws
    it('logs error (does not throw) when ALTER TABLE fails', async function () {
        let ddl = [
            'CREATE TABLE `t` (',
            '  `newcol` int(11) DEFAULT NULL,',
            '  PRIMARY KEY (`id`)',
            ') ENGINE=InnoDB'
        ].join('\n');
        let alterCalled = false;
        sinon.stub(db, 'doQuery').callsFake(async (sql) => {
            if (/information_schema\.columns/.test(sql)) return [];  // column missing
            if (/ALTER TABLE/.test(sql)) {
                alterCalled = true;
                throw new Error('alter fail');
            }
            return [];
        });
        let added = await db.addMissingColumns('t', ddl);
        assert.strictEqual(added, 0);       // ALTER failed, so not incremented
        assert.ok(alterCalled);
        assert.ok(console.error.called);
    });
});

// ═══════════════════════════════════════════════════════════════════════════
// 15. getBlockHashRow(): indexer vs decoder branching
// ═══════════════════════════════════════════════════════════════════════════
describe('Database.getBlockHashRow()', function () {
    afterEach(async function () { sinon.restore(); });

    it('indexer: returns row with ledger/actions/contract hashes', async function () {
        silenceConsole();
        let db = makeDb('indexer');
        let row = { block_index: 100, ledger_hash: 'abc', actions_hash: 'def', contract_hash: 'ghi' };
        sinon.stub(db, 'doQuery').resolves([row]);
        let result = await db.getBlockHashRow(100);
        assert.deepStrictEqual(result, row);
        let sql = db.doQuery.firstCall.args[0];
        assert.ok(sql.includes('ledger_hash'));
        await db.close();
    });

    it('indexer: returns null when no rows', async function () {
        silenceConsole();
        let db = makeDb('indexer');
        sinon.stub(db, 'doQuery').resolves([]);
        let result = await db.getBlockHashRow(999);
        assert.strictEqual(result, null);
        await db.close();
    });

    it('decoder: returns row with block_hash only', async function () {
        silenceConsole();
        let db = makeDb('decoder');
        let row = { block_index: 100, block_hash: 'xyz' };
        sinon.stub(db, 'doQuery').resolves([row]);
        let result = await db.getBlockHashRow(100);
        assert.deepStrictEqual(result, row);
        let sql = db.doQuery.firstCall.args[0];
        assert.ok(sql.includes('block_hash'));
        assert.ok(!sql.includes('ledger_hash'));
        await db.close();
    });

    it('decoder: returns null when no rows', async function () {
        silenceConsole();
        let db = makeDb('decoder');
        sinon.stub(db, 'doQuery').resolves([]);
        let result = await db.getBlockHashRow(999);
        assert.strictEqual(result, null);
        await db.close();
    });
});

// ═══════════════════════════════════════════════════════════════════════════
// 16. getBlockRows(): indexer vs decoder
// ═══════════════════════════════════════════════════════════════════════════
describe('Database.getBlockRows()', function () {
    afterEach(async function () { sinon.restore(); });

    it('indexer: uses ledger/actions/contract hash columns', async function () {
        silenceConsole();
        let db = makeDb('indexer');
        let rows = [{ block_index: 1 }];
        sinon.stub(db, 'doQuery').resolves(rows);
        let result = await db.getBlockRows(1, 10);
        assert.strictEqual(result, rows);
        let sql = db.doQuery.firstCall.args[0];
        assert.ok(sql.includes('ledger_hash'));
        await db.close();
    });

    it('decoder: uses block_hash column', async function () {
        silenceConsole();
        let db = makeDb('decoder');
        let rows = [{ block_index: 1 }];
        sinon.stub(db, 'doQuery').resolves(rows);
        let result = await db.getBlockRows(1, 10);
        assert.strictEqual(result, rows);
        let sql = db.doQuery.firstCall.args[0];
        assert.ok(sql.includes('block_hash'));
        assert.ok(!sql.includes('ledger_hash'));
        await db.close();
    });
});

// ═══════════════════════════════════════════════════════════════════════════
// 17. getFirstActionIndex()
// ═══════════════════════════════════════════════════════════════════════════
describe('Database.getFirstActionIndex()', function () {
    let db;
    beforeEach(function () { silenceConsole(); db = makeDb(); });
    afterEach(async function () { sinon.restore(); await db.close(); });

    it('returns Number when found', async function () {
        sinon.stub(db, 'doQuery').resolves([{ action_index: 42 }]);
        let result = await db.getFirstActionIndex(100);
        assert.strictEqual(result, 42);
    });

    it('returns null when no rows', async function () {
        sinon.stub(db, 'doQuery').resolves([]);
        let result = await db.getFirstActionIndex(100);
        assert.strictEqual(result, null);
    });
});

// ═══════════════════════════════════════════════════════════════════════════
// 18. getBlockScopedRows()
// ═══════════════════════════════════════════════════════════════════════════
describe('Database.getBlockScopedRows()', function () {
    let db;
    beforeEach(function () { silenceConsole(); db = makeDb(); });
    afterEach(async function () { sinon.restore(); await db.close(); });

    it('queries the given table by block_index', async function () {
        let rows = [{ id: 1 }];
        sinon.stub(db, 'doQuery').resolves(rows);
        let result = await db.getBlockScopedRows('blocks', 5);
        assert.strictEqual(result, rows);
        let sql = db.doQuery.firstCall.args[0];
        assert.ok(sql.includes('blocks'));
        assert.ok(sql.includes('block_index'));
    });
});

// ═══════════════════════════════════════════════════════════════════════════
// 19. getActionScopedRows()
// ═══════════════════════════════════════════════════════════════════════════
describe('Database.getActionScopedRows()', function () {
    let db;
    beforeEach(function () { silenceConsole(); db = makeDb(); });
    afterEach(async function () { sinon.restore(); await db.close(); });

    it('queries with action/transaction join', async function () {
        let rows = [{ action_index: 1 }];
        sinon.stub(db, 'doQuery').resolves(rows);
        let result = await db.getActionScopedRows('orders', 10);
        assert.strictEqual(result, rows);
        let sql = db.doQuery.firstCall.args[0];
        assert.ok(sql.includes('actions'));
        assert.ok(sql.includes('orders'));
    });
});

// ═══════════════════════════════════════════════════════════════════════════
// 19-a. getEmissionRowsForBlock()
// ═══════════════════════════════════════════════════════════════════════════
describe('Database.getEmissionRowsForBlock()', function () {
    let db;
    beforeEach(function () { silenceConsole(); db = makeDb(); });
    afterEach(async function () { sinon.restore(); await db.close(); });

    it('joins through contract_executions on execution_index (includes NULL action_index rows)', async function () {
        let rows = [{ execution_index: 1, emitted_action: 'SLASH', action_index: null, position: 0 }];
        sinon.stub(db, 'doQuery').resolves(rows);
        let result = await db.getEmissionRowsForBlock(10);
        assert.strictEqual(result, rows);
        let sql = db.doQuery.firstCall.args[0];
        assert.ok(sql.includes('contract_emissions'));
        assert.ok(sql.includes('contract_executions'));
        assert.ok(sql.includes('ce.action_index = em.execution_index'));
        // Must NOT use the action_index-scoped join (which drops NULL-action_index emissions).
        assert.ok(!sql.includes('a.action_index = em.action_index'));
    });

    it('selects only the four protocol columns, never em.* (would carry the id PK)', async function () {
        sinon.stub(db, 'doQuery').resolves([]);
        await db.getEmissionRowsForBlock(5);
        let sql = db.doQuery.firstCall.args[0];
        assert.ok(sql.includes('em.execution_index'));
        assert.ok(sql.includes('em.emitted_action'));
        assert.ok(sql.includes('em.action_index'));
        assert.ok(sql.includes('em.position'));
        assert.ok(!sql.includes('em.*'));
    });
});

// ═══════════════════════════════════════════════════════════════════════════
// 20. getTxScopedRows()
// ═══════════════════════════════════════════════════════════════════════════
describe('Database.getTxScopedRows()', function () {
    let db;
    beforeEach(function () { silenceConsole(); db = makeDb(); });
    afterEach(async function () { sinon.restore(); await db.close(); });

    it('queries with tx join', async function () {
        let rows = [{ tx_index: 1 }];
        sinon.stub(db, 'doQuery').resolves(rows);
        let result = await db.getTxScopedRows('transaction_outputs', 10);
        assert.strictEqual(result, rows);
        let sql = db.doQuery.firstCall.args[0];
        assert.ok(sql.includes('transactions'));
        assert.ok(sql.includes('transaction_outputs'));
    });
});

// ═══════════════════════════════════════════════════════════════════════════
// 21. getTransactions() / getActions()
// ═══════════════════════════════════════════════════════════════════════════
describe('Database.getTransactions()', function () {
    let db;
    beforeEach(function () { silenceConsole(); db = makeDb(); });
    afterEach(async function () { sinon.restore(); await db.close(); });

    it('returns rows from transactions table', async function () {
        let rows = [{ tx_index: 7 }];
        sinon.stub(db, 'doQuery').resolves(rows);
        let result = await db.getTransactions(10);
        assert.strictEqual(result, rows);
        let sql = db.doQuery.firstCall.args[0];
        assert.ok(sql.includes('transactions'));
    });
});

describe('Database.getActions()', function () {
    let db;
    beforeEach(function () { silenceConsole(); db = makeDb(); });
    afterEach(async function () { sinon.restore(); await db.close(); });

    it('returns rows from actions table', async function () {
        let rows = [{ action_index: 3 }];
        sinon.stub(db, 'doQuery').resolves(rows);
        let result = await db.getActions(10);
        assert.strictEqual(result, rows);
        let sql = db.doQuery.firstCall.args[0];
        assert.ok(sql.includes('actions'));
    });
});

// ═══════════════════════════════════════════════════════════════════════════
// 22. getTablePage() / getTableCount()
// ═══════════════════════════════════════════════════════════════════════════
describe('Database.getTablePage()', function () {
    let db;
    beforeEach(function () { silenceConsole(); db = makeDb(); });
    afterEach(async function () { sinon.restore(); await db.close(); });

    it('passes limit and offset to doQuery', async function () {
        sinon.stub(db, 'doQuery').resolves([]);
        await db.getTablePage('blocks', 100, 200);
        let args = db.doQuery.firstCall.args[1];
        assert.deepStrictEqual(args, [100, 200]);
    });
});

describe('Database.getTableCount()', function () {
    let db;
    beforeEach(function () { silenceConsole(); db = makeDb(); });
    afterEach(async function () { sinon.restore(); await db.close(); });

    it('returns count as Number', async function () {
        sinon.stub(db, 'doQuery').resolves([{ cnt: '42' }]);
        let result = await db.getTableCount('blocks');
        assert.strictEqual(result, 42);
    });
});

// ═══════════════════════════════════════════════════════════════════════════
// 23. getDatabaseStats()
// ═══════════════════════════════════════════════════════════════════════════
describe('Database.getDatabaseStats()', function () {
    let db;
    beforeEach(function () { silenceConsole(); db = makeDb(); });
    afterEach(async function () { sinon.restore(); await db.close(); });

    it('returns rows from information_schema query', async function () {
        let rows = [{ db_name: 'XChain_BTC', tables: 12 }];
        sinon.stub(db, 'doQuery').resolves(rows);
        let result = await db.getDatabaseStats();
        assert.strictEqual(result, rows);
        let sql = db.doQuery.firstCall.args[0];
        assert.ok(sql.includes('information_schema'));
    });
});

// ═══════════════════════════════════════════════════════════════════════════
// 24. truncateTable()
// ═══════════════════════════════════════════════════════════════════════════
describe('Database.truncateTable()', function () {
    let db;
    beforeEach(function () { silenceConsole(); db = makeDb(); });
    afterEach(async function () { sinon.restore(); await db.close(); });

    it('calls doQuery with TRUNCATE TABLE', async function () {
        sinon.stub(db, 'doQuery').resolves([]);
        await db.truncateTable('blocks');
        let sql = db.doQuery.firstCall.args[0];
        assert.ok(sql.includes('TRUNCATE TABLE'));
        assert.ok(sql.includes('blocks'));
    });
});

// ═══════════════════════════════════════════════════════════════════════════
// 25. recordHalt() / getActiveHalt() / clearHalt()
// ═══════════════════════════════════════════════════════════════════════════
describe('Database halt methods', function () {
    let db;
    beforeEach(function () { silenceConsole(); db = makeDb(); });
    afterEach(async function () { sinon.restore(); await db.close(); });

    it('getActiveHalt: returns the first row when rows present', async function () {
        let row = { id: 1, block_index: 500 };
        sinon.stub(db, 'doQuery').resolves([row]);
        let result = await db.getActiveHalt('indexer');
        assert.deepStrictEqual(result, row);
    });

    it('getActiveHalt: returns null when no rows', async function () {
        sinon.stub(db, 'doQuery').resolves([]);
        let result = await db.getActiveHalt('indexer');
        assert.strictEqual(result, null);
    });

    it('clearHalt: returns affectedRows', async function () {
        sinon.stub(db, 'doQuery').resolves({ affectedRows: 1 });
        let result = await db.clearHalt('indexer');
        assert.strictEqual(result, 1);
    });

    it('clearHalt: returns 0 when result is falsy', async function () {
        sinon.stub(db, 'doQuery').resolves(null);
        let result = await db.clearHalt('indexer');
        assert.strictEqual(result, 0);
    });

    it('recordHalt: idempotent (returns existing when block_index matches)', async function () {
        let existing = { id: 1, block_index: 100 };
        sinon.stub(db, 'getActiveHalt').resolves(existing);
        let doQ = sinon.stub(db, 'doQuery').resolves([]);
        let result = await db.recordHalt('indexer', 100, 'divergence', [], []);
        assert.deepStrictEqual(result, existing);
        assert.ok(doQ.notCalled);
    });

    it('recordHalt: inserts new halt when none active', async function () {
        let newHalt = { id: 2, block_index: 200 };
        let getHalt = sinon.stub(db, 'getActiveHalt');
        getHalt.onFirstCall().resolves(null);           // no existing
        getHalt.onSecondCall().resolves(newHalt);       // after insert
        sinon.stub(db, 'doQuery').resolves([]);
        let result = await db.recordHalt('indexer', 200, 'divergence', ['m1'], ['s1']);
        assert.deepStrictEqual(result, newHalt);
    });

    it('recordHalt: inserts new halt when existing block_index differs', async function () {
        let existingOther = { id: 1, block_index: 50 };
        let newHalt = { id: 2, block_index: 200 };
        let getHalt = sinon.stub(db, 'getActiveHalt');
        getHalt.onFirstCall().resolves(existingOther);  // different block
        getHalt.onSecondCall().resolves(newHalt);
        let doQ = sinon.stub(db, 'doQuery').resolves([]);
        let result = await db.recordHalt('indexer', 200, 'divergence', [], []);
        assert.deepStrictEqual(result, newHalt);
        assert.ok(doQ.calledOnce);
    });

    it('recordHalt: uses default "divergence" when reason is null, defaults mismatches/sources to []', async function () {
        let newHalt = { id: 3, block_index: 300 };
        let getHalt = sinon.stub(db, 'getActiveHalt');
        getHalt.onFirstCall().resolves(null);
        getHalt.onSecondCall().resolves(newHalt);
        let insertArgs;
        sinon.stub(db, 'doQuery').callsFake(async (sql, args) => {
            insertArgs = args;
            return [];
        });
        await db.recordHalt('indexer', 300, null, null, null);
        // Defaults should have been applied: reason→'divergence', mismatches→[], sources→[]
        assert.strictEqual(insertArgs[2], 'divergence');
        assert.strictEqual(insertArgs[3], '[]');
        assert.strictEqual(insertArgs[4], '[]');
    });
});

// ═══════════════════════════════════════════════════════════════════════════
// 26. replicateSchema(): integration of stubbed higher-level paths
// ═══════════════════════════════════════════════════════════════════════════
describe('Database.replicateSchema()', function () {
    let db, sourceDb;
    beforeEach(function () {
        silenceConsole();
        db       = makeDb('indexer');
        sourceDb = makeDb('indexer', 'source_db');
    });
    afterEach(async function () {
        sinon.restore();
        await db.close();
        await sourceDb.close();
    });

    it('creates a missing table and calls ensureReplicatedColumns', async function () {
        let validDdl = 'CREATE TABLE blocks (id INT NOT NULL, PRIMARY KEY (id))';
        let sourceCalls = [];
        sinon.stub(sourceDb, 'doQuery').callsFake(async (sql) => {
            sourceCalls.push(sql);
            if (/information_schema\.tables/.test(sql))
                return [{ table_name: 'blocks' }];
            if (/SHOW CREATE TABLE/.test(sql))
                return [{ 'Create Table': validDdl }];
            return [];
        });
        let targetCalls = [];
        sinon.stub(db, 'doQuery').callsFake(async (sql) => {
            targetCalls.push(sql);
            // Target has no tables initially
            if (/information_schema\.tables/.test(sql)) return [];
            return [];
        });
        sinon.stub(db, 'ensureReplicatedColumns').resolves();
        await db.replicateSchema(sourceDb);
        assert.ok(db.ensureReplicatedColumns.calledOnce);
        let creates = targetCalls.filter(s => s.includes('CREATE TABLE'));
        assert.ok(creates.length >= 1);
    });

    it('skips invalid table name (validateIdentifier fail)', async function () {
        sinon.stub(sourceDb, 'doQuery').callsFake(async (sql) => {
            if (/information_schema\.tables/.test(sql))
                return [{ table_name: 'bad-table-name' }];
            return [];
        });
        sinon.stub(db, 'doQuery').resolves([]);
        sinon.stub(db, 'ensureReplicatedColumns').resolves();
        // Should not throw
        await db.replicateSchema(sourceDb);
    });

    it('calls addMissingColumns for already-existing tables', async function () {
        let validDdl = 'CREATE TABLE blocks (id INT NOT NULL, PRIMARY KEY (id))';
        sinon.stub(sourceDb, 'doQuery').callsFake(async (sql) => {
            if (/information_schema\.tables/.test(sql)) return [{ table_name: 'blocks' }];
            if (/SHOW CREATE TABLE/.test(sql)) return [{ 'Create Table': validDdl }];
            return [];
        });
        sinon.stub(db, 'doQuery').callsFake(async (sql) => {
            // Target already has 'blocks'
            if (/information_schema\.tables/.test(sql)) return [{ table_name: 'blocks' }];
            return [];
        });
        sinon.stub(db, 'addMissingColumns').resolves(0);
        sinon.stub(db, 'ensureReplicatedColumns').resolves();
        await db.replicateSchema(sourceDb);
        assert.ok(db.addMissingColumns.calledOnce);
    });

    it('skips table when SHOW CREATE TABLE returns empty rows', async function () {
        sinon.stub(sourceDb, 'doQuery').callsFake(async (sql) => {
            if (/information_schema\.tables/.test(sql))
                return [{ table_name: 'blocks' }];
            if (/SHOW CREATE TABLE/.test(sql))
                return [];   // ← empty rows, triggers the continue
            return [];
        });
        sinon.stub(db, 'doQuery').resolves([]);
        sinon.stub(db, 'ensureReplicatedColumns').resolves();
        await db.replicateSchema(sourceDb);
    });

    it('skips table when SHOW CREATE TABLE row has no Create Table key', async function () {
        sinon.stub(sourceDb, 'doQuery').callsFake(async (sql) => {
            if (/information_schema\.tables/.test(sql))
                return [{ table_name: 'blocks' }];
            if (/SHOW CREATE TABLE/.test(sql))
                return [{}];   // ← row with no 'Create Table' field, triggers continue
            return [];
        });
        sinon.stub(db, 'doQuery').resolves([]);
        sinon.stub(db, 'ensureReplicatedColumns').resolves();
        await db.replicateSchema(sourceDb);
    });

    it('handles TABLE_NAME (uppercase) keys from information_schema rows', async function () {
        let validDdl = 'CREATE TABLE blocks (id INT NOT NULL, PRIMARY KEY (id))';
        sinon.stub(sourceDb, 'doQuery').callsFake(async (sql) => {
            if (/information_schema\.tables/.test(sql))
                return [{ TABLE_NAME: 'blocks' }];   // uppercase key
            if (/SHOW CREATE TABLE/.test(sql))
                return [{ 'Create Table': validDdl }];
            return [];
        });
        let targetCallCount = 0;
        sinon.stub(db, 'doQuery').callsFake(async (sql) => {
            if (/information_schema\.tables/.test(sql)) {
                targetCallCount++;
                if (targetCallCount === 1)
                    return [{ TABLE_NAME: 'blocks' }];  // target has 'blocks' with uppercase key
                return [{ TABLE_NAME: 'blocks' }];
            }
            return [];
        });
        sinon.stub(db, 'addMissingColumns').resolves(0);
        sinon.stub(db, 'ensureReplicatedColumns').resolves();
        await db.replicateSchema(sourceDb);
        assert.ok(db.addMissingColumns.calledOnce);
    });

    it('skips DDL that fails validateDdl (invalid DDL)', async function () {
        sinon.stub(sourceDb, 'doQuery').callsFake(async (sql) => {
            if (/information_schema\.tables/.test(sql)) return [{ table_name: 'blocks' }];
            if (/SHOW CREATE TABLE/.test(sql)) return [{ 'Create Table': 'DROP TABLE blocks' }];
            return [];
        });
        sinon.stub(db, 'doQuery').resolves([]);
        sinon.stub(db, 'ensureReplicatedColumns').resolves();
        // Should not throw
        await db.replicateSchema(sourceDb);
    });

    it('handles CREATE TABLE throw (deferred) and executes retry block', async function () {
        // Two source tables: 'parent' and 'child'.
        // First pass: parent succeeds (created=1), child throws (deferred, created stays 1).
        // created(1) < sourceTables.length(2) - existingSet.size(0) = 2 → retry fires.
        // In retry: parent is now in retrySet (skip), child is not → retry child CREATE.
        let validDdlParent = 'CREATE TABLE parent (id INT NOT NULL, PRIMARY KEY (id))';
        let validDdlChild  = 'CREATE TABLE child (id INT NOT NULL, PRIMARY KEY (id))';

        sinon.stub(sourceDb, 'doQuery').callsFake(async (sql) => {
            if (/information_schema\.tables/.test(sql))
                return [{ table_name: 'parent' }, { table_name: 'child' }];
            if (/SHOW CREATE TABLE `parent`/.test(sql)) return [{ 'Create Table': validDdlParent }];
            if (/SHOW CREATE TABLE `child`/.test(sql))  return [{ 'Create Table': validDdlChild }];
            return [];
        });

        let callCount = 0;
        sinon.stub(db, 'doQuery').callsFake(async (sql) => {
            // info_schema calls: first = existing tables (none), subsequent = retry set
            if (/information_schema\.tables/.test(sql)) {
                callCount++;
                if (callCount === 1) return [];                                  // first pass: target has no tables
                return [{ table_name: 'parent' }];                               // retry pass: parent was created
            }
            // CREATE TABLE parent → success
            if (/CREATE TABLE parent/.test(sql)) return [];
            // CREATE TABLE child → throw on first attempt
            if (/CREATE TABLE child/.test(sql)) throw new Error('FK not ready');
            return [];
        });
        sinon.stub(db, 'ensureReplicatedColumns').resolves();

        // Should not throw; the deferred+retry block should execute without error
        await db.replicateSchema(sourceDb);
        assert.ok(db.ensureReplicatedColumns.calledOnce);
    });

    it('retry block: skips invalid table name', async function () {
        // Force the retry path: one table that fails CREATE, one with a bad name in source
        let validDdl = 'CREATE TABLE child (id INT NOT NULL, PRIMARY KEY (id))';
        sinon.stub(sourceDb, 'doQuery').callsFake(async (sql) => {
            if (/information_schema\.tables/.test(sql))
                return [{ table_name: 'child' }];
            if (/SHOW CREATE TABLE/.test(sql)) return [{ 'Create Table': validDdl }];
            return [];
        });
        let targetCallCount = 0;
        sinon.stub(db, 'doQuery').callsFake(async (sql) => {
            if (/information_schema\.tables/.test(sql)) {
                targetCallCount++;
                if (targetCallCount === 1) return [];
                return [];  // child still missing → retry fires
            }
            // child CREATE fails
            if (/CREATE TABLE/.test(sql)) throw new Error('FK fail');
            return [];
        });
        sinon.stub(db, 'ensureReplicatedColumns').resolves();
        await db.replicateSchema(sourceDb);
    });

    it('retry block: skips table when SHOW CREATE TABLE returns empty (retry ddlRows empty)', async function () {
        // Trigger retry block: child fails CREATE first pass (created=0 < 1-0=1).
        // In retry, SHOW CREATE TABLE returns [] → ddlRows.length===0 → continue.
        let validDdl = 'CREATE TABLE child (id INT NOT NULL, PRIMARY KEY (id))';
        sinon.stub(sourceDb, 'doQuery').callsFake(async (sql) => {
            if (/information_schema\.tables/.test(sql))
                return [{ table_name: 'child' }];
            if (/SHOW CREATE TABLE/.test(sql)) return [{ 'Create Table': validDdl }];
            return [];
        });
        let targetInfoCalls = 0;
        sinon.stub(db, 'doQuery').callsFake(async (sql) => {
            if (/information_schema\.tables/.test(sql)) {
                targetInfoCalls++;
                return [];  // child always missing → retry fires
            }
            if (/CREATE TABLE/.test(sql)) throw new Error('FK fail');  // always fails
            return [];
        });
        // Override sourceDb.doQuery to return empty ddlRows in retry path
        let sourceQueryCount = 0;
        sourceDb.doQuery.restore();
        sinon.stub(sourceDb, 'doQuery').callsFake(async (sql) => {
            if (/information_schema\.tables/.test(sql))
                return [{ table_name: 'child' }];
            if (/SHOW CREATE TABLE/.test(sql)) {
                sourceQueryCount++;
                if (sourceQueryCount === 1) return [{ 'Create Table': validDdl }]; // first pass: has DDL
                return [];  // retry pass: empty rows → continue
            }
            return [];
        });
        sinon.stub(db, 'ensureReplicatedColumns').resolves();
        await db.replicateSchema(sourceDb);
    });

    it('retry block: skips table when Create Table key is missing (retry createSql null)', async function () {
        let validDdl = 'CREATE TABLE child (id INT NOT NULL, PRIMARY KEY (id))';
        let targetInfoCalls = 0;
        sinon.stub(db, 'doQuery').callsFake(async (sql) => {
            if (/information_schema\.tables/.test(sql)) {
                targetInfoCalls++;
                return [];
            }
            if (/CREATE TABLE/.test(sql)) throw new Error('FK fail');
            return [];
        });
        let sourceQueryCount = 0;
        sinon.stub(sourceDb, 'doQuery').callsFake(async (sql) => {
            if (/information_schema\.tables/.test(sql))
                return [{ table_name: 'child' }];
            if (/SHOW CREATE TABLE/.test(sql)) {
                sourceQueryCount++;
                if (sourceQueryCount === 1) return [{ 'Create Table': validDdl }];
                return [{}];  // retry: row without 'Create Table' key → createSql=undefined → continue
            }
            return [];
        });
        sinon.stub(db, 'ensureReplicatedColumns').resolves();
        await db.replicateSchema(sourceDb);
    });

    it('retry block: handles TABLE_NAME uppercase keys in retrySet and source rows', async function () {
        // Force retry block; retryTables and sourceTables use uppercase TABLE_NAME key.
        let validDdl = 'CREATE TABLE child (id INT NOT NULL, PRIMARY KEY (id))';
        let sourceQueryCount = 0;
        sinon.stub(sourceDb, 'doQuery').callsFake(async (sql) => {
            if (/information_schema\.tables/.test(sql))
                return [{ TABLE_NAME: 'child' }];  // ← uppercase key in source table list
            if (/SHOW CREATE TABLE/.test(sql)) {
                sourceQueryCount++;
                if (sourceQueryCount === 1) return [{ 'Create Table': validDdl }];
                return [];  // retry: empty → skip
            }
            return [];
        });
        let targetInfoCalls = 0;
        sinon.stub(db, 'doQuery').callsFake(async (sql) => {
            if (/information_schema\.tables/.test(sql)) {
                targetInfoCalls++;
                if (targetInfoCalls === 1) return [];
                // retry pass: return uppercase TABLE_NAME so retrySet uses the TABLE_NAME fallback
                return [{ TABLE_NAME: 'child' }];
            }
            if (/CREATE TABLE/.test(sql)) throw new Error('FK fail');
            return [];
        });
        sinon.stub(db, 'ensureReplicatedColumns').resolves();
        await db.replicateSchema(sourceDb);
    });

    it('retry block: successfully creates a deferred table (covers the success log)', async function () {
        // One source table 'child' that fails on first pass but succeeds on retry.
        // Setup: created=0, sourceTables.length=1, existingSet.size=0 → 0 < 1 → retry fires.
        let validDdl = 'CREATE TABLE child (id INT NOT NULL, PRIMARY KEY (id))';
        sinon.stub(sourceDb, 'doQuery').callsFake(async (sql) => {
            if (/information_schema\.tables/.test(sql))
                return [{ table_name: 'child' }];
            if (/SHOW CREATE TABLE `child`/.test(sql)) return [{ 'Create Table': validDdl }];
            return [];
        });

        let firstPassDone = false;
        let targetInfoCalls = 0;
        sinon.stub(db, 'doQuery').callsFake(async (sql) => {
            if (/information_schema\.tables/.test(sql)) {
                targetInfoCalls++;
                if (targetInfoCalls === 1) return [];   // first pass: target has no tables
                return [];                               // retry pass: child still missing → retry creates it
            }
            if (/CREATE TABLE child/.test(sql)) {
                if (!firstPassDone) {
                    firstPassDone = true;
                    throw new Error('FK not ready');    // first attempt fails
                }
                return [];                              // retry succeeds
            }
            return [];
        });
        sinon.stub(db, 'ensureReplicatedColumns').resolves();
        await db.replicateSchema(sourceDb);
        // console.log should have been called for 'Created table child (retry)'
        let logCalls = console.log.args.map(a => a[0]);
        assert.ok(logCalls.some(s => typeof s === 'string' && s.includes('retry')));
    });
});

// ═══════════════════════════════════════════════════════════════════════════
// Table-identifier guard (getTableCount / getTablePage / truncateTable)
// ═══════════════════════════════════════════════════════════════════════════
describe('Database: table identifier guard', function () {
    let db;
    beforeEach(function () { silenceConsole(); db = makeDb(); });
    afterEach(async function () { sinon.restore(); await db.close(); });

    const EVIL = 'blocks` WHERE 1=1 UNION SELECT 1 -- ';

    it('getTableCount rejects an unsafe identifier before querying', async function () {
        let spy = sinon.stub(db, 'doQuery').resolves([{ cnt: 0 }]);
        await assert.rejects(() => db.getTableCount(EVIL), /unsafe table identifier/);
        assert.ok(spy.notCalled, 'doQuery must not run for an unsafe identifier');
    });

    it('getTablePage rejects an unsafe identifier before querying', async function () {
        let spy = sinon.stub(db, 'doQuery').resolves([]);
        await assert.rejects(() => db.getTablePage(EVIL, 10, 0), /unsafe table identifier/);
        assert.ok(spy.notCalled);
    });

    it('truncateTable rejects an unsafe identifier before querying', async function () {
        let spy = sinon.stub(db, 'doQuery').resolves([]);
        await assert.rejects(() => db.truncateTable(EVIL), /unsafe table identifier/);
        assert.ok(spy.notCalled);
    });

    it('allows a normal table name through to the query', async function () {
        let spy = sinon.stub(db, 'doQuery').resolves([{ cnt: 42 }]);
        let n = await db.getTableCount('contract_stakes');
        assert.strictEqual(n, 42);
        assert.ok(spy.calledOnce);
        assert.ok(spy.firstCall.args[0].includes('`contract_stakes`'));
    });
});

// ═══════════════════════════════════════════════════════════════════════════
// ensureReplicaSecondaryIndexes(): votes append-only unique-key migration (M-20)
// A replica that bootstrapped before indexer 219da33 carries the stale
// UNIQUE(poll_voter_choice) key; append-only re-ballot rows then wedge it on
// ER_DUP_ENTRY (unhealable, since the applier's last-write-wins pre-delete was
// removed). The self-heal must drop the stale key and add the widened one.
// ═══════════════════════════════════════════════════════════════════════════
describe('Database.ensureReplicaSecondaryIndexes(): votes append-only migration', function () {
    // Fake doQuery that treats only `votes` as present and all other tables
    // (index_tickers/index_addresses/attests) as absent, so the pre-existing
    // ensure/relax steps no-op and the test isolates the votes migration. The
    // votes index checks resolve from votesIndexes: keys are index names.
    function stubDoQuery(db, votesIndexes) {
        let calls = [];
        sinon.stub(db, 'doQuery').callsFake(async (sql, params) => {
            calls.push({ sql, params });
            if (/information_schema\.tables/.test(sql)) {
                // The votes/attests existence checks inline the table name; the
                // index-ensure checks pass it as a bound param. Only `votes` is present.
                if (/table_name = 'votes'/.test(sql)) return [{ table_name: 'votes' }];
                return [];
            }
            if (/information_schema\.statistics/.test(sql)) {
                if (/poll_voter_action_choice'/.test(sql))
                    return votesIndexes.poll_voter_action_choice ? [{ index_name: 'poll_voter_action_choice' }] : [];
                if (/poll_voter_choice'/.test(sql))
                    return votesIndexes.poll_voter_choice ? [{ index_name: 'poll_voter_choice' }] : [];
                return [];
            }
            return [];
        });
        return calls;
    }

    let db;
    beforeEach(function () { silenceConsole(); db = makeDb('indexer'); });
    afterEach(async function () { sinon.restore(); await db.close(); });

    it('drops the stale poll_voter_choice and creates the widened unique key', async function () {
        // Pre-219da33 replica: stale key present, widened key absent.
        let calls = stubDoQuery(db, { poll_voter_choice: true, poll_voter_action_choice: false });
        await db.ensureReplicaSecondaryIndexes();
        let ddl = calls.map(c => c.sql);
        assert.ok(ddl.some(s => /ALTER TABLE `votes` DROP INDEX `poll_voter_choice`/.test(s)),
            'must drop the stale poll_voter_choice unique key');
        assert.ok(ddl.some(s => /CREATE UNIQUE INDEX `poll_voter_action_choice` ON `votes`/.test(s)),
            'must create the append-only widened unique key');
    });

    it('is a no-op when the widened key already exists (idempotent)', async function () {
        // Post-migration / fresh replica: stale absent, widened present.
        let calls = stubDoQuery(db, { poll_voter_choice: false, poll_voter_action_choice: true });
        await db.ensureReplicaSecondaryIndexes();
        let ddl = calls.map(c => c.sql);
        assert.ok(!ddl.some(s => /DROP INDEX `poll_voter_choice`/.test(s)), 'no drop when stale key absent');
        assert.ok(!ddl.some(s => /CREATE UNIQUE INDEX `poll_voter_action_choice`/.test(s)), 'no create when already present');
    });

    it('creates the widened key on a replica missing both (defensive)', async function () {
        let calls = stubDoQuery(db, { poll_voter_choice: false, poll_voter_action_choice: false });
        await db.ensureReplicaSecondaryIndexes();
        let ddl = calls.map(c => c.sql);
        assert.ok(!ddl.some(s => /DROP INDEX `poll_voter_choice`/.test(s)), 'nothing to drop');
        assert.ok(ddl.some(s => /CREATE UNIQUE INDEX `poll_voter_action_choice` ON `votes`/.test(s)),
            'still ensures the append-only key exists');
    });

    it('is a no-op for a decoder replica (indexer-only)', async function () {
        let dec = makeDb('decoder');
        let spy = sinon.stub(dec, 'doQuery').resolves([]);
        await dec.ensureReplicaSecondaryIndexes();
        assert.ok(spy.notCalled, 'decoder returns before any schema query');
        await dec.close();
    });
});
