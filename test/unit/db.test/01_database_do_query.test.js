// Copyright © 2025–2026 Dankest, LLC
// SPDX-License-Identifier: AGPL-3.0-or-later

'use strict';

const {
    assert,
    sinon,
    makeDb,
    fakeConn,
    silenceConsole,
} = require('./support/helpers');

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

});

describe('Database.doQuery()', function () {
    let db;
    beforeEach(function () { silenceConsole(); db = makeDb(); });
    afterEach(async function () { sinon.restore(); await db.close(); });

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

    it('on query error in non-tx path with opts.rethrow: logs, releases, AND re-throws (fail-closed)', async function () {
        // A fail-CLOSED reader (getActiveHalt) passes rethrow so a transient error
        // is not silently returned as []. The connection must still be released.
        let conn = fakeConn();
        conn.query.rejects(new Error('halt read fail'));
        sinon.stub(db, 'getConnection').resolves(conn);
        await assert.rejects(
            () => db.doQuery('SELECT 1', [], null, { rethrow: true }),
            /halt read fail/
        );
        assert.ok(conn.release.calledOnce, 'connection released even on the rethrow path');
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

});

describe('Database.getConnection(): circuit breaker', function () {
    let db;
    beforeEach(function () { silenceConsole(); db = makeDb(); });
    afterEach(async function () { sinon.restore(); await db.close(); });

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
