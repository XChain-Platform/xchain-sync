// Copyright © 2025–2026 Dankest, LLC
// SPDX-License-Identifier: AGPL-3.0-or-later

'use strict';

const {
    assert,
    sinon,
    fs,
    makeDbWithFakeMariadb,
    fakeMariadbWith,
    makeDb,
    fakeConn,
    silenceConsole,
} = require('./support/helpers');

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


describe('Database.beginReadSnapshot()', function () {
    let db;
    beforeEach(function () { silenceConsole(); db = makeDb(); });
    afterEach(async function () { sinon.restore(); await db.close(); });

    it('acquires a DEDICATED connection and runs SET / START TRANSACTION on it', async function () {
        let conn = fakeConn();
        sinon.stub(db, 'acquirePoolConnection').resolves(conn);
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
        sinon.stub(db, 'acquirePoolConnection').resolves(conn);
        await db.beginReadSnapshot();
        assert.strictEqual(db.transactionConnection, shared, 'writer connection left intact');
        assert.ok(shared.release.notCalled, 'snapshot does not release the writer connection');
    });

    it('on query error: releases the dedicated connection and throws (shared field untouched)', async function () {
        let conn = fakeConn();
        conn.query.rejects(new Error('snap fail'));
        sinon.stub(db, 'acquirePoolConnection').resolves(conn);
        await assert.rejects(
            () => db.beginReadSnapshot(),
            /beginReadSnapshot error/
        );
        assert.ok(conn.release.calledOnce);
        assert.strictEqual(db.transactionConnection, null);
    });
});


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

    it('creates table when it does not exist (calls createTableFromFile)', async function () {
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

});
