// Copyright © 2025–2026 Dankest, LLC
// SPDX-License-Identifier: AGPL-3.0-or-later

'use strict';

const assert      = require('assert');
const sinon       = require('sinon');
const fs          = require('fs');
const proxyquire  = require('proxyquire').noCallThru();

// Fixture host/port a real mariadb driver never dials, so a Database built
// through these factories cannot reach whatever actually listens on the
// loopback address with the 'u'/'p' fixture credentials.
const FIXTURE_HOST = 'unit-test-fixture';
const FIXTURE_PORT = 0;

// ─── proxyquire-based Database factory ──────────────────────────────────────
// mariadb is an ES module: sinon.stub(mariadb, 'createConnection') throws
// "ES Modules cannot be stubbed".  For tests that need to control
// createConnection we build a fresh Database class with a fake mariadb
// injected via proxyquire.

function makeDbWithFakeMariadb(fakeMariadb, dbType = 'indexer', dbName = 'replica_db', util = null) {
    let FakeDatabase = proxyquire('../../../../src/db', {
        mariadb: fakeMariadb
    });
    return new FakeDatabase(FIXTURE_HOST, FIXTURE_PORT, dbName, 'u', 'p', util || makeUtil(), dbType);
}

// Default Database export: the real class with mariadb itself faked, so any
// caller that constructs directly (rather than through makeDbWithFakeMariadb)
// still never opens a real socket. Plain functions, not sinon stubs: callers
// routinely sinon.stub(db.pool, 'getConnection') themselves, and sinon
// refuses to wrap a method that is already a stub.
const Database = proxyquire('../../../../src/db', {
    mariadb: {
        createPool:        () => ({ end: () => Promise.resolve(), getConnection: () => Promise.resolve(fakeConn()) }),
        createConnection:  () => Promise.resolve(fakeConn()),
        '@noCallThru':     true,
    },
});

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
    return new Database(FIXTURE_HOST, FIXTURE_PORT, dbName, 'u', 'p', util || makeUtil(), dbType);
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

module.exports = {
    assert,
    sinon,
    fs,
    Database,
    FIXTURE_HOST,
    FIXTURE_PORT,
    makeDbWithFakeMariadb,
    fakeMariadbWith,
    makeUtil,
    makeDb,
    fakeConn,
    silenceConsole,
};
