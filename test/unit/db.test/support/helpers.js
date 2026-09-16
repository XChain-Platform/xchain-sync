// Copyright © 2025–2026 Dankest, LLC
// SPDX-License-Identifier: AGPL-3.0-or-later

'use strict';

const assert      = require('assert');
const sinon       = require('sinon');
const fs          = require('fs');
const proxyquire  = require('proxyquire').noCallThru();
const Database    = require('../../../../src/db');

// ─── proxyquire-based Database factory ──────────────────────────────────────
// mariadb is an ES module: sinon.stub(mariadb, 'createConnection') throws
// "ES Modules cannot be stubbed".  For tests that need to control
// createConnection we build a fresh Database class with a fake mariadb
// injected via proxyquire.

function makeDbWithFakeMariadb(fakeMariadb, dbType = 'indexer', dbName = 'replica_db', util = null) {
    let FakeDatabase = proxyquire('../../../../src/db', {
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

module.exports = {
    assert,
    sinon,
    fs,
    Database,
    makeDbWithFakeMariadb,
    fakeMariadbWith,
    makeUtil,
    makeDb,
    fakeConn,
    silenceConsole,
};
