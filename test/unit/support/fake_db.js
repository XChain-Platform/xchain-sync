// Copyright © 2025–2026 Dankest, LLC
// SPDX-License-Identifier: AGPL-3.0-or-later

'use strict';

const sinon      = require('sinon');
const proxyquire = require('proxyquire').noCallThru();

// The Database constructor calls mariadb.createPool unconditionally, and the
// real driver starts dialing out in the background as soon as the pool
// exists, independent of whether a test ever issues a query. A fixture built
// straight from src/db therefore reaches whatever actually answers on the
// given host/port using the 'u'/'p' fixture credentials. Every mock-based
// db.js unit suite builds its instance through this factory instead, so
// mariadb itself is a fake and the fixture credentials never reach a live
// server.

function fakeConnection(queryResult = []) {
    return {
        query:            sinon.stub().resolves(queryResult),
        release:          sinon.stub().resolves(),
        beginTransaction: sinon.stub().resolves(),
        rollback:         sinon.stub().resolves(),
        commit:           sinon.stub().resolves(),
    };
}

const FakeDatabase = proxyquire('../../../src/db', {
    mariadb: {
        createPool:       () => ({ end: () => Promise.resolve(), getConnection: () => Promise.resolve(fakeConnection()) }),
        createConnection: () => Promise.resolve(fakeConnection()),
        '@noCallThru':    true,
    },
});

// dbName/user/pass/util/dbType match src/db's own constructor order, minus
// host/port: those are fixed to a fixture value no real driver ever dials.
function makeTestDatabase(dbName, user, pass, util, dbType) {
    return new FakeDatabase('unit-test-fixture', 0, dbName, user, pass, util, dbType);
}

module.exports = { fakeConnection, FakeDatabase, makeTestDatabase };
