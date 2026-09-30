// Copyright © 2025–2026 Dankest, LLC
// SPDX-License-Identifier: AGPL-3.0-or-later

'use strict';

const assert     = require('assert');
const sinon      = require('sinon');
const proxyquire = require('proxyquire').noCallThru();

function fakeMariadb(createPool){
    return {
        createPool,
        createConnection: sinon.stub(),
        '@noCallThru': true
    };
}

function makeDb(dbType, createPool){
    let FakeDatabase = proxyquire('../../src/db', { mariadb: fakeMariadb(createPool) });
    return new FakeDatabase('localhost', 3306, 'db_' + dbType, 'u', 'p', {
        isNull: (value) => value === null || value === undefined,
        throwError: (message) => { throw new Error(message); },
        sleep: sinon.stub().resolves(),
        logError: sinon.stub()
    }, dbType);
}

describe('Database pool session time zone', function(){
    for(let dbType of ['indexer', 'decoder']){
        it('pins the ' + dbType + ' pool to UTC while preserving date strings', function(){
            let createPool = sinon.stub().returns({ end: sinon.stub().resolves() });
            makeDb(dbType, createPool);

            sinon.assert.calledOnce(createPool);
            let config = createPool.firstCall.args[0];
            assert.strictEqual(config.timezone, 'Z');
            assert.strictEqual(config.dateStrings, true);
        });
    }
});
