// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

const assert = require('assert');
const sinon  = require('sinon');
const proxyquire = require('proxyquire').noCallThru();
const HashVerifier = require('../../../src/client/hash_verifier');
const { withDbMixins } = require('../../helpers/db_mixins.js');

// Queries read through named Database methods; the real ones are installed for any
// this fake does not stub, so they still reach the doQuery stub the suite inspects.
function createMockDb(){
    return withDbMixins({
        dbName: 'test_db',
        getLastBlock: sinon.stub().resolves(null),
        getBlockHashRow: sinon.stub().resolves(null),
        doQuery: sinon.stub().resolves([]),
        beginTransaction: sinon.stub().resolves(),
        commitTransaction: sinon.stub().resolves(true),
        rollbackTransaction: sinon.stub().resolves(),
        truncateTable: sinon.stub().resolves(),
        recordHalt: sinon.stub().resolves()
    });
}

function createMockApplier(){
    return {
        applyBlock: sinon.stub().resolves(),
        applyFullSnapshot: sinon.stub().resolves(),
        applyIncrementalSnapshot: sinon.stub().resolves()
    };
}

function createMockRollback(){
    return {
        rollback: sinon.stub().resolves()
    };
}

function createMockHashVerifier(){
    return new HashVerifier();
}

function createMockUtil(){
    return {
        sleep: sinon.stub().resolves(),
        startTimer: sinon.stub().returns(Date.now()),
        getTimer: sinon.stub().returns('0ms'),
        isNull: function(v){ return v === null || v === undefined || v === ''; },
        throwError: function(e){ throw new Error(e); },
        logError: sinon.stub()
    };
}

function createConfig(overrides){
    return Object.assign({
        SYNC_MODE: 'client',
        SYNC_SOURCES: 'http://source1.local,http://source2.local',
        VERIFY_HASHES: true,
        HASH_CONFIRM_TIMEOUT: 100,
        HASH_CONFIRM_STRICT: false,
        MAX_ROLLBACK_DEPTH: 100,
        WS_MAX_PAYLOAD: 1048576,
        SNAPSHOT_MAX_CONTENT: 536870912,
        CLIENT_RECONNECT_DELAY: 100,
        REPLICA_DB_HOST: 'localhost',
        REPLICA_DB_PORT: 3306,
        REPLICA_DB_USER: 'test',
        REPLICA_DB_PASS: 'test'
    }, overrides);
}

let db, applier, rollback, hashVerifier, util;

function registerHooks(){
    beforeEach(function(){
        db = createMockDb();
        applier = createMockApplier();
        rollback = createMockRollback();
        hashVerifier = createMockHashVerifier();
        util = createMockUtil();
        sinon.stub(console, 'error');
        sinon.stub(console, 'log');
    });

    afterEach(function(){
        sinon.restore();
    });
}

describe('ClientSync security', function(){
    registerHooks();

    // ── connectWebSocket: maxPayload option ──

    describe('connectWebSocket: maxPayload', function(){

        it('passes maxPayload to WebSocket constructor', function(){
            let wsConstructorCalls = [];
            let fakeWs = function(url, opts){
                wsConstructorCalls.push({ url, opts });
                return {
                    on: sinon.stub(),
                    close: sinon.stub()
                };
            };

            let ClientSync = proxyquire('../../../src/client/sync', {
                'axios': { get: sinon.stub() },
                'ws': fakeWs
            });

            let config = createConfig({ WS_MAX_PAYLOAD: 2097152 });
            let sync = new ClientSync('bitcoin', 'mainnet', db, applier, rollback, hashVerifier, config, util);
            sync.connectWebSocket('http://source1.local', 0);

            assert.strictEqual(wsConstructorCalls.length, 1);
            assert.strictEqual(wsConstructorCalls[0].opts.maxPayload, 2097152);
        });
    });
});
