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

    // ── WebSocket message handler: event validation ──

    describe('WebSocket message handler: event validation', function(){

        it('rejects message with unknown event type', async function(){
            let messageHandler = null;
            let fakeWs = function(url, opts){
                return {
                    on: function(event, handler){
                        if(event === 'message') messageHandler = handler;
                    },
                    close: sinon.stub()
                };
            };

            let ClientSync = proxyquire('../../../src/client/sync', {
                'axios': { get: sinon.stub() },
                'ws': fakeWs
            });

            let config = createConfig();
            let sync = new ClientSync('bitcoin', 'mainnet', db, applier, rollback, hashVerifier, config, util);
            sinon.stub(sync, 'handleEvent');
            sync.connectWebSocket('http://source1.local', 0);

            assert.ok(messageHandler, 'message handler should be registered');

            let invalidEvent = JSON.stringify({ type: 'DROP TABLE', block_index: 1 });
            // The ws 'message' handler is fire-and-serialize: it validates synchronously
            // then chains handleEvent onto the internal _wsEventChain rather than returning
            // a promise (concurrency fix; see ClientSync.connectWebSocket). An invalid event
            // is rejected before any chaining, so flush microtasks and assert.
            messageHandler(Buffer.from(invalidEvent));
            await (sync._wsEventChain || Promise.resolve());
            assert.strictEqual(sync.handleEvent.called, false);
            assert.strictEqual(console.error.called, true);
        });
    });
});

describe('ClientSync security', function(){
    registerHooks();

    describe('WebSocket message handler: event validation', function(){

        it('rejects non-JSON message without crashing', async function(){
            let messageHandler = null;
            let fakeWs = function(url, opts){
                return {
                    on: function(event, handler){
                        if(event === 'message') messageHandler = handler;
                    },
                    close: sinon.stub()
                };
            };

            let ClientSync = proxyquire('../../../src/client/sync', {
                'axios': { get: sinon.stub() },
                'ws': fakeWs
            });

            let config = createConfig();
            let sync = new ClientSync('bitcoin', 'mainnet', db, applier, rollback, hashVerifier, config, util);
            sinon.stub(sync, 'handleEvent');
            sync.connectWebSocket('http://source1.local', 0);

            // Fire-and-serialize handler (see the unknown-event-type test): a non-JSON
            // message is rejected synchronously in the try/catch before any chaining.
            messageHandler(Buffer.from('not valid json'));
            await (sync._wsEventChain || Promise.resolve());
            assert.strictEqual(sync.handleEvent.called, false);
        });
    });
});

describe('ClientSync security', function(){
    registerHooks();

    describe('WebSocket message handler: event validation', function(){

        it('accepts valid block event and calls handleEvent', async function(){
            let messageHandler = null;
            let fakeWs = function(url, opts){
                return {
                    on: function(event, handler){
                        if(event === 'message') messageHandler = handler;
                    },
                    close: sinon.stub()
                };
            };

            let ClientSync = proxyquire('../../../src/client/sync', {
                'axios': { get: sinon.stub() },
                'ws': fakeWs
            });

            let config = createConfig();
            let sync = new ClientSync('bitcoin', 'mainnet', db, applier, rollback, hashVerifier, config, util);
            sinon.stub(sync, 'handleEvent').resolves();
            sync.connectWebSocket('http://source1.local', 0);

            let validEvent = JSON.stringify({ type: 'block', block_index: 100, data: {} });
            // A valid event is chained onto _wsEventChain; await that internal chain so the
            // serialized handleEvent call has run before asserting.
            messageHandler(Buffer.from(validEvent));
            await (sync._wsEventChain || Promise.resolve());
            assert.strictEqual(sync.handleEvent.calledOnce, true);
        });
    });
});
