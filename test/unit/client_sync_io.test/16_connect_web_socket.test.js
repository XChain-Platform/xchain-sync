// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

const assert     = require('assert');
const sinon      = require('sinon');
const proxyquire = require('proxyquire');
const EventEmitter = require('events');
const ClientSync = require('../../../src/client/sync');
const Utility    = require('../../../src/util');
const HashVerifier = require('../../../src/client/hash_verifier');

function createMockDb(overrides){
    return Object.assign({
        dbName:           'test_db',
        dbType:           'indexer',
        getLastBlock:     sinon.stub().resolves(null),
        getBlockHashRow:  sinon.stub().resolves(null),
        doQuery:          sinon.stub().resolves([]),
        getActiveHalt:    sinon.stub().resolves(null),
        getTableCount:    sinon.stub().resolves(0),
        addMissingColumns: sinon.stub().resolves(),
        recordHalt:       sinon.stub().resolves({ block_index: 0 }),
        clearHalt:        sinon.stub().resolves(1)
    }, overrides || {});
}

function createMockApplier(){
    return {
        applyBlock:               sinon.stub().resolves(),
        applyFullSnapshot:        sinon.stub().resolves(),
        applyIncrementalSnapshot: sinon.stub().resolves()
    };
}

function createMockRollback(){
    return { rollback: sinon.stub().resolves() };
}

let ClientSyncWS, fakeWsInstance, FakeWS;

function setupWebSocketTest(){
    sinon.stub(console, 'log');
    sinon.stub(console, 'error');

    fakeWsInstance = new EventEmitter();
    fakeWsInstance.close    = sinon.stub();
    fakeWsInstance.send     = sinon.stub();
    fakeWsInstance.readyState = 1; // OPEN

    FakeWS = function(url, opts){
        // Attach the singleton fake instance
        Object.setPrototypeOf(fakeWsInstance, FakeWS.prototype);
        return fakeWsInstance;
    };
    // Give FakeWS the same OPEN constant as the real ws
    FakeWS.OPEN = 1;
    Object.setPrototypeOf(FakeWS.prototype, EventEmitter.prototype);

    ClientSyncWS = proxyquire('../../../src/client/sync', { ws: FakeWS });
}

function makeSyncWS(configOverrides){
    let db      = createMockDb();
    let applier = createMockApplier();
    let rb      = createMockRollback();
    let hv      = new HashVerifier();
    let util    = new Utility();
    let config  = Object.assign({
        SYNC_SOURCES:           'http://src1:3006',
        VERIFY_HASHES:          false,
        CLIENT_RECONNECT_DELAY: 5000,
        HASH_CONFIRM_TIMEOUT:   5000,
        SNAPSHOT_MAX_CONTENT:   200 * 1024 * 1024,
        WS_MAX_PAYLOAD:         50 * 1024 * 1024,
        MAX_ROLLBACK_DEPTH:     10,
        GAP_LOG_INTERVAL_MS:    30000
    }, configOverrides || {});
    let sync = new ClientSyncWS('bitcoin', 'mainnet', db, applier, rb, hv, config, util);
    return { sync, db, applier };
}
describe('ClientSync: connectWebSocket', function(){
    beforeEach(setupWebSocketTest);
    afterEach(function(){ sinon.restore(); });
it('stores ws in wsConns and registers handlers', function(){
        let { sync } = makeSyncWS();
        sync.running = true;

        sync.connectWebSocket('http://src1:3006', 0);

        assert.strictEqual(sync.wsConns[0], fakeWsInstance);
        assert.ok(fakeWsInstance.listenerCount('open')    > 0);
        assert.ok(fakeWsInstance.listenerCount('message') > 0);
        assert.ok(fakeWsInstance.listenerCount('close')   > 0);
        assert.ok(fakeWsInstance.listenerCount('error')   > 0);
    });

    it('uses ?sync_mode=infra-only query string when SYNC_MODE env is set', function(){
        // The mode is resolved ONCE in the constructor (which refuses infra-only while
        // any halting VERIFY_* gate is on), so the env must be set before construction
        // and the gates explicitly off for the client to build at all.
        let envKey = 'SYNC_MODE_BITCOIN';
        process.env[envKey] = 'infra-only';

        try {
            let { sync } = makeSyncWS({ VERIFY_RECOMPUTE: false, VERIFY_STATE_HASH: false, VERIFY_STATE_COMMITMENT: false });
            sync.connectWebSocket('http://src1:3006', 0);
            let logCalls = console.log.getCalls().map(c => c.args[0]);
            assert.ok(logCalls.some(m => m && m.indexOf('?sync_mode=infra-only') !== -1),
                'infra-only mode must be in the subscribe URL');
        } finally {
            delete process.env[envKey];
        }
    });

    it('triggers scheduleReconnect when WS constructor throws', function(){
        // Make FakeWS throw on construction
        let ThrowWS = function(){ throw new Error('ws construct fail'); };
        ThrowWS.OPEN = 1;
        let CSThrow = proxyquire('../../../src/client/sync', { ws: ThrowWS });
        let db = createMockDb(), applier = createMockApplier(), rb = createMockRollback();
        let hv = new HashVerifier(), util = new Utility();
        let config = {
            SYNC_SOURCES: 'http://src1:3006', VERIFY_HASHES: false,
            CLIENT_RECONNECT_DELAY: 9999999, HASH_CONFIRM_TIMEOUT: 5000,
            WS_MAX_PAYLOAD: 50*1024*1024, MAX_ROLLBACK_DEPTH: 10,
            SNAPSHOT_MAX_CONTENT: 200*1024*1024, GAP_LOG_INTERVAL_MS: 30000
        };
        let syncT = new CSThrow('bitcoin', 'mainnet', db, applier, rb, hv, config, util);
        syncT.running = false; // scheduleReconnect no-ops when not running

        // Should not throw
        syncT.connectWebSocket('http://src1:3006', 0);

        let errCalls = console.error.getCalls().map(c => c.args[0]);
        assert.ok(errCalls.some(m => m && m.indexOf('WebSocket connection error') !== -1));
    });
});

describe('ClientSync: connectWebSocket', function(){
    beforeEach(setupWebSocketTest);
    afterEach(function(){ sinon.restore(); });


    it('message handler: calls handleEvent for a valid block event', async function(){
        let { sync } = makeSyncWS();
        sync.running = true;
        sinon.stub(sync, 'handleEvent').resolves();

        sync.connectWebSocket('http://src1:3006', 0);

        let validEvent = JSON.stringify({ type: 'block', block_index: 5, ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c' });
        await fakeWsInstance.emit('message', Buffer.from(validEvent));

        assert.ok(sync.handleEvent.calledOnce, 'handleEvent must be called for valid event');
    });

    it('message handler: logs invalid WS event', async function(){
        let { sync } = makeSyncWS();
        sync.running = true;
        sinon.stub(sync, 'handleEvent').resolves();

        sync.connectWebSocket('http://src1:3006', 0);

        let invalidEvent = JSON.stringify({ type: 'unknown_type', block_index: 5 });
        await fakeWsInstance.emit('message', Buffer.from(invalidEvent));

        let errCalls = console.error.getCalls().map(c => c.args[0]);
        assert.ok(errCalls.some(m => m && m.indexOf('Invalid WS event') !== -1));
    });

    it('message handler: logs on malformed JSON', async function(){
        let { sync } = makeSyncWS();
        sync.running = true;
        sinon.stub(sync, 'handleEvent').resolves();

        sync.connectWebSocket('http://src1:3006', 0);

        await fakeWsInstance.emit('message', Buffer.from('{not json'));

        // Malformed JSON is caught at parse time (before the serialized event chain).
        let errCalls = console.error.getCalls().map(c => c.args[0]);
        assert.ok(errCalls.some(m => m && m.indexOf('Error parsing WebSocket message') !== -1));
    });
});

describe('ClientSync: connectWebSocket', function(){
    beforeEach(setupWebSocketTest);
    afterEach(function(){ sinon.restore(); });


    it('message handler: logs when handleEvent rejects', async function(){
        let { sync } = makeSyncWS();
        sync.running = true;
        sinon.stub(sync, 'handleEvent').rejects(new Error('handle boom'));

        sync.connectWebSocket('http://src1:3006', 0);

        // A well-formed event passes validation and reaches the serialized event
        // chain; its rejection is surfaced by the chain's .catch.
        await fakeWsInstance.emit('message', Buffer.from(JSON.stringify({ type: 'block', block_index: 5 })));
        await sync._wsEventChain;   // let the .then(handleEvent).catch settle

        let errCalls = console.error.getCalls().map(c => c.args[0]);
        assert.ok(errCalls.some(m => m && m.indexOf('Error handling WebSocket message') !== -1));
    });

    it('message handler: escalates mid-stream bootstrap exhaustion to process.exit(1)', async function(){
        // Permanent bootstrap exhaustion reached from live WS handling (the size-cap
        // fallback in runIncrementalCatchUp) must honor the supervised-restart
        // contract, not be swallowed by the chain's log-and-continue catch: a
        // swallowed exhaustion leaves the process alive but permanently stalled.
        let { sync } = makeSyncWS();
        sync.running = true;
        let exit = sinon.stub(process, 'exit');
        sinon.stub(sync, 'handleEvent')
            .rejects(new ClientSyncWS.BootstrapExhaustedError('all sync sources exhausted'));

        sync.connectWebSocket('http://src1:3006', 0);

        await fakeWsInstance.emit('message', Buffer.from(JSON.stringify({ type: 'block', block_index: 5 })));
        await sync._wsEventChain;   // let the .then(handleEvent).catch settle

        assert.ok(exit.calledOnceWithExactly(1), 'must exit(1) for a supervised restart');
        let errCalls = console.error.getCalls().map(c => c.args[0]);
        assert.ok(errCalls.some(m => m && m.indexOf('Bootstrap exhausted mid-stream') !== -1));
    });

    it('message handler: an ordinary handler error does NOT exit the process', async function(){
        let { sync } = makeSyncWS();
        sync.running = true;
        let exit = sinon.stub(process, 'exit');
        sinon.stub(sync, 'handleEvent').rejects(new Error('transient boom'));

        sync.connectWebSocket('http://src1:3006', 0);

        await fakeWsInstance.emit('message', Buffer.from(JSON.stringify({ type: 'block', block_index: 5 })));
        await sync._wsEventChain;

        assert.strictEqual(exit.called, false, 'log-and-continue must be preserved for transient errors');
        let errCalls = console.error.getCalls().map(c => c.args[0]);
        assert.ok(errCalls.some(m => m && m.indexOf('Error handling WebSocket message') !== -1));
    });
});

describe('ClientSync: connectWebSocket', function(){
    beforeEach(setupWebSocketTest);
    afterEach(function(){ sinon.restore(); });


    it('close handler: calls scheduleReconnect', function(){
        let { sync } = makeSyncWS();
        sync.running = false; // prevent actual reconnect timer

        sync.connectWebSocket('http://src1:3006', 0);
        fakeWsInstance.emit('close');

        let logCalls = console.log.getCalls().map(c => c.args[0]);
        assert.ok(logCalls.some(m => m && m.indexOf('WebSocket disconnected') !== -1));
    });

    it('error handler: logs the error message', function(){
        let { sync } = makeSyncWS();

        sync.connectWebSocket('http://src1:3006', 0);
        fakeWsInstance.emit('error', { message: 'ECONNREFUSED' });

        let errCalls = console.error.getCalls().map(c => c.args[0]);
        assert.ok(errCalls.some(m => m && m.indexOf('WebSocket error') !== -1));
    });

    it('scheduleReconnect: no-op when running=false', function(){
        let { sync } = makeSyncWS();
        sync.running = false;
        let clock = sinon.useFakeTimers();
        sinon.stub(sync, 'connectWebSocket');

        sync.scheduleReconnect('http://src1:3006', 0);
        clock.tick(10000);

        assert.strictEqual(sync.connectWebSocket.called, false);
        clock.restore();
    });

    it('scheduleReconnect: reconnects after CLIENT_RECONNECT_DELAY when running=true', function(){
        let { sync } = makeSyncWS({ CLIENT_RECONNECT_DELAY: 100 });
        sync.running = true;
        let clock = sinon.useFakeTimers();

        sinon.stub(sync, 'connectWebSocket');
        // Only the schedule, not the one registered in constructor
        sync.scheduleReconnect('http://src1:3006', 0);
        assert.strictEqual(sync.connectWebSocket.called, false);

        clock.tick(101);

        assert.ok(sync.connectWebSocket.calledOnce, 'must reconnect after delay');
        clock.restore();
    });
});

describe('ClientSync: connectWebSocket', function(){
    beforeEach(setupWebSocketTest);
    afterEach(function(){ sinon.restore(); });


    it('connectWebSockets iterates all sources', function(){
        let { sync } = makeSyncWS({ SYNC_SOURCES: 'http://src1:3006,http://src2:3006' });
        sinon.stub(sync, 'connectWebSocket');

        sync.connectWebSockets();

        assert.strictEqual(sync.connectWebSocket.callCount, 2);
        assert.strictEqual(sync.connectWebSocket.firstCall.args[0], 'http://src1:3006');
        assert.strictEqual(sync.connectWebSocket.secondCall.args[0], 'http://src2:3006');
    });

    it('open handler: logs connected message', function(){
        let { sync } = makeSyncWS();
        sync.running = false;

        sync.connectWebSocket('http://src1:3006', 0);
        fakeWsInstance.emit('open');

        let logCalls = console.log.getCalls().map(c => c.args[0]);
        assert.ok(logCalls.some(m => m && m.indexOf('WebSocket connected') !== -1));
    });
});
