/*********************************************************************
 *
 * Copyright © 2025-2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC - https://dankest.llc
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 ********************************************************************/

'use strict';

const assert = require('assert');
const EventEmitter = require('events');
const proxyquire = require('proxyquire');
const sinon = require('sinon');
const HashVerifier = require('./hash_verifier');
const Utility = require('../util');

const restartSockets = [];
function RestartWebSocket(){
    const socket = new EventEmitter();
    socket.close = sinon.stub();
    socket.terminate = sinon.stub();
    restartSockets.push(socket);
    return socket;
}
RestartWebSocket.OPEN = 1;
const RestartClientSync = proxyquire('./sync', { ws: RestartWebSocket });

function createDb(){
    return {
        dbName: 'restart_test',
        dbType: 'indexer',
        getLastBlock: sinon.stub().resolves(10),
        getBlockHashRow: sinon.stub().resolves(null),
        doQuery: sinon.stub().resolves([]),
        getActiveHalt: sinon.stub().resolves(null),
        getTableCount: sinon.stub().resolves(0),
        addMissingColumns: sinon.stub().resolves(),
        recordHalt: sinon.stub().resolves({ block_index: 0 }),
        clearHalt: sinon.stub().resolves(1)
    };
}

function createConfig(overrides){
    return Object.assign({
        SYNC_SOURCES: 'http://source:3006',
        VERIFY_HASHES: false,
        VERIFY_RECOMPUTE: false,
        VERIFY_STATE_HASH: false,
        VERIFY_STATE_COMMITMENT: false,
        CLIENT_RECONNECT_DELAY: 1,
        CLIENT_SOURCE_STALE_MS: 1000,
        HASH_CONFIRM_TIMEOUT: 5000,
        SNAPSHOT_MAX_CONTENT: 200 * 1024 * 1024,
        WS_MAX_PAYLOAD: 50 * 1024 * 1024,
        MAX_ROLLBACK_DEPTH: 10,
        GAP_LOG_INTERVAL_MS: 30000
    }, overrides || {});
}

describe('ClientSync source restart recovery', function(){
    afterEach(function(){
        sinon.restore();
    });

    it('applies blocks again after the source drops and returns mid-stream', async function(){
        restartSockets.length = 0;
        const sync = new RestartClientSync('bitcoin', 'mainnet', createDb(), {}, {},
            new HashVerifier(), createConfig(), new Utility());
        const applied = [];
        sinon.stub(sync, 'handleEvent').callsFake(async event => {
            applied.push(event.block_index);
            sync.lastAppliedBlock = event.block_index;
        });
        const catchUp = sinon.stub(sync, 'incrementalCatchUp').resolves();
        const clock = sinon.useFakeTimers();
        sync.lastAppliedBlock = 10;
        Reflect.set(sync, '_verifiedTip', 10);
        sync.running = true;

        sync.connectWebSocket('http://source:3006', 0);
        restartSockets[0].emit('open');
        restartSockets[0].emit('message', Buffer.from(JSON.stringify({ type: 'block', block_index: 11 })));
        await Reflect.get(sync, '_wsEventChain');

        restartSockets[0].emit('close');
        await clock.tickAsync(2);
        assert.strictEqual(restartSockets.length, 2);
        restartSockets[1].emit('open');
        await clock.tickAsync(0);
        restartSockets[1].emit('message', Buffer.from(JSON.stringify({ type: 'block', block_index: 12 })));
        await Reflect.get(sync, '_wsEventChain');

        assert.deepStrictEqual(applied, [11, 12]);
        assert.ok(catchUp.calledOnceWithExactly(11));
        sync.running = false;
        restartSockets[1].emit('close');
        clock.restore();
    });

    it('reconnects a stale stream only after a status read proves the source tip advanced', async function(){
        const socket = new EventEmitter();
        socket.close = sinon.stub();
        socket.terminate = sinon.stub();
        function FakeWebSocket(){ return socket; }
        FakeWebSocket.OPEN = 1;
        const get = sinon.stub();
        const ClientSync = proxyquire('./sync', { ws: FakeWebSocket, axios: { get } });
        const sync = new ClientSync('bitcoin', 'mainnet', createDb(), {}, {},
            new HashVerifier(), createConfig(), new Utility());
        sync.lastAppliedBlock = 10;
        sync.running = true;
        sync.connectWebSocket('http://source:3006', 0);
        socket.emit('open');
        socket.emit('message', Buffer.from(JSON.stringify({
            type: 'status', block_height: 10, source_block_height: 10
        })));
        await Reflect.get(sync, '_wsEventChain');
        const staleSeenAt = Reflect.get(sync, '_sourceLastWsEventAt').get(0);
        await new Promise(resolve => setTimeout(resolve, 5));

        get.resolves({ data: { source_height: 10, block_height: 10 } });
        await sync.reconnectStaleStreamIfSourceAdvanced('http://source:3006', 0, socket, 1, staleSeenAt);
        assert.strictEqual(socket.terminate.called, false);

        get.resolves({ data: { source_height: 11, block_height: 10 } });
        await sync.reconnectStaleStreamIfSourceAdvanced('http://source:3006', 0, socket, 1, staleSeenAt);
        assert.strictEqual(socket.terminate.calledOnce, true);
        sync.running = false;
        sync.cancelReconnect(0);
        sync.clearStreamWatchdog(0);
    });
});
