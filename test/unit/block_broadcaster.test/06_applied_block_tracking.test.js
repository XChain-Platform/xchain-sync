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
const WebSocket = require('ws');
const BlockBroadcaster = require('../../../src/server/block_broadcaster');

function mockWs(ip){
    let ws = {
        readyState: WebSocket.OPEN,
        bufferedAmount: 0,
        _syncBuffered: 0,
        _syncChain: null,
        _syncNetwork: null,
        _syncIp: null,
        send: sinon.stub(),
        close: sinon.stub(),
        on: sinon.stub()
    };
    return ws;
}

function mockReq(ip){
    return { headers: {}, socket: { remoteAddress: ip || '127.0.0.1' } };
}

let broadcaster, config;

function registerHooks(){
    beforeEach(function(){
        config = { WS_MAX_PER_IP: 3, WS_BACKPRESSURE_LIMIT: 50 };
        broadcaster = new BlockBroadcaster(config);
        sinon.stub(console, 'log');
    });

    afterEach(function(){
        sinon.restore();
    });
}

// Retrieve the inbound 'message' handler addSubscription registered on a ws.
function messageHandler(ws){
    let call = ws.on.getCalls().find(c => c.args[0] === 'message');
    return call ? call.args[1] : null;
}

describe('BlockBroadcaster', function(){
    registerHooks();

    describe('applied-block tracking', function(){
        it('initialises _syncLastSentBlock and _syncAppliedBlock to null', function(){
            let ws = mockWs();
            broadcaster.addSubscription(ws, mockReq('7.7.7.1'), 'bitcoin', 'mainnet');
            assert.strictEqual(ws._syncLastSentBlock, null);
            assert.strictEqual(ws._syncAppliedBlock, null);
        });

        it('updates _syncAppliedBlock when a heartbeat message arrives', function(){
            let ws = mockWs();
            broadcaster.addSubscription(ws, mockReq('7.7.7.2'), 'bitcoin', 'mainnet');
            let handler = messageHandler(ws);
            assert.ok(handler, 'message handler should be registered');
            handler(JSON.stringify({ type: 'heartbeat', appliedBlock: 42 }));
            assert.strictEqual(ws._syncAppliedBlock, 42);
        });

        it('ignores malformed or unknown inbound messages', function(){
            let ws = mockWs();
            broadcaster.addSubscription(ws, mockReq('7.7.7.3'), 'bitcoin', 'mainnet');
            let handler = messageHandler(ws);
            handler('not json');
            handler(JSON.stringify({ type: 'something-else', appliedBlock: 99 }));
            handler(JSON.stringify({ type: 'heartbeat' })); // no appliedBlock
            assert.strictEqual(ws._syncAppliedBlock, null);
        });

        // Stress-sweep 2026-07-08: a subscriber must not be able to forge its reported
        // lag with a non-integer/negative/infinite appliedBlock (mirrors the REST guard).
        it('ignores a heartbeat with a non-integer/negative/infinite appliedBlock', function(){
            let ws = mockWs();
            broadcaster.addSubscription(ws, mockReq('7.7.7.4'), 'bitcoin', 'mainnet');
            let handler = messageHandler(ws);
            for(let bad of [1.5, -1, NaN, Infinity, '5', null]){
                handler(JSON.stringify({ type: 'heartbeat', appliedBlock: bad }));
            }
            assert.strictEqual(ws._syncAppliedBlock, null); // none accepted
            handler(JSON.stringify({ type: 'heartbeat', appliedBlock: 0 }));
            assert.strictEqual(ws._syncAppliedBlock, 0); // a valid non-negative integer is accepted
        });
    });
});

describe('BlockBroadcaster', function(){
    registerHooks();

    describe('applied-block tracking', function(){
        it('updates _syncLastSentBlock to the block height on broadcast', function(){
            let ws1 = mockWs(), ws2 = mockWs();
            broadcaster.addSubscription(ws1, mockReq('7.7.7.4'), 'bitcoin', 'mainnet');
            broadcaster.addSubscription(ws2, mockReq('7.7.7.5'), 'bitcoin', 'mainnet');
            broadcaster.broadcast('bitcoin', 'mainnet', { type: 'block', block_index: 850000 });
            assert.strictEqual(ws1._syncLastSentBlock, 850000);
            assert.strictEqual(ws2._syncLastSentBlock, 850000);
        });

        it('does not advance _syncLastSentBlock for non-block events', function(){
            let ws = mockWs();
            broadcaster.addSubscription(ws, mockReq('7.7.7.6'), 'bitcoin', 'mainnet');
            broadcaster.broadcast('bitcoin', 'mainnet', { type: 'reorg', block_index: 850001 });
            assert.strictEqual(ws._syncLastSentBlock, null);
        });

        it('reports null appliedBlock and lag for a subscriber with no heartbeat', function(){
            let ws = mockWs();
            broadcaster.addSubscription(ws, mockReq('7.7.7.7'), 'bitcoin', 'mainnet');
            broadcaster.broadcast('bitcoin', 'mainnet', { type: 'block', block_index: 100 });
            let subs = broadcaster.getSubscribers('bitcoin', 'mainnet');
            assert.strictEqual(subs.length, 1);
            assert.strictEqual(subs[0].ip, '7.7.7.7');
            assert.strictEqual(subs[0].lastSentBlock, 100);
            assert.strictEqual(subs[0].appliedBlock, null);
            assert.strictEqual(subs[0].lag, null);
            assert.strictEqual(subs[0].heartbeatReceived, false);
            assert.strictEqual(subs[0].lagStatus, 'unknown');
        });
    });
});

describe('BlockBroadcaster', function(){
    registerHooks();

    describe('applied-block tracking', function(){
        it('reports lag = lastSentBlock - appliedBlock after a heartbeat', function(){
            let ws = mockWs();
            broadcaster.addSubscription(ws, mockReq('7.7.7.8'), 'bitcoin', 'mainnet');
            broadcaster.broadcast('bitcoin', 'mainnet', { type: 'block', block_index: 100 });
            messageHandler(ws)(JSON.stringify({ type: 'heartbeat', appliedBlock: 97 }));
            let subs = broadcaster.getSubscribers('bitcoin', 'mainnet');
            assert.strictEqual(subs[0].lastSentBlock, 100);
            assert.strictEqual(subs[0].appliedBlock, 97);
            assert.strictEqual(subs[0].lag, 3);
            assert.strictEqual(subs[0].heartbeatReceived, true);
            assert.strictEqual(subs[0].lagStatus, 'known');
        });

        it('reports heartbeatReceived true even when caught up (lag 0)', function(){
            let ws = mockWs();
            broadcaster.addSubscription(ws, mockReq('7.7.7.9'), 'bitcoin', 'mainnet');
            broadcaster.broadcast('bitcoin', 'mainnet', { type: 'block', block_index: 100 });
            messageHandler(ws)(JSON.stringify({ type: 'heartbeat', appliedBlock: 100 }));
            let subs = broadcaster.getSubscribers('bitcoin', 'mainnet');
            // lag 0 (caught up) must be distinguishable from lag null (unknown):
            // heartbeatReceived is the signal that disambiguates the two.
            assert.strictEqual(subs[0].lag, 0);
            assert.strictEqual(subs[0].heartbeatReceived, true);
            // caught up (lag 0) is 'known', distinct from a never-heartbeated 'unknown'.
            assert.strictEqual(subs[0].lagStatus, 'known');
        });

        it('returns an empty array for an unknown chain/network', function(){
            assert.deepStrictEqual(broadcaster.getSubscribers('unknown', 'test'), []);
        });
    });
});
