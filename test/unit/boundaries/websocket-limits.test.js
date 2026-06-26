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
const BlockBroadcaster = require('../../../src/BlockBroadcaster');

function mockWs(){
    return {
        readyState: WebSocket.OPEN,
        bufferedAmount: 0,
        _syncBuffered: 0,
        _syncChain: null, _syncNetwork: null, _syncIp: null,
        send: sinon.stub(), close: sinon.stub(), on: sinon.stub()
    };
}

function mockReq(ip){
    return { headers: {}, socket: { remoteAddress: ip || '127.0.0.1' } };
}

describe('Boundary: WebSocket Limits', function(){

    let broadcaster;

    afterEach(function(){ sinon.restore(); });

    describe('per-IP connection limit', function(){
        beforeEach(function(){
            broadcaster = new BlockBroadcaster({ WS_MAX_PER_IP: 3, WS_BACKPRESSURE_LIMIT: 50 });
            sinon.stub(console, 'log');
        });

        it('accepts connections 1, 2, 3 from same IP', function(){
            let ip = '1.1.1.1';
            for(let i = 0; i < 3; i++){
                let result = broadcaster.addSubscription(mockWs(), mockReq(ip), 'bitcoin', 'mainnet');
                assert.strictEqual(result, true);
            }
            assert.strictEqual(broadcaster.getSubscriberCount('bitcoin', 'mainnet'), 3);
        });

        it('rejects 4th connection from same IP', function(){
            let ip = '2.2.2.2';
            for(let i = 0; i < 3; i++)
                broadcaster.addSubscription(mockWs(), mockReq(ip), 'bitcoin', 'mainnet');

            let ws4 = mockWs();
            let result = broadcaster.addSubscription(ws4, mockReq(ip), 'bitcoin', 'mainnet');
            assert.strictEqual(result, false);
            assert.strictEqual(ws4.close.calledOnce, true);
            assert.strictEqual(ws4.close.firstCall.args[0], 1008);
        });

        it('accepts after one closes (3 → close 1 → add new)', function(){
            let ip = '3.3.3.3';
            let sockets = [];
            for(let i = 0; i < 3; i++){
                let ws = mockWs();
                broadcaster.addSubscription(ws, mockReq(ip), 'bitcoin', 'mainnet');
                sockets.push(ws);
            }
            broadcaster.removeSubscription(sockets[0]);
            let wsNew = mockWs();
            let result = broadcaster.addSubscription(wsNew, mockReq(ip), 'bitcoin', 'mainnet');
            assert.strictEqual(result, true);
            assert.strictEqual(broadcaster.getSubscriberCount('bitcoin', 'mainnet'), 3);
        });

        it('different IPs tracked independently', function(){
            for(let i = 0; i < 3; i++)
                broadcaster.addSubscription(mockWs(), mockReq('4.4.4.4'), 'bitcoin', 'mainnet');
            for(let i = 0; i < 3; i++)
                broadcaster.addSubscription(mockWs(), mockReq('5.5.5.5'), 'bitcoin', 'mainnet');
            assert.strictEqual(broadcaster.getSubscriberCount('bitcoin', 'mainnet'), 6);
        });
    });

    describe('per-IP limit = 1', function(){
        beforeEach(function(){
            broadcaster = new BlockBroadcaster({ WS_MAX_PER_IP: 1, WS_BACKPRESSURE_LIMIT: 50 });
            sinon.stub(console, 'log');
        });

        it('accepts first, rejects second', function(){
            let ip = '6.6.6.6';
            assert.strictEqual(broadcaster.addSubscription(mockWs(), mockReq(ip), 'b', 'm'), true);
            let ws2 = mockWs();
            assert.strictEqual(broadcaster.addSubscription(ws2, mockReq(ip), 'b', 'm'), false);
            assert.strictEqual(ws2.close.calledOnce, true);
        });
    });

    describe('backpressure (item 5410: drop only genuinely stalled peers)', function(){
        const MAX_BYTES = 1000;
        const STALL_MS  = 30000;
        beforeEach(function(){
            broadcaster = new BlockBroadcaster({ WS_MAX_PER_IP: 10, WS_BACKPRESSURE_MAX_BYTES: MAX_BYTES, WS_BACKPRESSURE_STALL_MS: STALL_MS });
            sinon.stub(console, 'log');
        });

        it('slow-but-draining peer is NEVER dropped, across many buffered sends (the 5410 regression)', function(){
            let ws = mockWs();
            ws._syncIp = 'test';
            // Buffer trends DOWN each send (peer is draining) but stays > 0: must survive.
            for(let b of [900, 800, 700, 600, 500, 400, 300, 200, 100, 50]){
                ws.bufferedAmount = b;
                broadcaster._send(ws, 'msg');
            }
            assert.strictEqual(ws.close.called, false);
            assert.strictEqual(ws._syncBackpressureSince, null); // downward progress kept resetting it
            assert.strictEqual(ws.send.callCount, 10);
        });

        it('drops a peer whose buffer exceeds the byte ceiling', function(){
            let ws = mockWs();
            ws._syncIp = 'test';
            ws.bufferedAmount = MAX_BYTES + 1;
            broadcaster._send(ws, 'msg');
            assert.strictEqual(ws.close.calledOnce, true);
            assert.strictEqual(ws.close.firstCall.args[0], 1008);
            assert.strictEqual(ws.send.called, false);
        });

        it('drops a peer whose buffer is non-draining past the stall window', function(){
            let ws = mockWs();
            ws._syncIp = 'test';
            ws.bufferedAmount = 100;                                   // below ceiling, non-empty
            ws._syncLastBuffered = 100;                               // flat: no downward progress
            ws._syncBackpressureSince = Date.now() - (STALL_MS + 1);  // window already elapsed
            broadcaster._send(ws, 'msg');
            assert.strictEqual(ws.close.calledOnce, true);
            assert.strictEqual(ws.close.firstCall.args[0], 1008);
        });

        it('does NOT drop a stalled-then-recovered peer (drain resets the window)', function(){
            let ws = mockWs();
            ws._syncIp = 'test';
            ws.bufferedAmount = 100;
            ws._syncLastBuffered = 200;                               // drained 200 -> 100: progress
            ws._syncBackpressureSince = Date.now() - (STALL_MS + 1);  // stale window, must be cleared
            broadcaster._send(ws, 'msg');
            assert.strictEqual(ws.close.called, false);
            assert.strictEqual(ws._syncBackpressureSince, null);
            assert.strictEqual(ws.send.calledOnce, true);
        });

        it('a fully-drained buffer keeps the peer healthy and clears any stall window', function(){
            let ws = mockWs();
            ws._syncIp = 'test';
            ws.bufferedAmount = 0;
            ws._syncBackpressureSince = Date.now() - (STALL_MS + 1);
            broadcaster._send(ws, 'msg');
            assert.strictEqual(ws.close.called, false);
            assert.strictEqual(ws._syncBackpressureSince, null);
            assert.strictEqual(ws.send.calledOnce, true);
        });

        it('arms but does not trip the stall window on the first non-draining send', function(){
            let ws = mockWs();
            ws._syncIp = 'test';
            ws.bufferedAmount = 100;
            broadcaster._send(ws, 'msg');
            assert.strictEqual(ws.close.called, false);
            assert.notStrictEqual(ws._syncBackpressureSince, null);   // window armed for next time
            assert.strictEqual(ws.send.calledOnce, true);
        });

        it('skips closed WebSocket', function(){
            let ws = mockWs();
            ws.readyState = WebSocket.CLOSED;
            broadcaster._send(ws, 'msg');
            assert.strictEqual(ws.send.called, false);
        });
    });
});
