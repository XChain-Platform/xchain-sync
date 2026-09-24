// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// Covers backpressure recovery. One part of websocket_limits.test.js.
const assert = require('assert');
const sinon  = require('sinon');
const BlockBroadcaster = require('../../../../src/server/block_broadcaster');
const { WebSocket, mockWs, registerHooks } = require('./helpers/websocket_limits_suite');

describe('Boundary: WebSocket Limits', function(){
    let broadcaster;
    registerHooks();
    describe('backpressure (item 5410: drop only genuinely stalled peers)', function(){
        const MAX_BYTES = 1000;
        const STALL_MS  = 30000;
        beforeEach(function(){
            broadcaster = new BlockBroadcaster({ WS_MAX_PER_IP: 10, WS_BACKPRESSURE_MAX_BYTES: MAX_BYTES, WS_BACKPRESSURE_STALL_MS: STALL_MS });
            sinon.stub(console, 'log');
        });

        it('does NOT drop a stalled-then-recovered peer (drain resets the window)', function(){
            let ws = mockWs();
            ws._syncIp = 'test';
            ws.bufferedAmount = 100;
            ws._syncLastBuffered = 200;                               // drained 200 -> 100: progress
            ws._syncBackpressureSince = Date.now() - (STALL_MS + 1);  // stale window, must be cleared
            broadcaster.send(ws, 'msg');
            assert.strictEqual(ws.close.called, false);
            assert.strictEqual(ws._syncBackpressureSince, null);
            assert.strictEqual(ws.send.calledOnce, true);
        });

        it('a fully-drained buffer keeps the peer healthy and clears any stall window', function(){
            let ws = mockWs();
            ws._syncIp = 'test';
            ws.bufferedAmount = 0;
            ws._syncBackpressureSince = Date.now() - (STALL_MS + 1);
            broadcaster.send(ws, 'msg');
            assert.strictEqual(ws.close.called, false);
            assert.strictEqual(ws._syncBackpressureSince, null);
            assert.strictEqual(ws.send.calledOnce, true);
        });

        it('arms but does not trip the stall window on the first non-draining send', function(){
            let ws = mockWs();
            ws._syncIp = 'test';
            ws.bufferedAmount = 100;
            broadcaster.send(ws, 'msg');
            assert.strictEqual(ws.close.called, false);
            assert.notStrictEqual(ws._syncBackpressureSince, null);   // window armed for next time
            assert.strictEqual(ws.send.calledOnce, true);
        });

        it('skips closed WebSocket', function(){
            let ws = mockWs();
            ws.readyState = WebSocket.CLOSED;
            broadcaster.send(ws, 'msg');
            assert.strictEqual(ws.send.called, false);
        });
    });
});
