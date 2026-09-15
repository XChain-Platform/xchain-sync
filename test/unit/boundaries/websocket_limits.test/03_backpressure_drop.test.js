// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// Covers backpressure drop boundaries. One part of websocket_limits.test.js.
const assert = require('assert');
const sinon  = require('sinon');
const BlockBroadcaster = require('../../../../src/server/block_broadcaster');
const { mockWs, registerHooks } = require('./helpers/websocket_limits_suite');

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

        it('slow-but-draining peer is NEVER dropped, across many buffered sends (the 5410 regression)', function(){
            let ws = mockWs();
            ws._syncIp = 'test';
            // Buffer trends DOWN each send (peer is draining) but stays > 0: must survive.
            for(let b of [900, 800, 700, 600, 500, 400, 300, 200, 100, 50]){
                ws.bufferedAmount = b;
                broadcaster.send(ws, 'msg');
            }
            assert.strictEqual(ws.close.called, false);
            assert.strictEqual(ws._syncBackpressureSince, null); // downward progress kept resetting it
            assert.strictEqual(ws.send.callCount, 10);
        });

        it('drops a peer whose buffer exceeds the byte ceiling', function(){
            let ws = mockWs();
            ws._syncIp = 'test';
            ws.bufferedAmount = MAX_BYTES + 1;
            broadcaster.send(ws, 'msg');
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
            broadcaster.send(ws, 'msg');
            assert.strictEqual(ws.close.calledOnce, true);
            assert.strictEqual(ws.close.firstCall.args[0], 1008);
        });
    });
});
