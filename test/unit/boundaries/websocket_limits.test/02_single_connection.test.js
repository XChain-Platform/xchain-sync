// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// Covers a single allowed connection. One part of websocket_limits.test.js.
const assert = require('assert');
const sinon  = require('sinon');
const BlockBroadcaster = require('../../../../src/server/block_broadcaster');
const { mockWs, mockReq, registerHooks } = require('./helpers/websocket_limits_suite');

describe('Boundary: WebSocket Limits', function(){
    let broadcaster;
    registerHooks();
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
});
