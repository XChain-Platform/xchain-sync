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
const BlockBroadcaster = require('../../../src/server/block_broadcaster');
const { mockWs, mockReq, registerHooks } = require('./websocket_limits.test/helpers/websocket_limits_suite');

describe('Boundary: WebSocket Limits', function(){
    let broadcaster;
    registerHooks();

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
});
