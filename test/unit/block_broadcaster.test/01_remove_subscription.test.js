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

describe('BlockBroadcaster', function(){
    registerHooks();

    describe('removeSubscription', function(){
        it('removes from subscribers and ipConnections', function(){
            let ws = mockWs();
            broadcaster.addSubscription(ws, mockReq('3.3.3.3'), 'bitcoin', 'mainnet');
            assert.strictEqual(broadcaster.getSubscriberCount('bitcoin', 'mainnet'), 1);
            broadcaster.removeSubscription(ws);
            assert.strictEqual(broadcaster.getSubscriberCount('bitcoin', 'mainnet'), 0);
        });

        it('clears metadata on ws', function(){
            let ws = mockWs();
            broadcaster.addSubscription(ws, mockReq(), 'bitcoin', 'mainnet');
            broadcaster.removeSubscription(ws);
            assert.strictEqual(ws._syncChain, null);
            assert.strictEqual(ws._syncNetwork, null);
        });

        it('handles ws with no metadata gracefully', function(){
            let ws = mockWs();
            broadcaster.removeSubscription(ws); // should not throw
        });

        it('cleans up empty sets from maps', function(){
            let ws = mockWs();
            broadcaster.addSubscription(ws, mockReq('4.4.4.4'), 'bitcoin', 'mainnet');
            broadcaster.removeSubscription(ws);
            assert.strictEqual(broadcaster.subscribers.has('bitcoin:mainnet'), false);
            assert.strictEqual(broadcaster.ipConnections.has('4.4.4.4'), false);
        });
    });
});
