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

    describe('send', function(){
        it('skips non-OPEN WebSocket', function(){
            let ws = mockWs();
            ws.readyState = WebSocket.CLOSED;
            broadcaster.send(ws, 'test');
            assert.strictEqual(ws.send.called, false);
        });

        it('closes ws when the send buffer exceeds the byte ceiling (item 5410)', function(){
            let bp = new BlockBroadcaster({ WS_MAX_PER_IP: 3, WS_BACKPRESSURE_MAX_BYTES: 1000, WS_BACKPRESSURE_STALL_MS: 30000 });
            let ws = mockWs();
            ws._syncIp = 'test';
            ws._syncChain = 'bitcoin';
            ws._syncNetwork = 'mainnet';
            ws.bufferedAmount = 1001;
            bp.send(ws, 'test');
            assert.strictEqual(ws.close.calledOnce, true);
        });

        it('clears the backpressure stall window when the buffer drains (item 5410)', function(){
            let bp = new BlockBroadcaster({ WS_MAX_PER_IP: 3, WS_BACKPRESSURE_MAX_BYTES: 1000, WS_BACKPRESSURE_STALL_MS: 30000 });
            let ws = mockWs();
            ws.bufferedAmount = 0;
            ws._syncBackpressureSince = Date.now() - 999999; // stale window
            bp.send(ws, 'test');
            assert.strictEqual(ws._syncBackpressureSince, null);
            assert.strictEqual(ws.close.called, false);
        });
    });
});
