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

    describe('broadcastStatus', function(){
        it('sends status to all subscribers', function(){
            broadcaster.updateStatus('bitcoin', 'mainnet', { block_height: 200 });
            let ws = mockWs();
            broadcaster.addSubscription(ws, mockReq(), 'bitcoin', 'mainnet');
            ws.send.resetHistory();
            broadcaster.broadcastStatus('bitcoin', 'mainnet');
            assert.strictEqual(ws.send.calledOnce, true);
            let sent = JSON.parse(ws.send.firstCall.args[0]);
            assert.strictEqual(sent.type, 'status');
        });

        it('does nothing when no status data', function(){
            let ws = mockWs();
            broadcaster.addSubscription(ws, mockReq(), 'bitcoin', 'mainnet');
            ws.send.resetHistory();
            broadcaster.broadcastStatus('bitcoin', 'mainnet');
            assert.strictEqual(ws.send.called, false);
        });
    });
});
