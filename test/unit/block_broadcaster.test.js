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
const BlockBroadcaster = require('../../src/server/block_broadcaster');

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

    describe('addSubscription', function(){
        it('adds ws to subscribers set', function(){
            let ws = mockWs();
            let req = mockReq('1.1.1.1');
            let result = broadcaster.addSubscription(ws, req, 'bitcoin', 'mainnet');
            assert.strictEqual(result, true);
            assert.strictEqual(broadcaster.getSubscriberCount('bitcoin', 'mainnet'), 1);
        });

        it('sets metadata on ws', function(){
            let ws = mockWs();
            broadcaster.addSubscription(ws, mockReq('1.1.1.1'), 'bitcoin', 'mainnet');
            assert.strictEqual(ws._syncChain, 'bitcoin');
            assert.strictEqual(ws._syncNetwork, 'mainnet');
            assert.strictEqual(ws._syncIp, '1.1.1.1');
        });

        it('rejects when per-IP limit exceeded', function(){
            let req = mockReq('2.2.2.2');
            for(let i = 0; i < 3; i++){
                broadcaster.addSubscription(mockWs(), req, 'bitcoin', 'mainnet');
            }
            let ws4 = mockWs();
            let result = broadcaster.addSubscription(ws4, req, 'bitcoin', 'mainnet');
            assert.strictEqual(result, false);
            assert.strictEqual(ws4.close.calledOnce, true);
        });

        it('sends initial status if available', function(){
            broadcaster.updateStatus('bitcoin', 'mainnet', { block_height: 100 });
            let ws = mockWs();
            broadcaster.addSubscription(ws, mockReq(), 'bitcoin', 'mainnet');
            assert.strictEqual(ws.send.calledOnce, true);
            let sent = JSON.parse(ws.send.firstCall.args[0]);
            assert.strictEqual(sent.type, 'status');
            assert.strictEqual(sent.block_height, 100);
        });
    });
});

describe('BlockBroadcaster', function(){
    registerHooks();

    describe('addSubscription', function(){
        it('does not send status if none available', function(){
            let ws = mockWs();
            broadcaster.addSubscription(ws, mockReq(), 'bitcoin', 'mainnet');
            assert.strictEqual(ws.send.called, false);
        });

        it('registers close and error handlers', function(){
            let ws = mockWs();
            broadcaster.addSubscription(ws, mockReq(), 'bitcoin', 'mainnet');
            assert.strictEqual(ws.on.calledWith('close'), true);
            assert.strictEqual(ws.on.calledWith('error'), true);
        });

        it('uses x-forwarded-for when TRUST_PROXY is true', function(){
            let trustedBroadcaster = new BlockBroadcaster({ WS_MAX_PER_IP: 3, WS_BACKPRESSURE_LIMIT: 50, TRUST_PROXY: true });
            let ws = mockWs();
            let req = { headers: { 'x-forwarded-for': '9.9.9.9' }, socket: { remoteAddress: '1.1.1.1' } };
            trustedBroadcaster.addSubscription(ws, req, 'bitcoin', 'mainnet');
            assert.strictEqual(ws._syncIp, '9.9.9.9');
        });
    });
});
