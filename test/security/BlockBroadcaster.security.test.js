// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available —
// contact legal@dankest.llc.

const assert = require('assert');
const sinon  = require('sinon');
const BlockBroadcaster = require('../../src/BlockBroadcaster');

function createMockWs(ip){
    return {
        readyState: 1, // WebSocket.OPEN
        close: sinon.stub(),
        send: sinon.stub(),
        on: sinon.stub(),
        _syncChain: null,
        _syncNetwork: null,
        _syncIp: null,
        _syncBuffered: 0,
        bufferedAmount: 0
    };
}

function createMockReq(socketIp, forwardedFor){
    let req = {
        headers: {},
        socket: { remoteAddress: socketIp }
    };
    if(forwardedFor)
        req.headers['x-forwarded-for'] = forwardedFor;
    return req;
}

describe('BlockBroadcaster security', function(){

    beforeEach(function(){
        sinon.stub(console, 'log');
    });

    afterEach(function(){
        sinon.restore();
    });

    // ── _getIp — TRUST_PROXY=false (default) ──

    describe('_getIp — TRUST_PROXY=false', function(){

        it('ignores x-forwarded-for when TRUST_PROXY is false', function(){
            let broadcaster = new BlockBroadcaster({ TRUST_PROXY: false, WS_MAX_PER_IP: 3, WS_BACKPRESSURE_LIMIT: 50 });
            let req = createMockReq('192.168.1.1', '10.0.0.1');
            let ip = broadcaster._getIp(req);
            assert.strictEqual(ip, '192.168.1.1');
        });

        it('uses socket remoteAddress when no forwarded header', function(){
            let broadcaster = new BlockBroadcaster({ TRUST_PROXY: false, WS_MAX_PER_IP: 3, WS_BACKPRESSURE_LIMIT: 50 });
            let req = createMockReq('172.16.0.1');
            let ip = broadcaster._getIp(req);
            assert.strictEqual(ip, '172.16.0.1');
        });

        it('returns unknown when no socket address and no forwarded header', function(){
            let broadcaster = new BlockBroadcaster({ TRUST_PROXY: false, WS_MAX_PER_IP: 3, WS_BACKPRESSURE_LIMIT: 50 });
            let req = { headers: {}, socket: { remoteAddress: undefined } };
            let ip = broadcaster._getIp(req);
            assert.strictEqual(ip, 'unknown');
        });
    });

    // ── _getIp — TRUST_PROXY=true ──

    describe('_getIp — TRUST_PROXY=true', function(){

        it('uses x-forwarded-for when TRUST_PROXY is true', function(){
            let broadcaster = new BlockBroadcaster({ TRUST_PROXY: true, WS_MAX_PER_IP: 3, WS_BACKPRESSURE_LIMIT: 50 });
            let req = createMockReq('192.168.1.1', '10.0.0.1');
            let ip = broadcaster._getIp(req);
            assert.strictEqual(ip, '10.0.0.1');
        });

        it('takes only first IP from chained x-forwarded-for', function(){
            let broadcaster = new BlockBroadcaster({ TRUST_PROXY: true, WS_MAX_PER_IP: 3, WS_BACKPRESSURE_LIMIT: 50 });
            let req = createMockReq('192.168.1.1', '10.0.0.1, 172.16.0.1, 192.168.1.1');
            let ip = broadcaster._getIp(req);
            assert.strictEqual(ip, '10.0.0.1');
        });

        it('trims whitespace from forwarded IP', function(){
            let broadcaster = new BlockBroadcaster({ TRUST_PROXY: true, WS_MAX_PER_IP: 3, WS_BACKPRESSURE_LIMIT: 50 });
            let req = createMockReq('192.168.1.1', '  10.0.0.1  , 172.16.0.1');
            let ip = broadcaster._getIp(req);
            assert.strictEqual(ip, '10.0.0.1');
        });

        it('falls back to socket when x-forwarded-for absent and TRUST_PROXY true', function(){
            let broadcaster = new BlockBroadcaster({ TRUST_PROXY: true, WS_MAX_PER_IP: 3, WS_BACKPRESSURE_LIMIT: 50 });
            let req = createMockReq('192.168.1.1');
            let ip = broadcaster._getIp(req);
            assert.strictEqual(ip, '192.168.1.1');
        });
    });

    // ── Per-IP limit not bypassed by header spoofing ──

    describe('per-IP limit with TRUST_PROXY=false', function(){

        it('spoofed x-forwarded-for cannot bypass per-IP limit', function(){
            let config = { TRUST_PROXY: false, WS_MAX_PER_IP: 2, WS_BACKPRESSURE_LIMIT: 50 };
            let broadcaster = new BlockBroadcaster(config);

            // Attacker sends 3 connections from same socket IP, each with different x-forwarded-for
            let ws1 = createMockWs();
            let req1 = createMockReq('1.1.1.1', '10.0.0.1');
            let added1 = broadcaster.addSubscription(ws1, req1, 'bitcoin', 'mainnet');
            assert.strictEqual(added1, true);

            let ws2 = createMockWs();
            let req2 = createMockReq('1.1.1.1', '10.0.0.2');
            let added2 = broadcaster.addSubscription(ws2, req2, 'bitcoin', 'mainnet');
            assert.strictEqual(added2, true);

            let ws3 = createMockWs();
            let req3 = createMockReq('1.1.1.1', '10.0.0.3');
            let added3 = broadcaster.addSubscription(ws3, req3, 'bitcoin', 'mainnet');
            // Third connection should be rejected — all are from 1.1.1.1 regardless of spoofed header
            assert.strictEqual(added3, false);
            assert.strictEqual(ws3.close.calledOnce, true);
        });

        it('different socket IPs are not affected by per-IP limit', function(){
            let config = { TRUST_PROXY: false, WS_MAX_PER_IP: 1, WS_BACKPRESSURE_LIMIT: 50 };
            let broadcaster = new BlockBroadcaster(config);

            let ws1 = createMockWs();
            let req1 = createMockReq('1.1.1.1');
            let added1 = broadcaster.addSubscription(ws1, req1, 'bitcoin', 'mainnet');
            assert.strictEqual(added1, true);

            let ws2 = createMockWs();
            let req2 = createMockReq('2.2.2.2');
            let added2 = broadcaster.addSubscription(ws2, req2, 'bitcoin', 'mainnet');
            assert.strictEqual(added2, true);
        });
    });
});
