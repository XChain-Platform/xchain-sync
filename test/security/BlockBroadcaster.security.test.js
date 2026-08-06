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

    // ── _getIp: TRUST_PROXY=false (default) ──

    describe('_getIp: TRUST_PROXY=false', function(){

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

    // ── _getIp: TRUST_PROXY=true ──

    // ── _getIp: TRUST_PROXY=true ──
    //
    // TRUST_PROXY means one trusted hop, the co-located Apache, which APPENDS the peer
    // it saw to the right of X-Forwarded-For. The rightmost entry is therefore the only
    // address our own infrastructure vouched for; anything left of it is client-supplied.

    describe('_getIp: TRUST_PROXY=true', function(){

        it('uses x-forwarded-for when TRUST_PROXY is true', function(){
            let broadcaster = new BlockBroadcaster({ TRUST_PROXY: true, WS_MAX_PER_IP: 3, WS_BACKPRESSURE_LIMIT: 50 });
            let req = createMockReq('192.168.1.1', '10.0.0.1');
            let ip = broadcaster._getIp(req);
            assert.strictEqual(ip, '10.0.0.1');
        });

        it('keys on the address the trusted proxy appended, not the client-supplied leading entry', function(){
            let broadcaster = new BlockBroadcaster({ TRUST_PROXY: true, WS_MAX_PER_IP: 3, WS_BACKPRESSURE_LIMIT: 50 });
            // Client sent "10.0.0.1, 172.16.0.1"; Apache appended the real peer 203.0.113.7.
            let req = createMockReq('192.168.1.1', '10.0.0.1, 172.16.0.1, 203.0.113.7');
            let ip = broadcaster._getIp(req);
            assert.strictEqual(ip, '203.0.113.7');
        });

        it('a forged leading entry does not become the rate-limit key', function(){
            let broadcaster = new BlockBroadcaster({ TRUST_PROXY: true, WS_MAX_PER_IP: 3, WS_BACKPRESSURE_LIMIT: 50 });
            // Attacker claims to be a peer validator; the appended address is what counts.
            let req = createMockReq('127.0.0.1', '198.51.100.9, 203.0.113.7');
            let ip = broadcaster._getIp(req);
            assert.notStrictEqual(ip, '198.51.100.9');
            assert.strictEqual(ip, '203.0.113.7');
        });

        it('a long forged prefix still keys on the appended rightmost address', function(){
            let broadcaster = new BlockBroadcaster({ TRUST_PROXY: true, WS_MAX_PER_IP: 3, WS_BACKPRESSURE_LIMIT: 50 });
            let forged = [];
            for(let i = 0; i < 64; i++) forged.push('10.0.0.' + i);
            let req = createMockReq('127.0.0.1', forged.join(', ') + ', 203.0.113.7');
            let ip = broadcaster._getIp(req);
            assert.strictEqual(ip, '203.0.113.7');
        });

        it('trims whitespace around the appended address', function(){
            let broadcaster = new BlockBroadcaster({ TRUST_PROXY: true, WS_MAX_PER_IP: 3, WS_BACKPRESSURE_LIMIT: 50 });
            let req = createMockReq('192.168.1.1', '10.0.0.1  ,   172.16.0.1  ');
            let ip = broadcaster._getIp(req);
            assert.strictEqual(ip, '172.16.0.1');
        });

        it('falls back to socket when x-forwarded-for absent and TRUST_PROXY true', function(){
            let broadcaster = new BlockBroadcaster({ TRUST_PROXY: true, WS_MAX_PER_IP: 3, WS_BACKPRESSURE_LIMIT: 50 });
            let req = createMockReq('192.168.1.1');
            let ip = broadcaster._getIp(req);
            assert.strictEqual(ip, '192.168.1.1');
        });

        it('falls back to socket on a trailing-comma header rather than keying on an empty string', function(){
            let broadcaster = new BlockBroadcaster({ TRUST_PROXY: true, WS_MAX_PER_IP: 3, WS_BACKPRESSURE_LIMIT: 50 });
            let req = createMockReq('192.168.1.1', '10.0.0.1,');
            let ip = broadcaster._getIp(req);
            assert.strictEqual(ip, '192.168.1.1');
        });

        it('falls back to socket on a whitespace-only header rather than keying on an empty string', function(){
            let broadcaster = new BlockBroadcaster({ TRUST_PROXY: true, WS_MAX_PER_IP: 3, WS_BACKPRESSURE_LIMIT: 50 });
            let req = createMockReq('192.168.1.1', '   ');
            let ip = broadcaster._getIp(req);
            assert.strictEqual(ip, '192.168.1.1');
        });
    });

    // ── Per-IP limit through the trusted proxy ──

    describe('per-IP limit with TRUST_PROXY=true', function(){

        it('rotating the forged leading entry does not buy extra connections', function(){
            let config = { TRUST_PROXY: true, WS_MAX_PER_IP: 2, WS_BACKPRESSURE_LIMIT: 50 };
            let broadcaster = new BlockBroadcaster(config);

            // All three arrive from the same real client 203.0.113.7 (appended by Apache);
            // only the forgeable prefix changes between them.
            let results = [];
            for(let i = 1; i <= 3; i++){
                let ws  = createMockWs();
                let req = createMockReq('127.0.0.1', '10.0.0.' + i + ', 203.0.113.7');
                results.push({ ws, added: broadcaster.addSubscription(ws, req, 'bitcoin', 'mainnet') });
            }

            assert.strictEqual(results[0].added, true);
            assert.strictEqual(results[1].added, true);
            assert.strictEqual(results[2].added, false);
            assert.strictEqual(results[2].ws.close.calledOnce, true);
        });

        it('two real clients behind the proxy get independent buckets', function(){
            let config = { TRUST_PROXY: true, WS_MAX_PER_IP: 2, WS_BACKPRESSURE_LIMIT: 50 };
            let broadcaster = new BlockBroadcaster(config);

            // Client A exhausts its cap.
            for(let i = 0; i < 2; i++){
                let ws = createMockWs();
                let added = broadcaster.addSubscription(ws, createMockReq('127.0.0.1', '203.0.113.7'), 'bitcoin', 'mainnet');
                assert.strictEqual(added, true);
            }
            let wsAOver = createMockWs();
            assert.strictEqual(
                broadcaster.addSubscription(wsAOver, createMockReq('127.0.0.1', '203.0.113.7'), 'bitcoin', 'mainnet'),
                false
            );

            // Client B is unaffected: a different appended address is a different bucket.
            let wsB = createMockWs();
            assert.strictEqual(
                broadcaster.addSubscription(wsB, createMockReq('127.0.0.1', '198.51.100.4'), 'bitcoin', 'mainnet'),
                true
            );
            assert.strictEqual(wsB._syncIp, '198.51.100.4');
            assert.strictEqual(wsB.close.called, false);
        });

        it('a client cannot exhaust another client bucket by claiming its address in the prefix', function(){
            let config = { TRUST_PROXY: true, WS_MAX_PER_IP: 2, WS_BACKPRESSURE_LIMIT: 50 };
            let broadcaster = new BlockBroadcaster(config);

            // Attacker names the victim in the forgeable prefix, twice.
            for(let i = 0; i < 2; i++){
                let ws = createMockWs();
                let added = broadcaster.addSubscription(
                    ws, createMockReq('127.0.0.1', '198.51.100.4, 203.0.113.7'), 'bitcoin', 'mainnet');
                assert.strictEqual(added, true);
            }

            // The victim still connects: the attacker's traffic was booked to 203.0.113.7.
            let victim = createMockWs();
            assert.strictEqual(
                broadcaster.addSubscription(victim, createMockReq('127.0.0.1', '198.51.100.4'), 'bitcoin', 'mainnet'),
                true
            );
            assert.strictEqual(victim.close.called, false);
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
            // Third connection should be rejected: all are from 1.1.1.1 regardless of spoofed header
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
