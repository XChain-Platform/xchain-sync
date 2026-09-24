// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

const {
    assert,
    BlockBroadcaster,
    createMockReq,
    createMockWs,
    registerHooks,
    sinon
} = require('./helpers/block_broadcaster_security_suite');

function beforeHook() {
    sinon.stub(console, 'log');
}

function afterHook() {
    sinon.restore();
}


// Covers per-IP subscription limits. One part of block_broadcaster_security.test.js.
describe('BlockBroadcaster security', function(){
    registerHooks(beforeHook, afterHook);

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

    });
});

describe('BlockBroadcaster security', function(){
    registerHooks(beforeHook, afterHook);

    describe('per-IP limit with TRUST_PROXY=true', function(){

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

});

describe('BlockBroadcaster security', function(){
    registerHooks(beforeHook, afterHook);

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

