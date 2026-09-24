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
} = require('./block_broadcaster_security.test/helpers/block_broadcaster_security_suite');

function beforeHook() {
    sinon.stub(console, 'log');
}

function afterHook() {
    sinon.restore();
}

describe('BlockBroadcaster security', function(){
    registerHooks(beforeHook, afterHook);

    // ── getIp: TRUST_PROXY=false (default) ──

    describe('getIp: TRUST_PROXY=false', function(){

        it('ignores x-forwarded-for when TRUST_PROXY is false', function(){
            let broadcaster = new BlockBroadcaster({ TRUST_PROXY: false, WS_MAX_PER_IP: 3, WS_BACKPRESSURE_LIMIT: 50 });
            let req = createMockReq('192.168.1.1', '10.0.0.1');
            let ip = broadcaster.getIp(req);
            assert.strictEqual(ip, '192.168.1.1');
        });

        it('uses socket remoteAddress when no forwarded header', function(){
            let broadcaster = new BlockBroadcaster({ TRUST_PROXY: false, WS_MAX_PER_IP: 3, WS_BACKPRESSURE_LIMIT: 50 });
            let req = createMockReq('172.16.0.1');
            let ip = broadcaster.getIp(req);
            assert.strictEqual(ip, '172.16.0.1');
        });

        it('returns unknown when no socket address and no forwarded header', function(){
            let broadcaster = new BlockBroadcaster({ TRUST_PROXY: false, WS_MAX_PER_IP: 3, WS_BACKPRESSURE_LIMIT: 50 });
            let req = { headers: {}, socket: { remoteAddress: undefined } };
            let ip = broadcaster.getIp(req);
            assert.strictEqual(ip, 'unknown');
        });
    });

});

describe('BlockBroadcaster security', function(){
    registerHooks(beforeHook, afterHook);

    // ── getIp: TRUST_PROXY=true ──

    // ── getIp: TRUST_PROXY=true ──
    //
    // TRUST_PROXY means one trusted hop, the co-located Apache, which APPENDS the peer
    // it saw to the right of X-Forwarded-For. The rightmost entry is therefore the only
    // address our own infrastructure vouched for; anything left of it is client-supplied.

    describe('getIp: TRUST_PROXY=true', function(){

        it('uses x-forwarded-for when TRUST_PROXY is true', function(){
            let broadcaster = new BlockBroadcaster({ TRUST_PROXY: true, WS_MAX_PER_IP: 3, WS_BACKPRESSURE_LIMIT: 50 });
            let req = createMockReq('192.168.1.1', '10.0.0.1');
            let ip = broadcaster.getIp(req);
            assert.strictEqual(ip, '10.0.0.1');
        });

        it('keys on the address the trusted proxy appended, not the client-supplied leading entry', function(){
            let broadcaster = new BlockBroadcaster({ TRUST_PROXY: true, WS_MAX_PER_IP: 3, WS_BACKPRESSURE_LIMIT: 50 });
            // Client sent "10.0.0.1, 172.16.0.1"; Apache appended the real peer 203.0.113.7.
            let req = createMockReq('192.168.1.1', '10.0.0.1, 172.16.0.1, 203.0.113.7');
            let ip = broadcaster.getIp(req);
            assert.strictEqual(ip, '203.0.113.7');
        });

        it('a forged leading entry does not become the rate-limit key', function(){
            let broadcaster = new BlockBroadcaster({ TRUST_PROXY: true, WS_MAX_PER_IP: 3, WS_BACKPRESSURE_LIMIT: 50 });
            // Attacker claims to be a peer validator; the appended address is what counts.
            let req = createMockReq('127.0.0.1', '198.51.100.9, 203.0.113.7');
            let ip = broadcaster.getIp(req);
            assert.notStrictEqual(ip, '198.51.100.9');
            assert.strictEqual(ip, '203.0.113.7');
        });

        it('a long forged prefix still keys on the appended rightmost address', function(){
            let broadcaster = new BlockBroadcaster({ TRUST_PROXY: true, WS_MAX_PER_IP: 3, WS_BACKPRESSURE_LIMIT: 50 });
            let forged = [];
            for(let i = 0; i < 64; i++) forged.push('10.0.0.' + i);
            let req = createMockReq('127.0.0.1', forged.join(', ') + ', 203.0.113.7');
            let ip = broadcaster.getIp(req);
            assert.strictEqual(ip, '203.0.113.7');
        });

    });
});

describe('BlockBroadcaster security', function(){
    registerHooks(beforeHook, afterHook);

    describe('getIp: TRUST_PROXY=true', function(){

        it('trims whitespace around the appended address', function(){
            let broadcaster = new BlockBroadcaster({ TRUST_PROXY: true, WS_MAX_PER_IP: 3, WS_BACKPRESSURE_LIMIT: 50 });
            let req = createMockReq('192.168.1.1', '10.0.0.1  ,   172.16.0.1  ');
            let ip = broadcaster.getIp(req);
            assert.strictEqual(ip, '172.16.0.1');
        });

        it('falls back to socket when x-forwarded-for absent and TRUST_PROXY true', function(){
            let broadcaster = new BlockBroadcaster({ TRUST_PROXY: true, WS_MAX_PER_IP: 3, WS_BACKPRESSURE_LIMIT: 50 });
            let req = createMockReq('192.168.1.1');
            let ip = broadcaster.getIp(req);
            assert.strictEqual(ip, '192.168.1.1');
        });

        it('falls back to socket on a trailing-comma header rather than keying on an empty string', function(){
            let broadcaster = new BlockBroadcaster({ TRUST_PROXY: true, WS_MAX_PER_IP: 3, WS_BACKPRESSURE_LIMIT: 50 });
            let req = createMockReq('192.168.1.1', '10.0.0.1,');
            let ip = broadcaster.getIp(req);
            assert.strictEqual(ip, '192.168.1.1');
        });

        it('falls back to socket on a whitespace-only header rather than keying on an empty string', function(){
            let broadcaster = new BlockBroadcaster({ TRUST_PROXY: true, WS_MAX_PER_IP: 3, WS_BACKPRESSURE_LIMIT: 50 });
            let req = createMockReq('192.168.1.1', '   ');
            let ip = broadcaster.getIp(req);
            assert.strictEqual(ip, '192.168.1.1');
        });
    });

});
