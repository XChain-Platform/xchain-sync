// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.
//
// Behind a co-located reverse proxy, express resolved every
// request to 127.0.0.1 because 'trust proxy' was never set, so the snapshot
// rate limiters keyed one shared global bucket and a single caller could drain
// every validator's snapshot budget. These tests drive real HTTP through the
// production limiter instances (createRateLimiters) and the production key
// function (snapshotKey), not a re-declaration of them.

// src/api.js is the process entry point and patches console as it loads. The unit
// tier's bootstrap opts out of that; the security tier has no bootstrap, so this file
// opts out itself, or every later suite that stubs console stops seeing logger output.
process.env.XCHAIN_LOG_PATCH = '0';
const {
    assert,
    bootRealApi,
    CLIENT_A,
    CLIENT_B,
    get,
    listenRealApi,
    PROXY_HOST,
    registerHooks,
    sinon,
    SPOOFED,
    startHarness,
    trustProxyHops
} = require('./helpers/api_rate_limit_proxy_security_suite');

let harness = null;

function beforeHook() {
    // express-rate-limit logs a validation notice when it sees an
    // X-Forwarded-For header with 'trust proxy' off; that is the very
    // combination two of these tests exercise on purpose.
    sinon.stub(console, 'error');
    sinon.stub(console, 'warn');
}

async function afterHook() {
    if(harness) await harness.close();
    harness = null;
    sinon.restore();
}


// Covers explicit proxy modes. One part of api_rate_limit_proxy_security.test.js.
describe('API rate-limit proxy trust security', function(){
    registerHooks(beforeHook, afterHook);

    // ── TRUST_PROXY=false: the header is not evidence ──

    describe('TRUST_PROXY=false', function(){

        it('keys on the socket address and ignores a spoofed X-Forwarded-For', async function(){
            harness = await startHarness({ trustProxy: false });
            let res = await get(harness, SPOOFED);
            assert.strictEqual(res.status, 200);
            assert.strictEqual(res.body.ip, PROXY_HOST);
            assert.strictEqual(res.body.key, PROXY_HOST + '|indexer/BTC/mainnet');
        });

        it('does not let a spoofed header split one caller into separate buckets', async function(){
            harness = await startHarness({ trustProxy: false, cfg: { SNAPSHOT_RATE_FULL: 2 } });
            let first  = await get(harness, '203.0.113.1');
            let second = await get(harness, '203.0.113.2');
            let third  = await get(harness, '203.0.113.3');
            assert.strictEqual(first.status, 200);
            assert.strictEqual(second.status, 200);
            assert.strictEqual(third.status, 429, 'rotating X-Forwarded-For must not buy a fresh budget');
        });
    });

});

describe('API rate-limit proxy trust security', function(){
    registerHooks(beforeHook, afterHook);

    // ── TRUST_PROXY=true: one hop resolves the real client ──

    describe('TRUST_PROXY=true', function(){

        it('keys on the client address the proxy appended, not the socket', async function(){
            harness = await startHarness({ trustProxy: true });
            let res = await get(harness, CLIENT_A);
            assert.strictEqual(res.status, 200);
            assert.strictEqual(res.body.ip, CLIENT_A);
            assert.strictEqual(res.body.key, CLIENT_A + '|indexer/BTC/mainnet');
        });

        it('ignores a client-supplied entry to the LEFT of the proxy-appended address', async function(){
            harness = await startHarness({ trustProxy: true });
            let res = await get(harness, SPOOFED + ', ' + CLIENT_A);
            assert.strictEqual(res.status, 200);
            assert.strictEqual(res.body.ip, CLIENT_A, 'one trusted hop must stop at the address Apache observed');
            assert.ok(!res.body.key.includes(SPOOFED), 'spoofed entry leaked into the rate-limit key');
        });

        it('ignores a long spoofed prefix, however many entries the client prepends', async function(){
            harness = await startHarness({ trustProxy: true });
            let res = await get(harness, '203.0.113.1, 203.0.113.2, 203.0.113.3, ' + CLIENT_A);
            assert.strictEqual(res.status, 200);
            assert.strictEqual(res.body.ip, CLIENT_A);
        });

    });
});

describe('API rate-limit proxy trust security', function(){
    registerHooks(beforeHook, afterHook);

    describe('TRUST_PROXY=true', function(){

        // The actual defect in : without the seam both clients resolve to
        // 127.0.0.1, share one bucket, and the first caller 429s the second.
        it('gives two different client addresses independent snapshot buckets', async function(){
            harness = await startHarness({ trustProxy: true, cfg: { SNAPSHOT_RATE_FULL: 2 } });

            assert.strictEqual((await get(harness, CLIENT_A)).status, 200);
            assert.strictEqual((await get(harness, CLIENT_A)).status, 200);
            assert.strictEqual((await get(harness, CLIENT_A)).status, 429, 'client A should have exhausted its own budget');

            let other = await get(harness, CLIENT_B);
            assert.strictEqual(other.status, 200, 'client B was 429d by client A exhausting a shared global bucket');
            assert.strictEqual(other.body.key, CLIENT_B + '|indexer/BTC/mainnet');
        });

        it('keeps one client on separate buckets per chain, as the key intends', async function(){
            harness = await startHarness({ trustProxy: true, cfg: { SNAPSHOT_RATE_FULL: 1 } });
            assert.strictEqual((await get(harness, CLIENT_A, '/snapshot/indexer/BTC/mainnet')).status, 200);
            assert.strictEqual((await get(harness, CLIENT_A, '/snapshot/indexer/BTC/mainnet')).status, 429);
            assert.strictEqual((await get(harness, CLIENT_A, '/snapshot/indexer/LTC/mainnet')).status, 200);
            assert.strictEqual((await get(harness, CLIENT_A, '/snapshot/decoder/BTC/mainnet')).status, 200);
        });

        // Trusting the forwarded header is what makes IPv6 rotation reachable:
        // a /64 holder could otherwise mint a fresh bucket per request.
        it('collapses an IPv6 client to its network so rotation buys no new budget', async function(){
            harness = await startHarness({ trustProxy: true, cfg: { SNAPSHOT_RATE_FULL: 2 } });
            assert.strictEqual((await get(harness, '2001:db8:0:1::1')).status, 200);
            assert.strictEqual((await get(harness, '2001:db8:0:1::2')).status, 200);
            let third = await get(harness, '2001:db8:0:1::3');
            assert.strictEqual(third.status, 429, 'addresses in one IPv6 allocation must share a bucket');
        });
    });
});

