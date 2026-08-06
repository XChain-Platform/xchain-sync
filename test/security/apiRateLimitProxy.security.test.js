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
// : behind the co-located Apache reverse proxy, express resolved every
// request to 127.0.0.1 because 'trust proxy' was never set, so the snapshot
// rate limiters keyed one shared global bucket and a single caller could drain
// every validator's snapshot budget. These tests drive real HTTP through the
// production limiter instances (createRateLimiters) and the production key
// function (snapshotKey), not a re-declaration of them.

const assert     = require('assert');
const sinon      = require('sinon');
const express    = require('express');
const proxyquire = require('proxyquire');
const { trustProxyHops, snapshotKey, createRateLimiters } = require('../../src/api');

// A stand-in for the co-located Apache: the socket peer is loopback and the
// real client address arrives appended to X-Forwarded-For.
const PROXY_HOST = '127.0.0.1';

const CLIENT_A = '198.51.100.7';
const CLIENT_B = '198.51.100.8';
const SPOOFED  = '203.0.113.66';

// Boots a throwaway server carrying the same proxy-trust seam and the same
// snapshot limiter the service mounts. The echo route reports both the IP
// express resolved and the exact bucket key the limiter charged.
async function startHarness(options){
    let cfg = Object.assign({
        SNAPSHOT_RATE_FULL:     100,
        SNAPSHOT_RATE_INCR:     100,
        TRANSPARENCY_RATE_LIMIT: 100
    }, options.cfg || {});

    let app = express();
    app.set('trust proxy', trustProxyHops(options.trustProxy));

    let limiters = createRateLimiters(cfg);
    app.use(limiters.backstopLimiter);
    app.get('/snapshot/:dbType/:chain/:network', limiters.fullSnapshotLimiter, (req, res) => {
        res.json({ ip: req.ip, key: snapshotKey(req) });
    });

    let server = await new Promise((resolve) => {
        let s = app.listen(0, PROXY_HOST, () => resolve(s));
    });

    return {
        port:  server.address().port,
        close: () => new Promise((resolve) => server.close(resolve))
    };
}

// One request through the harness. forwardedFor is the X-Forwarded-For value as
// it would look leaving Apache (client-supplied entries first, real client last).
async function get(harness, forwardedFor, path){
    let headers = {};
    if(forwardedFor) headers['x-forwarded-for'] = forwardedFor;
    let res  = await fetch('http://' + PROXY_HOST + ':' + harness.port + (path || '/snapshot/indexer/BTC/mainnet'), { headers });
    let body = null;
    try { body = await res.json(); } catch(e){ body = null; }
    return { status: res.status, body };
}

// Boots the REAL startApi() with the network edges stubbed out, and hands back
// the express app it built. Nothing else reaches the production wiring: the
// harness above builds its own app, so without this a deleted app.set() in
// startApi would go unnoticed.
async function bootRealApi(trustProxyEnv, extraEnv){
    let env   = Object.assign({ TRUST_PROXY: trustProxyEnv }, extraEnv || {});
    let prior = {};
    for(let key of Object.keys(env)){
        prior[key] = process.env[key];
        if(env[key] === undefined) delete process.env[key];
        else process.env[key] = env[key];
    }

    let capturedApp = null;
    let fakeServer  = { on: () => {}, listen: () => {} };
    let fakeWss     = { on: () => {}, clients: new Set(), handleUpgrade: () => {} };

    class SyncServiceStub {
        start(){ return Promise.resolve(); }
        getBroadcaster(){ return null; }
        getChains(){ return []; }
        getDatabase(){ return null; }
    }

    // startApi arms three long-lived intervals; a test process must not inherit them.
    let intervals = sinon.stub(global, 'setInterval').returns(null);

    try {
        let api = proxyquire('../../src/api', {
            'http': { createServer: (app) => { capturedApp = app; return fakeServer; } },
            'ws':   { Server: function(){ return fakeWss; } },
            './SyncService': SyncServiceStub
        });
        await api.startApi();
    } finally {
        intervals.restore();
        for(let key of Object.keys(prior)){
            if(prior[key] === undefined) delete process.env[key];
            else process.env[key] = prior[key];
        }
    }

    return capturedApp;
}

// Puts a real startApi()-built app on a loopback port. Its snapshot route 404s
// (the stubbed SyncService owns no databases), which is fine: the limiter has
// already charged the bucket by then, so 404 means "served" and 429 means
// "rate limited" just as they would against a real source.
async function listenRealApi(app){
    let server = await new Promise((resolve) => {
        let s = app.listen(0, PROXY_HOST, () => resolve(s));
    });
    return {
        port:  server.address().port,
        close: () => new Promise((resolve) => server.close(resolve))
    };
}

describe('API rate-limit proxy trust security', function(){

    let harness = null;

    beforeEach(function(){
        // express-rate-limit logs a validation notice when it sees an
        // X-Forwarded-For header with 'trust proxy' off; that is the very
        // combination two of these tests exercise on purpose.
        sinon.stub(console, 'error');
        sinon.stub(console, 'warn');
    });

    afterEach(async function(){
        if(harness) await harness.close();
        harness = null;
        sinon.restore();
    });

    // ── trustProxyHops: one hop, never true ──

    describe('trustProxyHops', function(){

        it('returns exactly one hop when TRUST_PROXY is on', function(){
            assert.strictEqual(trustProxyHops(true), 1);
        });

        it('never returns true, which express-rate-limit rejects as permissive', function(){
            assert.notStrictEqual(trustProxyHops(true), true);
        });

        it('disables forwarded-header trust when TRUST_PROXY is off', function(){
            assert.strictEqual(trustProxyHops(false), false);
            assert.strictEqual(trustProxyHops(undefined), false);
            assert.strictEqual(trustProxyHops(''), false);
        });
    });

    // ── the setting reaches the app startApi actually serves ──

    describe('startApi wiring', function(){

        it('sets one trusted hop on the served app when TRUST_PROXY is on', async function(){
            let app = await bootRealApi('true');
            assert.ok(app, 'startApi did not build an express app');
            assert.strictEqual(app.get('trust proxy'), 1);
        });

        it('leaves the served app untrusting when TRUST_PROXY is unset', async function(){
            let app = await bootRealApi(undefined);
            assert.ok(app, 'startApi did not build an express app');
            assert.strictEqual(app.get('trust proxy'), false);
        });

        //  end to end, against the app the service actually serves:
        // exhausting one validator's snapshot budget must not 429 the next.
        it('does not let one client drain the snapshot budget of another', async function(){
            let app = await bootRealApi('true', { SNAPSHOT_RATE_FULL: '2' });
            harness = await listenRealApi(app);

            assert.strictEqual((await get(harness, CLIENT_A)).status, 404);
            assert.strictEqual((await get(harness, CLIENT_A)).status, 404);
            assert.strictEqual((await get(harness, CLIENT_A)).status, 429, 'client A should have exhausted its own budget');
            assert.strictEqual((await get(harness, CLIENT_B)).status, 404, 'client B was 429d by a shared global bucket');
        });

        it('shares one bucket across forged headers when TRUST_PROXY is unset', async function(){
            let app = await bootRealApi(undefined, { SNAPSHOT_RATE_FULL: '2' });
            harness = await listenRealApi(app);

            assert.strictEqual((await get(harness, CLIENT_A)).status, 404);
            assert.strictEqual((await get(harness, CLIENT_A)).status, 404);
            assert.strictEqual((await get(harness, CLIENT_B)).status, 429, 'a directly-exposed process must not believe the header');
        });
    });

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
