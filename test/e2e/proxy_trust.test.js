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
// E2E: client attribution behind a reverse proxy.
//
// Behind a co-located proxy every request arrives from loopback, so the only
// evidence of who the caller is sits in X-Forwarded-For. TRUST_PROXY decides
// whether that evidence is believed, and trustProxyHops bounds it to ONE hop:
// the proxy appends the peer it saw to the right of the header, so exactly the
// rightmost entry is vouched for and everything left of it is client-supplied.
// These tests drive real HTTP and real WebSocket upgrades through the e2e
// server helper, which builds its trust-proxy setting and its limiter
// instances from src/api.js, so a break in either seam fails here.
//
// Both budget surfaces are covered because they read the forwarded chain by
// different code: the HTTP snapshot limiters through express req.ip, and the
// WS_MAX_PER_IP cap through BlockBroadcaster's own header parsing.

const assert        = require('assert');
const sinon         = require('sinon');
const WebSocket     = require('ws');
const setup         = require('./helpers/setup');
const fixtures      = require('./helpers/fixtures');
const ServerProcess = require('./helpers/serverProcess');
const { drainUntil } = require('./helpers/waitFor');

const SERVER_PORT = 29950;

// Two genuine clients and an address a caller can only have typed itself.
const CLIENT_A = '198.51.100.7';
const CLIENT_B = '198.51.100.8';
const SPOOFED  = '203.0.113.66';

describe('E2E: Proxy-trust client attribution', function() {

    let sourceDb, server;
    let sockets = [];

    before(async function() {
        this.timeout(60000);
        await setup.globalSetup();
        sourceDb = setup.getSourceDb();
        // A few blocks so /snapshot has something to serve: the limiter charges
        // the bucket either way, but an empty source answers 404 and that would
        // read as "served" only by accident. No test here mutates the source, so
        // one seeding covers the file.
        await fixtures.seedBlocks(sourceDb, 1, 5);

        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });

    after(async function() {
        sinon.restore();
        if (server) { await server.stop(); server = null; }
        await setup.globalTeardown();
    });

    afterEach(async function() {
        for (let ws of sockets) { try { ws.close(); } catch (e) {} }
        sockets = [];
        if (server) { await server.stop(); server = null; }
    });

    // A server whose limiter counters start empty, so each test owns its budgets.
    async function startServer(overrides) {
        server = new ServerProcess(sourceDb, SERVER_PORT);
        Object.assign(server.config, overrides);
        await server.start();
        return server;
    }

    // One /snapshot request. `forwardedFor` is the header as it leaves the
    // proxy: client-supplied entries first, the address the proxy observed last.
    // The response body is drained because a full snapshot is a gzip stream and
    // an undrained socket would linger past the test.
    async function snapshotHit(forwardedFor) {
        let headers = {};
        if (forwardedFor) headers['x-forwarded-for'] = forwardedFor;
        let res = await fetch(server.getUrl() + '/snapshot/indexer/bitcoin/mainnet', { headers });
        await res.arrayBuffer();
        return {
            status:    res.status,
            remaining: Number(res.headers.get('ratelimit-remaining'))
        };
    }

    // Open a subscription and report whether the per-IP cap let it live. The cap
    // is applied after the upgrade completes, so a refused subscriber sees the
    // socket open and then a 1008 close frame; the settle window is what tells
    // an accepted socket apart from one about to be closed.
    async function subscribe(forwardedFor) {
        let ws = new WebSocket(server.getWsUrl() + '/subscribe/indexer/bitcoin/mainnet', {
            headers: { 'x-forwarded-for': forwardedFor }
        });
        sockets.push(ws);

        let closeCode = null;
        ws.on('close', (code) => { closeCode = code; });

        await new Promise((resolve, reject) => {
            ws.once('open',  resolve);
            ws.once('close', resolve);
            ws.once('error', reject);
        });

        // Report-don't-throw is the point here: "no close arrived inside the
        // window" is the answer being recorded, not a failure, so this drains
        // rather than asserting. Going through the shared helper keeps the poll
        // interval and the bound in one place instead of hand-rolled per site.
        await drainUntil(() => closeCode !== null, 750, 25);

        return { accepted: closeCode === null, closeCode };
    }

    describe('HTTP snapshot budgets with TRUST_PROXY on', function() {

        it('charges the forwarded client rather than the proxy socket address', async function() {
            this.timeout(30000);
            await startServer({ TRUST_PROXY: true, SNAPSHOT_RATE_FULL: 2 });

            let first  = await snapshotHit(CLIENT_A);
            let second = await snapshotHit(CLIENT_B);

            assert.strictEqual(first.status, 200);
            assert.strictEqual(first.remaining, 1, 'the first client should have spent one of its own two requests');
            assert.strictEqual(second.status, 200);
            assert.strictEqual(second.remaining, 1,
                'a second client started on a budget the first had already spent, so both were keyed on the proxy address');
        });

        it('does not let one client exhaust another client budget', async function() {
            this.timeout(30000);
            await startServer({ TRUST_PROXY: true, SNAPSHOT_RATE_FULL: 2 });

            assert.strictEqual((await snapshotHit(CLIENT_A)).status, 200);
            assert.strictEqual((await snapshotHit(CLIENT_A)).status, 200);
            assert.strictEqual((await snapshotHit(CLIENT_A)).status, 429, 'the client should have exhausted its own budget');
            assert.strictEqual((await snapshotHit(CLIENT_B)).status, 200,
                'a distinct client was refused, so one shared bucket is serving every caller');
        });

        it('ignores entries beyond the one trusted hop, so a forged prefix buys no budget', async function() {
            this.timeout(30000);
            await startServer({ TRUST_PROXY: true, SNAPSHOT_RATE_FULL: 2 });

            let plain = await snapshotHit(CLIENT_A);
            assert.strictEqual(plain.status, 200);
            assert.strictEqual(plain.remaining, 1);

            // Same trusted rightmost entry, so this must land in the client's own
            // bucket and count down, not open a bucket named after the forged entry.
            let prefixed = await snapshotHit(SPOOFED + ', ' + CLIENT_A);
            assert.strictEqual(prefixed.status, 200);
            assert.strictEqual(prefixed.remaining, 0,
                'a forged leading entry was believed and minted a fresh bucket');

            // However many entries the caller prepends, the trusted hop count is
            // still one, so the budget stays spent.
            let deepPrefix = await snapshotHit('203.0.113.1, 203.0.113.2, 203.0.113.3, ' + CLIENT_A);
            assert.strictEqual(deepPrefix.status, 429,
                'a longer forged prefix escaped the hop bound and bought a new budget');
        });
    });

    describe('HTTP snapshot budgets with TRUST_PROXY off', function() {

        it('keys every forwarded caller on the socket address', async function() {
            this.timeout(30000);
            await startServer({ TRUST_PROXY: false, SNAPSHOT_RATE_FULL: 2 });

            assert.strictEqual((await snapshotHit(CLIENT_A)).status, 200);
            assert.strictEqual((await snapshotHit(CLIENT_B)).status, 200);
            assert.strictEqual((await snapshotHit(SPOOFED)).status, 429,
                'an untrusting server read the forwarded header and split one caller into separate buckets');
        });
    });

    describe('WebSocket per-IP cap', function() {

        it('gives each forwarded client its own connection allowance when TRUST_PROXY is on', async function() {
            this.timeout(30000);
            await startServer({ TRUST_PROXY: true, WS_MAX_PER_IP: 1 });

            let first = await subscribe(CLIENT_A);
            assert.strictEqual(first.accepted, true, 'the first subscription should be within the cap');

            let second = await subscribe(CLIENT_A);
            assert.strictEqual(second.accepted, false, 'the same client should be capped at one connection');
            assert.strictEqual(second.closeCode, 1008);

            let forged = await subscribe(SPOOFED + ', ' + CLIENT_A);
            assert.strictEqual(forged.accepted, false,
                'a forged leading entry bought the capped client a second connection');

            let other = await subscribe(CLIENT_B);
            assert.strictEqual(other.accepted, true,
                'a distinct client was capped by another client connection, so the cap is global rather than per-client');
        });

        it('collapses every subscriber onto the socket address when TRUST_PROXY is off', async function() {
            this.timeout(30000);
            await startServer({ TRUST_PROXY: false, WS_MAX_PER_IP: 1 });

            let first = await subscribe(CLIENT_A);
            assert.strictEqual(first.accepted, true);

            let second = await subscribe(CLIENT_B);
            assert.strictEqual(second.accepted, false,
                'an untrusting server believed the forwarded header and handed out a second connection');
            assert.strictEqual(second.closeCode, 1008);
        });
    });
});
