// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// Requests rejected by the JSON body parser (400, 413) or the API-key check
// (401) must still spend the caller's backstop budget, so the app-wide
// limiter has to run ahead of both.

const assert = require('assert');
const http   = require('http');
const { createApp } = require('../../../src/api');

const KEY   = 'limiter-order-key';
const LIMIT = 5;

function provider() {
    return {
        isReady:                () => true,
        getHubConfigAgeSeconds: () => 0,
        getChains:              () => [],
        getDatabase:            () => null,
        getBroadcaster:         () => null,
        getSnapshotBuilder:     () => null,
        getPoller:              () => null,
        getTransparencyLog:     () => null,
        getClientSync:          () => null
    };
}

const cfg = () => ({
    SYNC_MODE: 'server',
    SYNC_API_KEY: KEY,
    TRUST_PROXY: false,
    SNAPSHOT_RATE_FULL: 100,
    SNAPSHOT_RATE_INCR: 100,
    TRANSPARENCY_RATE_LIMIT: 100
});

// Each case gets a fresh app so its backstop bucket starts empty.
async function withApp(fn) {
    let server = http.createServer(createApp(provider(), cfg()));
    await new Promise((r) => server.listen(0, '127.0.0.1', r));
    try {
        await fn('http://127.0.0.1:' + server.address().port);
    } finally {
        server.closeAllConnections();
        await new Promise((r) => server.close(r));
    }
}

const auth = { authorization: 'Bearer ' + KEY };

describe('backstop limiter runs before the body parser and API-key check', function () {
    let prev;

    before(function () {
        prev = process.env.SYNC_RATE_LIMIT_RPM;
        process.env.SYNC_RATE_LIMIT_RPM = String(LIMIT);
    });

    after(function () {
        if (prev === undefined) delete process.env.SYNC_RATE_LIMIT_RPM;
        else process.env.SYNC_RATE_LIMIT_RPM = prev;
    });

    async function exhaust(base, send, expectedStatus) {
        for (let i = 0; i < LIMIT; i++) {
            let res = await send(base);
            assert.strictEqual(res.status, expectedStatus, 'request ' + i);
            await res.arrayBuffer();
        }
        let res = await send(base);
        assert.strictEqual(res.status, 429);
        assert.strictEqual((await res.json()).code, 'RATE_LIMITED');
        // A well-formed authenticated call from the same caller is refused too.
        let ok = await fetch(base + '/health', { headers: auth });
        assert.strictEqual(ok.status, 429);
    }

    it('counts malformed JSON bodies and then refuses with 429', async function () {
        await withApp((base) => exhaust(base, (b) => fetch(b + '/validator-heartbeat/indexer/BTC/mainnet', {
            method: 'POST',
            headers: Object.assign({ 'content-type': 'application/json' }, auth),
            body: '{not json'
        }), 400));
    });

    it('counts oversize bodies and then refuses with 429', async function () {
        let big = JSON.stringify({ pad: 'x'.repeat(20 * 1024) });
        await withApp((base) => exhaust(base, (b) => fetch(b + '/validator-heartbeat/indexer/BTC/mainnet', {
            method: 'POST',
            headers: Object.assign({ 'content-type': 'application/json' }, auth),
            body: big
        }), 413));
    });

    it('counts wrong API keys and then refuses with 429', async function () {
        await withApp((base) => exhaust(base, (b) => fetch(b + '/health', {
            headers: { authorization: 'Bearer wrong' }
        }), 401));
    });

    it('still serves a valid call under the limit', async function () {
        await withApp(async (base) => {
            let res = await fetch(base + '/health', { headers: auth });
            assert.strictEqual(res.status, 200);
            assert.strictEqual((await res.json()).status, 'healthy');
        });
    });
});
