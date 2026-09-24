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
const http   = require('http');
const { createApp } = require('../../src/api');

const KEY = 'unit-test-key';

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

function listen(cfg) {
    let server = http.createServer(createApp(provider(), cfg));
    return new Promise((resolve) => server.listen(0, '127.0.0.1', () => resolve(server)));
}

const baseCfg = () => ({
    SYNC_MODE: 'server',
    SYNC_API_KEY: KEY,
    TRUST_PROXY: false,
    SNAPSHOT_RATE_FULL: 100,
    SNAPSHOT_RATE_INCR: 100,
    TRANSPARENCY_RATE_LIMIT: 100
});

describe('createApp against the real route table', function () {
    let server, base;

    before(async function () {
        server = await listen(baseCfg());
        base = 'http://127.0.0.1:' + server.address().port;
    });

    after(function (done) {
        server.closeAllConnections();
        server.close(done);
    });

    it('rejects a keyless request with 401', async function () {
        let res = await fetch(base + '/health');
        assert.strictEqual(res.status, 401);
        assert.deepStrictEqual(await res.json(), { error: 'Unauthorized' });
    });

    it('rejects a wrong bearer key with 401', async function () {
        let res = await fetch(base + '/health', { headers: { authorization: 'Bearer nope' } });
        assert.strictEqual(res.status, 401);
    });

    it('serves /health to the correct bearer key', async function () {
        let res = await fetch(base + '/health', { headers: { authorization: 'Bearer ' + KEY } });
        assert.strictEqual(res.status, 200);
        let body = await res.json();
        assert.strictEqual(body.status, 'healthy');
        assert.strictEqual(body.mode, 'server');
    });

    it('returns the real 400 for an invalid dbType once authenticated', async function () {
        let res = await fetch(base + '/status/bogus/BTC/mainnet', { headers: { authorization: 'Bearer ' + KEY } });
        assert.strictEqual(res.status, 400);
        assert.strictEqual((await res.json()).code, 'BAD_REQUEST');
    });

    it('keeps /halt/clear closed when no key is configured', async function () {
        let open = await listen(Object.assign(baseCfg(), { SYNC_API_KEY: '' }));
        try {
            let res = await fetch('http://127.0.0.1:' + open.address().port + '/halt/clear/indexer/BTC/mainnet', { method: 'POST' });
            assert.strictEqual(res.status, 401);
        } finally {
            open.closeAllConnections();
            await new Promise((r) => open.close(r));
        }
    });
});
