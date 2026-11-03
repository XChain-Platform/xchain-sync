// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// /checkpoint/.../range must serve exactly the range the caller asked for.
// Bounds that are not plain unsigned decimal integers (exponent, hex, decimal
// fraction, signed, empty, beyond the safe-integer range) are refused with 400
// instead of being truncated by a lenient parse into a different range.

const assert = require('assert');
const http   = require('http');
const { createApp } = require('../../../src/api');

const KEY = 'range-bounds-key';

describe('checkpoint range bounds are parsed strictly', function () {
    let calls, server, base;

    before(async function () {
        let db = {
            findCheckpointsInRange: async (...args) => { calls.push(args); return []; }
        };
        let provider = {
            isReady:                () => true,
            getHubConfigAgeSeconds: () => 0,
            getChains:              () => [],
            getDatabase:            () => db,
            getBroadcaster:         () => null,
            getSnapshotBuilder:     () => null,
            getPoller:              () => null,
            getTransparencyLog:     () => null,
            getClientSync:          () => null
        };
        let cfg = { SYNC_MODE: 'server', SYNC_API_KEY: KEY, TRUST_PROXY: false,
            SNAPSHOT_RATE_FULL: 1000, SNAPSHOT_RATE_INCR: 1000, TRANSPARENCY_RATE_LIMIT: 1000 };
        server = http.createServer(createApp(provider, cfg));
        await new Promise((r) => server.listen(0, '127.0.0.1', r));
        base = 'http://127.0.0.1:' + server.address().port;
    });

    after(async function () {
        server.closeAllConnections();
        await new Promise((r) => server.close(r));
    });

    beforeEach(function () { calls = []; });

    const get = (qs) => fetch(base + '/checkpoint/indexer/bitcoin/regtest/range?' + qs,
        { headers: { authorization: 'Bearer ' + KEY } });

    const bad = [
        'from=0&to=1e12', 'from=1e20&to=1e21', 'from=0x10&to=0x20', 'from=1.9&to=2.9',
        'from=%2B1&to=5', 'from=-1&to=5', 'from=&to=5', 'from=0&to=',
        'from=0&to=9007199254740993', 'from=1&to=12abc', 'from=5'
    ];
    for (const qs of bad) {
        it('refuses ' + qs + ' without querying the db', async function () {
            let res = await get(qs);
            assert.strictEqual(res.status, 400);
            assert.strictEqual((await res.json()).code, 'BAD_REQUEST');
            assert.strictEqual(calls.length, 0);
        });
    }

    for (const [qs, f, t] of [['from=0&to=999999999999', 0, 999999999999], ['from=5&to=5', 5, 5]]) {
        it('serves exactly ' + qs, async function () {
            let res = await get(qs);
            assert.strictEqual(res.status, 200);
            assert.strictEqual(calls.length, 1);
            let a = calls[0];
            assert.strictEqual(a[a.length - 3], f);
            assert.strictEqual(a[a.length - 2], t);
        });
    }
});
