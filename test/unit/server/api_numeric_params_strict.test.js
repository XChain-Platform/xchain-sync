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
const http = require('http');
const { createApp } = require('../../../src/api');

const KEY = 'numeric-params-key';
const BAD_VALUES = ['12abc', '-1', '1e9', '0x10', '1.5', '+1', '9007199254740993'];

// Exercise every createApp route that delegates numeric input to parseStrictUint.

function createProvider(calls) {
    const db = {
        getCheckpointAtHeight: async (...args) => { calls.push(['checkpoint', ...args]); return []; }
    };
    const builder = {
        streamIncrementalSnapshot: async (dbArg, value, res) => {
            calls.push(['snapshot', value]);
            res.json({ ok: true });
        },
        streamTableRowsById: async (dbArg, table, afterId, limit, res) => {
            calls.push(['rows', afterId, limit]);
            res.json({ ok: true });
        },
        streamDispensers: async (dbArg, afterTx, afterAddr, res) => {
            calls.push(['dispensers', afterTx, afterAddr]);
            res.json({ ok: true });
        }
    };
    const transparencyLog = {
        getPage: async (...args) => { calls.push(['roots', ...args]); return { results: [] }; },
        getProof: async (...args) => { calls.push(['proof', ...args]); return { proof: [] }; }
    };
    return {
        isReady: () => true,
        getHubConfigAgeSeconds: () => 0,
        getChains: () => [],
        getDatabase: () => db,
        getBroadcaster: () => null,
        getSnapshotBuilder: () => builder,
        getPoller: () => null,
        getTransparencyLog: () => transparencyLog,
        getClientSync: () => null
    };
}

function casesFor(value) {
    const encoded = encodeURIComponent(value);
    return [
        '/checkpoint/indexer/bitcoin/regtest/' + encoded,
        '/snapshot/indexer/bitcoin/regtest/since/' + encoded,
        '/snapshot-rows/indexer/bitcoin/regtest/index_addresses?after_id=' + encoded,
        '/snapshot-rows/indexer/bitcoin/regtest/index_addresses?limit=' + encoded,
        '/snapshot-rows/indexer/bitcoin/regtest/index_addresses?max_block=' + encoded,
        '/snapshot-dispensers/decoder/bitcoin/regtest?after_tx=' + encoded,
        '/snapshot-dispensers/decoder/bitcoin/regtest?after_addr=' + encoded,
        '/transparency/indexer/bitcoin/regtest/roots?page=' + encoded,
        '/transparency/indexer/bitcoin/regtest/roots?limit=' + encoded,
        '/transparency/indexer/bitcoin/regtest/proof/' + encoded
    ];
}

describe('API numeric parameters are parsed strictly', function () {
    let calls, server, base;

    before(async function () {
        calls = [];
        const cfg = { SYNC_MODE: 'server', SYNC_API_KEY: KEY, TRUST_PROXY: false,
            SNAPSHOT_RATE_FULL: 1000, SNAPSHOT_RATE_INCR: 1000, TRANSPARENCY_RATE_LIMIT: 1000 };
        server = http.createServer(createApp(createProvider(calls), cfg));
        await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
        base = 'http://127.0.0.1:' + server.address().port;
    });

    after(async function () {
        server.closeAllConnections();
        await new Promise((resolve) => server.close(resolve));
    });

    beforeEach(function () { calls.length = 0; });

    it('refuses malformed unsigned integers before downstream work', async function () {
        for (const value of BAD_VALUES) {
            for (const path of casesFor(value)) {
                const res = await fetch(base + path, { headers: { authorization: 'Bearer ' + KEY } });
                assert.strictEqual(res.status, 400, path);
                assert.strictEqual((await res.json()).code, 'BAD_REQUEST', path);
            }
        }
        assert.deepStrictEqual(calls, []);
    });

    it('passes exact integer values and keeps absent query defaults', async function () {
        const paths = casesFor('42');
        for (const path of paths) await fetch(base + path, { headers: { authorization: 'Bearer ' + KEY } });
        assert.deepStrictEqual(calls, [
            ['checkpoint', 'BTC', 'regtest', 42], ['snapshot', 42], ['rows', 42, undefined],
            ['rows', 0, 42], ['rows', 0, undefined], ['dispensers', 42, NaN],
            ['dispensers', NaN, 42], ['roots', 42, 100], ['roots', 0, 42], ['proof', 42]
        ]);
    });
});
