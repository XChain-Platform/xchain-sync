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

const KEY = 'repair-table-key';

function provider(calls){
    return {
        isReady: () => true,
        getHubConfigAgeSeconds: () => 0,
        getChains: () => [],
        getDatabase: () => ({ dbType: 'indexer' }),
        getSnapshotBuilder: () => ({
            streamRepairTable: async (db, table, res) => {
                calls.push(table);
                res.json({ table });
            }
        }),
        getBroadcaster: () => null,
        getPoller: () => null,
        getTransparencyLog: () => null,
        getClientSync: () => null
    };
}

async function withServer(mode, drive){
    let calls = [];
    let cfg = { SYNC_MODE: mode, SYNC_API_KEY: KEY, TRUST_PROXY: false,
        SNAPSHOT_RATE_FULL: 1000, SNAPSHOT_RATE_INCR: 1000 };
    let server = http.createServer(createApp(provider(calls), cfg));
    await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
    try {
        let base = 'http://127.0.0.1:' + server.address().port;
        await drive(base, calls);
    } finally {
        server.closeAllConnections();
        await new Promise(resolve => server.close(resolve));
    }
}

describe('repair-table API route', function(){
    it('delegates an indexer repair request to the snapshot builder', async function(){
        await withServer('server', async (base, calls) => {
            let res = await fetch(base + '/snapshot-table/indexer/bitcoin/regtest/mappings_actions',
                { headers: { authorization: 'Bearer ' + KEY } });
            assert.strictEqual(res.status, 200);
            assert.deepStrictEqual(await res.json(), { table: 'mappings_actions' });
            assert.deepStrictEqual(calls, ['mappings_actions']);
        });
    });

    it('rejects decoder and non-server requests before the builder runs', async function(){
        await withServer('server', async (base, calls) => {
            let res = await fetch(base + '/snapshot-table/decoder/bitcoin/regtest/mappings_actions',
                { headers: { authorization: 'Bearer ' + KEY } });
            assert.strictEqual(res.status, 400);
            assert.strictEqual((await res.json()).code, 'REPAIR_TABLE_NOT_ALLOWED');
            assert.deepStrictEqual(calls, []);
        });
        await withServer('client', async (base, calls) => {
            let res = await fetch(base + '/snapshot-table/indexer/bitcoin/regtest/mappings_actions',
                { headers: { authorization: 'Bearer ' + KEY } });
            assert.strictEqual(res.status, 403);
            assert.deepStrictEqual(calls, []);
        });
    });
});
