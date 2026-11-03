// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// Every malformed path, query or body must get a 4xx JSON {error, code} answer
// with no stack, and unrouted methods a JSON 404, instead of express's HTML.

const assert = require('assert');
const http   = require('http');
const { createApp } = require('../../src/api');

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

function request(port, method, path, body) {
    return new Promise((resolve, reject) => {
        let headers = {};
        if(body !== undefined) headers['content-type'] = 'application/json';
        let q = http.request({ host: '127.0.0.1', port, method, path, headers }, (r) => {
            let b = '';
            r.on('data', (c) => { b += c; });
            r.on('end', () => resolve({ status: r.statusCode, type: r.headers['content-type'] || '', body: b }));
        });
        q.on('error', reject);
        if(body !== undefined) q.write(body);
        q.end();
    });
}

const cases = [
    ['GET',    '/status/indexer/%E0%A4%A/regtest', undefined],
    ['GET',    '/status/indexer/%/regtest', undefined],
    ['POST',   '/validator-heartbeat/indexer/bitcoin/regtest', '{"validator_id":'],
    ['POST',   '/validator-heartbeat/indexer/bitcoin/regtest', 'null'],
    ['PUT',    '/status', undefined],
    ['DELETE', '/status', undefined],
    ['PATCH',  '/status', undefined]
];

describe('xchain-sync REST error handling', function () {
    let server, port;
    let origError;
    before(async function () {
        origError = console.error;
        console.error = () => {};
        let app = createApp(provider(), {
            SYNC_MODE: 'server', SYNC_API_KEY: '', TRUST_PROXY: false,
            SNAPSHOT_RATE_FULL: 100, SNAPSHOT_RATE_INCR: 100
        });
        server = http.createServer(app);
        await new Promise((r) => server.listen(0, '127.0.0.1', r));
        port = server.address().port;
    });
    after(function () {
        console.error = origError;
        return new Promise((r) => server.close(r));
    });

    for(let [method, path, body] of cases) {
        it(`${method} ${path} ${body === undefined ? '' : body} answers 4xx JSON with no stack`, async function () {
            let r = await request(port, method, path, body);
            assert.ok(r.status >= 400 && r.status < 500, 'status ' + r.status);
            assert.ok(/application\/json/.test(r.type), 'content-type ' + r.type);
            let j = JSON.parse(r.body);
            assert.strictEqual(typeof j.error, 'string');
            assert.strictEqual(typeof j.code, 'string');
            for(let bad of ['at ', 'node_modules', '<pre>', 'URIError', 'SyntaxError'])
                assert.ok(!r.body.includes(bad), 'body leaks ' + bad + ': ' + r.body);
        });
    }
});
