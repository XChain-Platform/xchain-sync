// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// Requests the body parser rejects (400, 413, 415) must answer a bounded JSON
// error that discloses nothing about the host, and must not write a
// multi-line stack trace to the service log. With NODE_ENV unset, express's
// default handler would return err.stack as HTML, install paths included.

const assert = require('assert');
const http   = require('http');
const { createApp } = require('../../../src/api');

const URL_PATH = '/validator-heartbeat/indexer/bitcoin/regtest';

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
    SYNC_API_KEY: 'body-parser-json-key',
    TRUST_PROXY: false,
    SNAPSHOT_RATE_FULL: 100,
    SNAPSHOT_RATE_INCR: 100,
    TRANSPARENCY_RATE_LIMIT: 100
});

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

describe('body-parser rejections answer bounded JSON', function () {
    let prevEnv, prevErr, logged;

    beforeEach(function () {
        prevEnv = process.env.NODE_ENV;
        delete process.env.NODE_ENV;
        prevErr = console.error;
        logged = [];
        console.error = (...args) => { logged.push(args.map(String).join(' ')); };
    });

    afterEach(function () {
        console.error = prevErr;
        if (prevEnv === undefined) delete process.env.NODE_ENV;
        else process.env.NODE_ENV = prevEnv;
    });

    async function check(send, status) {
        await withApp(async (base) => {
            let res  = await send(base);
            let text = await res.text();
            assert.strictEqual(res.status, status);
            assert.match(res.headers.get('content-type'), /application\/json/);
            assert.doesNotMatch(text, /node_modules|\/home\/|\/Users\/|\n\s+at /);
            assert.doesNotMatch(text, /<pre>|<!DOCTYPE/i);
            let body = JSON.parse(text);
            assert.ok(body.error && body.code);
        });
        assert.ok(logged.length <= 1, 'at most one log entry, got ' + logged.length);
        for (let line of logged) {
            assert.ok(!/\n/.test(line), 'single-line log entry');
            assert.ok(!/node_modules|\n\s+at /.test(line));
        }
    }

    it('malformed JSON without a key is 400 JSON, not 401', async function () {
        await check((b) => fetch(b + URL_PATH, {
            method: 'POST',
            headers: { 'content-type': 'application/json' },
            body: '{"validator_id":'
        }), 400);
    });

    it('oversize body is 413 JSON', async function () {
        await check((b) => fetch(b + URL_PATH, {
            method: 'POST',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify({ pad: 'x'.repeat(17 * 1024) })
        }), 413);
    });

    it('unsupported charset is a 4xx JSON error', async function () {
        await withApp(async (base) => {
            let res = await fetch(base + URL_PATH, {
                method: 'POST',
                headers: { 'content-type': 'application/json; charset=utf-16' },
                body: '{}'
            });
            let text = await res.text();
            assert.ok(res.status >= 400 && res.status < 500);
            assert.match(res.headers.get('content-type'), /application\/json/);
            assert.doesNotMatch(text, /node_modules|\/home\/|<pre>/);
        });
    });
});
