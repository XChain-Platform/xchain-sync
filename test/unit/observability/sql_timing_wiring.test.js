// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

'use strict';

const assert = require('assert');
const http = require('http');
const mariadb = require('mariadb');
const proxyquire = require('proxyquire');

const TIMING_URL = 'https://timing.test.invalid/api/v1/sql-timing';
const ENV_KEYS = [
    'SQL_TIMING_ENABLED',
    'SQL_TIMING_URL',
    'SQL_TIMING_KEY',
    'SQL_TIMING_PROJECT',
    'SQL_TIMING_INTERVAL_MS',
    'SQL_TIMING_STATEMENT_RATE',
    'SYNC_MODE'
];

function timingEnv() {
    return {
        SQL_TIMING_ENABLED: '1',
        SQL_TIMING_URL: TIMING_URL,
        SQL_TIMING_KEY: 'unit-test-key',
        SQL_TIMING_PROJECT: 'xchain-platform',
        SQL_TIMING_INTERVAL_MS: '5',
        SQL_TIMING_STATEMENT_RATE: '0',
        SYNC_MODE: 'client'
    };
}

function appConfig() {
    return {
        SYNC_MODE: 'client',
        SYNC_API_KEY: '',
        TRUST_PROXY: false,
        SNAPSHOT_RATE_FULL: 100,
        SNAPSHOT_RATE_INCR: 100,
        TRANSPARENCY_RATE_LIMIT: 100
    };
}

function clientState() {
    return {
        lastKnownServerBlock: null,
        sourceHeightStale: true,
        upstreamReplica: {},
        halted: false,
        trainActivation: null,
        truncated: false,
        sourceQuorum: null,
        sourcesConfigured: null,
        sourcesActive: null,
        sourcesAgreeing: null,
        sourcesEvicted: []
    };
}

function provider(dbRef) {
    return {
        getChains: () => [{ coin: 'BTC', network: 'regtest', dbType: 'indexer' }],
        getDatabase: () => dbRef.db,
        getClientSyncState: () => clientState(),
        getClientSync: () => null,
        getSnapshotBuilder: () => null,
        getPoller: () => null,
        getBroadcaster: () => null,
        getTransparencyLog: () => null
    };
}

async function waitForLine(lines) {
    const deadline = Date.now() + 1000;
    while (lines.length === 0 && Date.now() < deadline) {
        await new Promise((resolve) => setTimeout(resolve, 10));
    }
    assert.strictEqual(lines.length, 1);
    return JSON.parse(lines[0].trim());
}

describe('SQL timing wiring', function () {
    this.timeout(10000);

    const originalEnv = Object.fromEntries(ENV_KEYS.map((key) => [key, process.env[key]]));
    const nativeFetch = global.fetch;
    const shipped = [];
    const pool = { query: async () => [] };
    let server;
    let baseUrl;

    before(async function () {
        Object.assign(process.env, timingEnv());
        global.fetch = (url, options) => {
            if (url === TIMING_URL) {
                shipped.push(options.body);
                return Promise.resolve({ status: 200, body: { cancel: () => Promise.resolve() } });
            }
            return nativeFetch(url, options);
        };

        // Build the app before opening the Database, as startApi does: the shared timing
        // instance may predate this env, and createApp is what reconfigures it.
        const { createApp } = require('../../../src/api');
        const dbRef = { db: null };
        const app = createApp(provider(dbRef), appConfig());
        const Database = proxyquire('../../../src/db', {
            mariadb: Object.assign({}, mariadb, { createPool: () => pool })
        });
        const db = new Database('127.0.0.1', 3306, 'test', 'test', 'test', {}, 'indexer');
        let queried = false;
        db.getLastBlock = async () => {
            if (!queried) {
                queried = true;
                await db.pool.query('SELECT 1');
            }
            return null;
        };
        db.listExistingTables = async () => new Set();
        db.getSyncState = undefined;
        dbRef.db = db;

        server = http.createServer(app);
        await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
        baseUrl = 'http://127.0.0.1:' + server.address().port;
    });

    after(async function () {
        if (server) {
            server.closeAllConnections();
            await new Promise((resolve) => server.close(resolve));
        }
        global.fetch = nativeFetch;
        for (const [key, value] of Object.entries(originalEnv)) {
            if (value === undefined) delete process.env[key];
            else process.env[key] = value;
        }
        // Re-read the restored env so later suites see the shared instance disabled.
        await require('../../../src/observability').installSqlTiming(null, { service: 'xchain-sync' }).stop();
    });

    it('emits the status route with its wrapped pool query count', async function () {
        const response = await fetch(baseUrl + '/status');
        assert.strictEqual(response.status, 200);
        await response.json();

        const line = await waitForLine(shipped);
        assert.strictEqual(line.route, 'GET /xchain-sync/status');
        assert.strictEqual(line.query_count, 1);
        assert.strictEqual(line.project, 'xchain-platform');
    });
});
