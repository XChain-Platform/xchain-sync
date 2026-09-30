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
const sinon  = require('sinon');
const proxyquire = require('proxyquire').noCallThru();

// Stub mariadb so db.js can be required without ESM issues
const mockPool = {
    getConnection: sinon.stub().resolves({ query: sinon.stub(), release: sinon.stub() }),
    end: sinon.stub().resolves()
};
const mariadbStub = {
    createPool: sinon.stub().returns(mockPool),
    createConnection: sinon.stub().resolves({ query: sinon.stub().resolves([]), end: sinon.stub().resolves() })
};

// Capture the proxyquired Database so tests can stub its prototype directly,
// rather than driving discoverChains' internal `new Database()` calls through
// the raw mariadb stub (whose pooled connection.query returns undefined).
const Database = proxyquire('../../../src/db', { 'mariadb': mariadbStub });
const SyncService = proxyquire('../../../src/sync_service', { './db': Database });
const TransparencyLog = require('../../../src/server/transparency_log');
const ClientSync   = require('../../../src/client/sync');
const ServerPoller = require('../../../src/server/poller');
const fs = require('fs');
const path = require('path');

function indexerCfg(over){
    return Object.assign({
        coin: 'bitcoin', network: 'mainnet', dbType: 'indexer',
        db_host: 'srchost', db_port: 3306, db_name: 'btc_idx', db_user: 'u', db_pass: 'p'
    }, over || {});
}

let service, config;

function registerHooks(){

    beforeEach(function(){
        config = {
            SYNC_MODE: 'server',
            HUB_API_HOST: 'localhost',
            HUB_PORT: 10000,
            HUB_REPOLL_INTERVAL: 300000,
            BLOCK_POLL_INTERVAL: 3000,
            SYNC_SOURCES: '',
            VERIFY_HASHES: true,
            REPLICA_DB_HOST: 'localhost',
            REPLICA_DB_PORT: 3306,
            REPLICA_DB_USER: 'user',
            REPLICA_DB_PASS: 'pass',
            WS_MAX_PER_IP: 3,
            WS_BACKPRESSURE_LIMIT: 50,
            CLIENT_RECONNECT_DELAY: 5000,
            HASH_CONFIRM_TIMEOUT: 5000
        };
        service = new SyncService(config);
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
        // startPollerForChain / startClientSyncForChain run start() as an unawaited
        // background promise whose .catch calls process.exit(1) on crash (for container
        // restart). With mocked deps those promises reject after the test moves on; stub
        // exit so a background crash can't tear down the mocha process mid-run.
        sinon.stub(process, 'exit');
    });

    afterEach(function(){
        sinon.restore();
    });

}

describe("SyncService", function(){

    registerHooks();

    // api.js listens before start() runs, and start() can sit in
    // waitForHub for MAX_HUB_WAIT_MS (default 5 minutes). /health's per-chain loop
    // has nothing to degrade on while getChains() is empty, so the probe reported
    // 'healthy' with zero pollers running. isReady() is what /health gates on now.
    describe('startup readiness (isReady)', function(){
        it('is not ready before start()', function(){
            assert.strictEqual(service.isReady(), false);
        });

        it('is still not ready while start() waits on the hub', async function(){
            let release;
            sinon.stub(service, 'waitForHub').returns(new Promise(res => { release = res; }));
            sinon.stub(service, 'discoverChains').resolves([]);
            sinon.stub(service, 'startServerMode').resolves();
            sinon.stub(service, 'scheduleHubRepoll');

            const started = service.start();
            assert.strictEqual(service.isReady(), false, 'ready must stay false for the whole hub-wait window');
            release();
            await started;
            assert.strictEqual(service.isReady(), true);
        });

        it('is ready after start() completes with a legitimately empty chain set', async function(){
            sinon.stub(service, 'waitForHub').resolves();
            sinon.stub(service, 'discoverChains').resolves([]);
            sinon.stub(service, 'startServerMode').resolves();
            sinon.stub(service, 'scheduleHubRepoll');

            await service.start();
            // Discovered-and-empty (everything SYNC_EXCLUDEd) is healthy, not starting:
            // readiness is about startup completing, never about chain count.
            assert.strictEqual(service.getChains().length, 0);
            assert.strictEqual(service.isReady(), true);
        });

        // Wiring guard: the /health route is registered inside startApi()'s closure
        // and is not reachable from a require. A readiness flag nothing gates on is
        // exactly the defect this item is about.
        it('gates GET /health on readiness before the per-chain loop', function(){
            const src  = fs.readFileSync(path.join(__dirname, '../../../src/api.js'), 'utf8');
            const route = src.slice(src.indexOf("app.get('/health'"), src.indexOf("app.get('/status'"));
            assert.ok(/isReady\(\)/.test(route), '/health does not consult isReady()');
            assert.ok(/status:\s+'starting'/.test(route), "/health does not report 'starting'");
            assert.ok(route.indexOf('isReady()') < route.indexOf('syncService.getChains()'),
                'the readiness gate must precede the per-chain loop it substitutes for');
        });
    });
});
