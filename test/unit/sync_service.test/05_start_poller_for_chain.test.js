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

    describe('startPollerForChain', function(){
        it('does not create duplicate pollers', function(){
            service.broadcaster = { broadcast: sinon.stub(), updateStatus: sinon.stub() };
            let db = { getLastBlock: sinon.stub(), doQuery: sinon.stub() };
            let cfg = { coin: 'bitcoin', network: 'mainnet' };

            service.startPollerForChain('bitcoin:mainnet', db, cfg);
            assert.strictEqual(service.pollers.size, 1);

            service.startPollerForChain('bitcoin:mainnet', db, cfg);
            assert.strictEqual(service.pollers.size, 1);
        });

        it('exits the process when the background poller crashes', async function(){
            sinon.stub(ServerPoller.prototype, 'start').rejects(new Error('poller crash'));
            service.broadcaster = { broadcast: sinon.stub(), updateStatus: sinon.stub() };
            service.startPollerForChain('bitcoin:mainnet:indexer', { dbType: 'indexer' }, indexerCfg());
            await new Promise(r => setImmediate(r));
            assert.ok(process.exit.calledWith(1));
        });
    });

});
