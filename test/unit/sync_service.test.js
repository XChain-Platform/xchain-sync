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
const Database = proxyquire('../../src/db', { 'mariadb': mariadbStub });
const SyncService = proxyquire('../../src/sync_service', { './db': Database });
const TransparencyLog = require('../../src/server/transparency_log');
const ClientSync   = require('../../src/client/sync');
const ServerPoller = require('../../src/server/poller');
const fs = require('fs');
const path = require('path');

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

    describe('constructor', function(){
        it('initializes empty maps', function(){
            assert.strictEqual(service.databases.size, 0);
            assert.strictEqual(service.pollers.size, 0);
            assert.strictEqual(service.clientSyncs.size, 0);
        });

        it('creates a HubClient', function(){
            assert.ok(service.hubClient);
        });

        it('creates a HashVerifier', function(){
            assert.ok(service.hashVerifier);
        });

        it('broadcaster is null initially', function(){
            assert.strictEqual(service.broadcaster, null);
        });
    });
});

describe("SyncService", function(){

    registerHooks();

    describe('getDatabase', function(){
        it('returns null for unknown chain/network', function(){
            assert.strictEqual(service.getDatabase('bitcoin', 'mainnet'), null);
        });

        it('returns db for known chain/network (indexer default)', function(){
            let mockDb = { doQuery: sinon.stub() };
            service.databases.set('bitcoin:mainnet:indexer', { db: mockDb, config: {}, dbType: 'indexer' });
            assert.strictEqual(service.getDatabase('bitcoin', 'mainnet'), mockDb);
        });

        it('returns decoder db when dbType=decoder is requested', function(){
            let indexerDb = { name: 'indexer' };
            let decoderDb = { name: 'decoder' };
            service.databases.set('bitcoin:mainnet:indexer', { db: indexerDb, config: {}, dbType: 'indexer' });
            service.databases.set('bitcoin:mainnet:decoder', { db: decoderDb, config: {}, dbType: 'decoder' });
            assert.strictEqual(service.getDatabase('bitcoin', 'mainnet', 'decoder'), decoderDb);
            assert.strictEqual(service.getDatabase('bitcoin', 'mainnet', 'indexer'), indexerDb);
        });
    });
});

describe("SyncService", function(){

    registerHooks();

    describe('getChains', function(){
        it('returns empty array when no chains', function(){
            assert.deepStrictEqual(service.getChains(), []);
        });

        it('returns array of chain/network/dbType triples', function(){
            service.databases.set('bitcoin:mainnet:indexer', { db: {}, config: { coin: 'bitcoin', network: 'mainnet', dbType: 'indexer' } });
            service.databases.set('litecoin:testnet:indexer', { db: {}, config: { coin: 'litecoin', network: 'testnet', dbType: 'indexer' } });
            let chains = service.getChains();
            assert.strictEqual(chains.length, 2);
            assert.deepStrictEqual(chains[0], { coin: 'bitcoin', network: 'mainnet', dbType: 'indexer' });
        });
    });
});

describe("SyncService", function(){

    registerHooks();

    describe('getTransparencyLog', function(){
        it('returns null for unknown chain/network', function(){
            assert.strictEqual(service.getTransparencyLog('bitcoin', 'mainnet'), null);
        });

        it('returns poller transparency log when poller exists', function(){
            let mockLog = { recordBlock: sinon.stub() };
            service.databases.set('bitcoin:mainnet:indexer', { db: {}, config: {}, dbType: 'indexer' });
            service.pollers.set('bitcoin:mainnet:indexer', { transparencyLog: mockLog });
            assert.strictEqual(service.getTransparencyLog('bitcoin', 'mainnet'), mockLog);
        });

        it('creates a temporary TransparencyLog when no poller', function(){
            let mockDb = {};
            service.databases.set('bitcoin:mainnet:indexer', { db: mockDb, config: {}, dbType: 'indexer' });
            let log = service.getTransparencyLog('bitcoin', 'mainnet');
            assert.ok(log instanceof TransparencyLog);
        });
    });
});

describe("SyncService", function(){

    registerHooks();

    describe('getBroadcaster', function(){
        it('returns null before server mode started', function(){
            assert.strictEqual(service.getBroadcaster(), null);
        });
    });
});

describe("SyncService", function(){

    registerHooks();

    describe('getSnapshotBuilder', function(){
        it('returns null before server mode started', function(){
            assert.strictEqual(service.getSnapshotBuilder(), null);
        });
    });
});

describe("SyncService", function(){

    registerHooks();

    describe('waitForHub', function(){
        it('resolves immediately when hub is alive', async function(){
            sinon.stub(service.hubClient, 'ping').resolves(true);
            sinon.stub(service.util, 'sleep').resolves();
            await service.waitForHub();
            assert.strictEqual(service.hubClient.ping.calledOnce, true);
        });

        it('retries until hub responds', async function(){
            let stub = sinon.stub(service.hubClient, 'ping');
            stub.onFirstCall().resolves(false);
            stub.onSecondCall().resolves(false);
            stub.onThirdCall().resolves(true);
            sinon.stub(service.util, 'sleep').resolves();

            await service.waitForHub();
            assert.strictEqual(stub.callCount, 3);
        });
    });
});
