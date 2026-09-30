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

function stubDiscoveryDb(){
    sinon.stub(Database.prototype, 'createDatabase').resolves(true);
    sinon.stub(Database.prototype, 'verifyDatabaseOnce').resolves(true);
    sinon.stub(Database.prototype, 'replicateSchema').resolves();
    sinon.stub(Database.prototype, 'verifySyncTables').resolves(true);
    sinon.stub(Database.prototype, 'ensureReplicatedColumns').resolves();
    sinon.stub(Database.prototype, 'ensureReplicaSecondaryIndexes').resolves();
    sinon.stub(Database.prototype, 'close').resolves();
}

describe("SyncService", function(){

    registerHooks();

    describe('discoverChains', function(){
        it('skips already-known chains', async function(){
            service.databases.set('bitcoin:mainnet:indexer', { db: {}, config: {}, dbType: 'indexer' });
            sinon.stub(service.hubClient, 'getIndexerConfigs').resolves([{
                coin: 'bitcoin', network: 'mainnet', dbType: 'indexer',
                db_host: 'db', db_port: 3306, db_name: 'btc', db_user: 'u', db_pass: 'p'
            }]);
            sinon.stub(service.hubClient, 'getDecoderConfigs').resolves([]);

            let newChains = await service.discoverChains();
            assert.strictEqual(newChains.length, 0);
        });

        it('SYNC_EXCLUDE drops a listed chain before any DB pool / ClientSync is created', async function(){
            config.SYNC_MODE = 'client';
            config.SYNC_EXCLUDE = ['bitcoin:mainnet:indexer'];
            service = new SyncService(config);
            sinon.stub(service.hubClient, 'getIndexerConfigs').resolves([
                indexerCfg(), // bitcoin:mainnet:indexer -> excluded
                indexerCfg({ coin: 'litecoin', db_name: 'ltc_idx' }) // kept
            ]);
            sinon.stub(service.hubClient, 'getDecoderConfigs').resolves([]);
            let createDb = sinon.stub(Database.prototype, 'createDatabase').resolves(true);
            sinon.stub(Database.prototype, 'verifyDatabaseOnce').resolves(true);
            sinon.stub(Database.prototype, 'replicateSchema').resolves();
            sinon.stub(Database.prototype, 'verifySyncTables').resolves(true);
            sinon.stub(Database.prototype, 'ensureReplicatedColumns').resolves();
            sinon.stub(Database.prototype, 'ensureReplicaSecondaryIndexes').resolves();
            sinon.stub(Database.prototype, 'close').resolves();
            let startSync = sinon.stub(service, 'startClientSyncForChain');

            let newChains = await service.discoverChains();

            assert.strictEqual(newChains.length, 1, 'only the non-excluded chain is set up');
            assert.strictEqual(service.databases.has('bitcoin:mainnet:indexer'), false, 'excluded chain absent');
            assert.strictEqual(service.databases.has('litecoin:mainnet:indexer'), true, 'kept chain present');
            // The excluded chain is skipped before createDatabase, so it never opens a pool.
            assert.strictEqual(createDb.callCount, 1);
            assert.strictEqual(startSync.callCount, 1);
        });
    });
});

describe("SyncService", function(){

    registerHooks();

    describe('discoverChains', function(){
        // An unmatched SYNC_BOOTSTRAP_DEPTH_* key is not inert. It resolves to
        // depth 0, the FULL-history snapshot branch, so a typo'd key silently starts the
        // unbounded bootstrap it was set to prevent (measured on the 2026-08-10 DOGE
        // reseed). Discovery must refuse it BEFORE any ClientSync is started.
        it('client mode REFUSES a bootstrap-depth key naming no discovered chain, before starting any sync', async function(){
            config.SYNC_MODE = 'client';
            config.SYNC_BOOTSTRAP_DEPTH = { 'LTC:TESTNET': 50000 };
            config.SYNC_BOOTSTRAP_DEPTH_ENV_KEYS = ['SYNC_BOOTSTRAP_DEPTH_LTC_TESTNET'];
            service = new SyncService(config);
            sinon.stub(service.hubClient, 'getIndexerConfigs').resolves([indexerCfg()]); // bitcoin:mainnet only
            sinon.stub(service.hubClient, 'getDecoderConfigs').resolves([]);
            stubDiscoveryDb();
            let startSync = sinon.stub(service, 'startClientSyncForChain');

            await assert.rejects(() => service.discoverChains(),
                /SYNC_BOOTSTRAP_DEPTH_LTC_TESTNET/);
            assert.strictEqual(startSync.callCount, 0, 'no ClientSync started on a refused config');
        });

        // A malformed CHECKPOINT_VALIDATORS_* is not inert either: it resolves to the same
        // null an ABSENT override does, so verifyCheckpointQuorum skips the anchor on a
        // replica whose operator armed VERIFY_CHECKPOINT_QUORUM believing it on. Discovery
        // must refuse it on the same pass, before any ClientSync replicates a block.
        it('client mode REFUSES a malformed checkpoint pin override, before starting any sync', async function(){
            const PINKEY = 'CHECKPOINT_VALIDATORS_BITCOIN_MAINNET';
            config.SYNC_MODE = 'client';
            config.VERIFY_CHECKPOINT_QUORUM = true;
            process.env[PINKEY] = '[{"pubkey":"aa","weight":100}]';   // weight not a string, no source
            service = new SyncService(config);
            sinon.stub(service.hubClient, 'getIndexerConfigs').resolves([indexerCfg()]);
            sinon.stub(service.hubClient, 'getDecoderConfigs').resolves([]);
            stubDiscoveryDb();
            let startSync = sinon.stub(service, 'startClientSyncForChain');

            try {
                await assert.rejects(() => service.discoverChains(), new RegExp(PINKEY));
                assert.strictEqual(startSync.callCount, 0, 'no ClientSync started on a refused pin override');
            } finally { delete process.env[PINKEY]; }
        });
    });
});

describe("SyncService", function(){

    registerHooks();

    describe('discoverChains', function(){
        it('client mode accepts a well-formed checkpoint pin override', async function(){
            const PINKEY = 'CHECKPOINT_VALIDATORS_BITCOIN_MAINNET';
            config.SYNC_MODE = 'client';
            config.VERIFY_CHECKPOINT_QUORUM = true;
            process.env[PINKEY] = JSON.stringify([{ pubkey: 'ab'.repeat(32), weight: '100', source: 'S1' }]);
            service = new SyncService(config);
            sinon.stub(service.hubClient, 'getIndexerConfigs').resolves([indexerCfg()]);
            sinon.stub(service.hubClient, 'getDecoderConfigs').resolves([]);
            stubDiscoveryDb();
            let startSync = sinon.stub(service, 'startClientSyncForChain');

            try {
                let newChains = await service.discoverChains();
                assert.strictEqual(newChains.length, 1);
                assert.strictEqual(startSync.callCount, 1, 'a valid override does not block startup');
            } finally { delete process.env[PINKEY]; }
        });

        it('client mode accepts a bootstrap-depth key whose chain the hub published under its full name', async function(){
            config.SYNC_MODE = 'client';
            config.SYNC_BOOTSTRAP_DEPTH = { 'BTC:MAINNET': 50000 };
            config.SYNC_BOOTSTRAP_DEPTH_ENV_KEYS = ['SYNC_BOOTSTRAP_DEPTH_BTC_MAINNET'];
            service = new SyncService(config);
            sinon.stub(service.hubClient, 'getIndexerConfigs').resolves([indexerCfg()]); // coin: 'bitcoin'
            sinon.stub(service.hubClient, 'getDecoderConfigs').resolves([]);
            stubDiscoveryDb();
            let startSync = sinon.stub(service, 'startClientSyncForChain');

            let newChains = await service.discoverChains();
            assert.strictEqual(newChains.length, 1);
            assert.strictEqual(startSync.callCount, 1);
        });

        it('server mode ignores bootstrap-depth keys entirely (the var governs nothing there)', async function(){
            config.SYNC_MODE = 'server';
            config.SYNC_BOOTSTRAP_DEPTH = { 'LTC:TESTNET': 50000 };
            config.SYNC_BOOTSTRAP_DEPTH_ENV_KEYS = ['SYNC_BOOTSTRAP_DEPTH_LTC_TESTNET'];
            service = new SyncService(config);
            sinon.stub(service.hubClient, 'getIndexerConfigs').resolves([indexerCfg()]);
            sinon.stub(service.hubClient, 'getDecoderConfigs').resolves([]);
            stubDiscoveryDb();
            sinon.stub(service, 'startPollerForChain');

            let newChains = await service.discoverChains();
            assert.strictEqual(newChains.length, 1);
        });

    });
});

describe("SyncService", function(){

    registerHooks();

    describe('discoverChains', function(){
        it('client mode (source reachable): replicates schema, verifies tables, starts a ClientSync', async function(){
            config.SYNC_MODE = 'client';
            service = new SyncService(config);
            sinon.stub(service.hubClient, 'getIndexerConfigs').resolves([indexerCfg()]);
            sinon.stub(service.hubClient, 'getDecoderConfigs').resolves([]);
            sinon.stub(Database.prototype, 'createDatabase').resolves(true);
            sinon.stub(Database.prototype, 'verifyDatabaseOnce').resolves(true);
            let repl = sinon.stub(Database.prototype, 'replicateSchema').resolves();
            sinon.stub(Database.prototype, 'verifySyncTables').resolves(true);
            sinon.stub(Database.prototype, 'ensureReplicatedColumns').resolves();
            sinon.stub(Database.prototype, 'ensureReplicaSecondaryIndexes').resolves();
            sinon.stub(Database.prototype, 'close').resolves();
            let startSync = sinon.stub(service, 'startClientSyncForChain');

            let newChains = await service.discoverChains();
            assert.strictEqual(newChains.length, 1);
            assert.strictEqual(service.databases.size, 1);
            assert.strictEqual(repl.calledOnce, true);
            assert.strictEqual(startSync.calledOnce, true);
        });

        it('client mode (source unreachable): falls through to server /schema fetch; still verifies sync tables for decoder', async function(){
            config.SYNC_MODE = 'client';
            service = new SyncService(config);
            sinon.stub(service.hubClient, 'getIndexerConfigs').resolves([]);
            sinon.stub(service.hubClient, 'getDecoderConfigs').resolves([indexerCfg({ dbType: 'decoder', db_name: 'btc_dec' })]);
            sinon.stub(Database.prototype, 'createDatabase').resolves(true);
            sinon.stub(Database.prototype, 'verifyDatabaseOnce').rejects(new Error('unreachable'));
            let repl = sinon.stub(Database.prototype, 'replicateSchema').resolves();
            let vst  = sinon.stub(Database.prototype, 'verifySyncTables').resolves(true);
            sinon.stub(Database.prototype, 'ensureReplicatedColumns').resolves();
            sinon.stub(Database.prototype, 'ensureReplicaSecondaryIndexes').resolves();
            sinon.stub(Database.prototype, 'close').resolves();
            sinon.stub(service, 'startClientSyncForChain');

            await service.discoverChains();
            assert.strictEqual(repl.called, false, 'no schema replication when the source DB is unreachable');
            // verifySyncTables runs for decoder replicas too (it is dbType-aware
            // internally: decoder gets sync_halt only). Without it the halt
            // table never exists and every client start logs a 1146 probe error.
            assert.strictEqual(vst.called, true, 'verifySyncTables runs for decoder replicas');
            assert.strictEqual(service.databases.size, 1);
        });
    });
});

describe("SyncService", function(){

    registerHooks();

    describe('discoverChains', function(){
        it('server mode with REPLICA_DB_HOST re-serves from the local replica', async function(){
            config.SYNC_MODE = 'server';
            config.REPLICA_DB_HOST = 'localreplica';
            service = new SyncService(config);
            sinon.stub(service.hubClient, 'getIndexerConfigs').resolves([indexerCfg()]);
            sinon.stub(service.hubClient, 'getDecoderConfigs').resolves([]);
            sinon.stub(Database.prototype, 'verifySyncTables').resolves(true);
            let collation = sinon.stub(Database.prototype, 'assertStakeWeightOrderingCollation').resolves();
            let startPoller = sinon.stub(service, 'startPollerForChain');

            await service.discoverChains();
            let entry = service.databases.get('bitcoin:mainnet:indexer');
            assert.strictEqual(entry.db.host, 'localreplica');
            assert.strictEqual(startPoller.calledOnce, true);
            assert.strictEqual(collation.calledOnce, true);
        });

        it('server mode without REPLICA_DB_HOST connects to the hub-provided coordinates', async function(){
            config.SYNC_MODE = 'server';
            delete config.REPLICA_DB_HOST;
            service = new SyncService(config);
            sinon.stub(service.hubClient, 'getIndexerConfigs').resolves([indexerCfg()]);
            sinon.stub(service.hubClient, 'getDecoderConfigs').resolves([]);
            sinon.stub(Database.prototype, 'verifySyncTables').resolves(true);
            let collation = sinon.stub(Database.prototype, 'assertStakeWeightOrderingCollation').resolves();
            sinon.stub(service, 'startPollerForChain');

            await service.discoverChains();
            let entry = service.databases.get('bitcoin:mainnet:indexer');
            assert.strictEqual(entry.db.host, 'srchost');
            assert.strictEqual(collation.calledOnce, true);
        });
    });
});
