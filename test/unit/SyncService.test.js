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
// rather than driving _discoverChains' internal `new Database()` calls through
// the raw mariadb stub (whose pooled connection.query returns undefined).
const Database = proxyquire('../../src/db', { 'mariadb': mariadbStub });
const SyncService = proxyquire('../../src/SyncService', { './db': Database });
const TransparencyLog = require('../../src/TransparencyLog');
const ClientSync   = require('../../src/ClientSync');
const ServerPoller = require('../../src/ServerPoller');

function indexerCfg(over){
    return Object.assign({
        coin: 'bitcoin', network: 'mainnet', dbType: 'indexer',
        db_host: 'srchost', db_port: 3306, db_name: 'btc_idx', db_user: 'u', db_pass: 'p'
    }, over || {});
}

describe('SyncService', function(){

    let service, config;

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
        // _startPollerForChain / _startClientSyncForChain run start() as an unawaited
        // background promise whose .catch calls process.exit(1) on crash (for container
        // restart). With mocked deps those promises reject after the test moves on; stub
        // exit so a background crash can't tear down the mocha process mid-run.
        sinon.stub(process, 'exit');
    });

    afterEach(function(){
        sinon.restore();
    });

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

    describe('getBroadcaster', function(){
        it('returns null before server mode started', function(){
            assert.strictEqual(service.getBroadcaster(), null);
        });
    });

    describe('getSnapshotBuilder', function(){
        it('returns null before server mode started', function(){
            assert.strictEqual(service.getSnapshotBuilder(), null);
        });
    });

    describe('_waitForHub', function(){
        it('resolves immediately when hub is alive', async function(){
            sinon.stub(service.hubClient, 'ping').resolves(true);
            sinon.stub(service.util, 'sleep').resolves();
            await service._waitForHub();
            assert.strictEqual(service.hubClient.ping.calledOnce, true);
        });

        it('retries until hub responds', async function(){
            let stub = sinon.stub(service.hubClient, 'ping');
            stub.onFirstCall().resolves(false);
            stub.onSecondCall().resolves(false);
            stub.onThirdCall().resolves(true);
            sinon.stub(service.util, 'sleep').resolves();

            await service._waitForHub();
            assert.strictEqual(stub.callCount, 3);
        });
    });

    describe('_discoverChains', function(){
        it('skips already-known chains', async function(){
            service.databases.set('bitcoin:mainnet:indexer', { db: {}, config: {}, dbType: 'indexer' });
            sinon.stub(service.hubClient, 'getIndexerConfigs').resolves([{
                coin: 'bitcoin', network: 'mainnet', dbType: 'indexer',
                db_host: 'db', db_port: 3306, db_name: 'btc', db_user: 'u', db_pass: 'p'
            }]);
            sinon.stub(service.hubClient, 'getDecoderConfigs').resolves([]);

            let newChains = await service._discoverChains();
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
            sinon.stub(Database.prototype, 'close').resolves();
            let startSync = sinon.stub(service, '_startClientSyncForChain');

            let newChains = await service._discoverChains();

            assert.strictEqual(newChains.length, 1, 'only the non-excluded chain is set up');
            assert.strictEqual(service.databases.has('bitcoin:mainnet:indexer'), false, 'excluded chain absent');
            assert.strictEqual(service.databases.has('litecoin:mainnet:indexer'), true, 'kept chain present');
            // The excluded chain is skipped before createDatabase, so it never opens a pool.
            assert.strictEqual(createDb.callCount, 1);
            assert.strictEqual(startSync.callCount, 1);
        });

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
            sinon.stub(Database.prototype, 'close').resolves();
            let startSync = sinon.stub(service, '_startClientSyncForChain');

            let newChains = await service._discoverChains();
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
            sinon.stub(Database.prototype, 'close').resolves();
            sinon.stub(service, '_startClientSyncForChain');

            await service._discoverChains();
            assert.strictEqual(repl.called, false, 'no schema replication when the source DB is unreachable');
            // verifySyncTables runs for decoder replicas too (it is dbType-aware
            // internally: decoder gets sync_halt only). Without it the halt
            // table never exists and every client start logs a 1146 probe error.
            assert.strictEqual(vst.called, true, 'verifySyncTables runs for decoder replicas');
            assert.strictEqual(service.databases.size, 1);
        });

        it('server mode with REPLICA_DB_HOST re-serves from the local replica', async function(){
            config.SYNC_MODE = 'server';
            config.REPLICA_DB_HOST = 'localreplica';
            service = new SyncService(config);
            sinon.stub(service.hubClient, 'getIndexerConfigs').resolves([indexerCfg()]);
            sinon.stub(service.hubClient, 'getDecoderConfigs').resolves([]);
            sinon.stub(Database.prototype, 'verifySyncTables').resolves(true);
            let startPoller = sinon.stub(service, '_startPollerForChain');

            await service._discoverChains();
            let entry = service.databases.get('bitcoin:mainnet:indexer');
            assert.strictEqual(entry.db.host, 'localreplica');
            assert.strictEqual(startPoller.calledOnce, true);
        });

        it('server mode without REPLICA_DB_HOST connects to the hub-provided coordinates', async function(){
            config.SYNC_MODE = 'server';
            delete config.REPLICA_DB_HOST;
            service = new SyncService(config);
            sinon.stub(service.hubClient, 'getIndexerConfigs').resolves([indexerCfg()]);
            sinon.stub(service.hubClient, 'getDecoderConfigs').resolves([]);
            sinon.stub(Database.prototype, 'verifySyncTables').resolves(true);
            sinon.stub(service, '_startPollerForChain');

            await service._discoverChains();
            let entry = service.databases.get('bitcoin:mainnet:indexer');
            assert.strictEqual(entry.db.host, 'srchost');
        });
    });

    describe('_startServerMode', function(){
        it('creates broadcaster and snapshotBuilder', async function(){
            await service._startServerMode();
            assert.ok(service.broadcaster);
            assert.ok(service.snapshotBuilder);
        });

        it('starts a poller for each discovered database', async function(){
            let startPoller = sinon.stub(service, '_startPollerForChain');
            service.databases.set('a:b:indexer', { db: {}, config: indexerCfg() });
            service.databases.set('c:d:decoder', { db: {}, config: indexerCfg({ dbType: 'decoder' }) });
            await service._startServerMode();
            assert.strictEqual(startPoller.callCount, 2);
        });
    });

    describe('_startClientMode', function(){
        it('starts a ClientSync for each discovered database', async function(){
            let startSync = sinon.stub(service, '_startClientSyncForChain');
            service.databases.set('a:b:indexer', { db: {}, config: indexerCfg() });
            service.databases.set('c:d:indexer', { db: {}, config: indexerCfg({ coin: 'litecoin' }) });
            await service._startClientMode();
            assert.strictEqual(startSync.callCount, 2);
        });
    });

    describe('_startClientSyncForChain', function(){
        it('creates a ClientSync once and is idempotent on the same key', function(){
            sinon.stub(ClientSync.prototype, 'start').resolves();
            let db  = { dbType: 'indexer' };
            let cfg = indexerCfg();
            service._startClientSyncForChain('bitcoin:mainnet:indexer', db, cfg);
            assert.strictEqual(service.clientSyncs.size, 1);
            service._startClientSyncForChain('bitcoin:mainnet:indexer', db, cfg);
            assert.strictEqual(service.clientSyncs.size, 1);
        });

        it('exits the process when the background ClientSync crashes', async function(){
            let err = new Error('sync crash');
            sinon.stub(ClientSync.prototype, 'start').rejects(err);
            service._startClientSyncForChain('bitcoin:mainnet:indexer', { dbType: 'indexer' }, indexerCfg());
            // Let the unawaited .catch run.
            await new Promise(r => setImmediate(r));
            assert.ok(process.exit.calledWith(1));
        });
    });

    describe('_startPollerForChain', function(){
        it('does not create duplicate pollers', function(){
            service.broadcaster = { broadcast: sinon.stub(), updateStatus: sinon.stub() };
            let db = { getLastBlock: sinon.stub(), doQuery: sinon.stub() };
            let cfg = { coin: 'bitcoin', network: 'mainnet' };

            service._startPollerForChain('bitcoin:mainnet', db, cfg);
            assert.strictEqual(service.pollers.size, 1);

            service._startPollerForChain('bitcoin:mainnet', db, cfg);
            assert.strictEqual(service.pollers.size, 1);
        });

        it('exits the process when the background poller crashes', async function(){
            sinon.stub(ServerPoller.prototype, 'start').rejects(new Error('poller crash'));
            service.broadcaster = { broadcast: sinon.stub(), updateStatus: sinon.stub() };
            service._startPollerForChain('bitcoin:mainnet:indexer', { dbType: 'indexer' }, indexerCfg());
            await new Promise(r => setImmediate(r));
            assert.ok(process.exit.calledWith(1));
        });
    });

    describe('_scheduleHubRepoll', function(){
        it('sets up an interval', function(){
            let clock = sinon.useFakeTimers();
            service._scheduleHubRepoll();
            clock.restore();
        });

        it('re-discovers chains on each interval tick and logs new chains', async function(){
            let clock = sinon.useFakeTimers();
            let disc = sinon.stub(service, '_discoverChains').resolves([{ key: 'x' }]);
            service._scheduleHubRepoll();
            await clock.tickAsync(config.HUB_REPOLL_INTERVAL);
            assert.strictEqual(disc.calledOnce, true);
            clock.restore();
        });

        it('logs (does not throw) when a re-poll fails', async function(){
            let clock = sinon.useFakeTimers();
            sinon.stub(service, '_discoverChains').rejects(new Error('repoll boom'));
            service._scheduleHubRepoll();
            await clock.tickAsync(config.HUB_REPOLL_INTERVAL);
            assert.ok(console.error.getCalls().some(c => /Hub re-poll error/.test(c.args[0])));
            clock.restore();
        });
    });

    describe('_waitForHub timeout', function(){
        it('exits the process after MAX_HUB_WAIT_MS with no hub', async function(){
            config.MAX_HUB_WAIT_MS = 0;
            service = new SyncService(config);
            // process.exit is already stubbed in beforeEach; reconfigure it to throw so
            // the otherwise-infinite while(true) loop unwinds after the timeout branch.
            process.exit.callsFake(() => { throw new Error('PROC_EXIT'); });
            sinon.stub(service.hubClient, 'ping').resolves(false);
            sinon.stub(service.util, 'sleep').resolves();
            await assert.rejects(() => service._waitForHub(), /PROC_EXIT/);
            assert.ok(process.exit.calledWith(1));
        });
    });

    describe('getHubConfigAgeSeconds', function(){
        it('returns null when the hub has never answered', function(){
            service.hubClient.lastSuccessfulFetchAt = null;
            assert.strictEqual(service.getHubConfigAgeSeconds(), null);
        });
        it('returns whole seconds since the last successful fetch', function(){
            service.hubClient.lastSuccessfulFetchAt = Date.now() - 5000;
            let age = service.getHubConfigAgeSeconds();
            assert.ok(age >= 5 && age <= 6);
        });
    });

    describe('getClientSyncState', function(){
        it('returns nulls/false when no sync exists for the key', function(){
            assert.deepStrictEqual(service.getClientSyncState('bitcoin', 'mainnet'),
                { lastKnownServerBlock: null, sourceHeightStale: null, halted: false,
                  haltInfo: null, truncated: false, bootstrapBase: null });
        });
        it('reports a live sync, including halt info when halted', function(){
            let fakeSync = {
                lastKnownServerBlock: 42,
                isSourceHeightStale: () => false,
                isHalted: () => true,
                getHaltInfo: () => ({ blockIndex: 42, reason: 'divergence' }),
                isTruncated: () => true,
                getBootstrapBase: () => 850000
            };
            service.clientSyncs.set('bitcoin:mainnet:indexer', fakeSync);
            let state = service.getClientSyncState('bitcoin', 'mainnet');
            assert.strictEqual(state.lastKnownServerBlock, 42);
            assert.strictEqual(state.sourceHeightStale, false);
            assert.strictEqual(state.halted, true);
            assert.deepStrictEqual(state.haltInfo, { blockIndex: 42, reason: 'divergence' });
            assert.strictEqual(state.truncated, true);
            assert.strictEqual(state.bootstrapBase, 850000);
        });
        it('omits halt info for a healthy sync', function(){
            service.clientSyncs.set('bitcoin:mainnet:indexer',
                { lastKnownServerBlock: 7, isSourceHeightStale: () => null,
                  isHalted: () => false, getHaltInfo: () => ({}),
                  isTruncated: () => false, getBootstrapBase: () => null });
            let state = service.getClientSyncState('bitcoin', 'mainnet');
            assert.strictEqual(state.halted, false);
            assert.strictEqual(state.haltInfo, null);
            assert.strictEqual(state.truncated, false);
            assert.strictEqual(state.bootstrapBase, null);
        });
    });

    describe('getClientSync', function(){
        it('returns the live sync or null', function(){
            assert.strictEqual(service.getClientSync('bitcoin', 'mainnet'), null);
            let sync = {};
            service.clientSyncs.set('bitcoin:mainnet:decoder', sync);
            assert.strictEqual(service.getClientSync('bitcoin', 'mainnet', 'decoder'), sync);
        });
    });

    describe('mode branching in start', function(){
        it('calls _startServerMode for server mode', async function(){
            sinon.stub(service, '_waitForHub').resolves();
            sinon.stub(service, '_discoverChains').resolves([]);
            sinon.stub(service, '_startServerMode').resolves();
            sinon.stub(service, '_scheduleHubRepoll');

            await service.start();
            assert.strictEqual(service._startServerMode.calledOnce, true);
        });

        it('calls _startClientMode for client mode', async function(){
            config.SYNC_MODE = 'client';
            service = new SyncService(config);
            sinon.stub(service, '_waitForHub').resolves();
            sinon.stub(service, '_discoverChains').resolves([]);
            sinon.stub(service, '_startClientMode').resolves();
            sinon.stub(service, '_scheduleHubRepoll');

            await service.start();
            assert.strictEqual(service._startClientMode.calledOnce, true);
        });
    });
});
