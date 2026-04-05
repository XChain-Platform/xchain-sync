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

const SyncService = proxyquire('../../src/SyncService', {
    './db': proxyquire('../../src/db', { 'mariadb': mariadbStub })
});
const TransparencyLog = require('../../src/TransparencyLog');

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

        it('returns db for known chain/network', function(){
            let mockDb = { doQuery: sinon.stub() };
            service.databases.set('bitcoin:mainnet', { db: mockDb, config: {} });
            assert.strictEqual(service.getDatabase('bitcoin', 'mainnet'), mockDb);
        });
    });

    describe('getChains', function(){
        it('returns empty array when no chains', function(){
            assert.deepStrictEqual(service.getChains(), []);
        });

        it('returns array of chain/network pairs', function(){
            service.databases.set('bitcoin:mainnet', { db: {}, config: { coin: 'bitcoin', network: 'mainnet' } });
            service.databases.set('litecoin:testnet', { db: {}, config: { coin: 'litecoin', network: 'testnet' } });
            let chains = service.getChains();
            assert.strictEqual(chains.length, 2);
            assert.deepStrictEqual(chains[0], { coin: 'bitcoin', network: 'mainnet' });
        });
    });

    describe('getTransparencyLog', function(){
        it('returns null for unknown chain/network', function(){
            assert.strictEqual(service.getTransparencyLog('bitcoin', 'mainnet'), null);
        });

        it('returns poller transparency log when poller exists', function(){
            let mockLog = { recordBlock: sinon.stub() };
            service.databases.set('bitcoin:mainnet', { db: {}, config: {} });
            service.pollers.set('bitcoin:mainnet', { transparencyLog: mockLog });
            assert.strictEqual(service.getTransparencyLog('bitcoin', 'mainnet'), mockLog);
        });

        it('creates a temporary TransparencyLog when no poller', function(){
            let mockDb = {};
            service.databases.set('bitcoin:mainnet', { db: mockDb, config: {} });
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
            service.databases.set('bitcoin:mainnet', { db: {}, config: {} });
            sinon.stub(service.hubClient, 'getIndexerConfigs').resolves([{
                coin: 'bitcoin', network: 'mainnet',
                db_host: 'db', db_port: 3306, db_name: 'btc', db_user: 'u', db_pass: 'p'
            }]);

            let newChains = await service._discoverChains();
            assert.strictEqual(newChains.length, 0);
        });
    });

    describe('_startServerMode', function(){
        it('creates broadcaster and snapshotBuilder', async function(){
            await service._startServerMode();
            assert.ok(service.broadcaster);
            assert.ok(service.snapshotBuilder);
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
    });

    describe('_scheduleHubRepoll', function(){
        it('sets up an interval', function(){
            let clock = sinon.useFakeTimers();
            service._scheduleHubRepoll();
            clock.restore();
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
