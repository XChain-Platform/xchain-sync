// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.
//
// SYNC_META_RETENTION_BLOCKS in CLIENT mode.
//
// The window used to be read in exactly one place, startPollerForChain, so a client
// never built a TransparencyLog and never pruned. The source's own
// DELETE FROM sync_meta is not carried over replication either (ServerPoller streams
// one row per block, ClientApplier only INSERT IGNOREs), so a configured window was
// inert and a replica's sync_meta grew for the life of the chain.

const assert = require('assert');
const sinon  = require('sinon');
const proxyquire = require('proxyquire').noCallThru();

const mockPool = {
    getConnection: sinon.stub().resolves({ query: sinon.stub(), release: sinon.stub() }),
    end: sinon.stub().resolves()
};
const mariadbStub = {
    createPool: sinon.stub().returns(mockPool),
    createConnection: sinon.stub().resolves({ query: sinon.stub().resolves([]), end: sinon.stub().resolves() })
};

const Database        = proxyquire('../../src/db', { 'mariadb': mariadbStub });
const SyncService     = proxyquire('../../src/SyncService', { './db': Database });
const TransparencyLog = require('../../src/server/transparency_log');
const ClientSync      = require('../../src/client/sync');

function baseConfig(over){
    return Object.assign({
        SYNC_MODE: 'client',
        HUB_API_HOST: 'localhost', HUB_PORT: 10000, HUB_REPOLL_INTERVAL: 300000,
        BLOCK_POLL_INTERVAL: 3000, SYNC_SOURCES: '', VERIFY_HASHES: true,
        REPLICA_DB_HOST: 'localhost', REPLICA_DB_PORT: 3306,
        REPLICA_DB_USER: 'u', REPLICA_DB_PASS: 'p',
        REPLICA_DB_READONLY: false,
        MERKLE_EPOCH_SIZE: 100,
        SYNC_META_RETENTION_BLOCKS: 0,
        SYNC_META_RETENTION_INTERVAL_MS: 60000
    }, over || {});
}

describe('client-mode sync_meta retention', function(){

    let service, clock;

    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
        sinon.stub(console, 'warn');
    });

    afterEach(function(){
        if(service && service._syncMetaRetentionTimer){
            clearInterval(service._syncMetaRetentionTimer);
            service._syncMetaRetentionTimer = null;
        }
        if(clock){ clock.restore(); clock = null; }
        sinon.restore();
    });

    it('starts no timer when the window is 0 (the shipped default)', function(){
        service = new SyncService(baseConfig());
        service.databases.set('bitcoin:mainnet:indexer', { db: {}, config: {}, dbType: 'indexer' });
        service.startSyncMetaRetention();
        assert.strictEqual(service._syncMetaRetentionTimer, undefined,
            'a zero window must arm nothing at all: no timer, no deletes');
    });

    it('starts no timer in server mode, where recordBlock already prunes', function(){
        service = new SyncService(baseConfig({ SYNC_MODE: 'server', SYNC_META_RETENTION_BLOCKS: 500 }));
        service.databases.set('bitcoin:mainnet:indexer', { db: {}, config: {}, dbType: 'indexer' });
        service.startSyncMetaRetention();
        assert.strictEqual(service._syncMetaRetentionTimer, undefined);
    });

    it('sweeps every indexer DB on the timer, and no decoder DB', async function(){
        clock = sinon.useFakeTimers();
        const prune = sinon.stub(TransparencyLog.prototype, 'pruneSyncMeta')
            .resolves({ enabled: true, deleted: 0 });

        service = new SyncService(baseConfig({ SYNC_META_RETENTION_BLOCKS: 500 }));
        const idxA = { tag: 'idxA' }, idxB = { tag: 'idxB' }, dec = { tag: 'dec' };
        service.databases.set('bitcoin:mainnet:indexer',  { db: idxA, config: {}, dbType: 'indexer' });
        service.databases.set('litecoin:mainnet:indexer', { db: idxB, config: {}, dbType: 'indexer' });
        service.databases.set('bitcoin:mainnet:decoder',  { db: dec,  config: {}, dbType: 'decoder' });

        service.startSyncMetaRetention();
        assert.ok(service._syncMetaRetentionTimer, 'an armed window must start the timer');
        assert.strictEqual(prune.callCount, 0, 'nothing is swept before the first tick');

        await clock.tickAsync(60000);
        assert.strictEqual(prune.callCount, 2,
            'both indexer DBs swept, the decoder DB skipped (it has no sync_meta)');
        const swept = prune.getCalls().map(c => c.thisValue.db.tag).sort();
        assert.deepStrictEqual(swept, ['idxA', 'idxB']);
        // The log the sweep drives must carry the window, or pruneSyncMeta no-ops.
        assert.strictEqual(prune.getCall(0).thisValue.retentionBlocks, 500);
        assert.strictEqual(prune.getCall(0).thisValue.readOnly, false);
    });

    it('honours REPLICA_DB_READONLY: a serve-only replica deletes nothing', async function(){
        clock = sinon.useFakeTimers();
        const prune = sinon.stub(TransparencyLog.prototype, 'pruneSyncMeta').callThrough();

        service = new SyncService(baseConfig({ SYNC_META_RETENTION_BLOCKS: 500, REPLICA_DB_READONLY: true }));
        service.databases.set('bitcoin:mainnet:indexer',
            { db: { doQuery: sinon.stub().resolves([]) }, config: {}, dbType: 'indexer' });
        service.startSyncMetaRetention();
        await clock.tickAsync(60000);

        assert.strictEqual(prune.callCount, 1);
        const result = await prune.returnValues[0];
        assert.strictEqual(result.skipped, true);
        assert.strictEqual(result.reason, 'read_only');
        assert.strictEqual(result.deleted, 0);
    });

    it('one DB failing does not stop the sweep or kill the loop', async function(){
        clock = sinon.useFakeTimers();
        const prune = sinon.stub(TransparencyLog.prototype, 'pruneSyncMeta');
        prune.onFirstCall().rejects(new Error('deadlock'));
        prune.resolves({ enabled: true, deleted: 0 });

        service = new SyncService(baseConfig({ SYNC_META_RETENTION_BLOCKS: 500 }));
        service.databases.set('bitcoin:mainnet:indexer',  { db: { tag: 'a' }, config: {}, dbType: 'indexer' });
        service.databases.set('litecoin:mainnet:indexer', { db: { tag: 'b' }, config: {}, dbType: 'indexer' });
        service.startSyncMetaRetention();

        await clock.tickAsync(60000);
        assert.strictEqual(prune.callCount, 2, 'the second DB is still swept after the first throws');
        await clock.tickAsync(60000);
        assert.strictEqual(prune.callCount, 4, 'and the timer survives to the next tick');
    });

    it('getTransparencyLog passes readOnly and the window instead of dropping them', function(){
        service = new SyncService(baseConfig({ SYNC_META_RETENTION_BLOCKS: 500, REPLICA_DB_READONLY: true }));
        service.databases.set('bitcoin:mainnet:indexer', { db: {}, config: {}, dbType: 'indexer' });
        // Client mode has no pollers, so this fallback is the ONLY log a client ever gets.
        const log = service.getTransparencyLog('bitcoin', 'mainnet');
        assert.ok(log instanceof TransparencyLog);
        assert.strictEqual(log.retentionBlocks, 500, 'the window was dropped on the floor');
        assert.strictEqual(log.readOnly, true, 'readOnly was dropped on the floor');
    });
});

describe('sync_meta count parity while the client window is armed', function(){

    function clientSync(config){
        const cs = Object.create(ClientSync.prototype);
        cs.config  = config;
        cs.dbType  = 'indexer';
        cs.chain   = 'bitcoin';
        cs.network = 'mainnet';
        cs.db      = { getTableCount: async () => 0 };
        return cs;
    }

    it('is armed only for a writable client with a positive window', function(){
        const armed = (over) => clientSync(baseConfig(over)).syncMetaWindowArmed();
        assert.strictEqual(armed({ SYNC_META_RETENTION_BLOCKS: 500 }), true);
        assert.strictEqual(armed({ SYNC_META_RETENTION_BLOCKS: 0 }), false, 'default window');
        assert.strictEqual(armed({ SYNC_META_RETENTION_BLOCKS: 'abc' }), false, 'unparseable window');
        assert.strictEqual(armed({ SYNC_META_RETENTION_BLOCKS: 500, SYNC_MODE: 'server' }), false);
        assert.strictEqual(armed({ SYNC_META_RETENTION_BLOCKS: 500, REPLICA_DB_READONLY: true }), false);
    });

    it('excludes sync_meta from the shortfall check only while the window is armed', async function(){
        sinon.stub(console, 'error');
        try {
            const counts = { sync_meta: 1000, blocks: 1000 };

            const armed = clientSync(baseConfig({ SYNC_META_RETENTION_BLOCKS: 500 }));
            const armedMismatches = await armed.verifyTableCounts(counts, undefined, {});
            assert.deepStrictEqual(armedMismatches.map(m => m.table), ['blocks'],
                'a pruning client must not report its own retention as a replication hole');

            const off = clientSync(baseConfig({ SYNC_META_RETENTION_BLOCKS: 0 }));
            const offMismatches = await off.verifyTableCounts(counts, undefined, {});
            assert.deepStrictEqual(offMismatches.map(m => m.table).sort(), ['blocks', 'sync_meta'],
                'with no window nothing is pruned locally, so sync_meta stays strictly compared');
        } finally {
            sinon.restore();
        }
    });
});
