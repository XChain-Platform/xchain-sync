// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// Silent-partial-replication visibility .
//
// Every apply path tolerates MariaDB errno 1146 so a replica whose schema lags
// its source does not wedge on a table the source has gained. The cost is that
// the gap is invisible: the replica reports halted:false and lag_blocks:0 while
// entire tables never arrive, and the only trace is a repeating
// ER_NO_SUCH_TABLE stack per table per apply (observed live: the six BET tables
// on the regtest replicas, ~480 error lines in 20 minutes under a green
// /status). These tests pin the two signals that replace log-scraping: the
// startup WARN naming the tables, and the /status `missing_tables` array.

const assert     = require('assert');
const sinon      = require('sinon');
const proxyquire = require('proxyquire');
const ClientSync = require('../../src/ClientSync');
const HashVerifier = require('../../src/HashVerifier');
const Utility    = require('../../src/utility');
const { getReplicatedTables, missingReplicatedTables } = require('../../src/replicatedTables');

// The BET family: the tables the live observation found missing on a replica
// whose source was already writing them.
const BET_TABLES = ['bets', 'bet_statuses', 'bet_resolves', 'bet_feeds',
                    'bet_feed_statuses', 'bet_cancels'];

// A schema listing that holds every replicated table except `absent`.
function presentExcept(dbType, absent){
    let skip = new Set(absent);
    return new Set(getReplicatedTables(dbType).filter(t => !skip.has(t)));
}

describe('missingReplicatedTables ', function(){

    it('returns an empty array when the schema carries every replicated table', function(){
        let present = new Set(getReplicatedTables('indexer'));
        assert.deepStrictEqual(missingReplicatedTables(present, 'indexer'), []);
    });

    it('names every replicated table the schema lacks, sorted', function(){
        let missing = missingReplicatedTables(presentExcept('indexer', BET_TABLES), 'indexer');
        // Only assert on tables this build actually replicates: the BET family is
        // registry-driven, so guard against a build where it is not yet streamed.
        let expected = BET_TABLES.filter(t => getReplicatedTables('indexer').includes(t)).sort();
        assert.ok(expected.length > 0, 'expected the BET tables to be in the replicated set');
        assert.deepStrictEqual(missing, expected);
    });

    it('ignores extra local tables (only source-side coverage matters)', function(){
        let present = new Set(getReplicatedTables('indexer'));
        present.add('some_operator_local_table');
        assert.deepStrictEqual(missingReplicatedTables(present, 'indexer'), []);
    });

    it('scopes to the dbType (decoder set, not indexer set)', function(){
        let present = new Set(getReplicatedTables('decoder'));
        assert.deepStrictEqual(missingReplicatedTables(present, 'decoder'), []);
        // The same decoder schema is missing most of the indexer set.
        assert.ok(missingReplicatedTables(present, 'indexer').length > 0);
    });

    it('returns null (unknown), never [], when the table listing is unavailable', function(){
        // [] would read as "nothing missing" and certify a replica whose schema
        // could not even be read.
        assert.strictEqual(missingReplicatedTables(null, 'indexer'), null);
        assert.strictEqual(missingReplicatedTables(undefined, 'indexer'), null);
        assert.strictEqual(missingReplicatedTables(['bets'], 'indexer'), null);
    });
});

describe('ClientSync._warnMissingTables ', function(){

    function makeSync(dbOverrides, dbType){
        let db = Object.assign({
            dbName:            'test_db',
            dbType:            dbType || 'indexer',
            getLastBlock:      sinon.stub().resolves(null),
            getBlockHashRow:   sinon.stub().resolves(null),
            doQuery:           sinon.stub().resolves([]),
            getActiveHalt:     sinon.stub().resolves(null),
            getTableCount:     sinon.stub().resolves(0),
            listExistingTables: sinon.stub().resolves(new Set(getReplicatedTables(dbType || 'indexer')))
        }, dbOverrides || {});
        let applier = { applyBlock: sinon.stub().resolves() };
        let rb      = { rollback: sinon.stub().resolves() };
        let config  = {
            SYNC_SOURCES:           'http://src1:3006',
            VERIFY_HASHES:          false,
            CLIENT_RECONNECT_DELAY: 5000,
            HASH_CONFIRM_TIMEOUT:   5000,
            SNAPSHOT_MAX_CONTENT:   1024,
            WS_MAX_PAYLOAD:         1024,
            MAX_ROLLBACK_DEPTH:     10,
            GAP_LOG_INTERVAL_MS:    30000
        };
        let sync = new ClientSync('bitcoin', 'mainnet', db, applier, rb,
                                  new HashVerifier(), config, new Utility());
        return { sync, db };
    }

    let warnStub, errorStub;
    beforeEach(function(){
        warnStub  = sinon.stub(console, 'warn');
        errorStub = sinon.stub(console, 'error');
        sinon.stub(console, 'log');
    });
    afterEach(function(){ sinon.restore(); });

    it('warns ONCE naming every missing table', async function(){
        let { sync } = makeSync({
            listExistingTables: sinon.stub().resolves(presentExcept('indexer', BET_TABLES))
        });

        await sync._warnMissingTables();

        let calls = warnStub.getCalls().filter(c => String(c.args[0]).indexOf('MISSING_REPLICATED_TABLES') === 0);
        assert.strictEqual(calls.length, 1, 'expected exactly one missing-table WARN');
        let msg = calls[0].args[0];
        for(let t of BET_TABLES){
            if(!getReplicatedTables('indexer').includes(t)) continue;
            assert.ok(msg.indexOf(t) !== -1, 'WARN should name ' + t);
        }
        assert.ok(msg.indexOf('bitcoin/mainnet/indexer') !== -1, 'WARN should identify the replica');
    });

    it('stays silent when the schema is complete', async function(){
        let { sync } = makeSync();
        await sync._warnMissingTables();
        let calls = warnStub.getCalls().filter(c => String(c.args[0]).indexOf('MISSING_REPLICATED_TABLES') === 0);
        assert.strictEqual(calls.length, 0);
        assert.deepStrictEqual(sync.getMissingTables(), []);
    });

    it('exposes the list via getMissingTables()', async function(){
        let { sync } = makeSync({
            listExistingTables: sinon.stub().resolves(presentExcept('indexer', ['bets']))
        });
        await sync._warnMissingTables();
        assert.deepStrictEqual(sync.getMissingTables(), ['bets']);
    });

    it('is advisory: a listing failure never throws, and leaves the list unknown (null)', async function(){
        let { sync } = makeSync({
            listExistingTables: sinon.stub().rejects(new Error('connection lost'))
        });
        await sync._warnMissingTables();  // must not reject
        assert.strictEqual(sync.getMissingTables(), null);
        assert.ok(errorStub.called, 'the failure itself should be logged');
    });

    it('reports null before the check has run', function(){
        let { sync } = makeSync();
        assert.strictEqual(sync.getMissingTables(), null);
    });

    it('runs during start(), before the replica enters live-follow', async function(){
        // The signal is worthless if nothing calls it: pin the wiring, and pin the
        // ORDER (an operator must see the gap named before the replica starts
        // reporting halted:false / lag_blocks:0 against tables that never arrive).
        let { sync } = makeSync({
            getLastBlock:    sinon.stub().resolves(100),
            getBlockHashRow: sinon.stub().resolves({ block_index: 100 }),
            listExistingTables: sinon.stub().resolves(presentExcept('indexer', ['bets']))
        });
        sinon.stub(sync, '_fetchAndApplySchema').resolves();
        sinon.stub(sync, '_incrementalCatchUp').resolves();
        let connect = sinon.stub(sync, '_connectWebSockets').callsFake(() => { sync.running = false; });
        let warn = sinon.spy(sync, '_warnMissingTables');

        await sync.start();

        assert.strictEqual(warn.callCount, 1, 'start() must run the missing-table check');
        assert.ok(warn.calledBefore(connect), 'the check must run before live-follow opens');
        assert.deepStrictEqual(sync.getMissingTables(), ['bets']);
    });
});

describe('/status missing_tables ', function(){

    // api.js reads SYNC_MODE once at require time, so each mode gets its own
    // freshly-loaded copy of the module.
    function loadApi(mode){
        let prior = process.env.SYNC_MODE;
        process.env.SYNC_MODE = mode;
        let api = proxyquire('../../src/api', {});
        if(prior === undefined) delete process.env.SYNC_MODE; else process.env.SYNC_MODE = prior;
        return api;
    }

    function mockDb(present, dbType){
        return {
            dbName:  'replica_db',
            dbType:  dbType || 'indexer',
            getLastBlock:    sinon.stub().resolves(100),
            getBlockHashRow: sinon.stub().resolves({ block_index: 100, block_time: 1, ledger_hash: 'a',
                                                     actions_hash: 'b', contract_hash: 'c' }),
            getTableCount:   sinon.stub().resolves(7),
            listExistingTables: present instanceof Error
                ? sinon.stub().rejects(present)
                : sinon.stub().resolves(present)
        };
    }

    // A client-mode SyncService that reports the exact shape the live incident
    // showed: caught up, not halted, nothing wrong.
    function mockClientSyncService(){
        return {
            getClientSyncState: () => ({
                lastKnownServerBlock: 100, sourceHeightStale: false, halted: false, haltInfo: null,
                truncated: false, bootstrapBase: null, sourceQuorum: 1, sourcesConfigured: 1,
                sourcesActive: 1, sourcesAgreeing: 1, sourcesEvicted: []
            })
        };
    }

    afterEach(function(){ sinon.restore(); });

    it('names the missing tables on a replica that otherwise looks perfectly healthy', async function(){
        let { buildStatusRow } = loadApi('client');
        let row = await buildStatusRow(mockClientSyncService(),
                                       mockDb(presentExcept('indexer', BET_TABLES)),
                                       'indexer', 'bitcoin', 'mainnet');

        // The exact green-status shape that hid the gap in production.
        assert.strictEqual(row.halted, false);
        assert.strictEqual(row.lag_blocks, 0);
        // table_counts cannot show the gap: an absent table is simply not a key.
        for(let t of BET_TABLES) assert.ok(!(t in row.table_counts));
        // missing_tables can, and does.
        let expected = BET_TABLES.filter(t => getReplicatedTables('indexer').includes(t)).sort();
        assert.deepStrictEqual(row.missing_tables, expected);
    });

    it('is an empty array on a complete replica', async function(){
        let { buildStatusRow } = loadApi('client');
        let row = await buildStatusRow(mockClientSyncService(),
                                       mockDb(new Set(getReplicatedTables('indexer'))),
                                       'indexer', 'bitcoin', 'mainnet');
        assert.deepStrictEqual(row.missing_tables, []);
    });

    it('is null, not [], when the table listing fails', async function(){
        let { buildStatusRow } = loadApi('client');
        let row = await buildStatusRow(mockClientSyncService(),
                                       mockDb(new Error('information_schema unavailable')),
                                       'indexer', 'bitcoin', 'mainnet');
        assert.strictEqual(row.missing_tables, null);
    });

    it('scopes to the decoder replicated set on a decoder replica', async function(){
        let { buildStatusRow } = loadApi('client');
        let row = await buildStatusRow(mockClientSyncService(),
                                       mockDb(presentExcept('decoder', ['pubkeys']), 'decoder'),
                                       'decoder', 'bitcoin', 'mainnet');
        assert.deepStrictEqual(row.missing_tables, ['pubkeys']);
    });

    it('is published in server mode too (a source missing a table it should stream)', async function(){
        let { buildStatusRow } = loadApi('server');
        let syncService = {
            getBroadcaster: () => ({
                statusData: new Map([['bitcoin:mainnet:indexer', { block_height: 100, source_block_height: 100 }]]),
                getSubscribers: () => []
            }),
            getSnapshotBuilder: () => ({ snapshotsServed: 0, snapshotsRejected: 0 })
        };
        let row = await buildStatusRow(syncService,
                                       mockDb(presentExcept('indexer', ['bets'])),
                                       'indexer', 'bitcoin', 'mainnet');
        assert.deepStrictEqual(row.missing_tables, ['bets']);
    });
});
