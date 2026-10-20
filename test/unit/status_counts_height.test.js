// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// Every /status field a follower compares at a height must have been computed at that
// height. table_counts carry their own label (table_counts_height), read by bracketing
// the count loop with the live tip, and the content-parity lookup id ceiling comes from
// the poller's read at the broadcast height rather than from the live MAX(id).

const assert     = require('assert');
const sinon      = require('sinon');
const proxyquire = require('proxyquire');
const replicatedTables = require('../../src/schema/replicated_tables');

function loadApi(mode, env){
    let saved = {};
    let vars = Object.assign({ SYNC_MODE: mode }, env || {});
    for(let k of Object.keys(vars)){ saved[k] = process.env[k]; process.env[k] = vars[k]; }
    let api = proxyquire('../../src/api', {});
    for(let k of Object.keys(vars)){
        if(saved[k] === undefined) delete process.env[k]; else process.env[k] = saved[k];
    }
    return api;
}

function mockDb(tips, dbType){
    let getLastBlock = sinon.stub();
    tips.forEach((t, i) => getLastBlock.onCall(i).resolves(t));
    getLastBlock.resolves(tips[tips.length - 1]);
    return {
        dbName: 'db', dbType: dbType || 'indexer',
        getLastBlock,
        getBlockHashRow: sinon.stub().resolves({ block_index: 99, block_time: 1, ledger_hash: 'a',
                                                 actions_hash: 'b', contract_hash: 'c' }),
        getTableCount:   sinon.stub().resolves(7),
        listExistingTables: sinon.stub().resolves(new Set(['blocks']))
    };
}

function serverSyncService(pollerStatus, poller){
    return {
        getBroadcaster: () => ({
            getStatus: () => pollerStatus,
            getSubscribers: () => []
        }),
        getSnapshotBuilder: () => ({ snapshotsServed: 0, snapshotsRejected: 0 }),
        getPoller: () => poller || null
    };
}

function clientSyncService(){
    return {
        getClientSync: () => ({ getMissingTables: () => [] }),
        getClientSyncState: () => ({
            lastKnownServerBlock: 100, sourceHeightStale: false, halted: false, haltInfo: null,
            truncated: false, bootstrapBase: null, sourceQuorum: 1, sourcesConfigured: 1,
            sourcesActive: 1, sourcesAgreeing: 1, sourcesEvicted: []
        })
    };
}

const POLLED_99 = { block_height: 99, source_block_height: 99 };

describe('/status table_counts_height', function(){

    afterEach(function(){ sinon.restore(); });

    it('labels server counts with the live tip they were read at, apart from block_height', async function(){
        // The poller has broadcast 99 while the database already holds 100: the counts
        // describe 100, and saying so is what stops a follower at 99 comparing them.
        let { buildStatusRow } = loadApi('server');
        let row = await buildStatusRow(serverSyncService(POLLED_99), mockDb([100, 100]),
                                       'indexer', 'bitcoin', 'mainnet');
        assert.strictEqual(row.block_height, 99);
        assert.strictEqual(row.table_counts_height, 100);
        assert.strictEqual(row.table_counts.blocks, 7);
    });

    it('publishes null when a block lands between the two tip reads', async function(){
        let { buildStatusRow } = loadApi('server');
        let row = await buildStatusRow(serverSyncService(POLLED_99), mockDb([99, 100]),
                                       'indexer', 'bitcoin', 'mainnet');
        assert.strictEqual(row.table_counts_height, null);
    });

    it('publishes null when a rollback lands between the two tip reads', async function(){
        let { buildStatusRow } = loadApi('server');
        let row = await buildStatusRow(serverSyncService(POLLED_99), mockDb([99, 97]),
                                       'indexer', 'bitcoin', 'mainnet');
        assert.strictEqual(row.table_counts_height, null);
    });

    it('publishes null when the tip cannot be read', async function(){
        let { buildStatusRow } = loadApi('server');
        let db = mockDb([99]);
        db.getLastBlock = sinon.stub().rejects(new Error('pool closed'));
        let row = await buildStatusRow(serverSyncService(POLLED_99), db, 'indexer', 'bitcoin', 'mainnet');
        assert.strictEqual(row.table_counts_height, null);
    });

    it('labels client counts the same way', async function(){
        let { buildStatusRow } = loadApi('client');
        // First read is the row's own block_height, then the bracket pair.
        let steady = await buildStatusRow(clientSyncService(), mockDb([100, 100, 100]),
                                          'indexer', 'bitcoin', 'mainnet');
        assert.strictEqual(steady.table_counts_height, 100);
        let moved = await buildStatusRow(clientSyncService(), mockDb([100, 100, 101]),
                                         'indexer', 'bitcoin', 'mainnet');
        assert.strictEqual(moved.table_counts_height, null);
    });
});

describe('/status content-parity lookup id ceiling', function(){

    afterEach(function(){ sinon.restore(); });

    const idTable = replicatedTables.contentParityPlan('indexer').find(s => s.bound === 'id').table;

    function parityDb(){
        let db = mockDb([99, 99]);
        db.listExistingTables  = sinon.stub().resolves(new Set([idTable]));
        db.getMaxRowId         = sinon.stub().resolves(50);
        db.getContentIdWindowRows = sinon.stub().callsFake(async (t, from, to) =>
            [{ id: to, v: 'x' }]);
        db.getContentWindowRows = sinon.stub().resolves([]);
        return db;
    }

    function pollerWith(ceiling){
        return { getContentParityIdCeiling: () => ceiling };
    }

    it('bounds lookups by the poller ceiling read at the published height, not the live MAX(id)', async function(){
        let { buildStatusRow } = loadApi('server', { TABLE_CONTENT_PARITY_CHECK: 'true' });
        let db = parityDb();
        let ceiling = { block: 99, ids: { [idTable]: 40 } };
        let row = await buildStatusRow(serverSyncService(POLLED_99, pollerWith(ceiling)), db,
                                       'indexer', 'bitcoin', 'mainnet');
        assert.strictEqual(row.table_content_parity.tables[idTable].id_max, '40');
        assert.strictEqual(db.getMaxRowId.called, false, 'the live ceiling includes ids past height 99');
    });

    it('omits a lookup the ceiling does not cover rather than reading its live MAX(id)', async function(){
        let { buildStatusRow } = loadApi('server', { TABLE_CONTENT_PARITY_CHECK: 'true' });
        let db = parityDb();
        let row = await buildStatusRow(serverSyncService(POLLED_99, pollerWith({ block: 99, ids: {} })), db,
                                       'indexer', 'bitcoin', 'mainnet');
        assert.ok(!(idTable in row.table_content_parity.tables));
        assert.strictEqual(db.getMaxRowId.called, false);
    });

    it('falls back to the live MAX(id) when the ceiling is labelled with another height', async function(){
        // Control for the two cases above: the same db and poller wiring with a stale
        // label must reach getMaxRowId, so those cases could have failed.
        let { buildStatusRow } = loadApi('server', { TABLE_CONTENT_PARITY_CHECK: 'true' });
        let db = parityDb();
        let row = await buildStatusRow(serverSyncService(POLLED_99, pollerWith({ block: 98, ids: { [idTable]: 40 } })), db,
                                       'indexer', 'bitcoin', 'mainnet');
        assert.strictEqual(db.getMaxRowId.called, true);
        assert.strictEqual(row.table_content_parity.tables[idTable].id_max, '50');
    });
});
