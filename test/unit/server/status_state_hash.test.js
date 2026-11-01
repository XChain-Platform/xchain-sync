// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// An indexer /status row publishes state_hash beside the three chained hashes, so a
// follower can compare in-place state across sources; a decoder row never carries it.

const assert     = require('assert');
const sinon      = require('sinon');
const proxyquire = require('proxyquire');

function loadApi(mode){
    let saved = process.env.SYNC_MODE;
    process.env.SYNC_MODE = mode;
    let api = proxyquire('../../../src/api', {});
    if(saved === undefined) delete process.env.SYNC_MODE; else process.env.SYNC_MODE = saved;
    return api;
}

function mockDb(dbType, state){
    return {
        dbName: 'db', dbType,
        getLastBlock:    sinon.stub().resolves(99),
        getBlockHashRow: sinon.stub().resolves({ block_index: 99, block_time: 1, block_hash: 'bh',
                                                 ledger_hash: 'a', actions_hash: 'b', contract_hash: 'c',
                                                 state_hash: state }),
        getTableCount:   sinon.stub().resolves(7),
        listExistingTables: sinon.stub().resolves(new Set(['blocks']))
    };
}

function serverSyncService(poller){
    return {
        getBroadcaster: () => ({
            getStatus: () => ({ block_height: 99, source_block_height: 99 }),
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
            lastKnownServerBlock: 99, sourceHeightStale: false, halted: false, haltInfo: null,
            truncated: false, bootstrapBase: null, sourceQuorum: 1, sourcesConfigured: 1,
            sourcesActive: 1, sourcesAgreeing: 1, sourcesEvicted: []
        })
    };
}

describe('/status state_hash', function(){

    // Load the API once up front so a cold module load does not eat a test's timeout.
    before(function(){ this.timeout(60000); loadApi('server'); });
    afterEach(function(){ sinon.restore(); });

    it('publishes state_hash on a server indexer row and omits it on a decoder row', async function(){
        let { buildStatusRow } = loadApi('server');
        let indexer = await buildStatusRow(serverSyncService(), mockDb('indexer', 'sh99'), 'indexer', 'bitcoin', 'mainnet');
        assert.strictEqual(indexer.state_hash, 'sh99');
        let decoder = await buildStatusRow(serverSyncService(), mockDb('decoder', 'sh99'), 'decoder', 'bitcoin', 'mainnet');
        assert.strictEqual('state_hash' in decoder, false);
    });

    it('marks and publishes a live hash row that matches the poller identity', async function(){
        let { buildStatusRow } = loadApi('server');
        let poller = { getRecordedIdentity: sinon.stub().returns('a|b|c') };
        let row = await buildStatusRow(serverSyncService(poller), mockDb('indexer', 'sh99'),
            'indexer', 'bitcoin', 'mainnet');
        assert.strictEqual(row.hash_identity_consistent, true);
        assert.strictEqual(row.ledger_hash, 'a');
        assert.strictEqual(row.state_hash, 'sh99');
    });

    it('withholds hashes when the polled height was rolled back from the live database', async function(){
        let { buildStatusRow } = loadApi('server');
        let db = mockDb('indexer', 'sh99');
        db.getBlockHashRow.resolves(null);
        let poller = { getRecordedIdentity: sinon.stub().returns('a|b|c') };
        let row = await buildStatusRow(serverSyncService(poller), db, 'indexer', 'bitcoin', 'mainnet');
        assert.strictEqual(row.block_height, 99);
        assert.strictEqual(row.hash_identity_consistent, false);
        assert.strictEqual(row.ledger_hash, null);
        assert.strictEqual(row.state_hash, null);
    });

    it('withholds new-branch live hashes while the poller still names the old identity', async function(){
        let { buildStatusRow } = loadApi('server');
        let poller = { getRecordedIdentity: sinon.stub().returns('old-l|old-a|old-c') };
        let row = await buildStatusRow(serverSyncService(poller), mockDb('indexer', 'new-state'),
            'indexer', 'bitcoin', 'mainnet');
        assert.strictEqual(row.hash_identity_consistent, false);
        assert.strictEqual(row.ledger_hash, null);
        assert.strictEqual(row.actions_hash, null);
        assert.strictEqual(row.contract_hash, null);
    });

    it('publishes state_hash on a client indexer row, null before it exists', async function(){
        let { buildStatusRow } = loadApi('client');
        let row = await buildStatusRow(clientSyncService(), mockDb('indexer', 'sh99'), 'indexer', 'bitcoin', 'mainnet');
        assert.strictEqual(row.state_hash, 'sh99');
        let early = await buildStatusRow(clientSyncService(), mockDb('indexer', undefined), 'indexer', 'bitcoin', 'mainnet');
        assert.strictEqual(early.state_hash, null);
    });
});
