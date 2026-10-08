/*********************************************************************
 *
 * Copyright © 2025–2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC – https://dankest.llc
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This file is part of XChain Platform. Licensed under the GNU Affero
 * General Public License v3.0 or later; see LICENSE.md. A commercial
 * license (without AGPL source-disclosure terms) is available -
 * contact legal@dankest.llc.
 *
 **********************************************************************
 * /status publishes the durable count of live recompute errors that held
 * the tip, so a replica stuck on a persistent recompute fault is visible.
 ********************************************************************/

const assert     = require('assert');
const sinon      = require('sinon');
const proxyquire = require('proxyquire');

function loadApi(mode){
    let prior = process.env.SYNC_MODE;
    process.env.SYNC_MODE = mode;
    let api = proxyquire('../../src/api', {});
    if(prior === undefined) delete process.env.SYNC_MODE; else process.env.SYNC_MODE = prior;
    return api;
}

function mockDb(syncState){
    return {
        dbName: 'replica_db', dbType: 'indexer',
        getLastBlock:    sinon.stub().resolves(100),
        getBlockHashRow: sinon.stub().resolves({ block_index: 100, block_time: 1, ledger_hash: 'a',
                                                 actions_hash: 'b', contract_hash: 'c' }),
        getTableCount:   sinon.stub().resolves(7),
        listExistingTables: sinon.stub().resolves(new Set()),
        getSyncState:    syncState
    };
}

const syncService = {
    getClientSync: () => ({ getMissingTables: () => [] }),
    getClientSyncState: () => ({
        lastKnownServerBlock: 100, sourceHeightStale: false, halted: false, haltInfo: null,
        truncated: false, bootstrapBase: null, sourceQuorum: 1, sourcesConfigured: 1,
        sourcesActive: 1, sourcesAgreeing: 1, sourcesEvicted: []
    })
};

describe('/status unverified recompute count', function(){
    afterEach(function(){ sinon.restore(); });

    it('publishes the durable count and last block', async function(){
        let { buildStatusRow } = loadApi('client');
        let store = { 'unverified_recompute_count:indexer': '3', 'unverified_recompute_last_block:indexer': '200' };
        let row = await buildStatusRow(syncService, mockDb(async k => (k in store ? store[k] : null)),
                                       'indexer', 'bitcoin', 'mainnet');
        assert.strictEqual(row.unverified_recompute_count, 3);
        assert.strictEqual(row.unverified_recompute_last_block, 200);
    });

    it('is zero with no last block when nothing was recorded', async function(){
        let { buildStatusRow } = loadApi('client');
        let row = await buildStatusRow(syncService, mockDb(async () => null), 'indexer', 'bitcoin', 'mainnet');
        assert.strictEqual(row.unverified_recompute_count, 0);
        assert.strictEqual(row.unverified_recompute_last_block, null);
    });

    it('is null when the sync state cannot be read', async function(){
        let { buildStatusRow } = loadApi('client');
        let row = await buildStatusRow(syncService, mockDb(async () => { throw new Error('down'); }),
                                       'indexer', 'bitcoin', 'mainnet');
        assert.strictEqual(row.unverified_recompute_count, null);
    });
});
