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
// A follower short replicated ROWS agrees on every committed hash: the hashes are
// computed on the source and replicated verbatim, so the row reports halted:false
// and lag_blocks 0 while the data is incomplete. The only trace used to be a
// TABLE_COUNT_MISMATCH error line, which also fires for a count read that raced the
// source's /status and therefore repeats forever (live case: a mainnet follower
// ~2k index_transactions rows short for weeks under a green /status). These tests
// pin the monitorable half of the fix: /status publishes the sweep's persistent-gap
// VERDICT as an array a monitor can alert on.

const assert     = require('assert');
const sinon      = require('sinon');
const proxyquire = require('proxyquire');

function loadClientApi(){
    let prior = process.env.SYNC_MODE;
    process.env.SYNC_MODE = 'client';
    let api = proxyquire('../../src/api', {});
    if(prior === undefined) delete process.env.SYNC_MODE; else process.env.SYNC_MODE = prior;
    return api;
}

function mockDb(){
    return {
        dbName: 'replica_db', dbType: 'indexer',
        getLastBlock:    sinon.stub().resolves(100),
        getBlockHashRow: sinon.stub().resolves({ block_index: 100, block_time: 1,
                                                 ledger_hash: 'a', actions_hash: 'b', contract_hash: 'c' }),
        getTableCount:   sinon.stub().resolves(0),
        listExistingTables: sinon.stub().resolves(new Set())
    };
}

// A SyncService whose live ClientSync reports `gaps` (omit clientSync entirely to
// model a service/build without the surface).
function mockService(clientSync){
    return {
        getClientSyncState: () => ({
            lastKnownServerBlock: 100, sourceHeightStale: false,
            upstreamReplica: { stale: false, secondsBehind: 1, sourceHeight: 100 },
            halted: false, haltInfo: null, truncated: false, bootstrapBase: null,
            sourceQuorum: 1, sourcesConfigured: 1, sourcesActive: 1,
            sourcesAgreeing: 1, sourcesEvicted: []
        }),
        getClientSync: () => clientSync || null
    };
}

const PERSISTENT_GAP = {
    table: 'index_transactions', delta: 1969, source_count: 4210394, local_count: 4208425,
    sweeps: 37, first_seen_at: 1757000000000, alerts: 4, repair_attempts: 2, last_block: 912345
};

describe('/status persistent replica gaps', function(){

    afterEach(function(){ sinon.restore(); });

    it('publishes the gap beside an otherwise green row', async function(){
        let { buildStatusRow } = loadClientApi();
        let row = await buildStatusRow(
            mockService({ getReplicaGaps: () => [PERSISTENT_GAP] }),
            mockDb(), 'indexer', 'bitcoin', 'mainnet');

        // The exact green shape the incident showed, now carrying the gap.
        assert.strictEqual(row.halted, false);
        assert.strictEqual(row.lag_blocks, 0);
        assert.strictEqual(row.replica_gaps.length, 1);
        assert.strictEqual(row.replica_gaps[0].table, 'index_transactions');
        assert.strictEqual(row.replica_gaps[0].delta, 1969);
        assert.strictEqual(row.replica_gaps[0].repair_attempts, 2);
    });

    it('is an empty array, not a missing key, when nothing is persistently short', async function(){
        // A monitor keyed on presence would fire on every healthy replica; one keyed
        // on a non-empty array needs the key to exist and be empty.
        let { buildStatusRow } = loadClientApi();
        let row = await buildStatusRow(
            mockService({ getReplicaGaps: () => [] }),
            mockDb(), 'indexer', 'bitcoin', 'mainnet');
        assert.deepStrictEqual(row.replica_gaps, []);
    });

    it('stays an empty array when no live client sync exposes the surface', async function(){
        let { buildStatusRow } = loadClientApi();
        for(let svc of [mockService(null), mockService({}), { getClientSyncState: mockService(null).getClientSyncState }]){
            let row = await buildStatusRow(svc, mockDb(), 'indexer', 'bitcoin', 'mainnet');
            assert.deepStrictEqual(row.replica_gaps, [],
                'an absent surface must degrade to "nothing to alert on", never crash /status');
        }
    });

    it('asks for the gaps of the row being built, not a default chain', async function(){
        let { buildStatusRow } = loadClientApi();
        let getClientSync = sinon.stub().returns({ getReplicaGaps: () => [] });
        let svc = mockService(null);
        svc.getClientSync = getClientSync;
        await buildStatusRow(svc, mockDb(), 'decoder', 'litecoin', 'regtest');
        assert.deepStrictEqual(getClientSync.firstCall.args, ['litecoin', 'regtest', 'decoder']);
    });
});
