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
// /status derived lag_blocks as source_height - block_height, and
// source_height falls back to db.getLastBlock(), a MAX(block_index) against the
// SERVED database. On a node fronting a native SQL replica those are one failure
// domain: replication stops applying, both heights freeze at the same number,
// Math.max(0, 0) publishes lag 0, and status certifies an hours-behind node as
// caught up. The client path already had CLIENT_SOURCE_STALE_MS for the same
// class; the server path fronting a replica had nothing.

const assert = require('assert');
const sinon = require('sinon');
const proxyquire = require('proxyquire');
const { applyReplicaFreshness } = require('../../src/api');
const config = require('../../src/config');

function baseRow(){
    return { block_height: 100, source_height: 100, lag_blocks: 0 };
}

describe('/status replication freshness', function(){

    it('withholds lag_blocks when the replication engine says the node is stale', function(){
        let row = applyReplicaFreshness(baseRow(), { replica_stale: true, replica_seconds_behind: null });
        assert.strictEqual(row.lag_blocks, null,
            'publishing 0 here is the defect: a frozen replica reads as caught up');
        assert.strictEqual(row.replica_stale, true);
    });

    it('keeps lag_blocks when replication is fresh', function(){
        let row = applyReplicaFreshness(baseRow(), { replica_stale: false, replica_seconds_behind: 4 });
        assert.strictEqual(row.lag_blocks, 0);
        assert.strictEqual(row.replica_seconds_behind, 4);
    });

    it('fails closed on a poller status that predates the fields', function(){
        // An older poller sends no replica_* keys at all. Absent evidence must not
        // read as "not stale" AND still surface an explicit unknown seconds-behind.
        let row = applyReplicaFreshness(baseRow(), { block_height: 100 });
        assert.strictEqual(row.replica_seconds_behind, null);
        assert.strictEqual(row.replica_stale, false, 'no signal is the pre-replica topology default');
    });

    // A follower's own lag_blocks is computed against a height its SOURCE published.
    // The source's status event says whether its own database was fit to publish that
    // height; discarding it left the follower certifying an upstream whose SQL replica
    // had stopped applying, because both of the server's heights freeze together and
    // its heartbeats keep source_height_stale false.
    describe('client row carries the upstream verdict', function(){
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

        function mockService(upstreamReplica){
            return {
                getClientSyncState: () => ({
                    lastKnownServerBlock: 100, sourceHeightStale: false, upstreamReplica,
                    halted: false, haltInfo: null, truncated: false, bootstrapBase: null,
                    sourceQuorum: 1, sourcesConfigured: 1, sourcesActive: 1,
                    sourcesAgreeing: 1, sourcesEvicted: []
                })
            };
        }

        afterEach(function(){ sinon.restore(); });

        it('publishes the upstream verdict beside a lag_blocks of 0', async function(){
            let { buildStatusRow } = loadClientApi();
            let row = await buildStatusRow(
                mockService({ stale: true, secondsBehind: 900, sourceHeight: 140 }),
                mockDb(), 'indexer', 'bitcoin', 'mainnet');

            // The exact green shape the incident showed, now qualified.
            assert.strictEqual(row.lag_blocks, 0);
            assert.strictEqual(row.source_height_stale, false);
            assert.strictEqual(row.upstream_replica_stale, true);
            assert.strictEqual(row.upstream_replica_seconds_behind, 900);
            assert.strictEqual(row.upstream_source_height, 140);
        });

        it('is unknown (null), never false, when no source reported the fields', async function(){
            let { buildStatusRow } = loadClientApi();
            let row = await buildStatusRow(
                mockService({ stale: null, secondsBehind: null, sourceHeight: null }),
                mockDb(), 'indexer', 'bitcoin', 'mainnet');
            assert.strictEqual(row.upstream_replica_stale, null);
            assert.strictEqual(row.upstream_replica_seconds_behind, null);
            assert.strictEqual(row.upstream_source_height, null);
        });
    });

    it('SYNC_REPLICA_MAX_LAG_S is configurable and defaults to 120s', function(){
        let saved = process.env.SYNC_REPLICA_MAX_LAG_S;
        try {
            delete process.env.SYNC_REPLICA_MAX_LAG_S;
            assert.strictEqual(config.getConfig()['SYNC_REPLICA_MAX_LAG_S'], 120);
            process.env.SYNC_REPLICA_MAX_LAG_S = '30';
            assert.strictEqual(config.getConfig()['SYNC_REPLICA_MAX_LAG_S'], 30);
        } finally {
            if(saved === undefined) delete process.env.SYNC_REPLICA_MAX_LAG_S;
            else process.env.SYNC_REPLICA_MAX_LAG_S = saved;
        }
    });
});
