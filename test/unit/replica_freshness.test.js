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
const fs = require('fs');
const path = require('path');
const sinon = require('sinon');
const proxyquire = require('proxyquire');
const { applyReplicaFreshness } = require('../../src/api');
const config = require('../../src/config');
const { getLogger } = require('../../src/observability');

function baseRow(){
    return { block_height: 100, source_height: 100, lag_blocks: 0 };
}

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

describe('per-chain rollback depth', function(){
    it('raises the unset default to the source undo window per chain and network', function(){
        assert.strictEqual(config.resolveMaxRollbackDepth('litecoin', 'testnet', 100, false), 5000);
        assert.strictEqual(config.resolveMaxRollbackDepth('LTC', 'mainnet', 100, false), 120);
        assert.strictEqual(config.resolveMaxRollbackDepth('LTC', 'regtest', 100, false), 120);
        assert.strictEqual(config.resolveMaxRollbackDepth('bitcoin', 'testnet', 100, false), 120);
        assert.strictEqual(config.resolveMaxRollbackDepth('bitcoin', 'mainnet', 100, false), 100);
        assert.strictEqual(config.resolveMaxRollbackDepth('BTC', 'regtest', 100, false), 100);
        for(const net of ['mainnet', 'testnet', 'regtest']){
            assert.strictEqual(config.resolveMaxRollbackDepth('dogecoin', net, 100, false), 120);
            assert.strictEqual(config.resolveMaxRollbackDepth('DOGE', net, 100, false), 120);
        }
    });
});

// The copy must track the tracker table: a shallower default halts on reorgs the source recovers
describe('source undo window mirror', function(){
    it('mirrors the utxo-tracker undo window table, inside the source ceiling', function(){
        for(const [key, w] of Object.entries(config.SOURCE_UNDO_WINDOW)){
            const [tick, net] = [key.split('_')[0], key.split('_')[1].toLowerCase()];
            assert.ok(config.resolveMaxRollbackDepth(tick, net, 100, false) >= w, key);
            assert.ok(w <= config.rollbackDepthSafeCeiling(tick, net), key);
        }
        const tracker = path.join(__dirname, '..', '..', '..', 'xchain-utxo-tracker', 'src', 'chain', 'undo_blocks.js');
        if(!fs.existsSync(tracker)){
            if(process.env.XCHAIN_REQUIRE_SIBLINGS === '1') assert.fail('xchain-utxo-tracker sibling is required');
            return this.skip();
        }
        const literal = fs.readFileSync(tracker, 'utf8').match(/const DEFAULT_UNDO_BLOCKS = (\{[^}]*\})/);
        assert.ok(literal, 'DEFAULT_UNDO_BLOCKS literal not found in the tracker');
        const table = JSON.parse(literal[1].replace(/(\w+):/g, '"$1":'));
        assert.deepStrictEqual({ ...config.SOURCE_UNDO_WINDOW }, table);
    });
});

describe('per-chain rollback depth overrides and ceiling', function(){
    it('preserves an explicit operator override', function(){
        assert.strictEqual(config.resolveMaxRollbackDepth('litecoin', 'testnet', 250, true), 250);
    });

    it('names the source reorg ceiling per chain, in ticker or full-name form', function(){
        assert.strictEqual(config.rollbackDepthSafeCeiling('litecoin', 'testnet'), 5006);
        assert.strictEqual(config.rollbackDepthSafeCeiling('LTC', 'testnet'), 5006);
        assert.strictEqual(config.rollbackDepthSafeCeiling('LTC', 'mainnet'), 126);
        assert.strictEqual(config.rollbackDepthSafeCeiling('bitcoin', 'testnet'), 126);
        assert.strictEqual(config.rollbackDepthSafeCeiling('dogecoin', 'regtest'), 126);
    });

    // Capture the config logger's error calls for one resolve, restoring it after.
    function resolveCapturingErrors(chain, network, depth, explicit){
        const logger = getLogger();
        const errorStub = sinon.stub(logger, 'error');
        try {
            const value = config.resolveMaxRollbackDepth(chain, network, depth, explicit);
            return { value, errors: errorStub.getCalls().map(c => String(c.args[0])) };
        } finally {
            errorStub.restore();
        }
    }

    it('warns, without clamping, when an override exceeds the source reorg ceiling', function(){
        const btc = resolveCapturingErrors('bitcoin', 'mainnet', 1000, true);
        assert.strictEqual(btc.value, 1000, 'the override is an operator knob and is never clamped');
        assert.ok(btc.errors.some(m => /above the source reorg ceiling \(126/.test(m)), btc.errors.join(' | '));
        const ltc = resolveCapturingErrors('litecoin', 'testnet', 6000, true);
        assert.strictEqual(ltc.value, 6000);
        assert.ok(ltc.errors.some(m => /above the source reorg ceiling \(5006/.test(m)), ltc.errors.join(' | '));
    });

    it('stays silent for the defaults and for an in-range override', function(){
        for(const args of [['bitcoin', 'mainnet', 100, false], ['litecoin', 'testnet', 100, false], ['dogecoin', 'mainnet', 100, false],
                           ['litecoin', 'testnet', 250, true], ['bitcoin', 'mainnet', 126, true]]){
            const r = resolveCapturingErrors(...args);
            assert.deepStrictEqual(r.errors, [], 'no warning for ' + args.join(','));
        }
        assert.strictEqual(resolveCapturingErrors('litecoin', 'testnet', 100, false).value, 5000);
    });
});

describe('/status replication freshness', function(){

    // A follower's own lag_blocks is computed against a height its SOURCE published.
    // The source's status event says whether its own database was fit to publish that
    // height; discarding it left the follower certifying an upstream whose SQL replica
    // had stopped applying, because both of the server's heights freeze together and
    // its heartbeats keep source_height_stale false.
    describe('client row carries the upstream verdict', function(){
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
});

describe('/status server row carries a protocol-client halt', function(){
    afterEach(function(){ sinon.restore(); });

    it('marks a durable halt stale even when native SQL replication is fresh', async function(){
            let prior = process.env.SYNC_MODE;
            process.env.SYNC_MODE = 'server';
            let { buildStatusRow } = proxyquire('../../src/api', {});
            if(prior === undefined) delete process.env.SYNC_MODE; else process.env.SYNC_MODE = prior;
            let db = mockDb();
            db.getActiveHalt = sinon.stub().resolves({ block_index: 99, reason: 'rollback-depth-exceeded' });
            let service = {
                getBroadcaster: () => ({
                    getStatus: () => ({ block_height: 100, source_block_height: 100,
                        replica_stale: false, replica_seconds_behind: 0 }),
                    getSubscribers: () => []
                }),
                getSnapshotBuilder: () => null
            };

            let row = await buildStatusRow(service, db, 'decoder', 'litecoin', 'testnet');
            assert.strictEqual(row.replica_halted, true);
            assert.strictEqual(row.replica_stale, true);
            assert.strictEqual(row.lag_blocks, null);
    });
});
