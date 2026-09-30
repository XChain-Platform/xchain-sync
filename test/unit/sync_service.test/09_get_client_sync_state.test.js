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
// rather than driving discoverChains' internal `new Database()` calls through
// the raw mariadb stub (whose pooled connection.query returns undefined).
const Database = proxyquire('../../../src/db', { 'mariadb': mariadbStub });
const SyncService = proxyquire('../../../src/sync_service', { './db': Database });
const TransparencyLog = require('../../../src/server/transparency_log');
const ClientSync   = require('../../../src/client/sync');
const ServerPoller = require('../../../src/server/poller');
const fs = require('fs');
const path = require('path');

function indexerCfg(over){
    return Object.assign({
        coin: 'bitcoin', network: 'mainnet', dbType: 'indexer',
        db_host: 'srchost', db_port: 3306, db_name: 'btc_idx', db_user: 'u', db_pass: 'p'
    }, over || {});
}

let service, config;

function registerHooks(){

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
        // startPollerForChain / startClientSyncForChain run start() as an unawaited
        // background promise whose .catch calls process.exit(1) on crash (for container
        // restart). With mocked deps those promises reject after the test moves on; stub
        // exit so a background crash can't tear down the mocha process mid-run.
        sinon.stub(process, 'exit');
    });

    afterEach(function(){
        sinon.restore();
    });

}

describe("SyncService", function(){

    registerHooks();

    describe('getClientSyncState', function(){
        it('returns nulls/false when no sync exists for the key', function(){
            assert.deepStrictEqual(service.getClientSyncState('bitcoin', 'mainnet'),
                { lastKnownServerBlock: null, sourceHeightStale: null,
                  upstreamReplica: { stale: null, secondsBehind: null, sourceHeight: null },
                  halted: false,
                  haltInfo: null, trainActivation: null, truncated: false, bootstrapBase: null,
                  sourceQuorum: null, sourcesConfigured: null, sourcesActive: null,
                  sourcesAgreeing: null, sourcesEvicted: [] });
        });
        it('reports a live sync, including halt info when halted', function(){
            let fakeSync = {
                lastKnownServerBlock: 42,
                isSourceHeightStale: () => false,
                isHalted: () => true,
                getHaltInfo: () => ({ blockIndex: 42, reason: 'divergence' }),
                isTruncated: () => true,
                getBootstrapBase: () => 850000,
                getSourceQuorum: () => 2,
                getConfiguredSourceCount: () => 3,
                getActiveSourceCount: () => 2,
                getSourcesAgreeing: () => 2,
                getEvictedSources: () => ['http://b:3006'],
                getTrainActivation: () => ({ status: 'pending', requiredRuleSet: '9.0.0', requiredAtHeight: 970000 })
            };
            service.clientSyncs.set('bitcoin:mainnet:indexer', fakeSync);
            let state = service.getClientSyncState('bitcoin', 'mainnet');
            assert.strictEqual(state.lastKnownServerBlock, 42);
            assert.deepStrictEqual(state.trainActivation,
                { status: 'pending', requiredRuleSet: '9.0.0', requiredAtHeight: 970000 });
            assert.strictEqual(state.sourceHeightStale, false);
            assert.strictEqual(state.halted, true);
            assert.deepStrictEqual(state.haltInfo, { blockIndex: 42, reason: 'divergence' });
            assert.strictEqual(state.truncated, true);
            assert.strictEqual(state.bootstrapBase, 850000);
            assert.strictEqual(state.sourceQuorum, 2);
            assert.strictEqual(state.sourcesConfigured, 3);
            assert.strictEqual(state.sourcesActive, 2);
            assert.strictEqual(state.sourcesAgreeing, 2);
            assert.deepStrictEqual(state.sourcesEvicted, ['http://b:3006']);
        });
    });
});

describe("SyncService", function(){

    registerHooks();

    describe('getClientSyncState', function(){
        it('omits halt info for a healthy sync', function(){
            service.clientSyncs.set('bitcoin:mainnet:indexer',
                { lastKnownServerBlock: 7, isSourceHeightStale: () => null,
                  isHalted: () => false, getHaltInfo: () => ({}),
                  isTruncated: () => false, getBootstrapBase: () => null,
                  getSourceQuorum: () => 1, getConfiguredSourceCount: () => 1,
                  getActiveSourceCount: () => 1, getSourcesAgreeing: () => null,
                  getEvictedSources: () => [] });
            let state = service.getClientSyncState('bitcoin', 'mainnet');
            assert.strictEqual(state.halted, false);
            assert.strictEqual(state.haltInfo, null);
            assert.strictEqual(state.truncated, false);
            assert.strictEqual(state.bootstrapBase, null);
            assert.strictEqual(state.sourceQuorum, 1);
            assert.deepStrictEqual(state.sourcesEvicted, []);
        });
    });
});
