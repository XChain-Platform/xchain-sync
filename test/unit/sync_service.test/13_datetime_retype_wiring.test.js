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
        sinon.stub(process, 'exit');
    });

    afterEach(function(){
        sinon.restore();
    });

}

describe("SyncService", function(){

    registerHooks();

    describe('discoverChains datetime retype wiring', function(){

        it('server mode calls ensureDatetimeColumns once per chain, after verifySyncTables, with includeFollowerDerived: false', async function(){
            config.SYNC_MODE = 'server';
            delete config.REPLICA_DB_HOST;
            service = new SyncService(config);
            sinon.stub(service.hubClient, 'getIndexerConfigs').resolves([indexerCfg()]);
            sinon.stub(service.hubClient, 'getDecoderConfigs').resolves([]);
            let vst = sinon.stub(Database.prototype, 'verifySyncTables').resolves(true);
            let dtr = sinon.stub(Database.prototype, 'ensureDatetimeColumns').resolves(0);
            sinon.stub(Database.prototype, 'assertStakeWeightOrderingCollation').resolves();
            sinon.stub(service, 'startPollerForChain');

            await service.discoverChains();

            assert.strictEqual(dtr.calledOnce, true);
            assert.deepStrictEqual(dtr.firstCall.args[0], { includeFollowerDerived: false });
            assert.strictEqual(dtr.calledAfter(vst), true);
        });

        it('client mode calls ensureDatetimeColumns once per chain, after verifySyncTables and before ensureReplicatedColumns, with includeFollowerDerived: true', async function(){
            config.SYNC_MODE = 'client';
            service = new SyncService(config);
            sinon.stub(service.hubClient, 'getIndexerConfigs').resolves([indexerCfg()]);
            sinon.stub(service.hubClient, 'getDecoderConfigs').resolves([]);
            sinon.stub(Database.prototype, 'createDatabase').resolves(true);
            sinon.stub(Database.prototype, 'verifyDatabaseOnce').resolves(true);
            sinon.stub(Database.prototype, 'replicateSchema').resolves();
            let vst = sinon.stub(Database.prototype, 'verifySyncTables').resolves(true);
            let dtr = sinon.stub(Database.prototype, 'ensureDatetimeColumns').resolves(0);
            let erc = sinon.stub(Database.prototype, 'ensureReplicatedColumns').resolves();
            sinon.stub(Database.prototype, 'ensureReplicaSecondaryIndexes').resolves();
            sinon.stub(Database.prototype, 'ensureReplicaUtf8mb4Columns').resolves();
            sinon.stub(Database.prototype, 'assertStakeWeightOrderingCollation').resolves();
            sinon.stub(Database.prototype, 'close').resolves();
            sinon.stub(service, 'startClientSyncForChain');

            await service.discoverChains();

            assert.strictEqual(dtr.calledOnce, true);
            assert.deepStrictEqual(dtr.firstCall.args[0], { includeFollowerDerived: true });
            assert.strictEqual(dtr.calledAfter(vst), true);
            assert.strictEqual(dtr.calledBefore(erc), true);
        });
    });
});
