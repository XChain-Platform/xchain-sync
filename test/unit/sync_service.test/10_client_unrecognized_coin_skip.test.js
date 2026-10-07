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

const mockPool = {
    getConnection: sinon.stub().resolves({ query: sinon.stub(), release: sinon.stub() }),
    end: sinon.stub().resolves()
};
const mariadbStub = {
    createPool: sinon.stub().returns(mockPool),
    createConnection: sinon.stub().resolves({ query: sinon.stub().resolves([]), end: sinon.stub().resolves() })
};
const Database = proxyquire('../../../src/db', { 'mariadb': mariadbStub });
const SyncService = proxyquire('../../../src/sync_service', { './db': Database });

function indexerCfg(over){
    return Object.assign({
        coin: 'bitcoin', network: 'mainnet', dbType: 'indexer',
        db_host: 'srchost', db_port: 3306, db_name: 'btc_idx', db_user: 'u', db_pass: 'p'
    }, over || {});
}

describe('SyncService client mode unrecognized coin skip', function(){
    let service;

    beforeEach(function(){
        service = new SyncService({
            SYNC_MODE: 'client', HUB_API_HOST: 'localhost', HUB_PORT: 10000,
            HUB_REPOLL_INTERVAL: 300000, REPLICA_DB_HOST: 'localhost', REPLICA_DB_PORT: 3306,
            REPLICA_DB_USER: 'user', REPLICA_DB_PASS: 'pass'
        });
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
        sinon.stub(process, 'exit');
        sinon.stub(Database.prototype, 'createDatabase').resolves(true);
        sinon.stub(Database.prototype, 'verifyDatabaseOnce').resolves(true);
        sinon.stub(Database.prototype, 'replicateSchema').resolves();
        sinon.stub(Database.prototype, 'verifySyncTables').resolves(true);
        sinon.stub(Database.prototype, 'ensureReplicatedColumns').resolves();
        sinon.stub(Database.prototype, 'ensureReplicaSecondaryIndexes').resolves();
        sinon.stub(Database.prototype, 'close').resolves();
        sinon.stub(service.hubClient, 'getIndexerConfigs').resolves([
            indexerCfg({ coin: 'notacoin', db_name: 'nac_idx' })
        ]);
        sinon.stub(service.hubClient, 'getDecoderConfigs').resolves([]);
    });

    afterEach(function(){
        sinon.restore();
    });

    it('removes an indexer chain when ClientRollback rejects its unrecognized coin', async function(){
        const logError = sinon.stub(require('../../../src/observability').getLogger(), 'error');
        const startDiscovered = sinon.spy(service, 'startDiscoveredChains');

        const firstDiscovery = await service.discoverChains();
        const secondDiscovery = await service.discoverChains();

        assert.strictEqual(service.databases.has('notacoin:mainnet:indexer'), false);
        assert.strictEqual(service.clientSyncs.has('notacoin:mainnet:indexer'), false);
        assert.deepStrictEqual(firstDiscovery, []);
        assert.deepStrictEqual(secondDiscovery, []);
        assert.strictEqual(Database.prototype.createDatabase.callCount, 1, 'the re-poll skips the rejected chain');
        assert.strictEqual(Database.prototype.close.callCount, 2, 'the source and rejected replica pools are closed');
        assert.strictEqual(startDiscovered.firstCall.returnValue, undefined,
            'startDiscoveredChains preserves its synchronous return contract');
        const skipLogs = logError.getCalls().filter(c => /notacoin:mainnet:indexer/.test(String(c.args[0])));
        assert.strictEqual(skipLogs.length, 1, 'the skip is logged once across hub re-polls');
    });

    it('does not convert another client startup failure into an unrecognized-coin skip', async function(){
        sinon.stub(service, 'startClientSyncForChain').throws(new Error('unrelated startup failure'));

        await assert.rejects(() => service.discoverChains(), /unrelated startup failure/);

        assert.strictEqual(service.unrecognizedCoinKeys.has('notacoin:mainnet:indexer'), false);
    });

    it('retains the registration when client startup accepts the chain', async function(){
        const startSync = sinon.stub(service, 'startClientSyncForChain');

        const discovered = await service.discoverChains();

        assert.strictEqual(service.databases.has('notacoin:mainnet:indexer'), true);
        assert.strictEqual(service.unrecognizedCoinKeys.has('notacoin:mainnet:indexer'), false);
        assert.deepStrictEqual(discovered.map(chain => chain.key), ['notacoin:mainnet:indexer']);
        assert.strictEqual(startSync.callCount, 1);
    });
});
