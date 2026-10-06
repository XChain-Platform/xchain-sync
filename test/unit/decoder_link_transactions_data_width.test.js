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

const { transactionsDataWidthReason, assertTransactionsDataWidth } = require('../../src/client/decoder_link');

const mockPool = {
    getConnection: sinon.stub().resolves({ query: sinon.stub(), release: sinon.stub() }),
    end: sinon.stub().resolves()
};
const mariadbStub = {
    createPool: sinon.stub().returns(mockPool),
    createConnection: sinon.stub().resolves({ query: sinon.stub().resolves([]), end: sinon.stub().resolves() })
};
const Database = proxyquire('../../src/db', { 'mariadb': mariadbStub });
const SyncService = proxyquire('../../src/sync_service', { './db': Database });

function decoderCfg(coin){
    return { coin, network: 'mainnet', dbType: 'decoder',
        db_host: 'srchost', db_port: 3306, db_name: coin + '_dec', db_user: 'u', db_pass: 'p' };
}

describe('decoder transactions.data width', function(){

    describe('transactionsDataWidthReason', function(){
        it('passes utf8mb4 in either key case and binary', function(){
            assert.strictEqual(transactionsDataWidthReason({ CHARACTER_SET_NAME: 'utf8mb4' }), null);
            assert.strictEqual(transactionsDataWidthReason({ character_set_name: 'UTF8MB4' }), null);
            assert.strictEqual(transactionsDataWidthReason({ CHARACTER_SET_NAME: 'binary' }), null);
        });
        it('refuses utf8mb3 and latin1', function(){
            assert.match(transactionsDataWidthReason({ CHARACTER_SET_NAME: 'utf8mb3' }), /needs utf8mb4/);
            assert.match(transactionsDataWidthReason({ CHARACTER_SET_NAME: 'latin1' }), /latin1/);
        });
        it('reads an absent column or unreadable charset as null', function(){
            assert.strictEqual(transactionsDataWidthReason(undefined), null);
            assert.strictEqual(transactionsDataWidthReason({ CHARACTER_SET_NAME: null }), null);
        });
    });

    describe('assertTransactionsDataWidth', function(){
        function fakeDb(rows, dbType){
            return { dbType: dbType || 'decoder', dbName: 'd', doQuery: sinon.stub().resolves(rows) };
        }
        it('throws for a narrow decoder column and reads with rethrow', async function(){
            let db = fakeDb([{ CHARACTER_SET_NAME: 'utf8mb3' }]);
            await assert.rejects(assertTransactionsDataWidth(db), /utf8mb4/);
            assert.deepStrictEqual(db.doQuery.firstCall.args[3], { rethrow: true });
        });
        it('passes a utf8mb4 column, an absent column, and skips indexer replicas', async function(){
            await assertTransactionsDataWidth(fakeDb([{ CHARACTER_SET_NAME: 'utf8mb4' }]));
            await assertTransactionsDataWidth(fakeDb([]));
            let idx = fakeDb([{ CHARACTER_SET_NAME: 'utf8mb3' }], 'indexer');
            await assertTransactionsDataWidth(idx);
            assert.strictEqual(idx.doQuery.called, false);
        });
        it('propagates a driver fault instead of passing', async function(){
            let db = { dbType: 'decoder', dbName: 'd', doQuery: sinon.stub().rejects(new Error('conn lost')) };
            await assert.rejects(assertTransactionsDataWidth(db), /conn lost/);
        });
    });

    describe('discoverChains wiring', function(){
        let service;
        beforeEach(function(){
            service = new SyncService({
                SYNC_MODE: 'client', HUB_API_HOST: 'localhost', HUB_PORT: 10000, HUB_REPOLL_INTERVAL: 300000,
                REPLICA_DB_HOST: 'localhost', REPLICA_DB_PORT: 3306, REPLICA_DB_USER: 'u', REPLICA_DB_PASS: 'p'
            });
            sinon.stub(console, 'log');
            sinon.stub(console, 'error');
            sinon.stub(service.hubClient, 'getIndexerConfigs').resolves([]);
            sinon.stub(service.hubClient, 'getDecoderConfigs').resolves([decoderCfg('narrowcoin'), decoderCfg('widecoin')]);
            for(let m of ['createDatabase', 'verifyDatabaseOnce', 'verifySyncTables', 'ensureReplicatedColumns',
                          'ensureReplicaSecondaryIndexes', 'ensureReplicaUtf8mb4Columns',
                          'assertStakeWeightOrderingCollation', 'ensureDatetimeColumns'])
                sinon.stub(Database.prototype, m).resolves(false);
            sinon.stub(Database.prototype, 'close').resolves();
            sinon.stub(service, 'startClientSyncForChain');
            sinon.stub(Database.prototype, 'doQuery').callsFake(async function(){
                let narrow = this.dbName === 'narrowcoin_dec';
                return [{ CHARACTER_SET_NAME: narrow ? 'utf8mb3' : 'utf8mb4' }];
            });
        });
        afterEach(function(){ sinon.restore(); });

        it('refuses the narrow chain only and registers the wide one', async function(){
            let chains = await service.discoverChains();
            assert.deepStrictEqual(chains.map(c => c.key), ['widecoin:mainnet:decoder']);
            assert.strictEqual(service.databases.has('narrowcoin:mainnet:decoder'), false);
            let closes = name => Database.prototype.close.thisValues.filter(d => d.dbName === name).length;
            assert.strictEqual(closes('narrowcoin_dec'), 2);
            assert.strictEqual(closes('widecoin_dec'), 1);
        });
    });
});
