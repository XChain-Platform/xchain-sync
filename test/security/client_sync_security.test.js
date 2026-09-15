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
const HashVerifier = require('../../src/client/hash_verifier');
const Database = require('../../src/db');
const { withDbMixins } = require('../helpers/db_mixins.js');

// Queries read through named Database methods; the real ones are installed for any
// this fake does not stub, so they still reach the doQuery stub the suite inspects.
function createMockDb(){
    return withDbMixins({
        dbName: 'test_db',
        getLastBlock: sinon.stub().resolves(null),
        getBlockHashRow: sinon.stub().resolves(null),
        doQuery: sinon.stub().resolves([]),
        beginTransaction: sinon.stub().resolves(),
        commitTransaction: sinon.stub().resolves(true),
        rollbackTransaction: sinon.stub().resolves(),
        truncateTable: sinon.stub().resolves(),
        recordHalt: sinon.stub().resolves()
    });
}

function createMockApplier(){
    return {
        applyBlock: sinon.stub().resolves(),
        applyFullSnapshot: sinon.stub().resolves(),
        applyIncrementalSnapshot: sinon.stub().resolves()
    };
}

function createMockRollback(){
    return {
        rollback: sinon.stub().resolves()
    };
}

function createMockHashVerifier(){
    return new HashVerifier();
}

function createMockUtil(){
    return {
        sleep: sinon.stub().resolves(),
        startTimer: sinon.stub().returns(Date.now()),
        getTimer: sinon.stub().returns('0ms'),
        isNull: function(v){ return v === null || v === undefined || v === ''; },
        throwError: function(e){ throw new Error(e); },
        logError: sinon.stub()
    };
}

function createConfig(overrides){
    return Object.assign({
        SYNC_MODE: 'client',
        SYNC_SOURCES: 'http://source1.local,http://source2.local',
        VERIFY_HASHES: true,
        HASH_CONFIRM_TIMEOUT: 100,
        HASH_CONFIRM_STRICT: false,
        MAX_ROLLBACK_DEPTH: 100,
        WS_MAX_PAYLOAD: 1048576,
        SNAPSHOT_MAX_CONTENT: 536870912,
        CLIENT_RECONNECT_DELAY: 100,
        REPLICA_DB_HOST: 'localhost',
        REPLICA_DB_PORT: 3306,
        REPLICA_DB_USER: 'test',
        REPLICA_DB_PASS: 'test'
    }, overrides);
}

let db, applier, rollback, hashVerifier, util;

function registerHooks(){
    beforeEach(function(){
        db = createMockDb();
        applier = createMockApplier();
        rollback = createMockRollback();
        hashVerifier = createMockHashVerifier();
        util = createMockUtil();
        sinon.stub(console, 'error');
        sinon.stub(console, 'log');
    });

    afterEach(function(){
        sinon.restore();
    });
}

describe('ClientSync security', function(){
    registerHooks();

    // ── fetchAndApplySchema: DDL validation ──

    describe('fetchAndApplySchema: DDL validation', function(){

        let ClientSync, axiosStub;

        beforeEach(function(){
            axiosStub = { get: sinon.stub() };
            ClientSync = proxyquire('../../src/client/sync', {
                'axios': axiosStub,
                'ws': sinon.stub()
            });
        });

        it('rejects DDL that starts with DROP TABLE', async function(){
            let config = createConfig();
            let sync = new ClientSync('bitcoin', 'mainnet', db, applier, rollback, hashVerifier, config, util);

            axiosStub.get.resolves({
                data: {
                    tables: {
                        evil_table: 'DROP TABLE blocks'
                    }
                }
            });

            await sync.fetchAndApplySchema('http://source1.local');
            // doQuery should not have been called with the DROP statement
            let dropCalls = db.doQuery.getCalls().filter(c => {
                return typeof c.args[0] === 'string' && c.args[0].includes('DROP');
            });
            assert.strictEqual(dropCalls.length, 0);
            assert.strictEqual(console.error.called, true);
        });
    });
});

describe('ClientSync security', function(){
    registerHooks();

    describe('fetchAndApplySchema: DDL validation', function(){

        let ClientSync, axiosStub;

        beforeEach(function(){
            axiosStub = { get: sinon.stub() };
            ClientSync = proxyquire('../../src/client/sync', {
                'axios': axiosStub,
                'ws': sinon.stub()
            });
        });

        it('rejects DDL containing CREATE TRIGGER', async function(){
            let config = createConfig();
            let sync = new ClientSync('bitcoin', 'mainnet', db, applier, rollback, hashVerifier, config, util);

            axiosStub.get.resolves({
                data: {
                    tables: {
                        evil: 'CREATE TRIGGER trg AFTER INSERT ON blocks FOR EACH ROW BEGIN END'
                    }
                }
            });

            await sync.fetchAndApplySchema('http://source1.local');
            let triggerCalls = db.doQuery.getCalls().filter(c => {
                return typeof c.args[0] === 'string' && c.args[0].includes('TRIGGER');
            });
            assert.strictEqual(triggerCalls.length, 0);
        });
    });
});

describe('ClientSync security', function(){
    registerHooks();

    describe('fetchAndApplySchema: DDL validation', function(){

        let ClientSync, axiosStub;

        beforeEach(function(){
            axiosStub = { get: sinon.stub() };
            ClientSync = proxyquire('../../src/client/sync', {
                'axios': axiosStub,
                'ws': sinon.stub()
            });
        });

        it('rejects invalid table name with special chars', async function(){
            let config = createConfig();
            let sync = new ClientSync('bitcoin', 'mainnet', db, applier, rollback, hashVerifier, config, util);

            axiosStub.get.resolves({
                data: {
                    tables: {
                        '../etc/passwd': 'CREATE TABLE test (id INT)'
                    }
                }
            });

            await sync.fetchAndApplySchema('http://source1.local');
            // The CREATE TABLE DDL should not have been executed
            let createCalls = db.doQuery.getCalls().filter(c => {
                return typeof c.args[0] === 'string' && c.args[0].startsWith('CREATE TABLE');
            });
            assert.strictEqual(createCalls.length, 0);
        });
    });
});

describe('ClientSync security', function(){
    registerHooks();

    describe('fetchAndApplySchema: DDL validation', function(){

        let ClientSync, axiosStub;

        beforeEach(function(){
            axiosStub = { get: sinon.stub() };
            ClientSync = proxyquire('../../src/client/sync', {
                'axios': axiosStub,
                'ws': sinon.stub()
            });
        });

        it('accepts valid CREATE TABLE DDL', async function(){
            let config = createConfig();
            let sync = new ClientSync('bitcoin', 'mainnet', db, applier, rollback, hashVerifier, config, util);

            // First call: information_schema check returns empty (table doesn't exist)
            // Second call: execute the DDL
            db.doQuery.onFirstCall().resolves([]);

            axiosStub.get.resolves({
                data: {
                    tables: {
                        blocks: 'CREATE TABLE blocks (block_index INT PRIMARY KEY)'
                    }
                }
            });

            await sync.fetchAndApplySchema('http://source1.local');
            let createCalls = db.doQuery.getCalls().filter(c => {
                return typeof c.args[0] === 'string' && c.args[0].includes('CREATE TABLE blocks');
            });
            assert.strictEqual(createCalls.length, 1);
        });
    });
});

describe('ClientSync security', function(){
    registerHooks();

    describe('fetchAndApplySchema: DDL validation', function(){

        let ClientSync, axiosStub;

        beforeEach(function(){
            axiosStub = { get: sinon.stub() };
            ClientSync = proxyquire('../../src/client/sync', {
                'axios': axiosStub,
                'ws': sinon.stub()
            });
        });

        it('continues processing when one table is invalid', async function(){
            let config = createConfig();
            let sync = new ClientSync('bitcoin', 'mainnet', db, applier, rollback, hashVerifier, config, util);

            // Both info_schema checks return empty
            db.doQuery.resolves([]);

            axiosStub.get.resolves({
                data: {
                    tables: {
                        'evil;DROP': 'CREATE TABLE evil (id INT)',
                        blocks: 'CREATE TABLE blocks (id INT)'
                    }
                }
            });

            await sync.fetchAndApplySchema('http://source1.local');
            // evil should be rejected (bad table name), blocks should be applied
            let createCalls = db.doQuery.getCalls().filter(c => {
                return typeof c.args[0] === 'string' && c.args[0].includes('CREATE TABLE blocks');
            });
            assert.strictEqual(createCalls.length, 1);
        });
    });
});

describe('ClientSync security', function(){
    registerHooks();

    describe('fetchAndApplySchema: DDL validation', function(){

        let ClientSync, axiosStub;

        beforeEach(function(){
            axiosStub = { get: sinon.stub() };
            ClientSync = proxyquire('../../src/client/sync', {
                'axios': axiosStub,
                'ws': sinon.stub()
            });
        });

        // A compromised/MITM'd sync server returns a CREATE TABLE for an
        // already-existing table whose new-column line embeds a bare comma:
        //   `evil` int DEFAULT 0, DROP COLUMN balance,
        // No semicolon, valid column name, single MariaDB statement: it slips
        // past validateDdl, validateIdentifier, and multipleStatements:false.
        // The schema-catch-up path (addMissingColumns) must skip the column
        // rather than splice it into a multi-action ALTER TABLE.
        it('does not splice a bare-comma multi-action ALTER on schema catch-up', async function(){
            // Real Db so the production addMissingColumns splice path runs;
            // only doQuery is stubbed so no real connection is opened.
            let realDb = new Database('localhost', 3306, 'test_db', 'u', 'p', util, 'indexer');
            let queries = [];
            sinon.stub(realDb, 'doQuery').callsFake(async function(sql){
                queries.push(sql);
                if(typeof sql === 'string' && sql.includes('information_schema.tables'))
                    return [{ table_name: 'balances' }];          // table already exists
                if(typeof sql === 'string' && sql.includes('information_schema.columns'))
                    return [{ column_name: 'id' }, { column_name: 'balance' }];
                return [];
            });

            let config = createConfig();
            let sync = new ClientSync('bitcoin', 'mainnet', realDb, applier, rollback, hashVerifier, config, util);

            let hostileDdl = [
                'CREATE TABLE `balances` (',
                '  `id` int(11) NOT NULL,',
                "  `balance` decimal(30,8) NOT NULL DEFAULT '0',",
                '  `evil` int DEFAULT 0, DROP COLUMN balance,',
                '  PRIMARY KEY (`id`)',
                ') ENGINE=InnoDB'
            ].join('\n');

            axiosStub.get.resolves({ data: { tables: { balances: hostileDdl } } });

            await sync.fetchAndApplySchema('http://source1.local');

            // No ALTER TABLE must have been issued for the injected column.
            let alterCalls = queries.filter(s => typeof s === 'string' && s.includes('ALTER TABLE'));
            assert.strictEqual(alterCalls.length, 0, 'injected column must not produce an ALTER');
            let dropCalls = queries.filter(s => typeof s === 'string' && s.includes('DROP COLUMN'));
            assert.strictEqual(dropCalls.length, 0, 'no DROP COLUMN must reach the database');
        });
    });
});
