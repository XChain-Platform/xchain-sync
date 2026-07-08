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

function createMockDb(){
    return {
        dbName: 'test_db',
        getLastBlock: sinon.stub().resolves(null),
        getBlockHashRow: sinon.stub().resolves(null),
        doQuery: sinon.stub().resolves([]),
        beginTransaction: sinon.stub().resolves(),
        commitTransaction: sinon.stub().resolves(true),
        rollbackTransaction: sinon.stub().resolves(),
        truncateTable: sinon.stub().resolves(),
        recordHalt: sinon.stub().resolves()
    };
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
    let HashVerifier = require('../../src/HashVerifier');
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

describe('ClientSync security', function(){

    let db, applier, rollback, hashVerifier, util;

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

    // ── _fetchAndApplySchema: DDL validation ──

    describe('_fetchAndApplySchema: DDL validation', function(){

        let ClientSync, axiosStub;

        beforeEach(function(){
            axiosStub = { get: sinon.stub() };
            ClientSync = proxyquire('../../src/ClientSync', {
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

            await sync._fetchAndApplySchema('http://source1.local');
            // doQuery should not have been called with the DROP statement
            let dropCalls = db.doQuery.getCalls().filter(c => {
                return typeof c.args[0] === 'string' && c.args[0].includes('DROP');
            });
            assert.strictEqual(dropCalls.length, 0);
            assert.strictEqual(console.error.called, true);
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

            await sync._fetchAndApplySchema('http://source1.local');
            let triggerCalls = db.doQuery.getCalls().filter(c => {
                return typeof c.args[0] === 'string' && c.args[0].includes('TRIGGER');
            });
            assert.strictEqual(triggerCalls.length, 0);
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

            await sync._fetchAndApplySchema('http://source1.local');
            // The CREATE TABLE DDL should not have been executed
            let createCalls = db.doQuery.getCalls().filter(c => {
                return typeof c.args[0] === 'string' && c.args[0].startsWith('CREATE TABLE');
            });
            assert.strictEqual(createCalls.length, 0);
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

            await sync._fetchAndApplySchema('http://source1.local');
            let createCalls = db.doQuery.getCalls().filter(c => {
                return typeof c.args[0] === 'string' && c.args[0].includes('CREATE TABLE blocks');
            });
            assert.strictEqual(createCalls.length, 1);
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

            await sync._fetchAndApplySchema('http://source1.local');
            // evil should be rejected (bad table name), blocks should be applied
            let createCalls = db.doQuery.getCalls().filter(c => {
                return typeof c.args[0] === 'string' && c.args[0].includes('CREATE TABLE blocks');
            });
            assert.strictEqual(createCalls.length, 1);
        });

        // A compromised/MITM'd sync server returns a CREATE TABLE for an
        // already-existing table whose new-column line embeds a bare comma:
        //   `evil` int DEFAULT 0, DROP COLUMN balance,
        // No semicolon, valid column name, single MariaDB statement: it slips
        // past validateDdl, validateIdentifier, and multipleStatements:false.
        // The schema-catch-up path (addMissingColumns) must skip the column
        // rather than splice it into a multi-action ALTER TABLE.
        it('does not splice a bare-comma multi-action ALTER on schema catch-up', async function(){
            const Database = require('../../src/db');
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

            await sync._fetchAndApplySchema('http://source1.local');

            // No ALTER TABLE must have been issued for the injected column.
            let alterCalls = queries.filter(s => typeof s === 'string' && s.includes('ALTER TABLE'));
            assert.strictEqual(alterCalls.length, 0, 'injected column must not produce an ALTER');
            let dropCalls = queries.filter(s => typeof s === 'string' && s.includes('DROP COLUMN'));
            assert.strictEqual(dropCalls.length, 0, 'no DROP COLUMN must reach the database');
        });
    });

    // ── _handleReorg: max rollback depth ──

    describe('_handleReorg: max rollback depth', function(){

        let ClientSync;

        beforeEach(function(){
            ClientSync = proxyquire('../../src/ClientSync', {
                'axios': { get: sinon.stub() },
                'ws': sinon.stub()
            });
        });

        it('allows rollback within MAX_ROLLBACK_DEPTH', async function(){
            let config = createConfig({ MAX_ROLLBACK_DEPTH: 5 });
            let sync = new ClientSync('bitcoin', 'mainnet', db, applier, rollback, hashVerifier, config, util);
            sync.lastAppliedBlock = 100;
            sync.lastHashes = { ledger_hash: 'abc', actions_hash: 'def', contract_hash: 'ghi' };

            await sync._handleReorg({ type: 'reorg', block_index: 96 }); // depth = 5
            assert.strictEqual(rollback.rollback.calledOnce, true);
            assert.strictEqual(rollback.rollback.firstCall.args[0], 96);
        });

        it('allows rollback of depth 1', async function(){
            let config = createConfig({ MAX_ROLLBACK_DEPTH: 5 });
            let sync = new ClientSync('bitcoin', 'mainnet', db, applier, rollback, hashVerifier, config, util);
            sync.lastAppliedBlock = 100;
            sync.lastHashes = null;

            await sync._handleReorg({ type: 'reorg', block_index: 100 }); // depth = 1
            assert.strictEqual(rollback.rollback.calledOnce, true);
        });

        it('HALTS (fails closed) when reorg exceeds MAX_ROLLBACK_DEPTH', async function(){
            let config = createConfig({ MAX_ROLLBACK_DEPTH: 5 });
            let sync = new ClientSync('bitcoin', 'mainnet', db, applier, rollback, hashVerifier, config, util);
            sync.lastAppliedBlock = 100;
            sync.lastHashes = null;

            await sync._handleReorg({ type: 'reorg', block_index: 95 }); // depth = 6
            // Must NOT roll back (too deep to rewind safely)...
            assert.strictEqual(rollback.rollback.called, false);
            // ...and must NOT fail open: a durable halt is recorded so the replica
            // stops applying instead of silently serving the orphaned fork.
            assert.strictEqual(sync.isHalted(), true);
            assert.strictEqual(sync.getHaltInfo().reason, 'max-rollback-depth-exceeded');
            assert.strictEqual(db.recordHalt.calledOnce, true);
            assert.strictEqual(db.recordHalt.firstCall.args[2], 'max-rollback-depth-exceeded');
            // lastAppliedBlock is left untouched: we did not advance, but we also
            // halt so the stale value can no longer be used to drop canonical blocks.
            assert.strictEqual(sync.lastAppliedBlock, 100);
        });

        it('HALTS the decoder track too (no recompute safety net)', async function(){
            // The decoder has no VERIFY_RECOMPUTE / VERIFY_STATE_HASH self-halt path,
            // so the max-depth halt is its ONLY protection against serving the fork.
            db.dbType = 'decoder';
            let config = createConfig({ MAX_ROLLBACK_DEPTH: 5 });
            let sync = new ClientSync('bitcoin', 'mainnet', db, applier, rollback, hashVerifier, config, util);
            assert.strictEqual(sync.dbType, 'decoder');
            sync.lastAppliedBlock = 100;
            sync.lastHashes = null;

            await sync._handleReorg({ type: 'reorg', block_index: 95 }); // depth = 6
            assert.strictEqual(rollback.rollback.called, false);
            assert.strictEqual(sync.isHalted(), true);
            assert.strictEqual(sync.getHaltInfo().reason, 'max-rollback-depth-exceeded');
            assert.strictEqual(db.recordHalt.calledOnce, true);
            assert.strictEqual(db.recordHalt.firstCall.args[0], 'decoder');
        });

        it('HALTS on deep rollback to block 1', async function(){
            let config = createConfig({ MAX_ROLLBACK_DEPTH: 100 });
            let sync = new ClientSync('bitcoin', 'mainnet', db, applier, rollback, hashVerifier, config, util);
            sync.lastAppliedBlock = 500;
            sync.lastHashes = null;

            await sync._handleReorg({ type: 'reorg', block_index: 1 }); // depth = 500
            assert.strictEqual(rollback.rollback.called, false);
            assert.strictEqual(sync.isHalted(), true);
            assert.strictEqual(sync.getHaltInfo().reason, 'max-rollback-depth-exceeded');
        });

        // Hardened 2026-07-08 re-sweep: a reorg with a null tip (empty replica) is now a
        // no-op, not a rollback. The old behavior set lastAppliedBlock = block_index - 1
        // from purely server-supplied data, inflating the in-memory tip past an empty DB
        // and wedging the replica (the same shape as the above-tip wedge). A hostile
        // server can no longer drive the cursor via a null-tip reorg. Unreachable on the
        // live path (the WS opens only after start()'s non-null guard), but guarded.
        it('ignores a reorg when lastAppliedBlock is null (no cursor inflation from server data)', async function(){
            let config = createConfig({ MAX_ROLLBACK_DEPTH: 5 });
            let sync = new ClientSync('bitcoin', 'mainnet', db, applier, rollback, hashVerifier, config, util);
            sync.lastAppliedBlock = null;

            await sync._handleReorg({ type: 'reorg', block_index: 50 });
            assert.strictEqual(rollback.rollback.called, false, 'nothing to roll back with no committed tip');
            assert.strictEqual(sync.lastAppliedBlock, null, 'the cursor must NOT be inflated from server-supplied block_index');
            assert.strictEqual(sync.isHalted(), false, 'a null-tip reorg is a benign no-op, not a halt');
        });
    });

    // ── _handleBlock: strict cross-source timeout ──

    describe('_handleBlock: strict cross-source timeout', function(){

        let ClientSync;

        beforeEach(function(){
            ClientSync = proxyquire('../../src/ClientSync', {
                'axios': { get: sinon.stub() },
                'ws': sinon.stub()
            });
        });

        it('HASH_CONFIRM_STRICT=true rejects block on timeout', async function(){
            let clock = sinon.useFakeTimers();
            let config = createConfig({
                HASH_CONFIRM_STRICT: true,
                HASH_CONFIRM_TIMEOUT: 100,
                VERIFY_HASHES: true
            });
            let sync = new ClientSync('bitcoin', 'mainnet', db, applier, rollback, hashVerifier, config, util);
            sync.lastAppliedBlock = 99;
            sync.lastHashes = { ledger_hash: 'a', actions_hash: 'b', contract_hash: 'c' };

            let event = {
                type: 'block',
                block_index: 100,
                ledger_hash: 'x',
                actions_hash: 'y',
                contract_hash: 'z',
                data: {}
            };

            // Only primary source (sourceIndex 0) reports, no second source
            await sync._handleBlock(event, 0);

            // Advance past the timeout
            await clock.tickAsync(200);

            // Block should NOT have been applied (strict mode)
            assert.strictEqual(applier.applyBlock.called, false);
            assert.strictEqual(console.error.called, true);
            let strictMsg = console.error.getCalls().find(c => c.args[0].includes('STRICT'));
            assert.ok(strictMsg, 'Expected STRICT error message');

            clock.restore();
        });

        it('HASH_CONFIRM_STRICT=false applies block on timeout', async function(){
            let clock = sinon.useFakeTimers();
            let config = createConfig({
                HASH_CONFIRM_STRICT: false,
                HASH_CONFIRM_TIMEOUT: 100,
                VERIFY_HASHES: true
            });
            let sync = new ClientSync('bitcoin', 'mainnet', db, applier, rollback, hashVerifier, config, util);
            sync.lastAppliedBlock = 99;
            sync.lastHashes = { ledger_hash: 'a', actions_hash: 'b', contract_hash: 'c' };

            let event = {
                type: 'block',
                block_index: 100,
                ledger_hash: 'x',
                actions_hash: 'y',
                contract_hash: 'z',
                data: {}
            };

            await sync._handleBlock(event, 0);

            // Advance past the timeout
            await clock.tickAsync(200);

            // Block SHOULD have been applied (non-strict, fallback to primary)
            assert.strictEqual(applier.applyBlock.calledOnce, true);

            clock.restore();
        });
    });

    // ── _connectWebSocket: maxPayload option ──

    describe('_connectWebSocket: maxPayload', function(){

        it('passes maxPayload to WebSocket constructor', function(){
            let wsConstructorCalls = [];
            let fakeWs = function(url, opts){
                wsConstructorCalls.push({ url, opts });
                return {
                    on: sinon.stub(),
                    close: sinon.stub()
                };
            };

            let ClientSync = proxyquire('../../src/ClientSync', {
                'axios': { get: sinon.stub() },
                'ws': fakeWs
            });

            let config = createConfig({ WS_MAX_PAYLOAD: 2097152 });
            let sync = new ClientSync('bitcoin', 'mainnet', db, applier, rollback, hashVerifier, config, util);
            sync._connectWebSocket('http://source1.local', 0);

            assert.strictEqual(wsConstructorCalls.length, 1);
            assert.strictEqual(wsConstructorCalls[0].opts.maxPayload, 2097152);
        });
    });

    // ── WebSocket message handler: event validation ──

    describe('WebSocket message handler: event validation', function(){

        it('rejects message with unknown event type', async function(){
            let messageHandler = null;
            let fakeWs = function(url, opts){
                return {
                    on: function(event, handler){
                        if(event === 'message') messageHandler = handler;
                    },
                    close: sinon.stub()
                };
            };

            let ClientSync = proxyquire('../../src/ClientSync', {
                'axios': { get: sinon.stub() },
                'ws': fakeWs
            });

            let config = createConfig();
            let sync = new ClientSync('bitcoin', 'mainnet', db, applier, rollback, hashVerifier, config, util);
            sinon.stub(sync, '_handleEvent');
            sync._connectWebSocket('http://source1.local', 0);

            assert.ok(messageHandler, 'message handler should be registered');

            let invalidEvent = JSON.stringify({ type: 'DROP TABLE', block_index: 1 });
            // The ws 'message' handler is fire-and-serialize: it validates synchronously
            // then chains _handleEvent onto the internal _wsEventChain rather than returning
            // a promise (concurrency fix; see ClientSync._connectWebSocket). An invalid event
            // is rejected before any chaining, so flush microtasks and assert.
            messageHandler(Buffer.from(invalidEvent));
            await (sync._wsEventChain || Promise.resolve());
            assert.strictEqual(sync._handleEvent.called, false);
            assert.strictEqual(console.error.called, true);
        });

        it('rejects non-JSON message without crashing', async function(){
            let messageHandler = null;
            let fakeWs = function(url, opts){
                return {
                    on: function(event, handler){
                        if(event === 'message') messageHandler = handler;
                    },
                    close: sinon.stub()
                };
            };

            let ClientSync = proxyquire('../../src/ClientSync', {
                'axios': { get: sinon.stub() },
                'ws': fakeWs
            });

            let config = createConfig();
            let sync = new ClientSync('bitcoin', 'mainnet', db, applier, rollback, hashVerifier, config, util);
            sinon.stub(sync, '_handleEvent');
            sync._connectWebSocket('http://source1.local', 0);

            // Fire-and-serialize handler (see the unknown-event-type test): a non-JSON
            // message is rejected synchronously in the try/catch before any chaining.
            messageHandler(Buffer.from('not valid json'));
            await (sync._wsEventChain || Promise.resolve());
            assert.strictEqual(sync._handleEvent.called, false);
        });

        it('accepts valid block event and calls _handleEvent', async function(){
            let messageHandler = null;
            let fakeWs = function(url, opts){
                return {
                    on: function(event, handler){
                        if(event === 'message') messageHandler = handler;
                    },
                    close: sinon.stub()
                };
            };

            let ClientSync = proxyquire('../../src/ClientSync', {
                'axios': { get: sinon.stub() },
                'ws': fakeWs
            });

            let config = createConfig();
            let sync = new ClientSync('bitcoin', 'mainnet', db, applier, rollback, hashVerifier, config, util);
            sinon.stub(sync, '_handleEvent').resolves();
            sync._connectWebSocket('http://source1.local', 0);

            let validEvent = JSON.stringify({ type: 'block', block_index: 100, data: {} });
            // A valid event is chained onto _wsEventChain; await that internal chain so the
            // serialized _handleEvent call has run before asserting.
            messageHandler(Buffer.from(validEvent));
            await (sync._wsEventChain || Promise.resolve());
            assert.strictEqual(sync._handleEvent.calledOnce, true);
        });
    });
});
