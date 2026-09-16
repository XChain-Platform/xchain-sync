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
const HashVerifier = require('../../../src/client/hash_verifier');
const { withDbMixins } = require('../../helpers/db_mixins.js');

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

function loadClientSync(){
    return proxyquire('../../../src/client/sync', {
        'axios': { get: sinon.stub() },
        'ws': sinon.stub()
    });
}

describe('ClientSync security', function(){
    registerHooks();

    // ── handleReorg: max rollback depth ──

    describe('handleReorg: max rollback depth', function(){

        let ClientSync;

        beforeEach(function(){
            ClientSync = loadClientSync();
        });

        it('allows rollback within MAX_ROLLBACK_DEPTH', async function(){
            let config = createConfig({ MAX_ROLLBACK_DEPTH: 5 });
            let sync = new ClientSync('bitcoin', 'mainnet', db, applier, rollback, hashVerifier, config, util);
            sync.lastAppliedBlock = 100;
            sync.lastHashes = { ledger_hash: 'abc', actions_hash: 'def', contract_hash: 'ghi' };

            await sync.handleReorg({ type: 'reorg', block_index: 96 }); // depth = 5
            assert.strictEqual(rollback.rollback.calledOnce, true);
            assert.strictEqual(rollback.rollback.firstCall.args[0], 96);
        });

        it('allows rollback of depth 1', async function(){
            let config = createConfig({ MAX_ROLLBACK_DEPTH: 5 });
            let sync = new ClientSync('bitcoin', 'mainnet', db, applier, rollback, hashVerifier, config, util);
            sync.lastAppliedBlock = 100;
            sync.lastHashes = null;

            await sync.handleReorg({ type: 'reorg', block_index: 100 }); // depth = 1
            assert.strictEqual(rollback.rollback.calledOnce, true);
        });
    });
});

describe('ClientSync security', function(){
    registerHooks();

    describe('handleReorg: max rollback depth', function(){

        let ClientSync;

        beforeEach(function(){
            ClientSync = loadClientSync();
        });

        it('HALTS (fails closed) when reorg exceeds MAX_ROLLBACK_DEPTH', async function(){
            let config = createConfig({ MAX_ROLLBACK_DEPTH: 5 });
            let sync = new ClientSync('bitcoin', 'mainnet', db, applier, rollback, hashVerifier, config, util);
            sync.lastAppliedBlock = 100;
            sync.lastHashes = null;

            await sync.handleReorg({ type: 'reorg', block_index: 95 }); // depth = 6
            // Must NOT roll back (too deep to rewind safely)...
            assert.strictEqual(rollback.rollback.called, false);
            // ...and must NOT fail open: a durable halt is recorded so the replica
            // stops applying instead of silently serving the orphaned fork.
            assert.strictEqual(sync.isHalted(), true);
            assert.strictEqual(sync.getHaltInfo().reason, 'max-rollback-depth-exceeded');
            assert.strictEqual(db.recordHalt.calledOnce, true);
            assert.strictEqual(db.recordHalt.firstCall.args[2], 'max-rollback-depth-exceeded');
            // lastAppliedBlock is left untouched: we did not advance, but we also
            // halt so the stale value can no longer drop canonical blocks.
            assert.strictEqual(sync.lastAppliedBlock, 100);
        });
    });
});

describe('ClientSync security', function(){
    registerHooks();

    describe('handleReorg: max rollback depth', function(){

        let ClientSync;

        beforeEach(function(){
            ClientSync = loadClientSync();
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

            await sync.handleReorg({ type: 'reorg', block_index: 95 }); // depth = 6
            assert.strictEqual(rollback.rollback.called, false);
            assert.strictEqual(sync.isHalted(), true);
            assert.strictEqual(sync.getHaltInfo().reason, 'max-rollback-depth-exceeded');
            assert.strictEqual(db.recordHalt.calledOnce, true);
            assert.strictEqual(db.recordHalt.firstCall.args[0], 'decoder');
        });
    });
});

describe('ClientSync security', function(){
    registerHooks();

    describe('handleReorg: max rollback depth', function(){

        let ClientSync;

        beforeEach(function(){
            ClientSync = loadClientSync();
        });

        it('HALTS on deep rollback to block 1', async function(){
            let config = createConfig({ MAX_ROLLBACK_DEPTH: 100 });
            let sync = new ClientSync('bitcoin', 'mainnet', db, applier, rollback, hashVerifier, config, util);
            sync.lastAppliedBlock = 500;
            sync.lastHashes = null;

            await sync.handleReorg({ type: 'reorg', block_index: 1 }); // depth = 500
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

            await sync.handleReorg({ type: 'reorg', block_index: 50 });
            assert.strictEqual(rollback.rollback.called, false, 'nothing to roll back with no committed tip');
            assert.strictEqual(sync.lastAppliedBlock, null, 'the cursor must NOT be inflated from server-supplied block_index');
            assert.strictEqual(sync.isHalted(), false, 'a null-tip reorg is a benign no-op, not a halt');
        });
    });
});
