// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

const assert     = require('assert');
const sinon      = require('sinon');
const axios      = require('axios');
const proxyquire = require('proxyquire');
const ClientSync = require('../../../src/client/sync');
const Utility    = require('../../../src/util');
const HashVerifier = require('../../../src/client/hash_verifier');
const { withDbMixins } = require('../../helpers/db_mixins.js');

function createMockDb(overrides){
    return Object.assign({
        dbName:           'test_db',
        dbType:           'indexer',
        getLastBlock:     sinon.stub().resolves(null),
        getBlockHashRow:  sinon.stub().resolves(null),
        doQuery:          sinon.stub().resolves([]),
        getActiveHalt:    sinon.stub().resolves(null),
        getTableCount:    sinon.stub().resolves(0),
        addMissingColumns: sinon.stub().resolves(),
        recordHalt:       sinon.stub().resolves({ block_index: 0 }),
        clearHalt:        sinon.stub().resolves(1)
    }, overrides || {});
}

function createMockApplier(){
    return {
        applyBlock:               sinon.stub().resolves(),
        applyFullSnapshot:        sinon.stub().resolves(),
        applyIncrementalSnapshot: sinon.stub().resolves()
    };
}

function createMockRollback(){
    return { rollback: sinon.stub().resolves() };
}

function makeSync(configOverrides, dbOverrides){
    let db      = createMockDb(dbOverrides);
    let applier = createMockApplier();
    let rb      = createMockRollback();
    let hv      = new HashVerifier();
    let util    = new Utility();
    let config  = Object.assign({
        SYNC_SOURCES:          'http://src1:3006',
        VERIFY_HASHES:         false,
        CLIENT_RECONNECT_DELAY: 5000,
        HASH_CONFIRM_TIMEOUT:  5000,
        SNAPSHOT_MAX_CONTENT:  200 * 1024 * 1024,
        WS_MAX_PAYLOAD:        50 * 1024 * 1024,
        MAX_ROLLBACK_DEPTH:    10,
        GAP_LOG_INTERVAL_MS:   30000
    }, configOverrides || {});
    let sync = new ClientSync('bitcoin', 'mainnet', withDbMixins(db), applier, rb, hv, config, util);
    return { sync, db, applier, rb, hv, util, config };
}
describe('ClientSync: misc branch coverage', function(){
    let sync, db;

    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });
    afterEach(function(){ sinon.restore(); });

    it('haltOnDivergence: uses defaults when mismatches/sources are falsy', async function(){
        ({ sync, db } = makeSync());

        await sync.haltOnDivergence(50, null, null, 'test-reason');

        assert.deepStrictEqual(sync._halted.mismatches, []);
        assert.deepStrictEqual(sync._halted.sources, []);
    });

    it('haltOnDivergence: logs on recordHalt failure but still halts in memory', async function(){
        ({ sync, db } = makeSync());
        db.recordHalt.rejects(new Error('db error'));

        await sync.haltOnDivergence(50, [], [], 'test-reason');

        assert.ok(sync.isHalted(), 'must still be halted even when recordHalt fails');
        let errCalls = console.error.getCalls().map(c => c.args[0]);
        assert.ok(errCalls.some(m => m && m.indexOf('CRITICAL') !== -1));
    });

    it('verifyRecompute: returns null for decoder dbType', async function(){
        ({ sync, db } = makeSync({}, { dbType: 'decoder' }));

        let result = await sync.verifyRecompute({ block_index: 5 });

        assert.strictEqual(result, null);
    });

    it('safeParse: returns raw string when JSON.parse fails', function(){
        ({ sync, db } = makeSync());

        let result = sync.safeParse('not-valid-json{{{');

        assert.strictEqual(result, 'not-valid-json{{{');
    });

    it('clearHalt: logs without "was halted" when _halted is null', async function(){
        ({ sync, db } = makeSync());
        sync._halted = null;

        await sync.clearHalt();

        let logCalls = console.log.getCalls().map(c => c.args[0]);
        assert.ok(logCalls.some(m => m && m.indexOf('CLEARED') !== -1));
        assert.ok(!logCalls.some(m => m && m.indexOf('was halted at block') !== -1));
    });
});

describe('ClientSync: misc branch coverage', function(){
    let sync, db;

    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });
    afterEach(function(){ sinon.restore(); });



    it('clearHalt: logs error when db.clearHalt throws', async function(){
        ({ sync, db } = makeSync());
        db.clearHalt.rejects(new Error('db fail'));
        sync._halted = { blockIndex: 5, reason: 'test', mismatches: [], sources: [], at: '2026-01-01' };

        await sync.clearHalt();

        let errCalls = console.error.getCalls().map(c => c.args[0]);
        assert.ok(errCalls.some(m => m && m.indexOf('clearHalt persistence failed') !== -1));
        assert.strictEqual(sync._halted, null, '_halted must still be cleared');
    });

    it('flushHeartbeat: swallows ws.send error', function(){
        ({ sync, db } = makeSync({ SYNC_SOURCES: 'http://src1:3006' }));
        sync.lastAppliedBlock = 10;
        // WS that throws on send
        sync.wsConns = [{ readyState: 1, send: () => { throw new Error('send fail'); } }];
        sinon.stub(axios, 'post').resolves();

        // Should not throw
        sync.flushHeartbeat();

        assert.strictEqual(sync._hbLastSentBlock, 10);
    });

    it('verifyTableCounts: treats NaN local count as 0', async function(){
        ({ sync, db } = makeSync());
        db.getTableCount.resolves(NaN);

        let mismatches = await sync.verifyTableCounts({ blocks: 5 });

        // local=NaN → reset to 0 → remote(5) > local(0) → mismatch
        assert.strictEqual(mismatches.length, 1);
        assert.strictEqual(mismatches[0].localCount, 0);
    });
});

describe('ClientSync: misc branch coverage', function(){
    let sync, db;

    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });
    afterEach(function(){ sinon.restore(); });



    it('constructor: validatorId falls back to "unknown" when hostname() is empty', function(){
        // Requires proxyquire to intercept the inline require('os') in the constructor
        const fakeOs = { hostname: () => '' };
        const ClientSyncOS = proxyquire('../../../src/client/sync', { os: fakeOs });
        const savedId = process.env.VALIDATOR_ID;
        delete process.env.VALIDATOR_ID;

        let db2 = createMockDb();
        let s = new ClientSyncOS('bitcoin', 'mainnet', db2, createMockApplier(), createMockRollback(),
            new HashVerifier(), {
                SYNC_SOURCES: 'http://src:3006', VERIFY_HASHES: false, CLIENT_RECONNECT_DELAY: 5000,
                HASH_CONFIRM_TIMEOUT: 5000, SNAPSHOT_MAX_CONTENT: 1000, WS_MAX_PAYLOAD: 1000,
                MAX_ROLLBACK_DEPTH: 10, GAP_LOG_INTERVAL_MS: 30000
            }, new Utility());

        assert.strictEqual(s.validatorId, 'unknown');

        if(savedId !== undefined) process.env.VALIDATOR_ID = savedId;
    });
});
