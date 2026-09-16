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
// ─────────────────────────────────────────────────────────────────────────────
// 4b. HASH_CONFIRM_STRICT carried into the catch-up path (M-22)
// A block rejected by the strict cross-source hash gate must not be silently
// re-applied single-source by the incremental catch-up path seconds later.
// ─────────────────────────────────────────────────────────────────────────────
describe('ClientSync: strict cross-source gate survives catch-up (M-22)', function(){
    let sync, db, applier;

    function makeStrictSync(){
        return makeSync({
            SYNC_SOURCES:        'http://src1:3006,http://src2:3006',
            VERIFY_HASHES:       true,
            HASH_CONFIRM_STRICT: true,
            HASH_CONFIRM_TIMEOUT: 1000
        });
    }

    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });
    afterEach(function(){ sinon.restore(); });

    it('a strict cross-source timeout records the block and retains its pending hash', async function(){
        let clock = sinon.useFakeTimers();
        ({ sync, db, applier } = makeStrictSync());
        // lastAppliedBlock=5 with null lastHashes: verifyChainContinuity returns
        // valid (no prev hashes), so block 6 reaches the cross-source stage.
        sync.lastAppliedBlock = 5;
        sync.lastHashes = null;
        let event = { type: 'block', block_index: 6,
            ledger_hash: 'L6', actions_hash: 'A6', contract_hash: 'C6' };

        // Only source 0 delivers; the confirmation timer will fire unmatched.
        await sync.handleBlock(event, 0);
        assert.ok(sync._applyTimers.has(6), 'confirmation timer armed for block 6');

        clock.tick(1000); // fire the HASH_CONFIRM_TIMEOUT

        assert.ok(sync._strictConfirmPending.has(6),
            'strict timeout must record the block as awaiting cross-source confirmation');
        assert.ok(sync.pendingHashes.has(6),
            'the first source hash is retained so a later second source can complete the pair');
        assert.ok(applier.applyBlock.notCalled, 'strict mode must not apply single-source');
        clock.restore();
    });

    it('incrementalCatchUp refuses to run single-source while a strict block is pending', async function(){
        ({ sync, db, applier } = makeStrictSync());
        let runner = sinon.stub(sync, 'runIncrementalCatchUp').resolves();
        sync._strictConfirmPending.add(6);

        await sync.incrementalCatchUp(6);

        assert.strictEqual(runner.callCount, 0,
            'catch-up must not fetch+apply the strict-rejected block single-source');
        let errs = console.error.getCalls().map(c => String(c.args[0]));
        assert.ok(errs.some(m => m.indexOf('HASH_CONFIRM_STRICT') !== -1),
            'must log why the catch-up was refused');
    });
});

describe('ClientSync: strict cross-source gate survives catch-up (M-22)', function(){
    let sync, db, applier;

    function makeStrictSync(){
        return makeSync({
            SYNC_SOURCES:        'http://src1:3006,http://src2:3006',
            VERIFY_HASHES:       true,
            HASH_CONFIRM_STRICT: true,
            HASH_CONFIRM_TIMEOUT: 1000
        });
    }

    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });
    afterEach(function(){ sinon.restore(); });



    it('catch-up proceeds normally once no strict block is pending', async function(){
        ({ sync, db, applier } = makeStrictSync());
        let runner = sinon.stub(sync, 'runIncrementalCatchUp').resolves();
        // _strictConfirmPending is empty (the default), so the gate is inert.
        await sync.incrementalCatchUp(6);
        assert.strictEqual(runner.callCount, 1, 'gate is inert when nothing is strict-pending');
    });

    it('a second source confirming the block clears the strict block and unblocks catch-up', async function(){
        let clock = sinon.useFakeTimers();
        ({ sync, db, applier } = makeStrictSync());
        sync.lastAppliedBlock = 5;
        sync.lastHashes = null;
        sinon.stub(sync, 'applyBlockEvent').resolves(); // isolate from the heavy apply path
        let event0 = { type: 'block', block_index: 6, ledger_hash: 'L6', actions_hash: 'A6', contract_hash: 'C6' };

        await sync.handleBlock(event0, 0);
        clock.tick(1000);
        assert.ok(sync._strictConfirmPending.has(6), 'block 6 is strict-pending after timeout');

        // Second source delivers block 6 with identical hashes: the pair now matches.
        let event1 = { type: 'block', block_index: 6, ledger_hash: 'L6', actions_hash: 'A6', contract_hash: 'C6' };
        await sync.handleBlock(event1, 1);

        assert.ok(!sync._strictConfirmPending.has(6),
            'cross-source confirmation must clear the strict block');
        assert.ok(sync.applyBlockEvent.calledOnce, 'the confirmed block applies through the normal path');

        // Gate now inert: catch-up runs again.
        clock.restore();
        let runner = sinon.stub(sync, 'runIncrementalCatchUp').resolves();
        await sync.incrementalCatchUp(7);
        assert.strictEqual(runner.callCount, 1, 'catch-up unblocked after confirmation');
    });
});
