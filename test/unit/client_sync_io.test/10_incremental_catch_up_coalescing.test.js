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
describe('ClientSync: incrementalCatchUp coalescing', function(){
    let sync, db;

    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });
    afterEach(function(){ sinon.restore(); });

    it('calls runIncrementalCatchUp once when no in-flight', async function(){
        ({ sync, db } = makeSync());
        sinon.stub(sync, 'runIncrementalCatchUp').resolves();

        await sync.incrementalCatchUp(1);

        assert.strictEqual(sync.runIncrementalCatchUp.callCount, 1);
    });

    it('sets _catchUpPending when already in-flight', async function(){
        ({ sync, db } = makeSync());
        // Simulate an in-flight operation with a never-resolving promise so
        // the guard triggers. We only care that _catchUpPending is set and
        // that the call does not start a second runner.
        let neverResolve = new Promise(() => {});
        sync._catchUpInFlight = neverResolve;
        sinon.stub(sync, 'runIncrementalCatchUp').resolves();

        // Call it (do NOT await; the in-flight guard returns synchronously)
        sync.incrementalCatchUp(1);

        assert.strictEqual(sync._catchUpPending, true, '_catchUpPending must be set');
        assert.strictEqual(sync.runIncrementalCatchUp.called, false,
            'runIncrementalCatchUp must not start when already in-flight');
    });

    it('re-runs once when _catchUpPending and progress was made', async function(){
        ({ sync, db } = makeSync());
        let runCount = 0;
        sync.running = true;
        sinon.stub(sync, 'runIncrementalCatchUp').callsFake(async function(){
            runCount++;
            if(runCount === 1){
                // Simulate progress + pending request arriving during first run
                sync.lastAppliedBlock = (sync.lastAppliedBlock || 0) + 1;
                sync._catchUpPending = true;
            }
            // Second run: no more pending
        });

        await sync.incrementalCatchUp(1);

        assert.strictEqual(sync.runIncrementalCatchUp.callCount, 2,
            'must re-run exactly once when pending + progress');
    });
});

describe('ClientSync: incrementalCatchUp coalescing', function(){
    let sync, db;

    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });
    afterEach(function(){ sinon.restore(); });



    it('does not re-run when pending but no progress was made', async function(){
        ({ sync, db } = makeSync());
        sync.running = true;
        sinon.stub(sync, 'runIncrementalCatchUp').callsFake(async function(){
            // Simulate pending arriving but no progress (lastAppliedBlock unchanged)
            sync._catchUpPending = true;
            // lastAppliedBlock stays null
        });

        await sync.incrementalCatchUp(1);

        assert.strictEqual(sync.runIncrementalCatchUp.callCount, 1,
            'must not loop forever when no progress');
    });
});
