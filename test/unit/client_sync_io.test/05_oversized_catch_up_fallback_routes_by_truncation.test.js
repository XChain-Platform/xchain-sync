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
describe('ClientSync: oversized catch-up fallback routes by truncation', function(){
    let sync, db, applier;
    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
        sinon.stub(console, 'warn');
    });
    afterEach(function(){ sinon.restore(); });

    it('truncated replica: size error routes to bootstrapFromHeightRetry, NOT the full snapshot @regression', async function(){
        // The full snapshot of a SYNC_BOOTSTRAP_DEPTH chain is oversized by definition,
        // so falling back to it would crash-loop. Route to the bounded height bootstrap.
        ({ sync, db, applier } = makeSync({ SYNC_BOOTSTRAP_DEPTH: { 'BTC:MAINNET': 50000 } }));
        assert.ok(sync._truncatedDepth >= 1, 'chain is truncated');
        sinon.stub(sync, 'syncLookupTablesPaged').resolves();
        let heightRetry = sinon.stub(sync, 'bootstrapFromHeightRetry').resolves();
        let fullSnap    = sinon.stub(sync, 'bootstrapFromSnapshot').resolves();
        db.getLastBlock.resolves(100);
        let sizeErr = new Error('maxContentLength size of X exceeded');
        sizeErr.code = 'ERR_FR_MAX_CONTENT_LENGTH_EXCEEDED';
        sinon.stub(axios, 'get').rejects(sizeErr);

        await sync.runIncrementalCatchUp();

        assert.ok(heightRetry.calledOnceWithExactly(sync._truncatedDepth),
            'bootstrapFromHeightRetry must be called with the truncation depth');
        assert.strictEqual(fullSnap.called, false,
            'the full snapshot must NOT be fetched on a truncated replica');
    });

    it('full-history replica: size error still routes to bootstrapFromSnapshot (unchanged) @regression', async function(){
        ({ sync, db, applier } = makeSync()); // _truncatedDepth 0
        assert.strictEqual(sync._truncatedDepth, 0);
        let heightRetry = sinon.stub(sync, 'bootstrapFromHeightRetry').resolves();
        let fullSnap    = sinon.stub(sync, 'bootstrapFromSnapshot').resolves();
        db.getLastBlock.resolves(100);
        let sizeErr = new Error('maxContentLength size of X exceeded');
        sizeErr.code = 'ERR_FR_MAX_CONTENT_LENGTH_EXCEEDED';
        sinon.stub(axios, 'get').rejects(sizeErr);

        await sync.runIncrementalCatchUp();

        assert.ok(fullSnap.calledOnce, 'full-history replica keeps the full-snapshot fallback');
        assert.strictEqual(heightRetry.called, false, 'height retry must not run for a full-history replica');
    });
});
