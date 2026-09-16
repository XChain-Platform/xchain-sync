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
describe('ClientSync: decoder completeness check on a truncated replica', function(){
    let sync, db;
    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });
    afterEach(function(){ sinon.restore(); });

    it('truncated decoder: block-windowed tables are excluded from the count check, lookups stay strict @regression', async function(){
        ({ sync, db } = makeSync({}, { dbType: 'decoder' }));
        sync._bootstrapBase = 950000; // isTruncated() -> true
        assert.ok(sync.isTruncated(), 'replica is truncated');
        let vtc = sinon.stub(sync, 'verifyTableCounts').resolves([]);
        sinon.stub(axios, 'get').resolves({ data: { table_counts: { blocks: 1, index_transactions: 1 } } });

        await sync.verifyDecoderCompleteness('http://src1:3006', 100);

        assert.ok(vtc.calledOnce);
        let excludes = vtc.firstCall.args[1];
        assert.ok(excludes instanceof Set, 'an exclusion Set is passed when truncated');
        for(let t of ['blocks', 'transactions', 'transaction_outputs'])
            assert.ok(excludes.has(t), t + ' (block-windowed) must be excluded on a truncated replica');
        assert.strictEqual(excludes.has('index_transactions'), false,
            'append-only lookups stay under the strict count check');
    });

    it('full-history decoder: no windowed exclusion, caller excludes pass through unchanged @regression', async function(){
        ({ sync, db } = makeSync({}, { dbType: 'decoder' }));
        sync._bootstrapBase = null; // isTruncated() -> false
        assert.strictEqual(sync.isTruncated(), false);
        let vtc = sinon.stub(sync, 'verifyTableCounts').resolves([]);
        sinon.stub(axios, 'get').resolves({ data: { table_counts: { blocks: 1 } } });

        await sync.verifyDecoderCompleteness('http://src1:3006', 100, new Set(['dispensers']));

        let excludes = vtc.firstCall.args[1];
        assert.ok(excludes.has('dispensers'), 'caller-supplied excludes pass through');
        assert.strictEqual(excludes.has('blocks'), false, 'block-windowed tables NOT excluded when full-history');
    });
});
