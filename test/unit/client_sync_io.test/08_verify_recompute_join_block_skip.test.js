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
describe('ClientSync verifyRecompute join-block skip', function(){
    let sync;
    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });
    afterEach(function(){ sinon.restore(); });

    it('skips recompute for the bootstrap join block (no base-1 predecessor)', async function(){
        ({ sync } = makeSync());
        sync._bootstrapBase = 950000;
        let compute = sinon.stub(sync.blockHasher, 'computeBlockHashes');

        let result = await sync.verifyRecompute({ block_index: 950000 }, { ledger_hash: 'lh' });

        assert.strictEqual(result, null, 'join block treated as clean');
        assert.strictEqual(compute.called, false, 'computeBlockHashes never invoked for the join block');
    });

    it('still recomputes every block above the join block', async function(){
        ({ sync } = makeSync());
        sync._bootstrapBase = 950000;
        let compute = sinon.stub(sync.blockHasher, 'computeBlockHashes')
            .resolves({ ledger_hash: 'lh', actions_hash: 'ah', contract_hash: 'ch' });

        let result = await sync.verifyRecompute({ block_index: 950001 },
            { ledger_hash: 'lh', actions_hash: 'ah', contract_hash: 'ch' });

        assert.strictEqual(result, null, 'matching block is clean');
        assert.ok(compute.calledOnceWith(950001), 'block above the join is recomputed');
    });

    it('full-history replica (null base) recomputes the lowest block too', async function(){
        ({ sync } = makeSync());
        // _bootstrapBase stays null
        let compute = sinon.stub(sync.blockHasher, 'computeBlockHashes')
            .resolves({ ledger_hash: 'lh', actions_hash: 'ah', contract_hash: 'ch' });

        await sync.verifyRecompute({ block_index: 0 }, { ledger_hash: 'lh', actions_hash: 'ah', contract_hash: 'ch' });

        assert.ok(compute.calledOnceWith(0), 'no skip when not a truncated replica');
    });
});
