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
describe('ClientSync: indexer head-fork re-delivery', function(){
    let sync, db, applier;
    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });
    afterEach(function(){ sinon.restore(); });

    it('indexer: at-tip re-delivery with a DIFFERENT hash triggers catch-up (lost 1-block reorg) @regression', async function(){
        ({ sync, db, applier } = makeSync()); // indexer
        sync.lastAppliedBlock = 100;
        sync.lastHashes = { ledger_hash: 'lh100', actions_hash: 'ah100', contract_hash: 'ch100' };
        sinon.stub(sync, 'incrementalCatchUp').resolves();

        await sync.handleBlock({ type: 'block', block_index: 100,
            ledger_hash: 'lh100-FORKED', actions_hash: 'ah100', contract_hash: 'ch100' }, 0);

        assert.ok(sync.incrementalCatchUp.calledOnce, 'a forked tip re-delivery must trigger catch-up');
        assert.strictEqual(applier.applyBlock.called, false, 'the orphaned tip is not applied');
    });

    it('indexer: at-tip re-delivery with IDENTICAL hashes is a silent skip (true duplicate) @regression', async function(){
        ({ sync, db, applier } = makeSync());
        sync.lastAppliedBlock = 100;
        sync.lastHashes = { ledger_hash: 'lh100', actions_hash: 'ah100', contract_hash: 'ch100' };
        sinon.stub(sync, 'incrementalCatchUp').resolves();

        await sync.handleBlock({ type: 'block', block_index: 100,
            ledger_hash: 'lh100', actions_hash: 'ah100', contract_hash: 'ch100' }, 0);

        assert.strictEqual(sync.incrementalCatchUp.called, false, 'a true duplicate must not trigger catch-up');
        assert.strictEqual(applier.applyBlock.called, false);
    });

    it('decoder head-fork behaviour is unchanged (block_hash mismatch still triggers catch-up) @regression', async function(){
        ({ sync, db, applier } = makeSync({}, { dbType: 'decoder' }));
        sync.lastAppliedBlock = 100;
        sync.lastHashes = { block_hash: 'bh100' };
        sinon.stub(sync, 'incrementalCatchUp').resolves();

        await sync.handleBlock({ type: 'block', block_index: 100, block_hash: 'bh100-FORKED' }, 0);

        assert.ok(sync.incrementalCatchUp.calledOnce, 'decoder fork detection unchanged');
    });
});
