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
const ClientSync = require('../../../../src/client/sync');
const Utility    = require('../../../../src/util');
const HashVerifier = require('../../../../src/client/hash_verifier');
const { withDbMixins } = require('../../../helpers/db_mixins.js');

function makeSync(configOverrides, dbOverrides){
    let db = Object.assign({
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
    }, dbOverrides || {});
    let applier = {
        applyBlock:               sinon.stub().resolves(),
        applyFullSnapshot:        sinon.stub().resolves(),
        applyIncrementalSnapshot: sinon.stub().resolves()
    };
    let config = Object.assign({
        SYNC_SOURCES:          'http://src1:3006',
        VERIFY_HASHES:         false,
        CLIENT_RECONNECT_DELAY: 5000,
        HASH_CONFIRM_TIMEOUT:  5000,
        SNAPSHOT_MAX_CONTENT:  200 * 1024 * 1024,
        WS_MAX_PAYLOAD:        50 * 1024 * 1024,
        MAX_ROLLBACK_DEPTH:    10,
        GAP_LOG_INTERVAL_MS:   30000
    }, configOverrides || {});
    let sync = new ClientSync('bitcoin', 'mainnet', withDbMixins(db), applier,
        { rollback: sinon.stub().resolves() }, new HashVerifier(), config, new Utility());
    return { sync, db, applier };
}

// Shared by the case groups below; each case assigns them from makeSync.
let sync, db, applier;

// Below-tip replacement: a mismatch at the first differing height rewinds, matching or absent evidence stays silent.
function belowTipRewindCases(){
    it('indexer: below-tip replacement rewinds from the first mismatching height @regression', async function(){
        ({ sync, db, applier } = makeSync());
        sync.lastAppliedBlock = 105;
        db.getBlockHashRow.resolves({ ledger_hash: 'old-l', actions_hash: 'same-a', contract_hash: 'same-c' });
        let rewind = sinon.stub(sync, 'rewindForkedHead').resolves();

        let action = sync.handlePreviouslyAppliedBlock({ type: 'block', block_index: 102,
            ledger_hash: 'new-l', actions_hash: 'same-a', contract_hash: 'same-c' });
        await action.completion;

        assert.ok(rewind.calledOnceWithExactly(102));
        assert.ok(db.getBlockHashRow.calledWith(102, null, { rethrow: true }));
        assert.strictEqual(applier.applyBlock.called, false);
    });

    it('indexer: matching, absent, and null-only below-tip evidence stay silent', async function(){
        ({ sync, db } = makeSync());
        sync.lastAppliedBlock = 105;
        let rewind = sinon.stub(sync, 'rewindForkedHead').resolves();
        db.getBlockHashRow.onCall(0).resolves({ ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c' });
        db.getBlockHashRow.onCall(1).resolves(null);
        db.getBlockHashRow.onCall(2).resolves({ ledger_hash: 'stored', actions_hash: null, contract_hash: null });

        for(let event of [
            { type: 'block', block_index: 101, ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c' },
            { type: 'block', block_index: 102, ledger_hash: 'different' },
            { type: 'block', block_index: 103, ledger_hash: null, actions_hash: 'incoming', contract_hash: null }
        ]){
            await sync.handlePreviouslyAppliedBlock(event).completion;
        }
        assert.strictEqual(rewind.called, false);
    });

    it('decoder: below-tip block_hash mismatch rewinds and a match skips', async function(){
        ({ sync, db } = makeSync({}, { dbType: 'decoder' }));
        sync.lastAppliedBlock = 105;
        db.getBlockHashRow.resolves({ block_hash: 'stored' });
        let rewind = sinon.stub(sync, 'rewindForkedHead').resolves();

        await sync.handlePreviouslyAppliedBlock(
            { type: 'block', block_index: 102, block_hash: 'replacement' }).completion;
        await sync.handlePreviouslyAppliedBlock(
            { type: 'block', block_index: 103, block_hash: 'stored' }).completion;
        assert.ok(rewind.calledOnceWithExactly(102));
    });
}

// Source replacements go through the reorg quorum gate; a block time or state hash difference alone also rewinds.
function belowTipQuorumAndChainedHashCases(){
    it('below-tip replacement from a source routes through the reorg quorum gate', async function(){
        ({ sync, db } = makeSync());
        sync.lastAppliedBlock = 105;
        db.getBlockHashRow.resolves({ ledger_hash: 'stored' });
        let sourceReorg = sinon.stub(sync, 'handleSourceReorg').resolves();
        let rewind = sinon.stub(sync, 'rewindForkedHead').resolves();

        await sync.handlePreviouslyAppliedBlock(
            { type: 'block', block_index: 102, ledger_hash: 'replacement' }, 2).completion;
        assert.ok(sourceReorg.calledOnceWithExactly(
            { type: 'reorg', block_index: 102 }, 2, true));
        assert.strictEqual(rewind.called, false);
    });

    it('indexer: a below-tip replacement with equal chained hashes but a different block time or state hash rewinds', async function(){
        ({ sync, db } = makeSync());
        sync.lastAppliedBlock = 105;
        let chained = { ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c' };
        let rewind = sinon.stub(sync, 'rewindForkedHead').resolves();
        db.getBlockHashRow.resolves(Object.assign({ block_time: 1000, state_hash: 's' }, chained));

        await sync.handlePreviouslyAppliedBlock(
            Object.assign({ type: 'block', block_index: 102, block_time: 1001, state_hash: 's' }, chained)).completion;
        assert.ok(rewind.calledOnceWithExactly(102), 'block time differs');
        await sync.handlePreviouslyAppliedBlock(
            Object.assign({ type: 'block', block_index: 103, block_time: 1000, state_hash: 'other' }, chained)).completion;
        assert.ok(rewind.calledTwice && rewind.secondCall.calledWithExactly(103), 'state hash differs');
    });
}

// Equal or null block time and state hash stay silent, and only the first mismatching height starts a rewind.
function belowTipSilenceAndOnceCases(){
    it('indexer: equal or null block time and state hash below the tip stay silent', async function(){
        ({ sync, db } = makeSync());
        sync.lastAppliedBlock = 105;
        let chained = { ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c' };
        let rewind = sinon.stub(sync, 'rewindForkedHead').resolves();
        db.getBlockHashRow.resolves(Object.assign({ block_time: 1000, state_hash: 's' }, chained));

        for(let extra of [
            { block_time: 1000, state_hash: 's' },
            { block_time: '1000', state_hash: null },
            { block_time: null, state_hash: null }
        ]){
            await sync.handlePreviouslyAppliedBlock(
                Object.assign({ type: 'block', block_index: 102 }, chained, extra)).completion;
        }
        assert.strictEqual(rewind.called, false);
    });

    it('only the first mismatching replacement height initiates a rewind', async function(){
        ({ sync, db } = makeSync());
        sync.lastAppliedBlock = 105;
        db.getBlockHashRow.callsFake(async height => ({ ledger_hash: height === 101 ? 'same' : 'old' }));
        let rewind = sinon.stub(sync, 'rewindForkedHead').callsFake(async height => {
            sync.lastAppliedBlock = height - 1;
        });

        await sync.handlePreviouslyAppliedBlock(
            { type: 'block', block_index: 101, ledger_hash: 'same' }).completion;
        await sync.handlePreviouslyAppliedBlock(
            { type: 'block', block_index: 102, ledger_hash: 'new' }).completion;
        assert.strictEqual(sync.handlePreviouslyAppliedBlock(
            { type: 'block', block_index: 103, ledger_hash: 'newer' }), null);
        assert.ok(rewind.calledOnceWithExactly(102));
    });
}

describe('ClientSync: indexer head-fork re-delivery', function(){

    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });
    afterEach(function(){ sinon.restore(); });

    belowTipRewindCases();

    belowTipQuorumAndChainedHashCases();

    belowTipSilenceAndOnceCases();
});
