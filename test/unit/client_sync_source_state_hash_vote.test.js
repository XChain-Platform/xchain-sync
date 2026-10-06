/*********************************************************************
 *
 * Copyright © 2025–2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC – https://dankest.llc
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This file is part of XChain Platform. Licensed under the GNU Affero
 * General Public License v3.0 or later; see LICENSE.md.
 *
 **********************************************************************
 * ClientSync: the multi-source vote counts state_hash, so sources that agree on
 * the ledger/actions/contract hashes but report different state hashes do not
 * form a quorum together.
 ********************************************************************/

const assert = require('assert');
const sinon  = require('sinon');
const ClientSync   = require('../../src/client/sync');
const Utility      = require('../../src/util');
const HashVerifier = require('../../src/client/hash_verifier');

function blockEvent(blockIndex, state){
    return { type: 'block', block_index: blockIndex,
        ledger_hash: 'lh', actions_hash: 'ah', contract_hash: 'ch', state_hash: state };
}

function makeSync(sourcesCsv){
    let db = {
        dbName: 'test_db', dbType: 'indexer',
        getLastBlock: sinon.stub().resolves(null),
        getBlockHashRow: sinon.stub().resolves(null),
        getActiveHalt: sinon.stub().resolves(null),
        recordHalt: sinon.stub().resolves({ block_index: 0 }),
        doQuery: sinon.stub().resolves([])
    };
    let applier = { applyBlock: sinon.stub().resolves(), applyFullSnapshot: sinon.stub().resolves() };
    let config = {
        SYNC_SOURCES: sourcesCsv, VERIFY_HASHES: true, HALT_ON_DIVERGENCE: true,
        HASH_CONFIRM_TIMEOUT: 5000, CLIENT_RECONNECT_DELAY: 5000,
        VERIFY_RECOMPUTE: false, VERIFY_STATE_HASH: false, VERIFY_STATE_COMMITMENT: false
    };
    let sync = new ClientSync('bitcoin', 'mainnet', db, applier,
        { rollback: sinon.stub().resolves() }, new HashVerifier(), config, new Utility());
    sync.lastAppliedBlock = 100;
    sync.lastHashes = { ledger_hash: 'lhX', actions_hash: 'ahX', contract_hash: 'chX' };
    return { sync, applier };
}

describe('ClientSync: multi-source state_hash vote @regression', function(){
    beforeEach(function(){ sinon.stub(console, 'log'); sinon.stub(console, 'warn'); sinon.stub(console, 'error'); });
    afterEach(function(){ sinon.restore(); });

    it('tuple key differs when only state_hash differs', function(){
        let { sync } = makeSync('http://a:3006,http://b:3006');
        let a = sync.hashTupleKey(blockEvent(101, 'S1'));
        let b = sync.hashTupleKey(blockEvent(101, 'S2'));
        assert.notStrictEqual(a, b);
        assert.strictEqual(sync.hashTupleKey(blockEvent(101, undefined)), sync.hashTupleKey(blockEvent(101, null)));
    });

    it('two sources with equal hashes but different state_hash halt with no-source-quorum', async function(){
        let { sync, applier } = makeSync('http://a:3006,http://b:3006');
        await sync.handleBlock(blockEvent(101, 'S1'), 0);
        await sync.handleBlock(blockEvent(101, 'S2'), 1);
        assert.strictEqual(applier.applyBlock.called, false);
        assert.strictEqual(sync.isHalted(), true);
        assert.strictEqual(sync.getHaltInfo().reason, 'no-source-quorum');
    });

    it('a state_hash-only dissenter is struck and the matching majority applies', async function(){
        let { sync, applier } = makeSync('http://a:3006,http://b:3006,http://c:3006');
        await sync.handleBlock(blockEvent(101, 'S1'), 0);
        await sync.handleBlock(blockEvent(101, 'BAD'), 2);
        assert.strictEqual(applier.applyBlock.called, false);
        await sync.handleBlock(blockEvent(101, 'S1'), 1);
        assert.strictEqual(applier.applyBlock.calledOnce, true);
        assert.deepStrictEqual(sync._sourceStrikes.get(2), [101]);
    });

    it('sources agreeing on every field including state_hash still apply', async function(){
        let { sync, applier } = makeSync('http://a:3006,http://b:3006');
        await sync.handleBlock(blockEvent(101, 'S1'), 0);
        await sync.handleBlock(blockEvent(101, 'S1'), 1);
        assert.strictEqual(applier.applyBlock.calledOnce, true);
        assert.strictEqual(sync.isHalted(), false);
    });
});
