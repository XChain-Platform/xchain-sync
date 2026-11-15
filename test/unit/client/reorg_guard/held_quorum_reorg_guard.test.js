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
const sinon = require('sinon');
const ClientSync = require('../../../../src/client/sync');
const HashVerifier = require('../../../../src/client/hash_verifier');
const Utility = require('../../../../src/util');

function blockEvent(blockIndex){
    return {
        type: 'block', block_index: blockIndex,
        ledger_hash: 'ledger', actions_hash: 'actions', contract_hash: 'contract'
    };
}

function makeSync(rollback){
    const db = {
        dbName: 'test_db', dbType: 'indexer',
        getLastBlock: sinon.stub().resolves(null),
        getBlockHashRow: sinon.stub().resolves(null),
        getActiveHalt: sinon.stub().resolves(null),
        recordHalt: sinon.stub().resolves({ block_index: 0 }),
        setSyncState: sinon.stub().resolves(),
        doQuery: sinon.stub().resolves([])
    };
    const applier = {
        applyBlock: sinon.stub().resolves(),
        applyFullSnapshot: sinon.stub().resolves()
    };
    const config = {
        SYNC_SOURCES: 'http://a:3006,http://b:3006,http://c:3006',
        VERIFY_HASHES: true,
        VERIFY_RECOMPUTE: false,
        VERIFY_STATE_HASH: false,
        VERIFY_STATE_COMMITMENT: false,
        HALT_ON_DIVERGENCE: true,
        HASH_CONFIRM_TIMEOUT: 5000,
        MAX_ROLLBACK_DEPTH: 10
    };
    const sync = new ClientSync('bitcoin', 'mainnet', db, applier, { rollback },
        new HashVerifier(), config, new Utility());
    sync.lastAppliedBlock = 100;
    sync.lastHashes = { ledger_hash: 'old', actions_hash: 'old', contract_hash: 'old' };
    return { sync, applier };
}

describe('ClientSync held quorum timer across a reorg @regression', function(){
    let clock;
    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
        sinon.stub(console, 'warn');
        clock = sinon.useFakeTimers();
    });
    afterEach(function(){ clock.restore(); sinon.restore(); });

    it('does not apply timer work that queued behind an in-progress rollback', async function(){
        let releaseRollback;
        const rollback = sinon.stub().callsFake(() => new Promise(resolve => {
            releaseRollback = resolve;
        }));
        const { sync, applier } = makeSync(rollback);
        sinon.stub(sync, 'beginRowRemoval').resolves();

        await sync.handleBlock(blockEvent(101), 0);
        const reorg = sync.handleReorg({ type: 'reorg', block_index: 98 });
        for(let i = 0; i < 5 && !rollback.called; i++) await Promise.resolve();
        assert.strictEqual(rollback.calledOnce, true, 'rollback holds the apply lock');

        clock.tick(5100);
        await Promise.resolve();
        releaseRollback();
        await reorg;
        for(let i = 0; i < 5; i++) await Promise.resolve();

        assert.strictEqual(applier.applyBlock.called, false);
        assert.strictEqual(sync.lastAppliedBlock, 97);
        assert.strictEqual(sync.pendingHashes.has(101), false);
    });
});
