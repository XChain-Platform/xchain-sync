// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// Covers the indexer continuity check when the tip height is known but its hashes are not.
// prepareLiveFollow and applyReorgRollback store whatever getBlockHashRow returns, which is
// null when the tip's blocks row is missing; a gap must still route to catch-up then.
const {
    assert, sinon, ClientSync, registerClientSyncHooks
} = require('../support');

let sync, applier, rollback, hashVerifier, config, util, db;

function assignState(state){
    ({ sync, db, applier, rollback, hashVerifier, config, util } = state);
}

describe('ClientSync indexer continuity with a null tip hash row', function(){
    registerClientSyncHooks(assignState);

    beforeEach(function(){
        config.SYNC_SOURCES = 'http://source1:3006';
        config.VERIFY_HASHES = false;
        sync = new ClientSync('bitcoin', 'mainnet', db, applier, rollback, hashVerifier, config, util);
        sync.lastAppliedBlock = 5;
        sync.lastHashes = null;
        sinon.stub(sync, 'incrementalCatchUp').resolves();
    });

    it('routes a gapped block to catch-up from the height after the tip', async function(){
        let action = sync.handleIndexerBlockContinuity({ type: 'block', block_index: 9 });
        assert.ok(action, 'a gapped block must not pass the continuity check');
        await action.completion;
        assert.strictEqual(sync.incrementalCatchUp.callCount, 1);
        assert.strictEqual(sync.incrementalCatchUp.firstCall.args[0], 6);
    });

    it('lets the contiguous block through without a catch-up', function(){
        let action = sync.handleIndexerBlockContinuity({ type: 'block', block_index: 6 });
        assert.strictEqual(action, null);
        assert.strictEqual(sync.incrementalCatchUp.called, false);
    });

    it('handleBlock does not apply a gapped block over the missing range', async function(){
        await sync.handleBlock({ type: 'block', block_index: 9,
            ledger_hash: 'L9', actions_hash: 'A9', contract_hash: 'C9' }, 0);
        assert.strictEqual(sync.incrementalCatchUp.callCount, 1);
        assert.strictEqual(applier.applyBlock.called, false);
    });
});
