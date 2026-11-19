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

// A replica at `tip` whose rollback is a stub, with the real apply lock.
function makeSync(tip, maxRollbackDepth = 500){
    let db = {
        dbName: 'indexer_db', dbType: 'indexer',
        getLastBlock: sinon.stub().resolves(null), getActiveHalt: sinon.stub().resolves(null),
        getBlockHashRow: sinon.stub().resolves(null), setSyncState: sinon.stub().resolves()
    };
    let rollback = { rollback: sinon.stub().resolves() };
    let config = { SYNC_SOURCES: 'http://src1:3006', MAX_ROLLBACK_DEPTH: maxRollbackDepth };
    let sync = new ClientSync('bitcoin', 'mainnet', db, { applyBlock: sinon.stub().resolves() },
        rollback, new HashVerifier(), config, new Utility());
    sync.lastAppliedBlock = tip;
    sinon.stub(sync, 'beginRowRemoval').resolves();
    return { sync, rollback };
}

// handleReorg tests the target against the tip before it takes the apply lock, so two
// reorgs that both pass at the old tip queue on the lock and the second one runs after
// the first has lowered the tip.
describe('ClientSync reorg rollback re-checks the tip under the apply lock @regression', function(){
    beforeEach(function(){ sinon.stub(console, 'log'); sinon.stub(console, 'warn'); sinon.stub(console, 'error'); });
    afterEach(function(){ sinon.restore(); });

    it('a target the tip fell below while queued on the lock is a no-op', async function(){
        let { sync, rollback } = makeSync(200);

        let first  = sync.applyReorgRollback({ type: 'reorg', block_index: 100 });
        let second = sync.applyReorgRollback({ type: 'reorg', block_index: 150 });

        assert.strictEqual(await first, true);
        assert.strictEqual(await second, false, 'the queued above-tip target must not roll back');
        assert.strictEqual(rollback.rollback.callCount, 1, 'only the first target deletes rows');
        assert.strictEqual(rollback.rollback.firstCall.args[0], 100);
        assert.strictEqual(sync.lastAppliedBlock, 99, 'the cursor must never move forward over unapplied heights');
    });

    it('a target at or below the lowered tip still rolls back', async function(){
        let { sync, rollback } = makeSync(200);

        let first  = sync.applyReorgRollback({ type: 'reorg', block_index: 150 });
        let second = sync.applyReorgRollback({ type: 'reorg', block_index: 100 });

        assert.strictEqual(await first, true);
        assert.strictEqual(await second, true);
        assert.deepStrictEqual(rollback.rollback.args.map(a => a[0]), [150, 100]);
        assert.strictEqual(sync.lastAppliedBlock, 99);
    });

    it('refuses when the rollback depth grows past the limit while waiting for the lock', async function(){
        const { sync, rollback } = makeSync(1000, 126);
        sinon.stub(sync, 'haltForExcessiveRollback').resolves();
        sinon.stub(sync, 'persistRollbackGuardState').resolves();
        let releaseLock;
        const held = sync.withApplyLock(() => new Promise(resolve => { releaseLock = resolve; }));
        await new Promise(resolve => setImmediate(resolve));

        const reorg = sync.handleReorg({ type: 'reorg', block_index: 875 });
        await new Promise(resolve => setImmediate(resolve));
        sync.lastAppliedBlock = 1001;
        releaseLock();
        await Promise.all([held, reorg]);

        assert.strictEqual(sync.haltForExcessiveRollback.calledOnceWithExactly(
            sinon.match({ block_index: 875 }), 127), true);
        assert.strictEqual(rollback.rollback.called, false);
        assert.strictEqual(sync.beginRowRemoval.called, false);
        assert.strictEqual(sync.persistRollbackGuardState.called, false);
        assert.deepStrictEqual(sync.rollbackGuard.toState(), { peak: null, low: null });
        assert.strictEqual(sync.lastAppliedBlock, 1001);
    });
});
