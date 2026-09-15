// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

const {
    assert, sinon, axios, ClientSync, createMockDb, registerClientSyncHooks
} = require('./support');

let sync, db, applier, rollback, hashVerifier, config, util;

function assignState(state){
    ({ sync, db, applier, rollback, hashVerifier, config, util } = state);
}

function registerStartGroup1Tests(){
    describe('start', function(){
        it('passes lastAppliedBlock + 1 to incremental catch-up when resuming a populated replica', async function(){
            db.getLastBlock.resolves(100);
            sinon.stub(sync, 'fetchAndApplySchema').resolves();
            sinon.stub(sync, 'incrementalCatchUp').resolves();
            sinon.stub(sync, 'connectWebSockets').callsFake(() => { sync.running = false; });

            await sync.start();

            assert.strictEqual(sync.incrementalCatchUp.calledOnce, true);
            // Must request the NEXT needed block, not the last already-applied one.
            // The server uses inclusive >= bounds, so passing 100 re-delivers block
            // 100's already-applied rows and the non-ignore INSERT throws on the
            // UNIQUE action_index, rolling back the whole catch-up (silent freeze).
            assert.strictEqual(sync.incrementalCatchUp.firstCall.args[0], 101);
        });

        it('reconciles the source schema on resume, BEFORE catch-up (creates zero-row tables added post-bootstrap)', async function(){
            // Regression: a replica bootstrapped before the source added a zero-row
            // table (polls / poll_results / vote_delegations / votes /
            // anchor_reward_reconcile_log on BTC) never received it, because nothing
            // streams for a zero-row table so no apply/heal ever fires. Resume must
            // re-apply the (idempotent, CREATE-only) source schema so a restart
            // converges the schema without any row flow, and it must run BEFORE
            // catch-up so the first applied/verified block sees a complete schema.
            db.getLastBlock.resolves(100);
            let order = [];
            sinon.stub(sync, 'fetchAndApplySchema').callsFake(async () => { order.push('schema'); });
            sinon.stub(sync, 'incrementalCatchUp').callsFake(async () => { order.push('catchup'); });
            sinon.stub(sync, 'connectWebSockets').callsFake(() => { sync.running = false; });

            await sync.start();

            assert.strictEqual(sync.fetchAndApplySchema.calledOnce, true);
            assert.strictEqual(sync.fetchAndApplySchema.firstCall.args[0], sync.sources[0]);
            assert.deepStrictEqual(order, ['schema', 'catchup'], 'schema reconcile must precede catch-up');
        });
    });
}

function registerStartGroup2Tests(){
    describe('start', function(){
        it('does NOT reconcile schema on the empty-replica bootstrap path (bootstrap fetches it itself)', async function(){
            db.getLastBlock.resolves(null);
            sinon.stub(sync, 'bootstrapFromSnapshot').callsFake(async () => { sync.lastAppliedBlock = 10; });
            sinon.stub(sync, 'fetchAndApplySchema').resolves();
            sinon.stub(sync, 'incrementalCatchUp').resolves();
            sinon.stub(sync, 'connectWebSockets').callsFake(() => { sync.running = false; });

            await sync.start();

            // The resume-path reconcile must not double-fetch on the bootstrap path
            // (bootstrapFromSnapshot already applies the schema).
            assert.strictEqual(sync.fetchAndApplySchema.called, false);
        });

        it('bootstraps from a full snapshot when the replica is empty', async function(){
            db.getLastBlock.resolves(null);
            // A successful bootstrap must commit a tip because start() refuses
            // to enter live-follow while lastAppliedBlock is still null.
            sinon.stub(sync, 'bootstrapFromSnapshot').callsFake(async () => { sync.lastAppliedBlock = 10; });
            sinon.stub(sync, 'incrementalCatchUp').resolves();
            sinon.stub(sync, 'connectWebSockets').callsFake(() => { sync.running = false; });

            await sync.start();

            assert.strictEqual(sync.bootstrapFromSnapshot.calledOnce, true);
            assert.strictEqual(sync.incrementalCatchUp.called, false);
            assert.strictEqual(sync.connectWebSockets.calledOnce, true);
        });
    });
}

describe('ClientSync', function(){
    registerClientSyncHooks(assignState);
    registerStartGroup1Tests();
    registerStartGroup2Tests();
});
