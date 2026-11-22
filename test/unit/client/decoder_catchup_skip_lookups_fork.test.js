// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

const zlib = require('zlib');
const {
    assert, sinon, axios, ClientSync, createMockDb
} = require('../client_sync.test/support');
const Utility = require('../../../src/util');
const HashVerifier = require('../../../src/client/hash_verifier');

// Builds a decoder replica committed at 100 whose catch-up snapshot omits
// tables.index_transactions (skip_lookups) and joins on parent id 7, a row created
// after the first lookup page; ctx.replicaLookups is what the replica holds.
function makeFixture(ctx){
    ctx.replicaLookups = [{ id: 5, hash: 'hash100' }];
    ctx.pages = 0;
    let db = createMockDb();
    db.dbType = 'decoder';
    db.getLastBlock.resolves(100);
    db.getBlockHashRow.resolves({ block_index: 100, block_hash: 'hash100' });
    db.findIndexTransactionsByIds = sinon.spy(async ids =>
        ctx.replicaLookups.filter(r => ids.map(String).includes(String(r.id))));
    ctx.applier = { applyIncrementalSnapshot: sinon.stub().resolves() };
    ctx.rollback = { rollback: sinon.stub().resolves() };
    let sync = new ClientSync('bitcoin', 'mainnet', db, ctx.applier, ctx.rollback, new HashVerifier(),
        { SYNC_SOURCES: 'http://source1:3006' }, new Utility());
    sync.lastAppliedBlock = 100;
    sync.lastHashes = { block_hash: 'hash100' };
    sync._truncatedDepth = 1;
    sinon.stub(sync, 'verifyDecoderCompleteness').resolves();
    sinon.stub(sync, 'shouldReconcileDispensers').returns(false);
    sinon.stub(sync, 'refreshTipHashes').resolves();
    sinon.stub(console, 'log');
    sinon.stub(console, 'error');
    let snapshot = { schema_version: 'x', since_block: 101, block_height: 103, tables: {
        blocks: [{ block_index: 101, block_hash_id: 8, previous_block_hash_id: 7 }] } };
    sinon.stub(axios, 'get').resolves({ data: zlib.gzipSync(JSON.stringify(snapshot)) });
    ctx.sync = sync;
}

describe('decoder skip_lookups catch-up fork guard', function(){
    let ctx, replicaLookups, applier, rollback, sync;

    beforeEach(function(){
        ctx = {};
        makeFixture(ctx);
        ({ replicaLookups, applier, rollback, sync } = ctx);
    });

    afterEach(function(){
        sinon.restore();
    });

    function stubPaging(onPage){
        sinon.stub(sync, 'syncLookupTablesPaged').callsFake(async () => { ctx.pages++; onPage(ctx.pages); });
    }

    it('re-pages before the link check and rewinds when the replacement parent differs', async function(){
        stubPaging(n => { if(n === 2) replicaLookups.push({ id: 7, hash: 'FORKED100' }); });

        await sync.runIncrementalCatchUp();

        assert.strictEqual(applier.applyIncrementalSnapshot.called, false, 'the window must not land on the orphan');
        assert.strictEqual(rollback.rollback.calledOnceWith(100), true);
        assert.strictEqual(sync.lastAppliedBlock, 99);
        assert.strictEqual(sync._catchUpPending, true);
        assert.strictEqual(ctx.pages, 2, 'the pre-check re-page ran before any apply');
    });

    it('applies the window when the re-paged parent matches the committed tip', async function(){
        stubPaging(n => { if(n === 2) replicaLookups.push({ id: 7, hash: 'hash100' }); });

        await sync.runIncrementalCatchUp();

        assert.strictEqual(rollback.rollback.called, false);
        assert.strictEqual(applier.applyIncrementalSnapshot.calledOnce, true);
        assert.strictEqual(sync.lastAppliedBlock, 103);
    });

    it('aborts and retries, applying nothing, when the parent lookup never resolves', async function(){
        stubPaging(() => {});

        await sync.runIncrementalCatchUp();

        assert.strictEqual(applier.applyIncrementalSnapshot.called, false, 'unresolved is never linked');
        assert.strictEqual(rollback.rollback.called, false, 'no rewind without proof');
        assert.strictEqual(sync.lastAppliedBlock, 100);
    });

    it('aborts when the committed tip hash cannot be read', async function(){
        stubPaging(n => { if(n === 2) replicaLookups.push({ id: 7, hash: 'FORKED100' }); });
        sync.db.getBlockHashRow.rejects(new Error('transient read fault'));

        await sync.runIncrementalCatchUp();

        assert.strictEqual(sync.db.getBlockHashRow.firstCall.args[2].rethrow, true);
        assert.strictEqual(applier.applyIncrementalSnapshot.called, false);
        assert.strictEqual(rollback.rollback.called, false);
    });
});

function stubPaging(ctx, onPage){
    sinon.stub(ctx.sync, 'syncLookupTablesPaged').callsFake(async () => { ctx.pages++; onPage(ctx.pages); });
}

function lagFixture(){
    const state = { ctx: {} };
    beforeEach(function(){
        state.ctx = {};
        makeFixture(state.ctx);
    });
    afterEach(function(){ sinon.restore(); });
    return state;
}

describe('decoder catch-up fork guard with a lagging cursor @regression', function(){
    const state = lagFixture();
    let ctx, replicaLookups, applier, rollback, sync;

    beforeEach(function(){
        ctx = state.ctx;
        ({ replicaLookups, applier, rollback, sync } = ctx);
    });

    it('resyncs a lagging cursor before rejecting a forked window', async function(){
        sync.lastAppliedBlock = 97;
        sync.lastHashes = { block_hash: 'hash97' };
        stubPaging(ctx, n => { if(n === 2) replicaLookups.push({ id: 7, hash: 'FORKED100' }); });

        await sync.runIncrementalCatchUp();

        assert.strictEqual(applier.applyIncrementalSnapshot.called, false, 'the forked window must not land');
        assert.strictEqual(rollback.rollback.calledOnceWith(100), true);
        assert.strictEqual(sync.lastAppliedBlock, 99);
        assert.strictEqual(sync._catchUpPending, true);
    });

    it('resyncs a lagging cursor and applies a linked window', async function(){
        sync.lastAppliedBlock = 97;
        sync.lastHashes = { block_hash: 'hash97' };
        stubPaging(ctx, n => { if(n === 2) replicaLookups.push({ id: 7, hash: 'hash100' }); });

        await sync.runIncrementalCatchUp();

        assert.strictEqual(rollback.rollback.called, false);
        assert.strictEqual(applier.applyIncrementalSnapshot.calledOnce, true);
        assert.strictEqual(sync.lastAppliedBlock, 103);
    });
});

describe('decoder catch-up fork guard when the tip moves during a lagging resync @regression', function(){
    const state = lagFixture();
    let ctx, replicaLookups, applier, rollback, sync;

    beforeEach(function(){
        ctx = state.ctx;
        ({ replicaLookups, applier, rollback, sync } = ctx);
    });

    it('defers to the moved-tip path when the committed tip changes during resync', async function(){
        sync.lastAppliedBlock = 97;
        sync.lastHashes = { block_hash: 'hash97' };
        sync.db.getLastBlock.onFirstCall().resolves(100);
        sync.db.getLastBlock.onSecondCall().resolves(101);
        sync.db.getLastBlock.onThirdCall().resolves(101);
        stubPaging(ctx, n => { if(n === 2) replicaLookups.push({ id: 7, hash: 'FORKED100' }); });

        await sync.runIncrementalCatchUp();

        assert.strictEqual(rollback.rollback.called, false);
        assert.strictEqual(applier.applyIncrementalSnapshot.called, false);
        assert.strictEqual(sync.lastAppliedBlock, 97);
    });

    it('defers when the committed tip moves during the link lookup', async function(){
        sync.lastAppliedBlock = 97;
        sync.lastHashes = { block_hash: 'hash97' };
        stubPaging(ctx, n => { if(n === 2) replicaLookups.push({ id: 7, hash: 'FORKED100' }); });
        const findRows = sync.db.findIndexTransactionsByIds;
        sync.db.findIndexTransactionsByIds = sinon.stub().callsFake(async ids => {
            const rows = await findRows(ids);
            sync.lastAppliedBlock = 101;
            sync.db.getLastBlock.resolves(101);
            return rows;
        });

        await sync.runIncrementalCatchUp();

        assert.strictEqual(rollback.rollback.called, false);
        assert.strictEqual(applier.applyIncrementalSnapshot.called, false);
        assert.strictEqual(sync.lastAppliedBlock, 101);
    });

    it('refuses a fork rewind whose expected tip moved before the rollback lock', async function(){
        sync.lastAppliedBlock = 101;

        const applied = await sync.applyReorgRollback({ block_index: 100 }, 100);

        assert.strictEqual(applied, false);
        assert.strictEqual(rollback.rollback.called, false);
        assert.strictEqual(sync.lastAppliedBlock, 101);
    });
});

describe('decoder catch-up fork rewind re-run flag @regression', function(){
    const state = lagFixture();
    let ctx, sync;

    beforeEach(function(){
        ctx = state.ctx;
        sync = ctx.sync;
        stubPaging(ctx, n => { if(n === 2) ctx.replicaLookups.push({ id: 7, hash: 'FORKED100' }); });
    });

    it('flags the follow-up pass when the cursor lagged the committed tip by one block', async function(){
        sync.lastAppliedBlock = 99;
        sync.lastHashes = { block_hash: 'hash99' };

        await sync.runIncrementalCatchUp();

        assert.strictEqual(sync.lastAppliedBlock, 99, 'the rewound cursor equals the one the runner captured');
        assert.strictEqual(sync._reorgGeneration, 1);
        assert.strictEqual(sync._catchUpPending, true);
        assert.strictEqual(sync._catchUpTipMoved, true);
    });

    it('runs the follow-up pass inside the same catch-up after a one-block-lag rewind', async function(){
        sync.lastAppliedBlock = 99;
        sync.lastHashes = { block_hash: 'hash99' };
        sync.running = true;
        sync.db.getLastBlock.resolves(99);
        sync.db.getLastBlock.onCall(0).resolves(100);
        sync.db.getLastBlock.onCall(1).resolves(100);
        const run = sinon.spy(sync, 'runIncrementalCatchUp');

        await sync.incrementalCatchUp(101);

        assert.strictEqual(run.callCount, 2);
    });

    it('leaves the flag clear when the rewind rolled nothing back', async function(){
        sync.lastAppliedBlock = 100;
        sinon.stub(sync, 'applyReorgRollback').resolves(false);

        await sync.runIncrementalCatchUp();

        assert.strictEqual(sync._reorgGeneration, 0);
        assert.strictEqual(sync._catchUpPending, true);
        assert.strictEqual(sync._catchUpTipMoved, false);
    });
});
