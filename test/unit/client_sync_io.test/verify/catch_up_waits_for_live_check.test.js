/*********************************************************************
 *
 * Copyright © 2025-2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC - https://dankest.llc
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This file is part of XChain Platform. Licensed under the GNU Affero
 * General Public License v3.0 or later; see LICENSE.md.
 *
 **********************************************************************/
/*
 * ClientSync: a catch-up pass that overlaps a live block whose post-commit checks are
 * still running applies nothing above that block until the checks settle, then re-runs
 * on fresh reads. The state-hash check recomputes against the stored tip, so a range
 * applied on top of it mid-check would make the check read newer state.
 */
const assert = require('assert');
const sinon  = require('sinon');
const { buildClient, hashesAt, KEY } = require('../../../helpers/verified_tip_client');

const ev = Object.assign({ block_index: 200, block_time: 1, state_hash: 'SH' }, hashesAt(200));
const flush = () => new Promise(r => setImmediate(r));
const range = { snapshotData: { block_height: 201, since_block: 201, tables: {} }, skipLookups: false };

function quiet(){
    beforeEach(function(){
        for(const m of ['log', 'error', 'warn', 'info']) sinon.stub(console, m);
    });
    afterEach(function(){ sinon.restore(); });
}

// Block 200 commits and its state-hash check waits on `release`; catch-up would apply 201.
function overlap(config){
    const c = buildClient({ tip: 199, config });
    c.state.set(KEY, '199');
    c.sync.lastAppliedBlock = 199;
    c.sync.running = true;
    c.sync.haltOnDivergence = sinon.stub().callsFake(async () => { c.sync._halted = { blockIndex: 200 }; });
    c.sync.scheduleHeartbeat = sinon.stub();
    let release;
    const verdict = new Promise(r => { release = r; });
    c.tipsSeenByCheck = [];
    c.sync.blockHasher.computeStateHash = sinon.stub().callsFake(h => {
        c.tipsSeenByCheck.push([h, c.db.tip]);
        return h === 200 ? verdict : Promise.resolve('SH');
    });
    c.sync.blockHasher.computeStateHashPreimage = sinon.stub().resolves({});
    c.applier.applyBlock = sinon.stub().callsFake(async () => { c.db.tip = 200; });
    c.applier.applyIncrementalSnapshot = sinon.stub().callsFake(async () => { c.db.tip = 201; });
    c.sync.downloadIncrementalCatchUp = sinon.stub().resolves(range);
    sinon.spy(c.sync, 'runIncrementalCatchUp');
    c.release = v => release(v);
    return c;
}

async function startOverlap(c){
    const live = c.sync.applyBlockEvent(ev);
    for(let i = 0; i < 20 && c.db.tip !== 200; i++) await flush();
    const catchUp = c.sync.incrementalCatchUp(201);
    for(let i = 0; i < 20; i++) await flush();
    return { live, catchUp };
}

const CONFIGS = [
    ['a replica without the verified-tip marker', { VERIFY_RECOMPUTE: false, VERIFY_STATE_HASH: true, VERIFY_HASHES: false }],
    ['a recompute replica', { VERIFY_RECOMPUTE: true, VERIFY_STATE_HASH: true, VERIFY_HASHES: false }]
];

describe('ClientSync: catch-up holds while a live check is pending @regression', function(){
    quiet();

    for(const [label, config] of CONFIGS){
        it('applies nothing above a block whose live check is pending, then resumes: ' + label, async function(){
            const c = overlap(config);
            const { live, catchUp } = await startOverlap(c);
            assert.strictEqual(c.applier.applyIncrementalSnapshot.called, false, 'applied mid-check');
            assert.strictEqual(c.db.tip, 200);
            assert.strictEqual(c.sync.downloadIncrementalCatchUp.called, false);
            assert.strictEqual(c.sync.runIncrementalCatchUp.callCount, 1, 'the held pass must not spin');
            c.release('SH');
            await live;
            await catchUp;
            assert.strictEqual(c.sync.haltOnDivergence.called, false);
            assert.ok(c.applier.applyIncrementalSnapshot.calledOnce);
            assert.strictEqual(c.db.tip, 201);
            assert.strictEqual(c.sync.runIncrementalCatchUp.callCount, 2);
            assert.deepStrictEqual(c.tipsSeenByCheck.filter(([h]) => h === 200), [[200, 200]]);
        });

        it('halts at the pending block and applies nothing when its check mismatches: ' + label, async function(){
            const c = overlap(config);
            const { live, catchUp } = await startOverlap(c);
            c.release('OTHER');
            await live;
            await catchUp;
            assert.strictEqual(c.sync.haltOnDivergence.firstCall.args[3], 'state-hash-divergence');
            assert.strictEqual(c.applier.applyIncrementalSnapshot.called, false);
            assert.strictEqual(c.db.tip, 200);
        });
    }

    it('reports a pending live check apart from a trailing marker in the cursor resync', async function(){
        const c = buildClient({ tip: 200, config: { VERIFY_RECOMPUTE: false } });
        c.sync.lastAppliedBlock = 199;
        c.sync.markLiveVerify(200, 1);
        const guard = { rowGeneration: c.sync.rowGeneration(), reorgGeneration: c.sync._reorgGeneration };
        assert.strictEqual(await c.sync.syncCursorToCommittedTip(200, guard), 'pending');
        assert.strictEqual(c.sync.lastAppliedBlock, 199);
    });
});

describe('ClientSync: the catch-up apply re-checks pending live checks under the lock @regression', function(){
    quiet();

    it('discards a range when a redelivered tip block starts its checks during the download', async function(){
        const c = buildClient({ tip: 200, config: { VERIFY_RECOMPUTE: false, VERIFY_HASHES: false } });
        c.sync.lastAppliedBlock = 200;
        c.sync.running = true;
        c.applier.applyIncrementalSnapshot = sinon.stub().callsFake(async () => { c.db.tip = 201; });
        let downloads = 0;
        c.sync.downloadIncrementalCatchUp = sinon.stub().callsFake(async () => {
            if(++downloads === 1) c.sync.markLiveVerify(200, 1);
            return range;
        });
        const catchUp = c.sync.incrementalCatchUp(201);
        for(let i = 0; i < 20; i++) await flush();
        assert.strictEqual(c.applier.applyIncrementalSnapshot.called, false, 'applied over a pending check');
        assert.strictEqual(c.db.tip, 200);
        c.sync.markLiveVerify(200, -1);
        await catchUp;
        assert.strictEqual(downloads, 2);
        assert.ok(c.applier.applyIncrementalSnapshot.calledOnce);
        assert.strictEqual(c.db.tip, 201);
    });

    it('downloads nothing while the tip block still finishes its checks with the cursor already on it', async function(){
        const c = buildClient({ tip: 200, config: { VERIFY_RECOMPUTE: false, VERIFY_HASHES: false } });
        c.sync.lastAppliedBlock = 200;
        c.sync.running = true;
        c.applier.applyIncrementalSnapshot = sinon.stub().callsFake(async () => { c.db.tip = 201; });
        c.sync.downloadIncrementalCatchUp = sinon.stub().resolves(range);
        c.sync.markLiveVerify(200, 1);
        const catchUp = c.sync.incrementalCatchUp(201);
        for(let i = 0; i < 20; i++) await flush();
        assert.strictEqual(c.sync.downloadIncrementalCatchUp.called, false, 'downloaded over a pending check');
        c.sync.markLiveVerify(200, -1);
        await catchUp;
        assert.ok(c.sync.downloadIncrementalCatchUp.calledOnce);
        assert.strictEqual(c.db.tip, 201);
    });

    it('resolves a waiter at once when nothing is pending and keeps one that is', async function(){
        const c = buildClient({ tip: 200 });
        await c.sync.waitForLiveVerifyAtOrBelow(200);
        c.sync.markLiveVerify(150, 1);
        let settled = false;
        const waiting = c.sync.waitForLiveVerifyAtOrBelow(200).then(() => { settled = true; });
        await c.sync.waitForLiveVerifyAtOrBelow(100);
        await flush();
        assert.strictEqual(settled, false);
        c.sync.markLiveVerify(150, -1);
        await waiting;
        assert.strictEqual(settled, true);
    });
});
