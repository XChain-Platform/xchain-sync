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
 * ClientSync: a block whose post-commit verification throws is held, the durable
 * verified-tip marker stays below it, and it is re-verified on redelivery, on the next
 * catch-up pass and on restart; a restart re-verifies every committed block above the
 * marker before any catch-up, and a reorg lowers the marker before it deletes rows.
 */
const assert = require('assert');
const sinon  = require('sinon');
const { buildClient, hashesAt, KEY } = require('../../../helpers/verified_tip_client');

const ev = Object.assign({ block_index: 200, block_time: 1, state_hash: 'SH' }, hashesAt(200));

function quiet(){
    beforeEach(function(){ sinon.stub(console, 'log'); sinon.stub(console, 'error'); sinon.stub(console, 'warn'); });
    afterEach(function(){ sinon.restore(); });
}

describe('ClientSync: post-commit verification retry @regression', function(){
    quiet();

    function held(){
        let c = buildClient({ tip: 199, config: { VERIFY_STATE_HASH: true } });
        c.state.set(KEY, '199');
        c.sync.lastAppliedBlock = 199;
        c.sync.haltOnDivergence = sinon.stub().resolves();
        c.sync.blockHasher.computeStateHash = sinon.stub().rejects(new Error('transient'));
        c.applier.applyBlock = sinon.stub().callsFake(async () => { c.db.tip = 200; });   // commits 200
        return c;
    }

    it('unverified: a throwing verification holds the tip and the marker', async function(){
        let { sync, state } = held();
        await sync.applyBlockEvent(ev);
        assert.strictEqual(sync.lastAppliedBlock, 199);
        assert.strictEqual(state.get(KEY), '199');
        assert.strictEqual(sync.haltOnDivergence.called, false);
    });

    it('redeliver: a redelivery re-verifies and advances the tip and the marker', async function(){
        let { sync, state } = held();
        await sync.applyBlockEvent(ev);
        sync.blockHasher.computeStateHash = sinon.stub().resolves('SH');
        await sync.applyBlockEvent(ev);
        assert.strictEqual(sync.lastAppliedBlock, 200);
        assert.strictEqual(state.get(KEY), '200');
    });

    it('mismatch: a redelivery that now mismatches halts and does not advance', async function(){
        let { sync, state } = held();
        await sync.applyBlockEvent(ev);
        sync.blockHasher.computeStateHash = sinon.stub().resolves('OTHER');
        sync.blockHasher.computeStateHashPreimage = sinon.stub().resolves({});
        await sync.applyBlockEvent(ev);
        assert.ok(sync.haltOnDivergence.calledOnce);
        assert.strictEqual(sync.haltOnDivergence.firstCall.args[3], 'state-hash-divergence');
        assert.strictEqual(sync.lastAppliedBlock, 199);
        assert.strictEqual(state.get(KEY), '199');
    });
});

// Block 200 commits but its state-hash check throws, so the cursor stays at 199.
function heldAtTip(){
    const c = buildClient({ tip: 199, config: { VERIFY_STATE_HASH: true } });
    c.state.set(KEY, '199');
    c.sync.lastAppliedBlock = 199;
    c.sync.haltOnDivergence = sinon.stub().resolves();
    c.sync.blockHasher.computeStateHash = sinon.stub().rejects(new Error('transient'));
    c.applier.applyBlock = sinon.stub().callsFake(async () => { c.db.tip = 200; });
    return c;
}

describe('ClientSync: catch-up reconciles a reverified committed tip @regression', function(){
    quiet();
    const held = heldAtTip;

    it('moves the cursor and hashes to the verified tip before a 404 download', async function(){
        const { sync, state } = held();
        await sync.applyBlockEvent(ev);
        sync.blockHasher.computeStateHash = sinon.stub().resolves('SH');
        sync.downloadIncrementalCatchUp = sinon.stub().rejects(
            Object.assign(new Error('not found'), { response: { status: 404 } }));
        sync.scheduleHeartbeat = sinon.stub();

        await sync.runIncrementalCatchUp();

        assert.strictEqual(sync.lastAppliedBlock, 200);
        assert.deepStrictEqual(sync.lastHashes, Object.assign({ block_index: 200 }, hashesAt(200)));
        assert.strictEqual(state.get(KEY), '200');
        assert.ok(sync.downloadIncrementalCatchUp.calledOnceWith('http://a:3006', 201));
        assert.ok(sync.scheduleHeartbeat.calledOnce);
        assert.strictEqual(sync.haltOnDivergence.called, false);
    });

    it('does not move or download when rows change before the apply lock', async function(){
        const { sync } = held();
        await sync.applyBlockEvent(ev);
        sync.blockHasher.computeStateHash = sinon.stub().resolves('SH');
        sync.downloadIncrementalCatchUp = sinon.stub().resolves({ snapshotData: {}, skipLookups: false });
        sinon.stub(sync, 'withApplyLock').callsFake(async fn => {
            sync.bumpRowGeneration();
            return fn();
        });

        await sync.runIncrementalCatchUp();

        assert.strictEqual(sync.lastAppliedBlock, 199);
        assert.strictEqual(sync.downloadIncrementalCatchUp.called, false);
    });
});

describe('ClientSync: catch-up cursor resync guards @regression', function(){
    quiet();
    const held = heldAtTip;

    it('holds a recompute replica whose marker is below the committed tip', async function(){
        const { sync, db } = held();
        db.tip = 200;
        sync._verifiedTip = 199;
        sync.scheduleHeartbeat = sinon.stub();
        const guard = { rowGeneration: sync.rowGeneration(), reorgGeneration: sync._reorgGeneration };

        const result = await sync.syncCursorToCommittedTip(200, guard);

        assert.strictEqual(result, 'held');
        assert.strictEqual(sync.lastAppliedBlock, 199);
        assert.strictEqual(sync.scheduleHeartbeat.called, false);
    });

    it('does not move or download when a row replacement completes during the tail check', async function(){
        const { sync } = held();
        await sync.applyBlockEvent(ev);
        sync.blockHasher.computeStateHash = sinon.stub().resolves('SH');
        sync.downloadIncrementalCatchUp = sinon.stub().resolves({ snapshotData: {}, skipLookups: false });
        const verify = sync.verifyUnverifiedTail.bind(sync);
        sinon.stub(sync, 'verifyUnverifiedTail').callsFake(async () => {
            const verdict = await verify();
            await sync.beginRowRemoval();
            sync.endRowRemoval();
            return verdict;
        });

        await sync.runIncrementalCatchUp();

        assert.strictEqual(sync.lastAppliedBlock, 199);
        assert.strictEqual(sync.downloadIncrementalCatchUp.called, false);
        assert.strictEqual(sync._catchUpPending, true, 'the pass must be re-queued on fresh reads');
    });

    it('keeps the cursor but still downloads when the marker trails the tip after the tail check', async function(){
        const { sync, db } = held();
        db.tip = 200;
        sync._verifiedTip = 199;
        sinon.stub(sync, 'verifyUnverifiedTail').resolves('ok');
        sync.downloadIncrementalCatchUp = sinon.stub().rejects(
            Object.assign(new Error('not found'), { response: { status: 404 } }));

        await sync.runIncrementalCatchUp();

        assert.strictEqual(sync.lastAppliedBlock, 199);
        assert.ok(sync.downloadIncrementalCatchUp.calledOnceWith('http://a:3006', 201));
    });
});

describe('ClientSync: catch-up cursor resync without a verified-tip marker @regression', function(){
    quiet();

    it('keeps the cursor but still downloads when the tip hash row is unresolved', async function(){
        const { sync, db } = buildClient({ tip: 200, config: { VERIFY_RECOMPUTE: false } });
        sync.lastAppliedBlock = 199;
        db.getBlockHashRow = sinon.stub().resolves(null);
        sync.downloadIncrementalCatchUp = sinon.stub().rejects(
            Object.assign(new Error('not found'), { response: { status: 404 } }));

        await sync.runIncrementalCatchUp();

        assert.strictEqual(sync.lastAppliedBlock, 199);
        assert.ok(sync.downloadIncrementalCatchUp.calledOnceWith('http://a:3006', 201));
    });

    it('resyncs a replica that does not track the verified tip', async function(){
        const { sync } = buildClient({ tip: 200, config: { VERIFY_RECOMPUTE: false } });
        sync.lastAppliedBlock = 199;
        sync.downloadIncrementalCatchUp = sinon.stub().rejects(
            Object.assign(new Error('not found'), { response: { status: 404 } }));
        sync.scheduleHeartbeat = sinon.stub();

        await sync.runIncrementalCatchUp();

        assert.strictEqual(sync.lastAppliedBlock, 200);
        assert.deepStrictEqual(sync.lastHashes, Object.assign({ block_index: 200 }, hashesAt(200)));
        assert.ok(sync.downloadIncrementalCatchUp.calledOnceWith('http://a:3006', 201));
    });
});

// A catch-up pass overlapping a live block whose post-commit check is still awaiting.
async function raceInFlightCheck(config){
    const c = buildClient({ tip: 199, config });
    c.sync.lastAppliedBlock = 199;
    c.sync.haltOnDivergence = sinon.stub().callsFake(async () => { c.sync._halted = { blockIndex: 200 }; });
    let release;
    const verdict = new Promise(r => { release = r; });
    c.sync.blockHasher.computeStateHash = sinon.stub().callsFake(() => verdict);
    c.sync.blockHasher.computeStateHashPreimage = sinon.stub().resolves({});
    c.applier.applyBlock = sinon.stub().callsFake(async () => { c.db.tip = 200; });
    c.sync.scheduleHeartbeat = sinon.stub();
    c.sync.downloadIncrementalCatchUp = sinon.stub().rejects(
        Object.assign(new Error('not found'), { response: { status: 404 } }));
    const flush = () => new Promise(r => setImmediate(r));
    const live = c.sync.applyBlockEvent(ev);
    for(let i = 0; i < 20 && c.db.tip !== 200; i++) await flush();
    const catchUp = c.sync.runIncrementalCatchUp();
    for(let i = 0; i < 20; i++) await flush();
    const before = { cursor: c.sync.lastAppliedBlock, heartbeats: c.sync.scheduleHeartbeat.callCount };
    release('OTHER');
    await live;
    await catchUp;
    return { before, after: c.sync.lastAppliedBlock, halted: c.sync.haltOnDivergence.called };
}

describe('ClientSync: catch-up resync waits for an in-flight live check @regression', function(){
    quiet();

    for(const [label, config] of [
        ['a replica without the verified-tip marker', { VERIFY_RECOMPUTE: false, VERIFY_STATE_HASH: true }],
        ['a recompute replica', { VERIFY_RECOMPUTE: true, VERIFY_STATE_HASH: true }]
    ]){
        it('does not publish a block whose live check later halts: ' + label, async function(){
            const r = await raceInFlightCheck(config);
            assert.strictEqual(r.before.cursor, 199);
            assert.strictEqual(r.before.heartbeats, 0);
            assert.strictEqual(r.halted, true);
            assert.strictEqual(r.after, 199);
        });
    }

    it('clears the in-flight mark once the live check settles', async function(){
        const { sync } = heldAtTip();
        await sync.applyBlockEvent(ev);
        assert.strictEqual(sync.liveVerifyPendingAtOrBelow(200), false);
    });
});

describe('ClientSync: restart re-verifies the blocks above the marker @regression', function(){
    quiet();

    function restartable(opts){
        let c = buildClient(opts);
        c.sync.fetchAndApplySchema = sinon.stub().resolves();
        c.sync.incrementalCatchUp = sinon.stub().resolves();
        return c;
    }

    it('a stored tip above the marker recomputes before catch-up, then resumes', async function(){
        let { sync, state } = restartable({ tip: 200 });
        state.set(KEY, '197');
        await sync.synchronizeStoredReplica();
        assert.deepStrictEqual(sync.blockHasher.computeBlockHashes.args.map(a => a[0]), [198, 199, 200]);
        assert.strictEqual(state.get(KEY), '200');
        assert.strictEqual(sync.lastAppliedBlock, 200);
        assert.ok(sync.incrementalCatchUp.calledOnceWith(201));
    });

    it('a block above the marker that fails its hash halts at startup and no catch-up runs', async function(){
        let { sync, state, db } = restartable({ tip: 200, bad: [199] });
        state.set(KEY, '197');
        await sync.synchronizeStoredReplica();
        assert.strictEqual(sync.isHalted(), true);
        assert.strictEqual(sync.getHaltInfo().blockIndex, 199);
        assert.ok(db.recordHalt.calledOnce);
        assert.strictEqual(state.get(KEY), '198');
        assert.strictEqual(sync.lastAppliedBlock, 200);
        assert.strictEqual(sync.incrementalCatchUp.called, false);
    });

    it('a marker equal to the stored tip recomputes nothing', async function(){
        let { sync, state } = restartable({ tip: 200 });
        state.set(KEY, '200');
        await sync.synchronizeStoredReplica();
        assert.strictEqual(sync.blockHasher.computeBlockHashes.called, false);
        assert.ok(sync.incrementalCatchUp.calledOnceWith(201));
    });
});

describe('ClientSync: first start without a marker @regression', function(){
    quiet();

    it('no marker: only the stored tip block recomputes, and the marker is recorded', async function(){
        let { sync, state } = buildClient({ tip: 200 });
        assert.strictEqual(await sync.verifyUnverifiedTail(), 'ok');
        assert.deepStrictEqual(sync.blockHasher.computeBlockHashes.args.map(a => a[0]), [200]);
        assert.strictEqual(state.get(KEY), '200');
    });

    it('an earlier build\'s pending-block key widens the first walk and is dropped after it', async function(){
        let { sync, state } = buildClient({ tip: 200 });
        state.set('post_commit_verify_pending:indexer', '198');
        assert.strictEqual(await sync.verifyUnverifiedTail(), 'ok');
        assert.deepStrictEqual(sync.blockHasher.computeBlockHashes.args.map(a => a[0]), [198, 199, 200]);
        assert.strictEqual(state.has('post_commit_verify_pending:indexer'), false);
    });

    it('an unreadable marker holds the replica rather than reading as absent', async function(){
        let { sync, db } = buildClient({ tip: 200 });
        db.getSyncState = sinon.stub().rejects(new Error('sync_state down'));
        assert.strictEqual(await sync.verifyUnverifiedTail(), 'held');
        assert.strictEqual(sync.blockHasher.computeBlockHashes.called, false);
        assert.strictEqual(sync.isHalted(), false);
    });
});

describe('ClientSync: a reorg lowers the marker before deleting rows @regression', function(){
    quiet();

    it('the marker is lowered durably before the rollback runs', async function(){
        let { sync, state, rollback, writes } = buildClient({ tip: 200 });
        state.set(KEY, '200');
        sync.lastAppliedBlock = 200;
        rollback.rollback = sinon.stub().callsFake(async () => { assert.deepStrictEqual(writes.filter(w => w[0] === KEY), [[KEY, '194']]); });
        await sync.applyReorgRollback({ block_index: 195 });
        assert.ok(rollback.rollback.calledOnceWith(195));
        assert.strictEqual(state.get(KEY), '194');
        assert.strictEqual(sync.lastAppliedBlock, 194);
    });

    it('a marker write that fails stops the rollback, and the reorg halts', async function(){
        let { sync, state, db, rollback } = buildClient({ tip: 200 });
        state.set(KEY, '200');
        sync.lastAppliedBlock = 200;
        db.setSyncState = sinon.stub().rejects(new Error('sync_state down'));
        await sync.handleReorg({ block_index: 195 });
        assert.strictEqual(rollback.rollback.called, false);
        assert.strictEqual(sync.getHaltInfo().reason, 'reorg-rollback-failed');
    });

    it('a marker already below the rollback height is left alone', async function(){
        let { sync, state, writes } = buildClient({ tip: 200 });
        state.set(KEY, '190');
        sync.lastAppliedBlock = 200;
        await sync.applyReorgRollback({ block_index: 195 });
        assert.strictEqual(writes.filter(w => w[0] === KEY).length, 0);
        assert.strictEqual(state.get(KEY), '190');
    });
});
