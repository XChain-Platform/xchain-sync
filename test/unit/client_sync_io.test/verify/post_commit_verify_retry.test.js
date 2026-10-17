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
