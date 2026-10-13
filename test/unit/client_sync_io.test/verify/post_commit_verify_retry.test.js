/*
 * ClientSync: a block whose post-commit verification throws is held, marked in
 * sync_state, re-verified on redelivery and on restart, and never advances unverified.
 */
const assert = require('assert');
const sinon  = require('sinon');
const ClientSync = require('../../../../src/client/sync');
const Utility = require('../../../../src/util');
const HashVerifier = require('../../../../src/client/hash_verifier');

const KEY = 'post_commit_verify_pending:indexer';

function build(lastBlock){
    const state = new Map();
    const db = {
        state, dbName: 'test_db', dbType: 'indexer',
        getLastBlock: sinon.stub().resolves(lastBlock === undefined ? null : lastBlock),
        getBlockHashRow: sinon.stub().resolves(null),
        doQuery: sinon.stub().resolves([]),
        getSyncState: sinon.stub().callsFake(async k => (state.has(k) ? state.get(k) : null)),
        setSyncState: sinon.stub().callsFake(async (k, v) => { state.set(k, v); }),
        recordHalt: sinon.stub().resolves({ block_index: 0 }),
        getActiveHalt: sinon.stub().resolves(null),
        clearHalt: sinon.stub().resolves(1)
    };
    const applier = { applyBlock: sinon.stub().resolves(), _lastComputedRoots: null };
    const config = { SYNC_SOURCES: 'http://a:3006', VERIFY_HASHES: true, HASH_CONFIRM_TIMEOUT: 5000,
        HALT_ON_DIVERGENCE: true, VERIFY_RECOMPUTE: false, VERIFY_STATE_HASH: true };
    const sync = new ClientSync('bitcoin', 'mainnet', db, applier,
        { rollback: sinon.stub().resolves() }, new HashVerifier(), config, new Utility());
    sync.lastAppliedBlock = 199;
    sync.haltOnDivergence = sinon.stub().resolves();
    return { sync, db, applier };
}

const ev = { block_index: 200, block_time: 1, state_hash: 'SH' };

describe('ClientSync: post-commit verification retry @regression', function(){
    beforeEach(function(){
        sinon.stub(console, 'log'); sinon.stub(console, 'error'); sinon.stub(console, 'warn');
    });
    afterEach(function(){ sinon.restore(); });

    it('unverified: a throwing verification holds the tip and marks sync_state', async function(){
        const { sync, db } = build();
        sync.blockHasher.computeStateHash = sinon.stub().rejects(new Error('transient'));
        await sync.applyBlockEvent(ev);
        assert.strictEqual(sync.lastAppliedBlock, 199);
        assert.strictEqual(db.state.get(KEY), '200');
        assert.strictEqual(sync.haltOnDivergence.called, false);
    });

    it('redeliver: a redelivery re-verifies, advances and clears the marker', async function(){
        const { sync, db } = build();
        const stub = sync.blockHasher.computeStateHash = sinon.stub().rejects(new Error('transient'));
        await sync.applyBlockEvent(ev);
        stub.resolves('SH');
        await sync.applyBlockEvent(ev);
        assert.strictEqual(sync.lastAppliedBlock, 200);
        assert.ok(!db.state.get(KEY), 'marker cleared');
    });

    it('mismatch: a redelivery that now mismatches halts and does not advance', async function(){
        const { sync } = build();
        const stub = sync.blockHasher.computeStateHash = sinon.stub().rejects(new Error('transient'));
        await sync.applyBlockEvent(ev);
        stub.resolves('OTHER');
        sync.blockHasher.computeStateHashPreimage = sinon.stub().resolves({});
        await sync.applyBlockEvent(ev);
        assert.ok(sync.haltOnDivergence.calledOnce);
        assert.strictEqual(sync.haltOnDivergence.firstCall.args[3], 'state-hash-divergence');
        assert.strictEqual(sync.lastAppliedBlock, 199);
    });

    it('restart: a stored tip equal to the marked block is rewound for re-verification', async function(){
        const { sync, db } = build(200);
        db.state.set(KEY, '200');
        assert.strictEqual(await sync.retryPostCommitVerification(200), 199);
        assert.strictEqual(sync._postCommitPending, 200);
    });

    it('restart: a clean store with no marker keeps its tip', async function(){
        const { sync } = build(200);
        assert.strictEqual(await sync.retryPostCommitVerification(200), 200);
        assert.strictEqual(await sync.retryPostCommitVerification(null), null);
    });

    it('reorg: a marker above the rolled-back tip is dropped, not restored', async function(){
        const { sync, db } = build(150);
        db.state.set(KEY, '200');
        assert.strictEqual(await sync.retryPostCommitVerification(150), 150);
        assert.ok(!db.state.get(KEY), 'stale marker cleared');
    });

    it('reorg: a live reorg to the marked height clears the marker', async function(){
        const { sync, db } = build();
        sync.blockHasher.computeStateHash = sinon.stub().rejects(new Error('transient'));
        await sync.applyBlockEvent(ev);
        sync.lastAppliedBlock = 199;
        await sync.applyReorgRollback({ block_index: 200 });
        assert.ok(!db.state.get(KEY));
    });
});
