// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// The walk recomputes the stored tip's state_hash the way the live path does, a cleared
// state-hash halt recurs, a halt copy in sync_state is restored and cleared, and a walk
// that overlaps a row removal drops its reads without a halt or a marker write.
const assert = require('assert');
const sinon  = require('sinon');
const { buildClient, hashesAt, KEY } = require('../../helpers/verified_tip_client');

function quiet(){
    beforeEach(function(){ for(const k of ['log', 'error', 'warn', 'info']) sinon.stub(console, k); });
    afterEach(function(){ sinon.restore(); });
}

// Stored hashes of every block, plus a stored state_hash on `stateAt`.
function withStoredStateHash(c, stateAt){
    c.db.getBlockHashRow = sinon.stub().callsFake(async h => (h <= c.db.tip
        ? Object.assign({ block_index: h, state_hash: h === stateAt ? 'SOURCE_STATE' : null }, hashesAt(h)) : null));
    c.sync.blockHasher.computeStateHashPreimage = sinon.stub().resolves({});
    return c;
}

describe('ClientSync: the walk re-runs the stored-tip state_hash check @regression', function(){
    quiet();

    it('a committed tip whose state_hash does not reproduce halts at startup, before catch-up', async function(){
        let c = withStoredStateHash(buildClient({ tip: 200, config: { VERIFY_STATE_HASH: true } }), 200);
        c.state.set(KEY, '199');
        c.sync.blockHasher.computeStateHash = sinon.stub().resolves('LOCAL_STATE');
        c.sync.fetchAndApplySchema = sinon.stub().resolves();
        c.sync.incrementalCatchUp = sinon.stub().resolves();
        await c.sync.synchronizeStoredReplica();
        assert.strictEqual(c.sync.getHaltInfo().reason, 'state-hash-divergence');
        assert.strictEqual(c.sync.getHaltInfo().blockIndex, 200);
        assert.strictEqual(c.state.get(KEY), '199');
        assert.strictEqual(c.sync.incrementalCatchUp.called, false);
    });

    it('a matching state_hash verifies the tip; blocks below the tip are not state-hashed', async function(){
        let c = withStoredStateHash(buildClient({ tip: 200, config: { VERIFY_STATE_HASH: true } }), 200);
        c.state.set(KEY, '197');
        c.sync.blockHasher.computeStateHash = sinon.stub().resolves('SOURCE_STATE');
        assert.strictEqual(await c.sync.verifyUnverifiedTail(), 'ok');
        assert.deepStrictEqual(c.sync.blockHasher.computeStateHash.args.map(a => a[0]), [200]);
        assert.strictEqual(c.state.get(KEY), '200');
    });

    it('a state_hash compute error holds without a halt or a marker write', async function(){
        let c = withStoredStateHash(buildClient({ tip: 200, config: { VERIFY_STATE_HASH: true } }), 200);
        c.state.set(KEY, '199');
        c.sync.blockHasher.computeStateHash = sinon.stub().rejects(new Error('transient'));
        assert.strictEqual(await c.sync.verifyUnverifiedTail(), 'held');
        assert.strictEqual(c.sync.isHalted(), false);
        assert.strictEqual(c.state.get(KEY), '199');
    });

    it('clearing a live state-hash halt does not vouch for the block: the next walk halts again', async function(){
        let c = withStoredStateHash(buildClient({ tip: 199, config: { VERIFY_STATE_HASH: true } }), 200);
        c.state.set(KEY, '199');
        c.sync.lastAppliedBlock = 199;
        c.applier.applyBlock = sinon.stub().callsFake(async () => { c.db.tip = 200; });
        c.sync.blockHasher.computeStateHash = sinon.stub().resolves('LOCAL_STATE');
        await c.sync.applyBlockEvent(Object.assign({ block_index: 200, block_time: 1, state_hash: 'SOURCE_STATE' }, hashesAt(200)));
        assert.strictEqual(c.sync.getHaltInfo().reason, 'state-hash-divergence');
        await c.sync.clearHalt();
        assert.strictEqual(await c.sync.verifyUnverifiedTail(), 'halted');
        assert.strictEqual(c.sync.getHaltInfo().reason, 'state-hash-divergence');
    });
});

describe('ClientSync: a halt copy in sync_state @regression', function(){
    quiet();

    it('a failed sync_halt write leaves a copy that a clear removes', async function(){
        let c = buildClient({ tip: 200 });
        c.db.recordHalt = sinon.stub().rejects(new Error('db down'));
        await c.sync.haltOnDivergence(200, [], ['http://a:3006'], 'no-source-quorum');
        c.sync.cancelHaltPersistRetry();
        assert.strictEqual(JSON.parse(c.state.get('halt_pending:indexer')).reason, 'no-source-quorum');
        await c.sync.clearHalt();
        assert.strictEqual(c.state.has('halt_pending:indexer'), false);
    });

    it('a restart restores the copied halt, writes sync_halt and drops the copy once it lands', async function(){
        let c = buildClient({ tip: 200 });
        c.state.set('halt_pending:indexer', JSON.stringify({ blockIndex: 200, reason: 'no-source-quorum', mismatches: [], sources: [] }));
        c.sync.incrementalCatchUp = sinon.stub().resolves();
        await c.sync.synchronizeStoredReplica();
        assert.strictEqual(c.sync.getHaltInfo().reason, 'no-source-quorum');
        assert.ok(c.db.recordHalt.calledOnce);
        assert.strictEqual(c.state.has('halt_pending:indexer'), false);
        assert.strictEqual(c.sync.incrementalCatchUp.called, false);
    });
});

describe('ClientSync: a walk overlapping a row removal @regression', function(){
    quiet();

    it('a mismatch read while a removal is open is dropped, not halted on', async function(){
        let { sync, state } = buildClient({ tip: 110, bad: [105] });
        state.set(KEY, '100');
        let real = sync.blockHasher.computeBlockHashes;
        sync.blockHasher.computeBlockHashes = sinon.stub().callsFake(async h => {
            if(h === 105) await sync.beginRowRemoval();
            return real(h);
        });
        assert.strictEqual(await sync.verifyUnverifiedTail(), 'held');
        assert.strictEqual(sync.isHalted(), false);
        assert.strictEqual(state.get(KEY), '100');
    });

    it('a walk does not start while a removal is open, and runs once it closes', async function(){
        let { sync, state } = buildClient({ tip: 110 });
        state.set(KEY, '100');
        await sync.beginRowRemoval();
        assert.strictEqual(await sync.verifyUnverifiedTail(), 'held');
        assert.strictEqual(sync.blockHasher.computeBlockHashes.called, false);
        sync.endRowRemoval();
        assert.strictEqual(await sync.verifyUnverifiedTail(), 'ok');
        assert.strictEqual(state.get(KEY), '110');
    });
});
