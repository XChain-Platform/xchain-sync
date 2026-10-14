// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// The halt_pending copy across restarts, clears and re-seeds, and the row-generation walk
// guard under steady reorgs. Each asserts the outcome the design promises.
const assert = require('assert');
const sinon  = require('sinon');
const ClientSync = require('../../../src/client/sync');
const HashVerifier = require('../../../src/client/hash_verifier');
const Utility = require('../../../src/util');
const { buildClient, KEY } = require('../../helpers/verified_tip_client');

const PENDING = 'halt_pending:indexer';

function quiet(){
    beforeEach(function(){ for(const k of ['log', 'error', 'warn', 'info']) sinon.stub(console, k); });
    afterEach(function(){ sinon.restore(); });
}

// A fresh process over the same stub DB, run through the start() steps up to catch-up.
function restartOver(c){
    let util = new Utility(); util.sleep = async () => {};
    let r = new ClientSync('bitcoin', 'mainnet', c.db, c.applier, c.rollback, new HashVerifier(),
        Object.assign({}, c.sync.config), util);
    r.blockHasher.computeBlockHashes = c.sync.blockHasher.computeBlockHashes;
    r.fetchAndApplySchema = sinon.stub().resolves();
    r.incrementalCatchUp = sinon.stub().resolves();
    r.running = false;
    return r;
}

// A non-recompute halt (no source quorum on block 201) whose sync_halt write failed:
// only the halt_pending copy carries it across a restart.
async function quorumHaltWithLostRow(c){
    c.state.set(KEY, '200');
    c.sync.lastAppliedBlock = 200;
    c.db.recordHalt = sinon.stub().rejects(new Error('db down'));
    await c.sync.haltOnDivergence(201, [{ hash: 'a' }, { hash: 'b' }], ['http://a:3006'], 'no-source-quorum');
    c.sync.cancelHaltPersistRetry();
    assert.ok(c.state.has(PENDING), 'precondition: the copy was written');
}

describe('ClientSync: the halt_pending copy across restarts @regression', function(){
    quiet();

    it('A. an unreadable copy must not read as "no halt" (fail-soft getSyncState returns null on a read fault)', async function(){
        let c = buildClient({ tip: 200 });
        await quorumHaltWithLostRow(c);
        // sync_meta.getSyncState without { rethrow } logs a SELECT fault and returns null.
        let real = c.db.getSyncState;
        c.db.getSyncState = sinon.stub().callsFake(async (k, o) => (k === PENDING && !(o && o.rethrow)) ? null : real(k, o));
        let r = restartOver(c);
        assert.strictEqual(await r.resolveStartupHalt(), 'continue');
        await r.synchronizeStoredReplica();
        assert.ok(r.isHalted() || !r.incrementalCatchUp.called,
            'a read fault on halt_pending resumed past the no-source-quorum halt; catch-up from ' +
            (r.incrementalCatchUp.firstCall && r.incrementalCatchUp.firstCall.args[0]));
    });

    it('B. a copy whose delete fails on clear must not re-halt the replica after the operator cleared it', async function(){
        let c = buildClient({ tip: 200 });
        await quorumHaltWithLostRow(c);
        c.db.deleteSyncState = sinon.stub().resolves(false);       // fail-soft delete (logged, not thrown)
        await c.sync.clearHalt();
        assert.strictEqual(c.sync.isHalted(), false);
        let r = restartOver(c);
        r.db.recordHalt = sinon.stub().resolves({ block_index: 201 });
        await r.resolveStartupHalt();
        await r.synchronizeStoredReplica();
        assert.strictEqual(r.isHalted(), false, 'the cleared halt came back from its stale copy: ' +
            JSON.stringify(r.getHaltInfo() && { block: r.getHaltInfo().blockIndex, reason: r.getHaltInfo().reason }));
    });

    it('C. a re-seed (empty replica, bootstrap) must either honour the copy before bootstrapping or drop it', async function(){
        let c = buildClient({ tip: 200 });
        await quorumHaltWithLostRow(c);
        c.db.tip = null;                                            // operator wiped the replicated rows
        let r = restartOver(c);
        r.bootstrapFromSnapshot = sinon.stub().callsFake(async () => { c.db.tip = 900; r.lastAppliedBlock = 900; });
        await r.resolveStartupHalt();
        await r.synchronizeStoredReplica();
        let bootstrapped = r.bootstrapFromSnapshot.called;
        // Next restart over the re-seeded replica.
        let r2 = restartOver(c);
        r2.db.recordHalt = sinon.stub().resolves({ block_index: 201 });
        await r2.resolveStartupHalt();
        await r2.synchronizeStoredReplica();
        assert.ok(!bootstrapped || !r2.isHalted(),
            'bootstrap ran past the copy, then the next restart re-halted the re-seeded replica at old block ' +
            (r2.getHaltInfo() && r2.getHaltInfo().blockIndex) + ' (tip 900)');
    });

});

describe('ClientSync: halt_pending copies are per dbType @regression', function(){
    quiet();

    it('D. a copy written by another dbType is never restored', async function(){
        let c = buildClient({ tip: 200 });
        c.state.set('halt_pending:decoder', JSON.stringify({ blockIndex: 150, reason: 'no-source-quorum' }));
        c.state.set(KEY, '200');
        let r = restartOver(c);
        await r.resolveStartupHalt();
        await r.synchronizeStoredReplica();
        assert.strictEqual(r.isHalted(), false);
        assert.ok(r.incrementalCatchUp.calledOnceWith(201));
    });
});

describe('ClientSync: the verified-tip walk under steady reorgs @regression', function(){
    quiet();

    // A removal opens and closes every `every` recomputes, as a reorg at the tip would.
    async function walkUnder(every, batch){
        let { sync, state } = buildClient({ tip: 3000, config: { VERIFY_RANGE_BATCH: batch } });
        state.set(KEY, '0');
        let n = 0;
        let real = sync.blockHasher.computeBlockHashes;
        sync.blockHasher.computeBlockHashes = sinon.stub().callsFake(async h => {
            if(++n % every === 0){ await sync.beginRowRemoval(); sync.endRowRemoval(); }
            return real(h);
        });
        let verdicts = [];
        for(let i = 0; i < 40; i++){
            let v = await sync.verifyUnverifiedTail();
            verdicts.push(v);
            if(v === 'ok') break;
        }
        return { marker: Number(state.get(KEY)), walks: verdicts.length, last: verdicts[verdicts.length - 1], recomputes: n };
    }

    it('E. a walk still finishes when a reorg lands every 300 recomputes (batch 500)', async function(){
        let r = await walkUnder(300, 500);
        assert.strictEqual(r.last, 'ok', 'starved: ' + JSON.stringify(r));
    });

    it('F. control: the same reorg rate with batch 100 finishes', async function(){
        let r = await walkUnder(300, 100);
        assert.strictEqual(r.last, 'ok', JSON.stringify(r));
    });
});

describe('ClientSync: halt copy read faults and the start-up hold @regression', function(){
    quiet();

    it('a copy that cannot be read holds the client as halt-state-check-failed, before any catch-up', async function(){
        let c = buildClient({ tip: 200 });
        c.state.set(KEY, '200');
        c.db.getSyncState = sinon.stub().callsFake(async k => {
            if(k === PENDING) throw new Error('sync_state down');
            return c.state.has(k) ? c.state.get(k) : null;
        });
        let r = restartOver(c);
        await r.synchronizeStoredReplica();
        assert.strictEqual(r.getHaltInfo().reason, 'halt-state-check-failed');
        assert.strictEqual(r.incrementalCatchUp.called, false);
    });

    it('a malformed copy holds the client, and a clear drops it', async function(){
        let c = buildClient({ tip: 200 });
        c.state.set(KEY, '200');
        c.state.set(PENDING, '{not json');
        let r = restartOver(c);
        await r.synchronizeStoredReplica();
        assert.strictEqual(r.getHaltInfo().reason, 'halt-copy-unreadable');
        await r.clearHalt();
        assert.strictEqual(c.state.has(PENDING), false);
    });

    it('a copy on an empty replica blocks the bootstrap, and start() idles on it', async function(){
        let c = buildClient({ tip: null });
        c.state.set(PENDING, JSON.stringify({ blockIndex: 201, reason: 'no-source-quorum' }));
        let r = restartOver(c);
        r.bootstrapFromSnapshot = sinon.stub().resolves();
        r.loadVerifiedCheckpointBlock = sinon.stub().resolves();
        r.holdForRollbackGuardState = sinon.stub().resolves('ok');
        r.waitForStartupHalt = sinon.stub().resolves('stop');
        await r.start();
        assert.strictEqual(r.bootstrapFromSnapshot.called, false);
        assert.ok(r.waitForStartupHalt.calledOnce, 'idles on the restored halt');
        assert.strictEqual(r.getHaltInfo().reason, 'no-source-quorum');
    });

    it('a delete that fails soft on clear is followed by an empty overwrite', async function(){
        let c = buildClient({ tip: 200 });
        await quorumHaltWithLostRow(c);
        c.db.deleteSyncState = sinon.stub().resolves(false);
        await c.sync.clearHalt();
        assert.strictEqual(c.state.get(PENDING), '');
    });
});

describe('ClientSync: the marker advances under a reorg every 300 recomputes, window 500 @regression', function(){
    quiet();

    it('each walk the reorg abandons still raises the durable marker', async function(){
        let { sync, state } = buildClient({ tip: 3000, config: { VERIFY_RANGE_BATCH: 500 } });
        state.set(KEY, '0');
        let n = 0;
        let real = sync.blockHasher.computeBlockHashes;
        sync.blockHasher.computeBlockHashes = sinon.stub().callsFake(async h => {
            n += 1;
            if(n % 300 === 0){ await sync.beginRowRemoval(); sync.endRowRemoval(); }
            return real(h);
        });
        let markers = [];
        let verdict = null;
        for(let i = 0; i < 40 && verdict !== 'ok'; i++){
            verdict = await sync.verifyUnverifiedTail();
            markers.push(Number(state.get(KEY)));
        }
        assert.strictEqual(verdict, 'ok');
        // Walk 1 verifies 1..299 before the reorg at block 300 (writes every 50: 250); walk 2
        // resumes at 300 and verifies to 598 (last write 549); walk 3 resumes at 599 (848).
        assert.deepStrictEqual(markers.slice(0, 3), [250, 549, 848], 'durable marker after each abandoned walk');
        assert.strictEqual(markers[markers.length - 1], 3000);
        let rising = markers.every((m, i) => i === 0 || m > markers[i - 1]);
        assert.ok(rising, 'marker rose on every walk: ' + JSON.stringify(markers));
    });
});
