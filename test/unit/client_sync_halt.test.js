/*********************************************************************
 *
 * Copyright © 2025–2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC – https://dankest.llc
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This file is part of XChain Platform. Licensed under the GNU Affero
 * General Public License v3.0 or later; see LICENSE.md. A commercial
 * license (without AGPL source-disclosure terms) is available -
 * contact legal@dankest.llc.
 *
 **********************************************************************
 * ClientSync: durable consensus-divergence HALT.
 *
 * On a CONFIRMED cross-source hash divergence (two honest sources committed
 * different consensus hashes for the same block; one is on a forked/Byzantine
 * chain. The client must HALT: stop applying, persist the halt durably (so it
 * survives restart), and require an explicit operator clear. It must never
 * silently pick one source and replicate onto a contested chain.
 ********************************************************************/

const assert = require('assert');
const sinon  = require('sinon');
const ClientSync = require('../../src/client/sync');
const Utility = require('../../src/util');
const HashVerifier = require('../../src/client/hash_verifier');

function createMockDb(){
    return {
        dbName: 'test_db',
        dbType: 'indexer',
        getLastBlock: sinon.stub().resolves(null),
        getBlockHashRow: sinon.stub().resolves(null),
        doQuery: sinon.stub().resolves([]),
        // sync_halt accessors
        recordHalt: sinon.stub().resolves({ block_index: 0 }),
        getActiveHalt: sinon.stub().resolves(null),
        clearHalt: sinon.stub().resolves(1)
    };
}

describe('ClientSync: divergence halt @regression', function(){
    let sync, db, applier, config, util;

    beforeEach(function(){
        db = createMockDb();
        applier = { applyBlock: sinon.stub().resolves(), applyFullSnapshot: sinon.stub().resolves(), applyIncrementalSnapshot: sinon.stub().resolves() };
        config = { SYNC_SOURCES: 'http://a:3006,http://b:3006', VERIFY_HASHES: true, HASH_CONFIRM_TIMEOUT: 5000, HALT_ON_DIVERGENCE: true };
        util = new Utility();
        sync = new ClientSync('bitcoin', 'mainnet', db, applier, { rollback: sinon.stub().resolves() }, new HashVerifier(), config, util);
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });
    afterEach(function(){ sinon.restore(); });

    const mism = [{ field: 'contract_hash', a: 'aaa', b: 'bbb' }];

    it('starts healthy (not halted)', function(){
        assert.strictEqual(sync.isHalted(), false);
        assert.strictEqual(sync.getHaltInfo(), null);
    });

    it('haltOnDivergence sets the halt, persists it durably, and clears pending hashes', async function(){
        sync.pendingHashes.set(101, {});
        await sync.haltOnDivergence(101, mism, ['http://a:3006', 'http://b:3006'], 'cross-source-divergence');

        assert.strictEqual(sync.isHalted(), true);
        assert.strictEqual(sync.getHaltInfo().blockIndex, 101);
        assert.deepStrictEqual(sync.getHaltInfo().mismatches, mism);
        assert.ok(db.recordHalt.calledOnce, 'halt must be persisted to sync_halt');
        assert.strictEqual(db.recordHalt.firstCall.args[0], 'indexer');
        assert.strictEqual(db.recordHalt.firstCall.args[1], 101);
        assert.strictEqual(sync.pendingHashes.size, 0, 'pending cross-source hashes are moot once halted');
    });

    it('a halted client REFUSES to apply blocks', async function(){
        await sync.haltOnDivergence(101, mism, ['http://a:3006', 'http://b:3006']);
        await sync.applyBlockEvent({ block_index: 102, ledger_hash: 'x', actions_hash: 'y', contract_hash: 'z' });
        assert.strictEqual(applier.applyBlock.called, false, 'no block may be applied while halted');
    });

    it('is idempotent: a second divergence does not double-record or change the halt block', async function(){
        await sync.haltOnDivergence(101, mism, []);
        await sync.haltOnDivergence(102, mism, []);
        assert.strictEqual(sync.getHaltInfo().blockIndex, 101, 'stays halted at the first contested block');
        assert.strictEqual(db.recordHalt.callCount, 1);
    });

});

describe('ClientSync: divergence halt @regression', function(){
    let sync, db, applier, config, util;

    beforeEach(function(){
        db = createMockDb();
        applier = { applyBlock: sinon.stub().resolves(), applyFullSnapshot: sinon.stub().resolves(), applyIncrementalSnapshot: sinon.stub().resolves() };
        config = { SYNC_SOURCES: 'http://a:3006,http://b:3006', VERIFY_HASHES: true, HASH_CONFIRM_TIMEOUT: 5000, HALT_ON_DIVERGENCE: true };
        util = new Utility();
        sync = new ClientSync('bitcoin', 'mainnet', db, applier, { rollback: sinon.stub().resolves() }, new HashVerifier(), config, util);
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });
    afterEach(function(){ sinon.restore(); });

    const mism = [{ field: 'contract_hash', a: 'aaa', b: 'bbb' }];

    it('clearHalt resumes the client and clears the durable record', async function(){
        await sync.haltOnDivergence(101, mism, []);
        assert.strictEqual(sync.isHalted(), true);

        const was = await sync.clearHalt();
        assert.strictEqual(sync.isHalted(), false);
        assert.strictEqual(was.blockIndex, 101);
        assert.ok(db.clearHalt.calledOnceWith('indexer'), 'durable halt record must be cleared');

        // Applying works again after a clear.
        await sync.applyBlockEvent({ block_index: 103, ledger_hash: 'x', actions_hash: 'y', contract_hash: 'z' });
        assert.ok(applier.applyBlock.calledOnce, 'client resumes applying after an operator clear');
    });

    it('a prior uncleared halt in sync_halt keeps the client halted on start (no silent resume)', async function(){
        db.getActiveHalt.resolves({ block_index: 55, reason: 'cross-source-divergence', mismatches: JSON.stringify(mism), sources: '[]', detected_at: '2026-06-07' });
        // start() enters an idle halt-wait loop (sleep-gated). Make the first idle
        // sleep stop the loop so the test returns promptly.
        sinon.stub(util, 'sleep').callsFake(() => { sync.running = false; return Promise.resolve(); });
        sync.running = true;
        await sync.start();
        assert.strictEqual(sync.isHalted(), true, 'must re-halt from the durable record, not catch up');
        assert.strictEqual(sync.getHaltInfo().blockIndex, 55);
        assert.strictEqual(db.getLastBlock.called, false, 'a halted client must not begin catch-up');
    });

    // Fix #3 (MED): a transient error reading sync_halt on start must FAIL CLOSED.
    // Previously the catch logged "continuing" and fell through to catch-up, so a
    // durably-halted replica could silently resume onto a contested chain when the
    // halt check merely hit a lock-wait / timeout.
    it('stays HALTED (idle, no catch-up) when the start-time halt check throws', async function(){
        db.getActiveHalt.rejects(new Error('sync_halt read blip'));
        // Stop the idle halt-wait loop on the first sleep so the test returns.
        sinon.stub(util, 'sleep').callsFake(() => { sync.running = false; return Promise.resolve(); });
        sync.running = true;
        await sync.start();
        assert.strictEqual(sync.isHalted(), true, 'an unreadable halt state must hold the client idle, not resume');
        assert.strictEqual(sync.getHaltInfo().reason, 'halt-state-check-failed');
        assert.strictEqual(db.getLastBlock.called, false, 'must NOT begin catch-up on an uncertain halt check');
    });
});

describe('ClientSync: divergence halt @regression', function(){
    let sync, db, applier, config, util;

    beforeEach(function(){
        db = createMockDb();
        applier = { applyBlock: sinon.stub().resolves(), applyFullSnapshot: sinon.stub().resolves(), applyIncrementalSnapshot: sinon.stub().resolves() };
        config = { SYNC_SOURCES: 'http://a:3006,http://b:3006', VERIFY_HASHES: true, HASH_CONFIRM_TIMEOUT: 5000, HALT_ON_DIVERGENCE: true };
        util = new Utility();
        sync = new ClientSync('bitcoin', 'mainnet', db, applier, { rollback: sinon.stub().resolves() }, new HashVerifier(), config, util);
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });
    afterEach(function(){ sinon.restore(); });

    // Failing closed is right; failing closed FOREVER on a single read is not. The idle
    // loop never retried, so a transient MariaDB blip left replication idle after the
    // database recovered, and the process stayed up so `unless-stopped` never restarted it.
    it('re-reads the halt table while idling, so a recovered database is not stalled forever', async function(){
        db.getActiveHalt.onFirstCall().rejects(new Error('sync_halt read blip'));
        db.getActiveHalt.onSecondCall().resolves({ block_index: 77, reason: 'cross-source-divergence',
            mismatches: '[]', sources: '[]', detected_at: '2026-09-12' });
        let sleeps = 0;
        sinon.stub(util, 'sleep').callsFake(() => { if(++sleeps >= 2) sync.running = false; return Promise.resolve(); });
        sync.running = true;
        await sync.start();

        assert.ok(db.getActiveHalt.callCount >= 2,
            'the idle loop must re-read the authoritative halt table, not sit on one failed read');
        assert.strictEqual(sync.getHaltInfo().reason, 'cross-source-divergence',
            'a positive re-read installs the REAL durable halt in place of the synthetic one');
        assert.strictEqual(sync.getHaltInfo().blockIndex, 77);
        assert.strictEqual(db.getLastBlock.called, false, 'and still no catch-up while halted');
    });

    // Clearing the stall was the only workaround an operator had, and it went straight
    // through db.clearHalt: the row a genuinely diverged replica depends on would be
    // deleted by the very act of recovering from a read the process never completed.
    it('clearing a halt-state-check-failed state does not wipe the unread sync_halt row', async function(){
        db.getActiveHalt.rejects(new Error('sync_halt read blip'));
        sinon.stub(util, 'sleep').callsFake(() => { sync.running = false; return Promise.resolve(); });
        sync.running = true;
        await sync.start();
        assert.strictEqual(sync.getHaltInfo().reason, 'halt-state-check-failed');

        const was = await sync.clearHalt();

        assert.strictEqual(sync.isHalted(), false, 'the in-memory stall flag is dropped');
        assert.strictEqual(was.reason, 'halt-state-check-failed');
        assert.strictEqual(db.clearHalt.called, false,
            'a halt state that was never READ must not delete the durable row');
    });
});
