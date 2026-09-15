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
const ClientSync = require('../../../src/client/sync');
const Utility = require('../../../src/util');
const HashVerifier = require('../../../src/client/hash_verifier');

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

function setupBoundarySync(){
    const db = createMockDb();
    db.getBlockHashRow = sinon.stub().resolves({
        ledger_hash: 'L', actions_hash: 'A', contract_hash: 'C'
    });
    const config = { SYNC_SOURCES: 'http://a:3006', VERIFY_HASHES: true, HASH_CONFIRM_TIMEOUT: 5000, HALT_ON_DIVERGENCE: true, VERIFY_RECOMPUTE: true };
    const util = new Utility();
    sinon.stub(util, 'sleep').resolves(); // no real backoff waits in tests
    const sync = new ClientSync('bitcoin', 'mainnet', db,
        { applyBlock: sinon.stub().resolves(), applyIncrementalSnapshot: sinon.stub().resolves() },
        { rollback: sinon.stub().resolves() }, new HashVerifier(), config, util);
    sinon.stub(console, 'log');
    sinon.stub(console, 'error');
    return { sync, db };
}

describe('ClientSync: bulk-range boundary recompute fails CLOSED @regression', function(){
    // The live path above fails OPEN on a recompute error (an infra fault must
    // not fork the validator). At a bulk-range boundary (catch-up join/terminal,
    // bootstrap terminal) that posture is a hole: the join recompute is the ONLY
    // check that catches a disconnect-spanning reorg stitched onto an orphaned
    // tip, so an error there that failed open would let the range through unverified.
    // verifyRangeBoundary must retry the recompute and then HALT durably.
    let sync, db;

    beforeEach(function(){ ({ sync, db } = setupBoundarySync()); });
    afterEach(function(){ sinon.restore(); });

    it('HALTS (recompute-error) when the recompute errors on every retry', async function(){
        sync.blockHasher.computeBlockHashes = sinon.stub().rejects(new Error('schema gap'));
        const halted = await sync.verifyRangeBoundary(500);

        assert.strictEqual(halted, true, 'caller must be told to stop');
        assert.strictEqual(sync.blockHasher.computeBlockHashes.callCount, 3, 'bounded retries before halting');
        assert.strictEqual(sync.isHalted(), true, 'an unverifiable range must not be served');
        assert.strictEqual(sync.getHaltInfo().reason, 'recompute-error');
        assert.strictEqual(sync.getHaltInfo().blockIndex, 500);
        assert.ok(db.recordHalt.calledOnce, 'halt persisted durably');
        assert.strictEqual(db.recordHalt.firstCall.args[2], 'recompute-error');
    });

    it('does NOT halt when a transient error clears within the retries', async function(){
        sync.blockHasher.computeBlockHashes = sinon.stub()
            .onFirstCall().rejects(new Error('transient'))
            .resolves({ ledger_hash: 'L', actions_hash: 'A', contract_hash: 'C' });
        const halted = await sync.verifyRangeBoundary(500);

        assert.strictEqual(halted, false);
        assert.strictEqual(sync.isHalted(), false, 'a recovered transient must not halt');
    });
});

describe('ClientSync: bulk-range boundary recompute fails CLOSED @regression', function(){
    let sync, db;

    beforeEach(function(){ ({ sync, db } = setupBoundarySync()); });
    afterEach(function(){ sinon.restore(); });

    it('HALTS (local-recompute-divergence) on a boundary hash mismatch', async function(){
        sync.blockHasher.computeBlockHashes = sinon.stub()
            .resolves({ ledger_hash: 'WRONG', actions_hash: 'A', contract_hash: 'C' });
        const halted = await sync.verifyRangeBoundary(500);

        assert.strictEqual(halted, true);
        assert.strictEqual(sync.getHaltInfo().reason, 'local-recompute-divergence');
    });

    it('skips (no halt) when the committed boundary hash is not yet resolvable', async function(){
        db.getBlockHashRow.resolves(null);
        sync.blockHasher.computeBlockHashes = sinon.stub().rejects(new Error('must not be called'));
        const halted = await sync.verifyRangeBoundary(500);

        assert.strictEqual(halted, false);
        assert.strictEqual(sync.blockHasher.computeBlockHashes.called, false, 'no recompute without a committed hash');
        assert.strictEqual(sync.isHalted(), false);
    });
});

describe('ClientSync: bulk-range boundary recompute fails CLOSED @regression', function(){
    let sync, db;

    beforeEach(function(){ ({ sync, db } = setupBoundarySync()); });
    afterEach(function(){ sinon.restore(); });

    // A FAILED committed-hash read is not an absent one. getBlockHashRow's default is
    // fail-soft (doQuery swallows a non-transactional query error to [] -> null), and
    // null is the "not yet resolvable" skip above, so a transient DB fault read through
    // that default returns false and tells bootstrap/catch-up the range was verified.
    it('HALTS (boundary-read-error) when the committed hash READ fails on every retry @regression', async function(){
        db.getBlockHashRow.rejects(new Error('ER_LOCK_WAIT_TIMEOUT: errno 1205'));
        sync.blockHasher.computeBlockHashes = sinon.stub().rejects(new Error('must not be called'));
        const halted = await sync.verifyRangeBoundary(500);

        assert.strictEqual(halted, true, 'an unverifiable range must not be served');
        assert.strictEqual(db.getBlockHashRow.callCount, 3, 'bounded retries before halting');
        assert.strictEqual(sync.isHalted(), true);
        assert.strictEqual(sync.getHaltInfo().reason, 'boundary-read-error');
        assert.strictEqual(sync.getHaltInfo().blockIndex, 500);
        assert.ok(db.recordHalt.calledOnce, 'halt persisted durably');
        assert.strictEqual(db.recordHalt.firstCall.args[2], 'boundary-read-error');
        assert.strictEqual(sync.blockHasher.computeBlockHashes.called, false);
    });

    it('reads the committed boundary hash FAIL-CLOSED (rethrow), not on the fail-soft default @regression', async function(){
        await sync.verifyRangeBoundary(500);
        const opts = db.getBlockHashRow.firstCall.args[2];
        assert.ok(opts && opts.rethrow === true,
            'a swallowed query error would otherwise be indistinguishable from an absent row');
    });
});

describe('ClientSync: bulk-range boundary recompute fails CLOSED @regression', function(){
    let sync, db;

    beforeEach(function(){ ({ sync, db } = setupBoundarySync()); });
    afterEach(function(){ sinon.restore(); });

    it('does NOT halt when a transient READ error clears within the retries @regression', async function(){
        db.getBlockHashRow = sinon.stub();
        db.getBlockHashRow.onFirstCall().rejects(new Error('transient'));
        db.getBlockHashRow.resolves({ ledger_hash: 'L', actions_hash: 'A', contract_hash: 'C' });
        sync.blockHasher.computeBlockHashes = sinon.stub()
            .resolves({ ledger_hash: 'L', actions_hash: 'A', contract_hash: 'C' });
        const halted = await sync.verifyRangeBoundary(500);

        assert.strictEqual(halted, false);
        assert.strictEqual(sync.isHalted(), false, 'a recovered transient must not halt');
        assert.strictEqual(db.getBlockHashRow.callCount, 2);
    });

    it('the LIVE path still fails open: a persistent error does not throw without failClosed', async function(){
        sync.blockHasher.computeBlockHashes = sinon.stub().rejects(new Error('infra fault'));
        const mismatches = await sync.verifyRecompute({ block_index: 500, ledger_hash: 'L', actions_hash: 'A', contract_hash: 'C' });

        assert.strictEqual(mismatches, null, 'live path returns null (fail-open) on a recompute error');
        assert.strictEqual(sync.blockHasher.computeBlockHashes.callCount, 1, 'no retries on the live path');
        assert.strictEqual(sync.isHalted(), false);
    });
});
