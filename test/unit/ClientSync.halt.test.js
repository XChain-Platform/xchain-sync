/*********************************************************************
 *
 * Copyright © 2025–2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC – https://dankest.llc
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This file is part of XChain Platform. Licensed under the GNU Affero
 * General Public License v3.0 or later; see LICENSE.md. A commercial
 * license (without AGPL source-disclosure terms) is available —
 * contact legal@dankest.llc.
 *
 **********************************************************************
 * ClientSync — durable consensus-divergence HALT.
 *
 * On a CONFIRMED cross-source hash divergence (two honest sources committed
 * different consensus hashes for the same block — one is on a forked/Byzantine
 * chain) the client must HALT: stop applying, persist the halt durably (so it
 * survives restart), and require an explicit operator clear. It must never
 * silently pick one source and replicate onto a contested chain.
 ********************************************************************/

const assert = require('assert');
const sinon  = require('sinon');
const ClientSync = require('../../src/ClientSync');
const Utility = require('../../src/utility');
const HashVerifier = require('../../src/HashVerifier');
const vectors = require('../fixtures/block-hash-vectors.json');

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

describe('ClientSync — divergence halt @regression', function(){
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

    it('_haltOnDivergence sets the halt, persists it durably, and clears pending hashes', async function(){
        sync.pendingHashes.set(101, {});
        await sync._haltOnDivergence(101, mism, ['http://a:3006', 'http://b:3006'], 'cross-source-divergence');

        assert.strictEqual(sync.isHalted(), true);
        assert.strictEqual(sync.getHaltInfo().blockIndex, 101);
        assert.deepStrictEqual(sync.getHaltInfo().mismatches, mism);
        assert.ok(db.recordHalt.calledOnce, 'halt must be persisted to sync_halt');
        assert.strictEqual(db.recordHalt.firstCall.args[0], 'indexer');
        assert.strictEqual(db.recordHalt.firstCall.args[1], 101);
        assert.strictEqual(sync.pendingHashes.size, 0, 'pending cross-source hashes are moot once halted');
    });

    it('a halted client REFUSES to apply blocks', async function(){
        await sync._haltOnDivergence(101, mism, ['http://a:3006', 'http://b:3006']);
        await sync._applyBlockEvent({ block_index: 102, ledger_hash: 'x', actions_hash: 'y', contract_hash: 'z' });
        assert.strictEqual(applier.applyBlock.called, false, 'no block may be applied while halted');
    });

    it('is idempotent — a second divergence does not double-record or change the halt block', async function(){
        await sync._haltOnDivergence(101, mism, []);
        await sync._haltOnDivergence(102, mism, []);
        assert.strictEqual(sync.getHaltInfo().blockIndex, 101, 'stays halted at the first contested block');
        assert.strictEqual(db.recordHalt.callCount, 1);
    });

    it('clearHalt resumes the client and clears the durable record', async function(){
        await sync._haltOnDivergence(101, mism, []);
        assert.strictEqual(sync.isHalted(), true);

        const was = await sync.clearHalt();
        assert.strictEqual(sync.isHalted(), false);
        assert.strictEqual(was.blockIndex, 101);
        assert.ok(db.clearHalt.calledOnceWith('indexer'), 'durable halt record must be cleared');

        // Applying works again after a clear.
        await sync._applyBlockEvent({ block_index: 103, ledger_hash: 'x', actions_hash: 'y', contract_hash: 'z' });
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
});

describe('ClientSync — independent recompute halt @regression', function(){
    let sync, db, applier, config, util;

    // db.doQuery feeds BlockHasher the canned golden rows IN CALL ORDER (one
    // sequence per computeBlockHashes pass), so the recompute yields the
    // indexer-authentic committed hashes from vectors.expected.
    function seqDb(results){
        let i = 0;
        return {
            dbName: 'test_db', dbType: 'indexer',
            getLastBlock: sinon.stub().resolves(null),
            getBlockHashRow: sinon.stub().resolves(null),
            doQuery: sinon.stub().callsFake(async () => results[i++]),
            recordHalt: sinon.stub().resolves({ block_index: 0 }),
            getActiveHalt: sinon.stub().resolves(null),
            clearHalt: sinon.stub().resolves(1)
        };
    }

    beforeEach(function(){
        db = seqDb(vectors.results.map(r => r.slice()));
        applier = { applyBlock: sinon.stub().resolves(), applyFullSnapshot: sinon.stub().resolves(), applyIncrementalSnapshot: sinon.stub().resolves() };
        // Single source + VERIFY_RECOMPUTE on: this is the path the cross-source
        // check cannot cover (one source, internally-consistent-but-wrong data).
        config = { SYNC_SOURCES: 'http://a:3006', VERIFY_HASHES: true, HASH_CONFIRM_TIMEOUT: 5000, HALT_ON_DIVERGENCE: true, VERIFY_RECOMPUTE: true };
        util = new Utility();
        sync = new ClientSync('bitcoin', 'mainnet', db, applier, { rollback: sinon.stub().resolves() }, new HashVerifier(), config, util);
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });
    afterEach(function(){ sinon.restore(); });

    // The headline RED->GREEN: rows that do NOT hash to the committed hash the
    // source published. The old verbatim transport check (committed-vs-committed)
    // PASSES this; independent recompute HALTS it — from a SINGLE source.
    it('HALTS when replicated rows do not hash to the committed block hash', async function(){
        const event = {
            block_index: vectors.block_index, block_time: 123,
            // committed hashes the source CLAIMS — deliberately not the rows' hash
            ledger_hash: 'forged_ledger', actions_hash: 'forged_actions', contract_hash: 'forged_contract'
        };
        await sync._applyBlockEvent(event);

        assert.ok(applier.applyBlock.calledOnce, 'block is applied, THEN recomputed');
        assert.strictEqual(sync.isHalted(), true, 'recompute mismatch must halt');
        assert.strictEqual(sync.getHaltInfo().reason, 'local-recompute-divergence');
        assert.strictEqual(sync.getHaltInfo().blockIndex, vectors.block_index);
        assert.strictEqual(sync.lastAppliedBlock, null, 'must NOT advance past an unverifiable block');
        assert.ok(db.recordHalt.calledOnce, 'halt persisted durably');
        assert.strictEqual(db.recordHalt.firstCall.args[2], 'local-recompute-divergence');
    });

    it('does NOT halt when rows hash to the committed block hash (clean block advances)', async function(){
        const event = Object.assign({ block_index: vectors.block_index, block_time: 123 }, {
            ledger_hash:   vectors.expected.ledger_hash,
            actions_hash:  vectors.expected.actions_hash,
            contract_hash: vectors.expected.contract_hash
        });
        await sync._applyBlockEvent(event);

        assert.ok(applier.applyBlock.calledOnce);
        assert.strictEqual(sync.isHalted(), false, 'a verified block must not halt');
        assert.strictEqual(sync.lastAppliedBlock, vectors.block_index, 'verified block advances the tip');
        assert.strictEqual(db.recordHalt.called, false);
    });

    it('skips recompute when VERIFY_RECOMPUTE is disabled (opt-out for plain replicas)', async function(){
        config['VERIFY_RECOMPUTE'] = false;
        const event = { block_index: vectors.block_index, block_time: 123, ledger_hash: 'forged', actions_hash: 'forged', contract_hash: 'forged' };
        await sync._applyBlockEvent(event);
        assert.strictEqual(sync.isHalted(), false, 'no recompute, no halt when opted out');
        assert.strictEqual(db.doQuery.called, false, 'recompute queries must not run when disabled');
        assert.strictEqual(sync.lastAppliedBlock, vectors.block_index);
    });

    it('a recompute DB error is logged but does NOT halt (no self-inflicted fork on infra faults)', async function(){
        db.doQuery = sinon.stub().rejects(new Error('transient DB error'));
        const event = { block_index: vectors.block_index, block_time: 123, ledger_hash: 'x', actions_hash: 'y', contract_hash: 'z' };
        await sync._applyBlockEvent(event);
        assert.strictEqual(sync.isHalted(), false, 'an infra error must not halt the validator');
        assert.strictEqual(sync.lastAppliedBlock, vectors.block_index, 'block still advances on a recompute error');
    });
});
