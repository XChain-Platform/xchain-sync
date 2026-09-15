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

describe('ClientSync: state_hash apply-time integrity halt @regression', function(){
    let sync, db, applier, config, util;

    beforeEach(function(){
        db = createMockDb();
        applier = { applyBlock: sinon.stub().resolves(), applyFullSnapshot: sinon.stub().resolves(), applyIncrementalSnapshot: sinon.stub().resolves() };
        // VERIFY_RECOMPUTE off (isolate the state_hash path); VERIFY_STATE_HASH defaults ON.
        config = { SYNC_SOURCES: 'http://a:3006', VERIFY_HASHES: true, HASH_CONFIRM_TIMEOUT: 5000, HALT_ON_DIVERGENCE: true, VERIFY_RECOMPUTE: false };
        util = new Utility();
        sync = new ClientSync('bitcoin', 'mainnet', db, applier, { rollback: sinon.stub().resolves() }, new HashVerifier(), config, util);
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });
    afterEach(function(){ sinon.restore(); });

    it('HALTS when the apply-time state_hash recompute disagrees with the source', async function(){
        sync.blockHasher.computeStateHash = sinon.stub().resolves('LOCAL_STATE');
        await sync.applyBlockEvent({ block_index: 200, block_time: 1, state_hash: 'SOURCE_STATE' });

        assert.ok(applier.applyBlock.calledOnce, 'block is applied, THEN the state_hash recomputed');
        assert.strictEqual(sync.isHalted(), true, 'a state_hash mismatch must halt');
        assert.strictEqual(sync.getHaltInfo().reason, 'state-hash-divergence');
        assert.strictEqual(sync.getHaltInfo().blockIndex, 200);
        assert.strictEqual(sync.lastAppliedBlock, null, 'must NOT advance past a state-divergent block');
        assert.ok(db.recordHalt.calledOnce && db.recordHalt.firstCall.args[2] === 'state-hash-divergence',
            'halt persisted durably with the state-hash reason');
    });

    it('does NOT halt when the recomputed state_hash matches (clean block advances)', async function(){
        sync.blockHasher.computeStateHash = sinon.stub().resolves('AGREED_STATE');
        await sync.applyBlockEvent({ block_index: 200, block_time: 1, state_hash: 'AGREED_STATE' });

        assert.strictEqual(sync.isHalted(), false, 'a matching state_hash must not halt');
        assert.strictEqual(sync.lastAppliedBlock, 200, 'verified block advances the tip');
    });

    it('SKIPS the check (no recompute, no halt) when the source sent a NULL state_hash (pre-feature block)', async function(){
        sync.blockHasher.computeStateHash = sinon.stub().resolves('LOCAL_STATE');
        await sync.applyBlockEvent({ block_index: 200, block_time: 1, state_hash: null });

        assert.strictEqual(sync.blockHasher.computeStateHash.called, false,
            'a NULL state_hash must skip the recompute entirely (fail-soft against a back-level source)');
        assert.strictEqual(sync.isHalted(), false);
        assert.strictEqual(sync.lastAppliedBlock, 200, 'the block still advances');
    });

    it('opts out cleanly when VERIFY_STATE_HASH=false (throwaway mirrors)', async function(){
        config['VERIFY_STATE_HASH'] = false;
        sync.blockHasher.computeStateHash = sinon.stub().resolves('LOCAL_STATE');
        await sync.applyBlockEvent({ block_index: 200, block_time: 1, state_hash: 'SOURCE_STATE' });

        assert.strictEqual(sync.blockHasher.computeStateHash.called, false, 'no recompute when opted out');
        assert.strictEqual(sync.isHalted(), false, 'no halt when opted out, even on a would-be mismatch');
        assert.strictEqual(sync.lastAppliedBlock, 200);
    });
});
