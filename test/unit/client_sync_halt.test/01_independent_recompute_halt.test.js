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
const vectors = require('../../fixtures/block-hash-vectors.json');

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

describe('ClientSync: independent recompute halt @regression', function(){
    let sync, db, applier, config, util;

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
    // PASSES this; independent recompute HALTS it, from a SINGLE source.
    it('HALTS when replicated rows do not hash to the committed block hash', async function(){
        const event = {
            block_index: vectors.block_index, block_time: 123,
            // committed hashes the source CLAIMS (deliberately not the rows' hash)
            ledger_hash: 'forged_ledger', actions_hash: 'forged_actions', contract_hash: 'forged_contract'
        };
        await sync.applyBlockEvent(event);

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
        await sync.applyBlockEvent(event);

        assert.ok(applier.applyBlock.calledOnce);
        assert.strictEqual(sync.isHalted(), false, 'a verified block must not halt');
        assert.strictEqual(sync.lastAppliedBlock, vectors.block_index, 'verified block advances the tip');
        assert.strictEqual(db.recordHalt.called, false);
    });
});

describe('ClientSync: independent recompute halt @regression', function(){
    let sync, db, applier, config, util;

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

    it('skips recompute when VERIFY_RECOMPUTE is disabled (opt-out for plain replicas)', async function(){
        config['VERIFY_RECOMPUTE'] = false;
        const event = { block_index: vectors.block_index, block_time: 123, ledger_hash: 'forged', actions_hash: 'forged', contract_hash: 'forged' };
        await sync.applyBlockEvent(event);
        assert.strictEqual(sync.isHalted(), false, 'no recompute, no halt when opted out');
        assert.strictEqual(db.doQuery.called, false, 'recompute queries must not run when disabled');
        assert.strictEqual(sync.lastAppliedBlock, vectors.block_index);
    });

    it('a recompute DB error is logged but does NOT halt (no self-inflicted fork on infra faults)', async function(){
        db.doQuery = sinon.stub().rejects(new Error('transient DB error'));
        const event = { block_index: vectors.block_index, block_time: 123, ledger_hash: 'x', actions_hash: 'y', contract_hash: 'z' };
        await sync.applyBlockEvent(event);
        assert.strictEqual(sync.isHalted(), false, 'an infra error must not halt the validator');
        assert.strictEqual(sync.lastAppliedBlock, vectors.block_index, 'block still advances on a recompute error');
    });
});
