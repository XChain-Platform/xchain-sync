/*********************************************************************
 *
 * Copyright © 2025–2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC – https://dankest.llc
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This file is part of XChain Platform. Licensed under the GNU Affero
 * General Public License v3.0 or later; see LICENSE.md.
 *
 **********************************************************************
 * ClientSync: a source whose block event dissents only after the honest quorum has
 * applied that block is a dissenter, not an uncontested reorg. Its forked re-delivery
 * of the head must not unwind the block at the confirmation timeout; it is struck,
 * and evicted at the strike threshold, as the block tally would strike it. A genuine
 * reorg that gathers its own quorum still unwinds, and a lone report at a tip no
 * live quorum applied keeps the designed liveness fallback.
 ********************************************************************/

const assert = require('assert');
const sinon  = require('sinon');
const ClientSync   = require('../../src/client/sync');
const Utility      = require('../../src/util');
const HashVerifier = require('../../src/client/hash_verifier');

function createMockDb(){
    return {
        dbName: 'test_db', dbType: 'indexer',
        getLastBlock: sinon.stub().resolves(null),
        getBlockHashRow: sinon.stub().resolves(null),
        getActiveHalt: sinon.stub().resolves(null),
        recordHalt: sinon.stub().resolves({ block_index: 0 }),
        doQuery: sinon.stub().resolves([])
    };
}

function blockEvent(blockIndex, h){
    return { type: 'block', block_index: blockIndex,
        ledger_hash: 'lh' + blockIndex + h, actions_hash: 'ah' + blockIndex + h,
        contract_hash: 'ch' + blockIndex + h };
}

// The honest block with the last hex digit of its ledger_hash flipped, as a tampering
// proxy rewrites it in the source's block event.
function tamperedEvent(blockIndex){
    let event = blockEvent(blockIndex, 'H');
    event.ledger_hash = event.ledger_hash + 'f';
    return event;
}

function makeSync(extraConfig){
    let applier = { applyBlock: sinon.stub().resolves(), applyFullSnapshot: sinon.stub().resolves() };
    let rollback = { rollback: sinon.stub().resolves() };
    let config = Object.assign({
        SYNC_SOURCES: 'http://a:3006,http://b:3006,http://c:3006', VERIFY_HASHES: true, HALT_ON_DIVERGENCE: true,
        HASH_CONFIRM_TIMEOUT: 5000, CLIENT_RECONNECT_DELAY: 5000,
        VERIFY_RECOMPUTE: false, VERIFY_STATE_HASH: false, VERIFY_STATE_COMMITMENT: false
    }, extraConfig || {});
    let sync = new ClientSync('dogecoin', 'regtest', createMockDb(), applier,
        rollback, new HashVerifier(), config, new Utility());
    sync.lastAppliedBlock = 100;
    sync.lastHashes = blockEvent(100, 'H');
    delete sync.lastHashes.type;
    delete sync.lastHashes.block_index;
    sinon.stub(sync, 'incrementalCatchUp').resolves();
    sinon.spy(sync, 'strikeSource');
    sinon.spy(sync, 'applyAgreedReorg');
    return { sync, rollback, applier };
}

const ctx = {};
async function pastTimeout(sync){ await ctx.clock.tickAsync(sync.config.HASH_CONFIRM_TIMEOUT + 100); }

// Sources 0 and 1 reach quorum on the honest block, which is applied.
async function applyHonest(sync, blockIndex){
    await sync.handleBlock(blockEvent(blockIndex, 'H'), 0);
    await sync.handleBlock(blockEvent(blockIndex, 'H'), 1);
    assert.strictEqual(sync.lastAppliedBlock, blockIndex, 'the honest quorum applied block ' + blockIndex);
}

function registerStrikeCases(){
    it('never unwinds the applied block on the late dissent and strikes its source', async function(){
        let { sync, rollback } = makeSync();
        await applyHonest(sync, 101);
        await sync.handleBlock(tamperedEvent(101), 2);
        await pastTimeout(sync);
        assert.strictEqual(rollback.rollback.called, false, 'the honest block is not rolled back');
        assert.strictEqual(sync.applyAgreedReorg.called, false, 'no uncontested-report unwind');
        assert.strictEqual(sync.incrementalCatchUp.called, false, 'no rewind catch-up');
        assert.strictEqual(sync.lastAppliedBlock, 101);
        assert.ok(sync.strikeSource.calledWith(2, 101), 'the dissenting source is struck');
        assert.strictEqual(sync._pendingReorgs.size, 0);
    });

    it('evicts a source that dissents late on SOURCE_EVICT_THRESHOLD blocks', async function(){
        let { sync, rollback } = makeSync();
        let threshold = sync._sourceEvictThreshold;
        for(let blk = 101; blk < 101 + threshold; blk++){
            await applyHonest(sync, blk);
            await sync.handleBlock(tamperedEvent(blk), 2);
            await pastTimeout(sync);
        }
        assert.strictEqual(rollback.rollback.called, false);
        assert.deepStrictEqual(sync.getEvictedSources(), ['http://c:3006']);
        assert.strictEqual(sync.getActiveSourceCount(), 2);
        assert.strictEqual(sync.isHalted(), false);
    });
}

// The committed row the mock DB returns for a height the honest quorum applied.
function storeHonestRows(sync){
    sync.db.getBlockHashRow.callsFake(async height => {
        let row = blockEvent(height, 'H');
        delete row.type;
        return row;
    });
}

function registerLateArrivalCases(){
    it('strikes the dissent when the next block is applied inside the confirmation window', async function(){
        let { sync, rollback } = makeSync();
        storeHonestRows(sync);
        await applyHonest(sync, 101);
        await sync.handleBlock(tamperedEvent(101), 2);
        await applyHonest(sync, 102);
        await pastTimeout(sync);
        assert.strictEqual(rollback.rollback.called, false, 'neither 101 nor 102 is rolled back');
        assert.strictEqual(sync.lastAppliedBlock, 102);
        assert.ok(sync.strikeSource.calledWith(2, 101));
    });

    it('strikes a dissent that arrives one block late, below the tip', async function(){
        let { sync, rollback } = makeSync();
        storeHonestRows(sync);
        await applyHonest(sync, 101);
        await applyHonest(sync, 102);
        await sync.handleBlock(tamperedEvent(101), 2);
        await pastTimeout(sync);
        assert.strictEqual(rollback.rollback.called, false, 'no deeper unwind for a delayed dissent');
        assert.strictEqual(sync.lastAppliedBlock, 102);
        assert.ok(sync.strikeSource.calledWith(2, 101));
    });
}

function registerFallbackCases(){
    it('holds no catch-up gate under strict confirmation for a refuted late dissent', async function(){
        let { sync, rollback } = makeSync({ HASH_CONFIRM_STRICT: true });
        await applyHonest(sync, 101);
        await sync.handleBlock(tamperedEvent(101), 2);
        await pastTimeout(sync);
        assert.strictEqual(rollback.rollback.called, false);
        assert.strictEqual(sync._strictReorgPending.has(101), false, 'the dissent does not block catch-up');
        assert.ok(sync.strikeSource.calledWith(2, 101));
    });

    it('still unwinds at once when a quorum member reports the same fork before the timeout', async function(){
        let { sync, rollback } = makeSync();
        await applyHonest(sync, 101);
        await sync.handleBlock(tamperedEvent(101), 2);
        await sync.handleBlock(tamperedEvent(101), 1);
        assert.strictEqual(rollback.rollback.calledOnceWith(101), true, 'a genuine reorg quorum unwinds');
        assert.strictEqual(sync.lastAppliedBlock, 100);
        await pastTimeout(sync);
        assert.strictEqual(sync.strikeSource.called, false, 'the first reporter of a real reorg is not struck');
    });

    it('keeps the lone-report fallback at a tip no live quorum applied', async function(){
        let { sync, rollback } = makeSync();
        await sync.handleBlock(tamperedEvent(100), 2);
        await pastTimeout(sync);
        assert.strictEqual(rollback.rollback.calledOnceWith(100), true, 'the uncontested report still unwinds');
        assert.strictEqual(sync.lastAppliedBlock, 99);
        assert.strictEqual(sync.strikeSource.called, false);
    });
}

describe('ClientSync: a dissent after the quorum applied the block is struck @regression', function(){
    beforeEach(function(){
        sinon.stub(console, 'log'); sinon.stub(console, 'warn'); sinon.stub(console, 'error');
        ctx.clock = sinon.useFakeTimers();
    });
    afterEach(function(){ ctx.clock.restore(); sinon.restore(); });

    registerStrikeCases();
    registerLateArrivalCases();
    registerFallbackCases();
});
