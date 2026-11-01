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
 * ClientSync: a source-reported reorg deletes rows only under the same rule that
 * admits a block. A quorum of sources reporting the same height unwinds at once;
 * a lone report waits out the confirmation timeout and then unwinds, or is refused
 * under strict confirmation; an evicted source's report is dropped. Internal
 * rewinds that carry no source keep unwinding immediately.
 ********************************************************************/

const assert = require('assert');
const sinon  = require('sinon');
const ClientSync   = require('../../src/client/sync');
const Utility      = require('../../src/util');
const HashVerifier = require('../../src/client/hash_verifier');

function createMockDb(dbType){
    return {
        dbName: 'test_db', dbType: dbType || 'indexer',
        getLastBlock: sinon.stub().resolves(null),
        getBlockHashRow: sinon.stub().resolves(null),
        getActiveHalt: sinon.stub().resolves(null),
        recordHalt: sinon.stub().resolves({ block_index: 0 }),
        doQuery: sinon.stub().resolves([])
    };
}

function reorgEvent(blockIndex){
    return { type: 'reorg', block_index: blockIndex };
}

// A block event whose hash tuple differs from the committed tip's.
function forkedBlockEvent(blockIndex){
    return { type: 'block', block_index: blockIndex,
        ledger_hash: 'lhY', actions_hash: 'ahY', contract_hash: 'chY' };
}

function makeSync(extraConfig, dbType){
    let applier = { applyBlock: sinon.stub().resolves(), applyFullSnapshot: sinon.stub().resolves() };
    let rollback = { rollback: sinon.stub().resolves() };
    let config = Object.assign({
        SYNC_SOURCES: 'http://a:3006,http://b:3006,http://c:3006', VERIFY_HASHES: true, HALT_ON_DIVERGENCE: true,
        HASH_CONFIRM_TIMEOUT: 5000, CLIENT_RECONNECT_DELAY: 5000,
        VERIFY_RECOMPUTE: false, VERIFY_STATE_HASH: false, VERIFY_STATE_COMMITMENT: false
    }, extraConfig || {});
    let sync = new ClientSync('bitcoin', 'mainnet', createMockDb(dbType), applier,
        rollback, new HashVerifier(), config, new Utility());
    sync.lastAppliedBlock = 100;
    sync.lastHashes = { ledger_hash: 'lhX', actions_hash: 'ahX', contract_hash: 'chX' };
    sinon.stub(sync, 'incrementalCatchUp').resolves();
    return { sync, rollback };
}

// Fake timers shared by the cases below; each beforeEach installs a fresh clock.
const reorgCtx = {};
async function pastTimeout(sync){ await reorgCtx.clock.tickAsync(sync.config.HASH_CONFIRM_TIMEOUT + 100); }

function registerQuorumCases(){
    it('holds a lone reorg report and unwinds once a second source reports the same height', async function(){
        let { sync, rollback } = makeSync();
        await sync.handleEvent(reorgEvent(95), 0);
        assert.strictEqual(rollback.rollback.called, false, 'one source alone never unwinds at once');
        assert.strictEqual(sync.lastAppliedBlock, 100);
        await sync.handleEvent(reorgEvent(95), 1);
        assert.strictEqual(rollback.rollback.calledOnce, true);
        assert.strictEqual(rollback.rollback.firstCall.args[0], 95);
        assert.strictEqual(sync.lastAppliedBlock, 94);
        await pastTimeout(sync);
        assert.strictEqual(rollback.rollback.calledOnce, true, 'the quorum cancels the timeout fallback');
    });

    it('does not count reports at different heights toward one quorum', async function(){
        let { sync, rollback } = makeSync();
        await sync.handleEvent(reorgEvent(95), 0);
        await sync.handleEvent(reorgEvent(96), 1);
        assert.strictEqual(rollback.rollback.called, false);
    });

    it('unwinds an uncontested lone report after the confirmation timeout by default', async function(){
        let { sync, rollback } = makeSync();
        await sync.handleEvent(reorgEvent(95), 0);
        assert.strictEqual(rollback.rollback.called, false);
        await pastTimeout(sync);
        assert.strictEqual(rollback.rollback.calledOnce, true, 'the silent-peer fallback matches the block path');
        assert.strictEqual(sync.lastAppliedBlock, 94);
        assert.strictEqual(sync._pendingReorgs.size, 0);
    });
}

function registerQuorumLifecycleCases(){

    it('escalates bootstrap exhaustion from a reorg timeout @regression', async function(){
        let { sync } = makeSync();
        let exit = sinon.stub(process, 'exit');
        sinon.stub(sync, 'applyAgreedReorg')
            .rejects(new ClientSync.BootstrapExhaustedError('exhausted'));
        await sync.handleEvent(reorgEvent(95), 0);

        await pastTimeout(sync);

        assert.ok(exit.calledOnceWithExactly(1));
        let logged = console.error.getCalls().map(c => String(c.args[0])).join('\n');
        assert.ok(logged.includes('Bootstrap exhausted mid-stream'));
    });

    it('tracks a timeout unwind until shutdown can drain it @regression', async function(){
        let { sync } = makeSync();
        let resolveApply;
        let pending = new Promise(resolve => { resolveApply = resolve; });
        sinon.stub(sync, 'applyAgreedReorg').returns(pending);
        await sync.handleEvent(reorgEvent(95), 0);

        reorgCtx.clock.tick(sync.config.HASH_CONFIRM_TIMEOUT + 100);
        await Promise.resolve();
        assert.strictEqual(sync._inFlightWork.size, 1);
        let drained = false;
        let stopping = sync.stop().then(() => { drained = true; });
        await Promise.resolve();
        assert.strictEqual(drained, false);
        resolveApply();
        await stopping;
        assert.strictEqual(drained, true);
    });

    it('refuses a lone report at the timeout under strict confirmation, and a later second source completes it', async function(){
        let { sync, rollback } = makeSync({ HASH_CONFIRM_STRICT: true });
        await sync.handleEvent(reorgEvent(95), 0);
        await pastTimeout(sync);
        assert.strictEqual(rollback.rollback.called, false, 'strict mode never unwinds on one source');
        assert.strictEqual(sync.lastAppliedBlock, 100);
        await sync.handleEvent(reorgEvent(95), 2);
        assert.strictEqual(rollback.rollback.calledOnce, true, 'the retained report still counts toward quorum');
        assert.strictEqual(sync.lastAppliedBlock, 94);
    });

    it('drops a reorg report from an evicted source', async function(){
        let { sync, rollback } = makeSync();
        sync._evictedSources.add(2);
        await sync.handleEvent(reorgEvent(95), 2);
        await pastTimeout(sync);
        assert.strictEqual(rollback.rollback.called, false);
        assert.strictEqual(sync.lastAppliedBlock, 100);
    });
}

function registerEvictionAndScopeCases(){
    it('holds a decoder forked head until a second source agrees', async function(){
        let { sync, rollback } = makeSync({ HASH_CONFIRM_STRICT: true }, 'decoder');
        sync.lastHashes = { block_hash: 'hash100' };
        await sync.handleBlock({ type: 'block', block_index: 100, block_hash: 'FORKED' }, 0);
        await pastTimeout(sync);
        assert.strictEqual(rollback.rollback.called, false);
        await sync.handleEvent(reorgEvent(100), 1);
        assert.strictEqual(rollback.rollback.calledOnceWith(100), true);
        assert.strictEqual(sync.lastAppliedBlock, 99);
    });

    it('drops a decoder forked head from an evicted source', async function(){
        let { sync, rollback } = makeSync({}, 'decoder');
        sync.lastHashes = { block_hash: 'hash100' };
        sync._evictedSources.add(2);
        await sync.handleBlock({ type: 'block', block_index: 100, block_hash: 'FORKED' }, 2);
        await pastTimeout(sync);
        assert.strictEqual(rollback.rollback.called, false);
    });

    it('holds a decoder parent mismatch until reorg quorum agrees', async function(){
        let { sync, rollback } = makeSync({ HASH_CONFIRM_STRICT: true }, 'decoder');
        sync.lastHashes = { block_hash: 'hash100' };
        let event = { type: 'block', block_index: 101, block_hash: 'hash101',
            data: { blocks: [{ block_index: 101, block_hash_id: 8, previous_block_hash_id: 7 }],
                index_transactions: [{ id: 7, hash: 'FORKED100' }] } };
        await sync.handleBlock(event, 0);
        assert.strictEqual(rollback.rollback.called, false);
        await sync.handleEvent(reorgEvent(100), 1);
        assert.strictEqual(rollback.rollback.calledOnceWith(100), true);
    });
}

function registerEvictedHeldCases(){

    it('does not unwind on a held report whose source was evicted while it waited', async function(){
        let { sync, rollback } = makeSync();
        await sync.handleEvent(reorgEvent(95), 0);
        sync._evictedSources.add(0);
        await pastTimeout(sync);
        assert.strictEqual(rollback.rollback.called, false);
        assert.strictEqual(sync._pendingReorgs.size, 0);
    });

    it('holds a re-delivered forked head from one source like a reorg report', async function(){
        let { sync, rollback } = makeSync({ HASH_CONFIRM_STRICT: true });
        await sync.handleBlock(forkedBlockEvent(100), 0);
        await pastTimeout(sync);
        assert.strictEqual(rollback.rollback.called, false, 'one source re-delivering its head never unwinds alone');
        await sync.handleEvent(reorgEvent(100), 1);
        assert.strictEqual(rollback.rollback.calledOnce, true, 'a reorg report at the same height agrees with it');
        assert.strictEqual(sync.lastAppliedBlock, 99);
        assert.strictEqual(sync.incrementalCatchUp.calledOnce, true, 'the head rewind still re-fetches the tip');
    });

    it('drops a re-delivered forked head from an evicted source', async function(){
        let { sync, rollback } = makeSync();
        sync._evictedSources.add(2);
        await sync.handleBlock(forkedBlockEvent(100), 2);
        await pastTimeout(sync);
        assert.strictEqual(rollback.rollback.called, false);
    });

    it('drops a held reorg report when a halt stops the live apply path', async function(){
        let { sync, rollback } = makeSync();
        await sync.handleEvent(reorgEvent(95), 0);
        for(let idx = 0; idx < 3; idx++){
            await sync.handleBlock({ type: 'block', block_index: 101,
                ledger_hash: 'lh' + idx, actions_hash: 'ah' + idx, contract_hash: 'ch' + idx }, idx);
        }
        assert.strictEqual(sync.isHalted(), true);
        await pastTimeout(sync);
        assert.strictEqual(rollback.rollback.called, false, 'a halted replica never unwinds on a held report');
        assert.strictEqual(sync._pendingReorgs.size, 0);
    });
}

function registerHaltAndInternalCases(){

    it('refuses fresh reorg reports while halted and resumes after clear', async function(){
        let { sync, rollback } = makeSync({ SYNC_SOURCES: 'http://a:3006' });
        sync._halted = { blockIndex: 101, reason: 'no-source-quorum' };
        await sync.handleEvent(reorgEvent(95), 0);
        assert.strictEqual(rollback.rollback.called, false);
        assert.strictEqual(sync._pendingReorgs.size, 0);
        sync._halted = null;
        await sync.handleEvent(reorgEvent(95), 0);
        assert.strictEqual(rollback.rollback.calledOnceWith(95), true);
    });

    it('refuses internal fork rewinds while halted', async function(){
        let { sync, rollback } = makeSync();
        sync._halted = { blockIndex: 101, reason: 'no-source-quorum' };
        await sync.rewindForkedHead(100);
        assert.strictEqual(rollback.rollback.called, false);
        assert.strictEqual(sync.lastAppliedBlock, 100);
    });

    it('refuses a rollback when a halt lands while waiting for the apply lock @regression', async function(){
        let { sync, rollback } = makeSync();
        sinon.stub(sync, 'withApplyLock').callsFake(async fn => {
            sync._halted = { blockIndex: 101, reason: 'no-source-quorum' };
            return fn();
        });

        await sync.handleReorg(reorgEvent(95));

        assert.strictEqual(rollback.rollback.called, false);
        assert.strictEqual(sync.lastAppliedBlock, 100);
    });

    it('keeps internal rewinds that carry no source immediate', async function(){
        let { sync, rollback } = makeSync({ HASH_CONFIRM_STRICT: true });
        await sync.rewindForkedHead(100);
        assert.strictEqual(rollback.rollback.calledOnce, true);
        assert.strictEqual(sync.lastAppliedBlock, 99);
        await sync.handleReorg(reorgEvent(97));
        assert.strictEqual(rollback.rollback.calledTwice, true);
        assert.strictEqual(sync.lastAppliedBlock, 96);
    });

    it('unwinds a lone report at once with a single active source', async function(){
        let { sync, rollback } = makeSync({ SYNC_SOURCES: 'http://a:3006' });
        await sync.handleEvent(reorgEvent(95), 0);
        assert.strictEqual(rollback.rollback.calledOnce, true);
    });
}

describe('ClientSync: source reorgs need the block quorum @regression', function(){
    beforeEach(function(){
        sinon.stub(console, 'log'); sinon.stub(console, 'warn'); sinon.stub(console, 'error');
        reorgCtx.clock = sinon.useFakeTimers();
    });
    afterEach(function(){ reorgCtx.clock.restore(); sinon.restore(); });

    registerQuorumCases();
    registerQuorumLifecycleCases();
    registerEvictionAndScopeCases();
    registerEvictedHeldCases();
    registerHaltAndInternalCases();
});
