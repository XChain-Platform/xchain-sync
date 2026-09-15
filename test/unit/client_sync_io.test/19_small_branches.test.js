// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

const assert     = require('assert');
const sinon      = require('sinon');
const ClientSync = require('../../../src/client/sync');
const Utility    = require('../../../src/util');
const HashVerifier = require('../../../src/client/hash_verifier');
const { withDbMixins } = require('../../helpers/db_mixins.js');

function createMockDb(overrides){
    return Object.assign({
        dbName: 'test_db', dbType: 'indexer',
        getLastBlock: sinon.stub().resolves(null), getBlockHashRow: sinon.stub().resolves(null),
        doQuery: sinon.stub().resolves([]), getActiveHalt: sinon.stub().resolves(null),
        getTableCount: sinon.stub().resolves(0), addMissingColumns: sinon.stub().resolves(),
        recordHalt: sinon.stub().resolves({ block_index: 0 }), clearHalt: sinon.stub().resolves(1)
    }, overrides || {});
}
function createMockApplier(){
    return { applyBlock: sinon.stub().resolves(), applyFullSnapshot: sinon.stub().resolves(),
        applyIncrementalSnapshot: sinon.stub().resolves() };
}
function createMockRollback(){ return { rollback: sinon.stub().resolves() }; }
function makeSync(configOverrides, dbOverrides){
    let db = createMockDb(dbOverrides), applier = createMockApplier(), rb = createMockRollback();
    let hv = new HashVerifier(), util = new Utility();
    let config = Object.assign({
        SYNC_SOURCES: 'http://src1:3006', VERIFY_HASHES: false, CLIENT_RECONNECT_DELAY: 5000,
        HASH_CONFIRM_TIMEOUT: 5000, SNAPSHOT_MAX_CONTENT: 200 * 1024 * 1024,
        WS_MAX_PAYLOAD: 50 * 1024 * 1024, MAX_ROLLBACK_DEPTH: 10, GAP_LOG_INTERVAL_MS: 30000
    }, configOverrides || {});
    let sync = new ClientSync('bitcoin', 'mainnet', withDbMixins(db), applier, rb, hv, config, util);
    return { sync, db, applier, rb, hv, util, config };
}

let sync, db, applier;

function setupSmallBranchTest(){
    sinon.stub(console, 'log');
    sinon.stub(console, 'error');
}

function makeCommitSync(){
    let made = makeSync({ VERIFY_STATE_COMMITMENT: true, VERIFY_RECOMPUTE: false, VERIFY_STATE_HASH: false });
    made.sync.lastAppliedBlock = 100;
    // The replica's OWN flag-day map says the commitment is live at this height.
    made.applier._lastComputedRoots = {
        balances_root: 'b'.repeat(64), block_merkle_root: 'm'.repeat(64), state_root: 's'.repeat(64)
    };
    sinon.stub(made.sync, 'haltOnDivergence').resolves();
    return made;
}
describe('ClientSync: small branches', function(){
    beforeEach(setupSmallBranchTest);
    afterEach(function(){ sinon.restore(); });
it('handleBlock (decoder): logs gap and triggers catch-up when blockIndex > lastApplied+1', async function(){
        ({ sync, db, applier } = makeSync({}, { dbType: 'decoder' }));
        sync.lastAppliedBlock = 10;
        sync.lastHashes = { block_hash: 'h10' };
        sinon.stub(sync, 'incrementalCatchUp').resolves();

        // blockIndex=13, gap of 2
        await sync.handleBlock({ type: 'block', block_index: 13, block_hash: 'h13' }, 0);

        assert.ok(sync.incrementalCatchUp.calledOnce, 'catch-up must be triggered on decoder gap');
        assert.strictEqual(applier.applyBlock.called, false);
    });

    it('handleBlock: STRICT mode rejects on timeout and records the strict block (M-22)', async function(){
        ({ sync, db, applier } = makeSync({
            SYNC_SOURCES: 'http://src1:3006,http://src2:3006',
            VERIFY_HASHES: true,
            HASH_CONFIRM_TIMEOUT: 200,
            HASH_CONFIRM_STRICT: true
        }));
        sync.lastAppliedBlock = 100;
        sync.lastHashes = { ledger_hash: 'lh100', actions_hash: 'ah100', contract_hash: 'ch100' };
        sinon.stub(sync.hashVerifier, 'verifyChainContinuity').returns({ valid: true });

        let clock = sinon.useFakeTimers();

        let event = {
            type: 'block', block_index: 101,
            ledger_hash: 'lh101', actions_hash: 'ah101', contract_hash: 'ch101'
        };
        // Source 0 sends, source 1 never sends
        await sync.handleBlock(event, 0);

        assert.strictEqual(applier.applyBlock.called, false, 'must not apply before timeout');

        await clock.tickAsync(300);

        // STRICT mode must not apply single-source on timeout, and must now record the
        // block as strict-pending so the catch-up path cannot re-apply it single-source
        // (M-22). The first source's hash is RETAINED (not deleted) so a later delivery
        // from the second source can still complete the pair and apply via the confirmed
        // path.
        assert.strictEqual(applier.applyBlock.called, false, 'STRICT mode must not apply on timeout');
        assert.strictEqual(sync.pendingHashes.has(101), true, 'pending hash retained for later cross-source confirmation');
        assert.strictEqual(sync._strictConfirmPending.has(101), true, 'block recorded as awaiting strict confirmation');

        let errCalls = console.error.getCalls().map(c => c.args[0]);
        assert.ok(errCalls.some(m => m && m.indexOf('STRICT') !== -1), 'STRICT log must appear');

        clock.restore();
    });
});

describe('ClientSync: small branches', function(){
    beforeEach(setupSmallBranchTest);
    afterEach(function(){ sinon.restore(); });


    it('handleBlock: HALT_ON_DIVERGENCE=true calls haltOnDivergence on hash mismatch', async function(){
        ({ sync, db, applier } = makeSync({
            SYNC_SOURCES: 'http://src1:3006,http://src2:3006',
            VERIFY_HASHES: true,
            HALT_ON_DIVERGENCE: true,
            HASH_CONFIRM_TIMEOUT: 5000
        }));
        sync.lastAppliedBlock = 100;
        sync.lastHashes = { ledger_hash: 'lh100', actions_hash: 'ah100', contract_hash: 'ch100' };
        sinon.stub(sync.hashVerifier, 'verifyChainContinuity').returns({ valid: true });
        sinon.stub(sync, 'haltOnDivergence').resolves();

        let event0 = {
            type: 'block', block_index: 101,
            ledger_hash: 'lhA', actions_hash: 'ahA', contract_hash: 'chA'
        };
        let event1 = {
            type: 'block', block_index: 101,
            ledger_hash: 'lhB', actions_hash: 'ahB', contract_hash: 'chB'
        };

        await sync.handleBlock(event0, 0);
        await sync.handleBlock(event1, 1);

        assert.ok(sync.haltOnDivergence.calledOnce, 'haltOnDivergence must be called on hash mismatch');
        assert.strictEqual(applier.applyBlock.called, false, 'must not apply contested blocks');
    });

    it('applyBlockEvent sets lastHashes to {block_hash} for decoder dbType', async function(){
        ({ sync, db, applier } = makeSync({}, { dbType: 'decoder' }));
        sync.lastAppliedBlock = null;

        await sync.applyBlockEvent({
            block_index: 5,
            block_hash:  'bh5',
            ledger_hash: 'lh5',
            actions_hash: 'ah5',
            contract_hash: 'ch5'
        });

        assert.deepStrictEqual(sync.lastHashes, { block_hash: 'bh5' });
    });
});

describe('ClientSync: small branches', function(){
    beforeEach(setupSmallBranchTest);
    afterEach(function(){ sinon.restore(); });


    it('handleReorg: exceeds MAX_ROLLBACK_DEPTH → HALTS and does NOT rollback', async function(){
        let rb;
        ({ sync, db, applier, rb } = makeSync({ MAX_ROLLBACK_DEPTH: 5 }));
        sync.lastAppliedBlock = 100;

        // rb.rollback is already a sinon stub (from createMockRollback)
        let rollbackStub = rb.rollback;

        // Reorg to block 10 → depth = 100 - 10 + 1 = 91 > 5
        await sync.handleReorg({ type: 'reorg', block_index: 10 });

        assert.strictEqual(rollbackStub.called, false, 'rollback must NOT be called when depth exceeded');
        // Fail closed: record a durable halt rather than returning and serving the fork.
        assert.ok(sync.isHalted(), 'must halt durably when reorg depth exceeds the ceiling');
        assert.strictEqual(sync.getHaltInfo().reason, 'max-rollback-depth-exceeded');
        assert.strictEqual(db.recordHalt.calledOnce, true, 'durable halt must be persisted');
    });

    // The decoder track has no VERIFY_RECOMPUTE/VERIFY_STATE_HASH net, so the
    // max-depth halt is its only guard against permanently serving the fork.
    it('handleReorg: decoder track also HALTS on max-depth exceed', async function(){
        let rb;
        ({ sync, db, applier, rb } = makeSync({ MAX_ROLLBACK_DEPTH: 5 }, { dbType: 'decoder' }));
        sync.lastAppliedBlock = 100;

        await sync.handleReorg({ type: 'reorg', block_index: 10 }); // depth = 91 > 5

        assert.strictEqual(rb.rollback.called, false);
        assert.ok(sync.isHalted(), 'decoder must halt durably; it has no recompute fallback');
        assert.strictEqual(sync.getHaltInfo().reason, 'max-rollback-depth-exceeded');
        assert.strictEqual(db.recordHalt.firstCall.args[0], 'decoder');
    });

    // Stress-sweep 2026-07-08: a reorg ABOVE the tip must be a no-op, never a cursor
    // advance. depth = tip - block_index + 1 goes <= 0 above the tip, so it never trips
    // MAX_ROLLBACK_DEPTH; the old code then ran a no-op rollback and set
    // lastAppliedBlock = block_index - 1, inflating the tip past real data and wedging
    // the replica (every later canonical block is then dropped by handleBlock).
    it('handleReorg: target ABOVE the tip is ignored (no rollback, no cursor advance)', async function(){
        let rb;
        ({ sync, db, applier, rb } = makeSync());
        sync.lastAppliedBlock = 100;

        await sync.handleReorg({ type: 'reorg', block_index: 105 }); // above tip

        assert.strictEqual(rb.rollback.called, false, 'must not roll back an above-tip target');
        assert.strictEqual(sync.isHalted(), false, 'an above-tip reorg is a no-op, not a halt');
        assert.strictEqual(sync.lastAppliedBlock, 100, 'the cursor must NOT advance past applied data');
    });
});

describe('ClientSync: small branches', function(){
    beforeEach(setupSmallBranchTest);
    afterEach(function(){ sinon.restore(); });


    it('handleReorg: a legitimate below-tip reorg still rolls back and moves the cursor', async function(){
        let rb;
        ({ sync, db, applier, rb } = makeSync()); // MAX_ROLLBACK_DEPTH default 10
        sync.lastAppliedBlock = 100;

        await sync.handleReorg({ type: 'reorg', block_index: 95 }); // depth = 6 <= 10

        assert.strictEqual(rb.rollback.calledOnceWith(95), true, 'below-tip reorg must roll back to the target');
        assert.strictEqual(sync.isHalted(), false);
        assert.strictEqual(sync.lastAppliedBlock, 94, 'cursor moves to block_index - 1 after a real rollback');
    });
});

describe('ClientSync: small branches', function(){
    beforeEach(setupSmallBranchTest);
    afterEach(function(){ sinon.restore(); });


    // uuid:4b95ddef. The apply-time state-commitment check is the only control that
    // writes a sync_halt marker for a replica/source root divergence. Gating it on the
    // WIRE carrying balances_root lets a source with no state_tree_roots row at an
    // ACTIVE height (failed compute, re-seed, or a hostile peer) serve nulls and skip
    // the whole check silently. The replica decides activation locally:
    // ClientApplier sets _lastComputedRoots only under isStateCommitmentActive and
    // clears it at applyBlock entry, so a non-null value plus a null wire root is a
    // locally-decidable contract violation, not "nothing to check".
    describe('applyBlockEvent: state-commitment roots withheld at an active height', function(){
it('HALTS with state-commitment-missing when the source omits balances_root', async function(){
            let { sync } = makeCommitSync();

            await sync.applyBlockEvent({
                type: 'block', block_index: 101,
                balances_root: null, block_merkle_root: 'm'.repeat(64), state_root: 's'.repeat(64)
            });

            assert.ok(sync.haltOnDivergence.calledOnce, 'a withheld balances_root must halt');
            assert.strictEqual(sync.haltOnDivergence.firstCall.args[3], 'state-commitment-missing');
            assert.deepStrictEqual(sync.haltOnDivergence.firstCall.args[1].map(m => m.field), ['balances_root']);
            assert.strictEqual(sync.lastAppliedBlock, 100, 'a halted block must not advance the tip');
        });

        it('HALTS when only block_merkle_root is withheld (a correct balances_root is not a pass)', async function(){
            let { sync } = makeCommitSync();

            await sync.applyBlockEvent({
                type: 'block', block_index: 101,
                balances_root: 'b'.repeat(64), block_merkle_root: null, state_root: 's'.repeat(64)
            });

            assert.ok(sync.haltOnDivergence.calledOnce, 'a withheld block_merkle_root must halt');
            assert.deepStrictEqual(sync.haltOnDivergence.firstCall.args[1].map(m => m.field), ['block_merkle_root']);
        });

        it('does NOT halt on a null state_root: ServerPoller nulls it for catch-up-burst blocks', async function(){
            let { sync } = makeCommitSync();

            await sync.applyBlockEvent({
                type: 'block', block_index: 101,
                balances_root: 'b'.repeat(64), block_merkle_root: 'm'.repeat(64), state_root: null
            });

            assert.strictEqual(sync.haltOnDivergence.called, false, 'a burst-null state_root is deliberate');
            assert.strictEqual(sync.lastAppliedBlock, 101, 'the block still applies');
        });
    });
});

describe('ClientSync: small branches', function(){
    beforeEach(setupSmallBranchTest);
    afterEach(function(){ sinon.restore(); });


    describe('applyBlockEvent: state-commitment roots withheld at an active height', function(){


        it('does NOT halt when the replica itself says the commitment is not active', async function(){
            let { sync, applier } = makeCommitSync();
            applier._lastComputedRoots = null;   // pre-flag-day, or a skipped/duplicate apply

            await sync.applyBlockEvent({
                type: 'block', block_index: 101,
                balances_root: null, block_merkle_root: null, state_root: null
            });

            assert.strictEqual(sync.haltOnDivergence.called, false, 'pre-flag-day nulls stay a skip');
            assert.strictEqual(sync.lastAppliedBlock, 101);
        });

        it('still halts with state-commitment-divergence on a root MISMATCH', async function(){
            let { sync } = makeCommitSync();

            await sync.applyBlockEvent({
                type: 'block', block_index: 101,
                balances_root: 'f'.repeat(64), block_merkle_root: 'm'.repeat(64), state_root: 's'.repeat(64)
            });

            assert.ok(sync.haltOnDivergence.calledOnce);
            assert.strictEqual(sync.haltOnDivergence.firstCall.args[3], 'state-commitment-divergence');
        });
    });
});

describe('ClientSync: small branches', function(){
    beforeEach(setupSmallBranchTest);
    afterEach(function(){ sinon.restore(); });


    describe('applyBlockEvent: state-commitment roots withheld at an active height', function(){


        // A post-commit verification ERROR must not launder the commitment gate.
        // applyBlock commits before the gates run, so a throw from the state-hash read
        // leaves the block committed with lastAppliedBlock unadvanced. The redelivery
        // gap detection then triggers takes applyBlock's duplicate early return, which
        // clears _lastComputedRoots, and the commitment comparison is gated on those
        // roots: without the carried stash the retry advances the tip with divergent
        // roots never compared. Database presence is not proof of verification.
        it('a state-hash read failure then redelivery still compares the commitment roots', async function(){
            let { sync, applier } = makeCommitSync();
            sync.config['VERIFY_STATE_HASH'] = true;
            let readStub = sinon.stub(sync.blockHasher, 'computeStateHash')
                .rejects(new Error('replica read failed'));

            // DIVERGENT balances_root: the comparison, once it runs, must halt.
            let event = { type: 'block', block_index: 101, state_hash: 'sh'.repeat(32),
                balances_root: 'f'.repeat(64), block_merkle_root: 'm'.repeat(64), state_root: 's'.repeat(64) };

            await sync.applyBlockEvent(event);
            assert.strictEqual(sync.haltOnDivergence.called, false,
                'the swallowed read error itself does not halt');
            assert.strictEqual(sync.lastAppliedBlock, 100, 'and does not advance the tip');

            // The redelivery: applyBlock early-returns on the duplicate, leaving null roots.
            applier._lastComputedRoots = null;
            readStub.resolves(event.state_hash);   // the transient read has recovered

            await sync.applyBlockEvent(event);

            assert.ok(sync.haltOnDivergence.calledOnce,
                'the retry must run the commitment comparison it skipped the first time');
            assert.strictEqual(sync.haltOnDivergence.firstCall.args[3], 'state-commitment-divergence');
            assert.strictEqual(sync.lastAppliedBlock, 100,
                'an unverified block must never advance the tip');
        });
    });
});

describe('ClientSync: small branches', function(){
    beforeEach(setupSmallBranchTest);
    afterEach(function(){ sinon.restore(); });


    describe('applyBlockEvent: state-commitment roots withheld at an active height', function(){


        it('clears the carried roots once every gate passes, so a later duplicate stays a skip', async function(){
            // Negative control for the test above: the stash must not outlive a clean
            // apply, or an honest duplicate of an already-verified block would be
            // re-compared against roots the gate has no business re-checking.
            let { sync, applier } = makeCommitSync();

            await sync.applyBlockEvent({ type: 'block', block_index: 101,
                balances_root: 'b'.repeat(64), block_merkle_root: 'm'.repeat(64), state_root: 's'.repeat(64) });
            assert.strictEqual(sync.lastAppliedBlock, 101, 'the clean block applied');
            assert.strictEqual(sync._unverifiedRoots, null, 'the stash is cleared once verified');

            applier._lastComputedRoots = null;
            await sync.applyBlockEvent({ type: 'block', block_index: 101,
                balances_root: 'f'.repeat(64), block_merkle_root: 'm'.repeat(64), state_root: 's'.repeat(64) });
            assert.strictEqual(sync.haltOnDivergence.called, false,
                'a duplicate of an already-verified block is still a skip, not a divergence');
        });
    });
});
