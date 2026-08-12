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
 * ClientSync: multi-source Byzantine M-of-N quorum.
 *
 * The live cross-source path applies a block once SOURCE_QUORUM active sources
 * publish the SAME ledger/actions/contract hash tuple, strikes (and eventually
 * evicts) dissenters instead of halting on any disagreement, and halts
 * (no-source-quorum) only when every active source has reported and no group
 * reaches quorum. The 2-source case behaves exactly as the prior pairwise path
 * (quorum 2; a 1-1 split has no majority and halts); a larger set tolerates a
 * Byzantine minority and evicts a persistent liar.
 ********************************************************************/

const assert = require('assert');
const sinon  = require('sinon');
const axios  = require('axios');
const ClientSync   = require('../../src/ClientSync');
const Utility      = require('../../src/utility');
const HashVerifier = require('../../src/HashVerifier');

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
function createMockApplier(){
    return { applyBlock: sinon.stub().resolves(), applyFullSnapshot: sinon.stub().resolves() };
}
// A block event carrying a given hash tuple. Same-hash events agree; different h => dissent.
function blockEvent(blockIndex, h){
    return { type: 'block', block_index: blockIndex,
        ledger_hash: 'lh' + h, actions_hash: 'ah' + h, contract_hash: 'ch' + h };
}

function makeSync(sourcesCsv, extraConfig){
    let db = createMockDb();
    let applier = createMockApplier();
    let config = Object.assign({
        SYNC_SOURCES: sourcesCsv, VERIFY_HASHES: true, HALT_ON_DIVERGENCE: true,
        HASH_CONFIRM_TIMEOUT: 5000, CLIENT_RECONNECT_DELAY: 5000,
        // Downstream apply-time verification gates (recompute / state-hash / state-
        // commitment) are exercised in their own suites and need real DB fixtures; off
        // here so these tests isolate the cross-source QUORUM decision from apply-time
        // recompute. The quorum logic runs entirely before _applyBlockEvent.
        VERIFY_RECOMPUTE: false, VERIFY_STATE_HASH: false, VERIFY_STATE_COMMITMENT: false
    }, extraConfig || {});
    let sync = new ClientSync('bitcoin', 'mainnet', db, applier,
        { rollback: sinon.stub().resolves() }, new HashVerifier(), config, new Utility());
    sync.lastAppliedBlock = 100;
    sync.lastHashes = { ledger_hash: 'lhX', actions_hash: 'ahX', contract_hash: 'chX' };
    return { sync, db, applier };
}

describe('ClientSync: multi-source Byzantine quorum @regression', function(){
    beforeEach(function(){ sinon.stub(console, 'log'); sinon.stub(console, 'warn'); sinon.stub(console, 'error'); });
    afterEach(function(){ sinon.restore(); });

    describe('effective quorum default (simple majority)', function(){
        it('N=1 -> quorum 1 (single-source posture)', function(){
            let { sync } = makeSync('http://a:3006');
            assert.strictEqual(sync.sourceQuorum, 1);
        });
        it('N=2 -> quorum 2 (both sources; a 1-1 split halts)', function(){
            let { sync } = makeSync('http://a:3006,http://b:3006');
            assert.strictEqual(sync.sourceQuorum, 2);
        });
        it('N=3 -> quorum 2 (majority)', function(){
            let { sync } = makeSync('http://a:3006,http://b:3006,http://c:3006');
            assert.strictEqual(sync.sourceQuorum, 2);
        });
        it('N=4 -> quorum 3 (=2f+1, tolerates f=1)', function(){
            let { sync } = makeSync('http://a:3006,http://b:3006,http://c:3006,http://d:3006');
            assert.strictEqual(sync.sourceQuorum, 3);
        });
        it('an explicit SOURCE_QUORUM is clamped to [1, N]', function(){
            assert.strictEqual(makeSync('http://a:3006,http://b:3006,http://c:3006', { SOURCE_QUORUM: 3 }).sync.sourceQuorum, 3);
            assert.strictEqual(makeSync('http://a:3006,http://b:3006,http://c:3006', { SOURCE_QUORUM: 9 }).sync.sourceQuorum, 3);
            assert.strictEqual(makeSync('http://a:3006,http://b:3006,http://c:3006', { SOURCE_QUORUM: 1 }).sync.sourceQuorum, 1);
        });
    });

    describe('3-source majority applies', function(){
        it('applies the block once 2 of 3 sources agree, without waiting for the 3rd', async function(){
            let { sync, applier } = makeSync('http://a:3006,http://b:3006,http://c:3006');
            await sync._handleBlock(blockEvent(101, 'X'), 0); // A: X
            assert.strictEqual(applier.applyBlock.called, false, 'one report is below quorum');
            await sync._handleBlock(blockEvent(101, 'X'), 1); // B: X -> quorum 2 reached
            assert.strictEqual(applier.applyBlock.calledOnce, true, '2 of 3 agreeing applies');
            assert.strictEqual(sync.getSourcesAgreeing(), 2);
            assert.strictEqual(sync.isHalted(), false);
        });

        it('strikes the dissenting minority source when the majority applies', async function(){
            let { sync, applier } = makeSync('http://a:3006,http://b:3006,http://c:3006');
            await sync._handleBlock(blockEvent(101, 'X'), 0); // A: X
            await sync._handleBlock(blockEvent(101, 'Y'), 2); // C: Y (dissent, no quorum yet)
            assert.strictEqual(applier.applyBlock.called, false);
            await sync._handleBlock(blockEvent(101, 'X'), 1); // B: X -> quorum, C struck
            assert.strictEqual(applier.applyBlock.calledOnce, true);
            assert.deepStrictEqual(sync._sourceStrikes.get(2), [101], 'the dissenter C accrued one strike');
            assert.strictEqual((sync._sourceStrikes.get(0) || []).length, 0, 'majority sources are not struck');
        });
    });

    describe('4-source: one Byzantine source is struck then evicted', function(){
        it('evicts the persistent dissenter after SOURCE_EVICT_THRESHOLD strikes, preserving liveness', async function(){
            let { sync, applier } = makeSync(
                'http://a:3006,http://b:3006,http://c:3006,http://d:3006',
                { SOURCE_EVICT_THRESHOLD: 3, SOURCE_STRIKE_WINDOW: 100 });
            // Quorum is 3. Honest A,B,C agree on X every block; Byzantine D always says Y.
            for(let blk = 101; blk <= 103; blk++){
                sync.lastAppliedBlock = blk - 1;
                await sync._handleBlock(blockEvent(blk, 'X'), 0); // A
                await sync._handleBlock(blockEvent(blk, 'Y'), 3); // D dissents
                await sync._handleBlock(blockEvent(blk, 'X'), 1); // B -> quorum 3? no, 2 so far
                await sync._handleBlock(blockEvent(blk, 'X'), 2); // C -> quorum 3 reached, D struck
            }
            assert.strictEqual(applier.applyBlock.callCount, 3, 'every block still applied (liveness preserved)');
            assert.ok(sync.getEvictedSources().includes('http://d:3006'), 'the Byzantine source D was evicted');
            assert.strictEqual(sync.getActiveSourceCount(), 3, 'active denominator dropped to 3');
            assert.strictEqual(sync.isHalted(), false, 'a Byzantine minority never halts the honest quorum');
        });

        it('does not reconnect an evicted source', function(){
            let { sync } = makeSync('http://a:3006,http://b:3006,http://c:3006,http://d:3006');
            sync.running = true;
            sync._evictedSources.add(3);
            let clock = sinon.useFakeTimers();
            let connect = sinon.stub(sync, '_connectWebSocket');
            sync._scheduleReconnect('http://d:3006', 3);
            clock.tick(sync.config.CLIENT_RECONNECT_DELAY + 100);
            assert.strictEqual(connect.called, false, 'evicted source is not reconnected');
            clock.restore();
        });
    });

    describe('no-source-quorum halt', function(){
        it('2-source tie (1-1 split, no majority) halts with reason no-source-quorum', async function(){
            let { sync, applier, db } = makeSync('http://a:3006,http://b:3006');
            await sync._handleBlock(blockEvent(101, 'X'), 0); // A: X
            await sync._handleBlock(blockEvent(101, 'Y'), 1); // B: Y -> all reported, no majority
            assert.strictEqual(applier.applyBlock.called, false, 'a contested block is never applied');
            assert.strictEqual(sync.isHalted(), true);
            assert.strictEqual(sync.getHaltInfo().reason, 'no-source-quorum');
            assert.ok(db.recordHalt.calledOnce, 'the halt is persisted durably');
        });

        it('4-source split with no majority (2-2) halts no-source-quorum', async function(){
            let { sync, applier } = makeSync('http://a:3006,http://b:3006,http://c:3006,http://d:3006');
            await sync._handleBlock(blockEvent(101, 'X'), 0);
            await sync._handleBlock(blockEvent(101, 'X'), 1); // X:[A,B] = 2 < quorum 3
            await sync._handleBlock(blockEvent(101, 'Y'), 2);
            await sync._handleBlock(blockEvent(101, 'Y'), 3); // all reported, best group 2 < 3
            assert.strictEqual(applier.applyBlock.called, false);
            assert.strictEqual(sync.isHalted(), true);
            assert.strictEqual(sync.getHaltInfo().reason, 'no-source-quorum');
        });

        it('log-only mode (HALT_ON_DIVERGENCE=false) refuses to apply but does not halt', async function(){
            let { sync, applier } = makeSync('http://a:3006,http://b:3006', { HALT_ON_DIVERGENCE: false });
            await sync._handleBlock(blockEvent(101, 'X'), 0);
            await sync._handleBlock(blockEvent(101, 'Y'), 1);
            assert.strictEqual(applier.applyBlock.called, false, 'contested block not applied in log-only mode');
            assert.strictEqual(sync.isHalted(), false, 'log-only mode does not halt');
        });
    });

    describe('backward-compatible 2-source behavior', function(){
        it('applies when both sources agree (unchanged from the pairwise path)', async function(){
            let { sync, applier } = makeSync('http://a:3006,http://b:3006');
            await sync._handleBlock(blockEvent(101, 'X'), 0);
            assert.strictEqual(applier.applyBlock.called, false);
            await sync._handleBlock(blockEvent(101, 'X'), 1);
            assert.strictEqual(applier.applyBlock.calledOnce, true);
        });

        it('single source applies immediately (quorum 1)', async function(){
            let { sync, applier } = makeSync('http://a:3006');
            await sync._handleBlock(blockEvent(101, 'X'), 0);
            assert.strictEqual(applier.applyBlock.calledOnce, true);
        });

        it('applies from primary on quorum timeout when the second source is silent', async function(){
            let clock = sinon.useFakeTimers();
            let { sync, applier } = makeSync('http://a:3006,http://b:3006');
            await sync._handleBlock(blockEvent(101, 'X'), 0);
            assert.strictEqual(applier.applyBlock.called, false);
            await clock.tickAsync(sync.config.HASH_CONFIRM_TIMEOUT + 100);
            assert.strictEqual(applier.applyBlock.calledOnce, true, 'liveness fallback applies from primary');
            clock.restore();
        });
    });

    describe('eviction guard (never below 2 active sources)', function(){
        it('does not evict a 3rd source down to a single-source posture', async function(){
            // N=3: evicting 1 leaves 2 (allowed). A second eviction would leave 1: suppressed.
            let { sync } = makeSync('http://a:3006,http://b:3006,http://c:3006',
                { SOURCE_EVICT_THRESHOLD: 1, SOURCE_STRIKE_WINDOW: 100 });
            sync._strikeSource(2, 101); // C evicted (2 active remain)
            assert.ok(sync._evictedSources.has(2));
            assert.strictEqual(sync.getActiveSourceCount(), 2);
            sync._strikeSource(1, 102); // would drop to 1 active: suppressed
            assert.strictEqual(sync._evictedSources.has(1), false, 'never evict below 2 active sources');
            assert.strictEqual(sync.getActiveSourceCount(), 2);
        });
    });

    describe('strike sliding window', function(){
        it('prunes strikes older than SOURCE_STRIKE_WINDOW so stale strikes do not evict', function(){
            let { sync } = makeSync('http://a:3006,http://b:3006,http://c:3006,http://d:3006',
                { SOURCE_EVICT_THRESHOLD: 3, SOURCE_STRIKE_WINDOW: 10 });
            sync._strikeSource(3, 100);
            sync._strikeSource(3, 101);
            // A strike 20 blocks later prunes the two old ones (outside the 10-block window).
            sync._strikeSource(3, 121);
            assert.deepStrictEqual(sync._sourceStrikes.get(3), [121], 'old strikes pruned');
            assert.strictEqual(sync._evictedSources.has(3), false, 'not evicted on stale strikes');
        });
    });

    describe('bootstrap quorum cross-check', function(){
        // Drive _bootstrapRotateSources with the snapshot fetch/apply stubbed out so the
        // test isolates the multi-source verify loop. sources[0] supplied the snapshot
        // (1 vote); the loop must seek SOURCE_QUORUM-1 additional agreeing sources.
        function stubBootstrap(sync, applier){
            sinon.stub(sync, '_fetchAndApplySchema').resolves();
            sinon.stub(sync, '_clearBootstrapBase').resolves();
            sinon.stub(axios, 'get').resolves({ data: Buffer.from(JSON.stringify({ block_height: 500 })) });
            applier.applyFullSnapshot = sinon.stub().resolves();
        }

        it('3 sources (quorum 2): stops after ONE agreeing secondary', async function(){
            let { sync, applier } = makeSync('http://a:3006,http://b:3006,http://c:3006');
            stubBootstrap(sync, applier);
            let verify = sinon.stub(sync, '_verifyAgainstSource').resolves('agree');
            let ok = await sync._bootstrapRotateSources();
            assert.strictEqual(ok, true);
            assert.strictEqual(verify.callCount, 1, 'one agreeing secondary reaches quorum 2');
            assert.strictEqual(verify.firstCall.args[0], 'http://b:3006');
        });

        it('4 sources (quorum 3): needs TWO agreeing secondaries', async function(){
            let { sync, applier } = makeSync('http://a:3006,http://b:3006,http://c:3006,http://d:3006');
            stubBootstrap(sync, applier);
            let verify = sinon.stub(sync, '_verifyAgainstSource').resolves('agree');
            await sync._bootstrapRotateSources();
            assert.strictEqual(verify.callCount, 2, 'two agreeing secondaries reach quorum 3');
        });

        it('skips an unreachable secondary and counts the next agreeing one toward quorum', async function(){
            let { sync, applier } = makeSync('http://a:3006,http://b:3006,http://c:3006');
            stubBootstrap(sync, applier);
            let verify = sinon.stub(sync, '_verifyAgainstSource');
            verify.onCall(0).resolves('unreachable'); // sources[1] down
            verify.onCall(1).resolves('agree');        // sources[2] agrees
            await sync._bootstrapRotateSources();
            assert.strictEqual(verify.callCount, 2, 'falls through the unreachable source to the next');
        });

        it('a divergent secondary halts the bootstrap (returns false)', async function(){
            let { sync, applier } = makeSync('http://a:3006,http://b:3006,http://c:3006');
            stubBootstrap(sync, applier);
            sinon.stub(sync, '_verifyAgainstSource').callsFake(async () => {
                sync._halted = { blockIndex: 500, reason: 'cross-source-divergence' };
                return 'halted';
            });
            let ok = await sync._bootstrapRotateSources();
            assert.strictEqual(ok, false, 'a divergence during bootstrap cross-check halts the round');
            assert.strictEqual(sync.isHalted(), true);
        });
    });

    describe('checkpoint freshness strict', function(){
        const ENVKEY = 'CHECKPOINT_VALIDATORS_BITCOIN_MAINNET';
        afterEach(function(){ delete process.env[ENVKEY]; });

        function pinAndFetch(sync, cp){
            process.env[ENVKEY] = JSON.stringify([{ pubkey: 'ab'.repeat(32), weight: '100', source: 'ab'.repeat(32) }]);
            sinon.stub(axios, 'get').resolves({ data: cp });
        }

        it('HALTS (checkpoint-freshness-stale) when the anchor trails the tip past the bound and strict is on', async function(){
            let { sync } = makeSync('http://a:3006', { CHECKPOINT_FRESHNESS_STRICT: true,
                CHECKPOINT_FRESHNESS_BLOCKS: 500, CHECKPOINT_VERIFY_INTERVAL: 1 });
            sync.lastAppliedBlock = 1000;
            sync._lastVerifiedCheckpointSeq = 4; // already anchored once -> strict is enforced
            pinAndFetch(sync, { block_index: 100, state_root: 'aa'.repeat(32), checkpoint_seq: 5 });
            await sync._verifyCheckpointQuorum();
            assert.strictEqual(sync.isHalted(), true);
            assert.strictEqual(sync.getHaltInfo().reason, 'checkpoint-freshness-stale');
        });

        // The cp here is unsigned, so it fails the downstream quorum verify and halts
        // as checkpoint-quorum-divergence; the freshness gate must NOT be what halts.
        // (A fully-signed happy-path checkpoint is covered in ClientSync.checkpointQuorum.test.js.)
        it('freshness gate does NOT fire when strict is on but no checkpoint has ever been anchored', async function(){
            let { sync } = makeSync('http://a:3006', { CHECKPOINT_FRESHNESS_STRICT: true,
                CHECKPOINT_FRESHNESS_BLOCKS: 500, CHECKPOINT_VERIFY_INTERVAL: 1 });
            sync.lastAppliedBlock = 1000;
            sync._lastVerifiedCheckpointSeq = null; // never anchored -> not enforced at startup
            pinAndFetch(sync, { block_index: 100, state_root: 'aa'.repeat(32), checkpoint_seq: 5 });
            await sync._verifyCheckpointQuorum();
            assert.notStrictEqual((sync.getHaltInfo() || {}).reason, 'checkpoint-freshness-stale',
                'a replica that never anchored is not halted on freshness');
        });

        it('freshness gate does NOT fire when strict is OFF even if the anchor is stale (default advisory)', async function(){
            let { sync } = makeSync('http://a:3006', { CHECKPOINT_FRESHNESS_STRICT: false,
                CHECKPOINT_FRESHNESS_BLOCKS: 500, CHECKPOINT_VERIFY_INTERVAL: 1 });
            sync.lastAppliedBlock = 1000;
            sync._lastVerifiedCheckpointSeq = 4;
            pinAndFetch(sync, { block_index: 100, state_root: 'aa'.repeat(32), checkpoint_seq: 5 });
            await sync._verifyCheckpointQuorum();
            assert.notStrictEqual((sync.getHaltInfo() || {}).reason, 'checkpoint-freshness-stale',
                'default freshness posture is advisory, never halts on freshness');
        });
    });
});
