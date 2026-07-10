// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

const assert = require('assert');
const sinon  = require('sinon');
const axios  = require('axios');
const ClientSync = require('../../src/ClientSync');
const Utility = require('../../src/utility');
const HashVerifier = require('../../src/HashVerifier');

function createMockDb(){
    return {
        dbName: 'test_db',
        getLastBlock: sinon.stub().resolves(null),
        getBlockHashRow: sinon.stub().resolves(null),
        // start() reads the durable halt table first; a clean (no-halt) read lets the
        // normal bootstrap/catch-up flow proceed. The halt check now fails CLOSED, so
        // this must resolve rather than be absent (an absent/erroring read holds idle).
        getActiveHalt: sinon.stub().resolves(null),
        doQuery: sinon.stub().resolves([])
    };
}

function createMockApplier(){
    return {
        applyBlock: sinon.stub().resolves(),
        applyFullSnapshot: sinon.stub().resolves(),
        applyIncrementalSnapshot: sinon.stub().resolves()
    };
}

function createMockRollback(){
    return {
        rollback: sinon.stub().resolves()
    };
}

describe('ClientSync', function(){

    let sync, db, applier, rollback, hashVerifier, config, util;

    beforeEach(function(){
        db = createMockDb();
        applier = createMockApplier();
        rollback = createMockRollback();
        hashVerifier = new HashVerifier();
        config = {
            SYNC_SOURCES: 'http://source1:3006,http://source2:3006',
            VERIFY_HASHES: true,
            CLIENT_RECONNECT_DELAY: 5000,
            HASH_CONFIRM_TIMEOUT: 5000
        };
        util = new Utility();
        sync = new ClientSync('bitcoin', 'mainnet', db, applier, rollback, hashVerifier, config, util);
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });

    afterEach(function(){
        sinon.restore();
    });

    describe('constructor', function(){
        it('parses SYNC_SOURCES into array', function(){
            assert.strictEqual(sync.sources.length, 2);
            assert.strictEqual(sync.sources[0], 'http://source1:3006');
            assert.strictEqual(sync.sources[1], 'http://source2:3006');
        });

        it('handles empty SYNC_SOURCES', function(){
            config.SYNC_SOURCES = '';
            let s = new ClientSync('bitcoin', 'mainnet', db, applier, rollback, hashVerifier, config, util);
            assert.strictEqual(s.sources.length, 0);
        });

        it('trims whitespace from sources', function(){
            config.SYNC_SOURCES = ' http://a:3006 , http://b:3006 ';
            let s = new ClientSync('bitcoin', 'mainnet', db, applier, rollback, hashVerifier, config, util);
            assert.strictEqual(s.sources[0], 'http://a:3006');
            assert.strictEqual(s.sources[1], 'http://b:3006');
        });
    });

    describe('_warnTrustPosture', function(){
        it('warns when running single-source (no cross-source rejection)', function(){
            config.SYNC_SOURCES = 'http://only-source:3006';
            let s = new ClientSync('bitcoin', 'mainnet', db, applier, rollback, hashVerifier, config, util);
            let warn = sinon.stub(console, 'warn');
            s._warnTrustPosture();
            assert.ok(warn.getCalls().some(c => /SINGLE-SOURCE/.test(c.args[0])));
        });

        it('does not warn about single-source with 2+ sources', function(){
            // default config has two sources
            let warn = sinon.stub(console, 'warn');
            sync._warnTrustPosture();
            assert.ok(!warn.getCalls().some(c => /SINGLE-SOURCE/.test(c.args[0])));
        });

        it('warns that the decoder path has no hash rejection', function(){
            let decoderDb = createMockDb(); decoderDb.dbType = 'decoder';
            let s = new ClientSync('bitcoin', 'mainnet', decoderDb, applier, rollback, hashVerifier, config, util);
            let warn = sinon.stub(console, 'warn');
            s._warnTrustPosture();
            assert.ok(warn.getCalls().some(c => /decoder replication has no hash-based rejection/.test(c.args[0])));
        });

        it('indexer with 2+ sources emits no trust warnings', function(){
            let warn = sinon.stub(console, 'warn');
            sync._warnTrustPosture();
            assert.strictEqual(warn.callCount, 0);
        });
    });

    describe('isSourceHeightStale', function(){
        // lastKnownServerBlock only advances on live WS events, so after a silent
        // disconnect it freezes and lag_blocks settles to 0 once the replica catches
        // up to the stale tip. isSourceHeightStale exposes that the live signal has
        // gone quiet so /status can flag the lag figure as computed against a stale tip.
        let clock;
        beforeEach(function(){
            clock = sinon.useFakeTimers();
            sync.config.CLIENT_SOURCE_STALE_MS = 180000;
        });
        afterEach(function(){
            clock.restore();
        });

        it('returns null before any WS event is seen', function(){
            assert.strictEqual(sync.isSourceHeightStale(), null);
        });

        it('returns false immediately after an event', function(){
            sync._lastWsEventAt = Date.now();
            assert.strictEqual(sync.isSourceHeightStale(), false);
        });

        it('stays fresh within the staleness window', function(){
            sync._lastWsEventAt = Date.now();
            clock.tick(179000);
            assert.strictEqual(sync.isSourceHeightStale(), false);
        });

        it('reports stale once the window elapses with no new event', function(){
            sync._lastWsEventAt = Date.now();
            clock.tick(180001);
            assert.strictEqual(sync.isSourceHeightStale(), true);
        });
    });

    describe('_logGap throttling', function(){
        // On a fast chain (e.g. Dogecoin testnet) the replica trails the tip and
        // would log a gap line per block (thousands/min). _logGap collapses that
        // into one line per window, folding in a suppressed count.
        it('logs the first occurrence immediately', function(){
            sync._gapLogIntervalMs = 30000;
            sync._logGap('gap', 1000);
            assert.strictEqual(console.log.calledOnce, true);
            assert.match(console.log.firstCall.args[0], /^gap/);
        });

        it('suppresses repeats within the window, then emits one summary with the count', function(){
            sync._gapLogIntervalMs = 30000;
            sync._logGap('gap', 1000);           // emits
            sync._logGap('gap', 5000);           // suppressed
            sync._logGap('gap', 10000);          // suppressed
            assert.strictEqual(console.log.callCount, 1);

            sync._logGap('gap', 40000);          // past window → emits summary
            assert.strictEqual(console.log.callCount, 2);
            assert.match(console.log.secondCall.args[0], /\+2 similar/);
        });

        it('resets the suppressed count after emitting a summary', function(){
            sync._gapLogIntervalMs = 30000;
            sync._logGap('gap', 1000);           // emits
            sync._logGap('gap', 5000);           // suppressed (+1)
            sync._logGap('gap', 40000);          // emits with (+1)
            sync._logGap('gap', 80000);          // emits, no leftover count
            assert.strictEqual(console.log.callCount, 3);
            assert.doesNotMatch(console.log.thirdCall.args[0], /similar/);
        });
    });

    describe('start', function(){
        it('passes lastAppliedBlock + 1 to incremental catch-up when resuming a populated replica', async function(){
            db.getLastBlock.resolves(100);
            sinon.stub(sync, '_incrementalCatchUp').resolves();
            sinon.stub(sync, '_connectWebSockets').callsFake(() => { sync.running = false; });

            await sync.start();

            assert.strictEqual(sync._incrementalCatchUp.calledOnce, true);
            // Must request the NEXT needed block, not the last already-applied one.
            // The server uses inclusive >= bounds, so passing 100 re-delivers block
            // 100's already-applied rows and the non-ignore INSERT throws on the
            // UNIQUE action_index, rolling back the whole catch-up (silent freeze).
            assert.strictEqual(sync._incrementalCatchUp.firstCall.args[0], 101);
        });

        it('bootstraps from a full snapshot when the replica is empty', async function(){
            db.getLastBlock.resolves(null);
            // A successful bootstrap commits a tip, required now that start() refuses
            // to enter live-follow while lastAppliedBlock is still null.
            sinon.stub(sync, '_bootstrapFromSnapshot').callsFake(async () => { sync.lastAppliedBlock = 10; });
            sinon.stub(sync, '_incrementalCatchUp').resolves();
            sinon.stub(sync, '_connectWebSockets').callsFake(() => { sync.running = false; });

            await sync.start();

            assert.strictEqual(sync._bootstrapFromSnapshot.calledOnce, true);
            assert.strictEqual(sync._incrementalCatchUp.called, false);
            assert.strictEqual(sync._connectWebSockets.calledOnce, true);
        });
    });

    describe('bootstrap-failure gating (empty-replica defect)', function(){
        // The defect: a swallowed bootstrap failure let start() proceed into
        // live-follow with lastAppliedBlock=null, applying the first WS block onto an
        // empty DB with every continuity/fork/duplicate guard (all gated on
        // lastAppliedBlock !== null) disabled: risking a durable halt or silent block loss.

        it('start() refuses live-follow when bootstrap leaves the replica empty', async function(){
            db.getLastBlock.resolves(null);
            // Bootstrap returns without committing a tip (the swallow it used to do).
            sinon.stub(sync, '_bootstrapFromSnapshot').resolves();
            let connect = sinon.stub(sync, '_connectWebSockets');

            await assert.rejects(() => sync.start(), /Refusing to enter live-follow/);
            assert.strictEqual(connect.called, false, 'must not open WebSockets onto an empty replica');
        });

        it('start() propagates a permanent bootstrap failure without live-following', async function(){
            db.getLastBlock.resolves(null);
            sinon.stub(sync, '_bootstrapFromSnapshot').rejects(new Error('all sync sources exhausted'));
            let connect = sinon.stub(sync, '_connectWebSockets');

            await assert.rejects(() => sync.start(), /all sync sources exhausted/);
            assert.strictEqual(connect.called, false);
        });

        it('_handleBlock refuses to apply a non-genesis block onto an empty replica', async function(){
            sync.lastAppliedBlock = null;
            sinon.stub(sync, '_incrementalCatchUp').resolves();

            await sync._handleBlock(
                { type: 'block', block_index: 5, ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c' }, 0);

            assert.strictEqual(applier.applyBlock.called, false, 'must not apply onto an empty DB');
            assert.strictEqual(sync._incrementalCatchUp.calledOnce, true);
        });
    });

    describe('_handleEvent', function(){
        it('routes block events to _handleBlock', async function(){
            sinon.stub(sync, '_handleBlock').resolves();
            await sync._handleEvent({ type: 'block', block_index: 10 }, 0);
            assert.strictEqual(sync._handleBlock.calledOnce, true);
        });

        it('routes reorg events to _handleReorg', async function(){
            sinon.stub(sync, '_handleReorg').resolves();
            await sync._handleEvent({ type: 'reorg', block_index: 10 }, 0);
            assert.strictEqual(sync._handleReorg.calledOnce, true);
        });

        it('detects gap on status event and triggers catch-up', async function(){
            sync.lastAppliedBlock = 50;
            sinon.stub(sync, '_incrementalCatchUp').resolves();
            await sync._handleEvent({ type: 'status', block_height: 55 }, 0);
            assert.strictEqual(sync._incrementalCatchUp.calledOnce, true);
            assert.strictEqual(sync._incrementalCatchUp.firstCall.args[0], 51);
        });

        it('does not trigger catch-up when no gap', async function(){
            sync.lastAppliedBlock = 50;
            sinon.stub(sync, '_incrementalCatchUp').resolves();
            await sync._handleEvent({ type: 'status', block_height: 51 }, 0);
            assert.strictEqual(sync._incrementalCatchUp.called, false);
        });

        it('does not trigger catch-up when lastAppliedBlock is null', async function(){
            sync.lastAppliedBlock = null;
            sinon.stub(sync, '_incrementalCatchUp').resolves();
            await sync._handleEvent({ type: 'status', block_height: 100 }, 0);
            assert.strictEqual(sync._incrementalCatchUp.called, false);
        });
    });

    describe('_handleBlock', function(){
        let blockEvent;

        beforeEach(function(){
            blockEvent = {
                type: 'block',
                block_index: 101,
                ledger_hash: 'lh101',
                actions_hash: 'ah101',
                contract_hash: 'ch101',
                data: { blocks: [{ block_index: 101 }] }
            };
            sync.lastAppliedBlock = 100;
            sync.lastHashes = { ledger_hash: 'lh100', actions_hash: 'ah100', contract_hash: 'ch100' };
        });

        it('skips blocks already applied', async function(){
            sync.lastAppliedBlock = 101;
            await sync._handleBlock(blockEvent, 0);
            assert.strictEqual(applier.applyBlock.called, false);
        });

        it('verifies chain continuity', async function(){
            sinon.spy(hashVerifier, 'verifyChainContinuity');
            // Single source mode to skip cross-source verification
            config.VERIFY_HASHES = false;
            sync = new ClientSync('bitcoin', 'mainnet', db, applier, rollback, hashVerifier, config, util);
            sync.lastAppliedBlock = 100;
            sync.lastHashes = { ledger_hash: 'lh100' };

            await sync._handleBlock(blockEvent, 0);
            assert.strictEqual(hashVerifier.verifyChainContinuity.calledOnce, true);
        });

        it('triggers catch-up on chain continuity failure', async function(){
            sinon.stub(hashVerifier, 'verifyChainContinuity').returns({ valid: false, reason: 'Block gap' });
            sinon.stub(sync, '_incrementalCatchUp').resolves();

            await sync._handleBlock(blockEvent, 0);
            assert.strictEqual(sync._incrementalCatchUp.calledOnce, true);
            assert.strictEqual(applier.applyBlock.called, false);
        });

        describe('cross-source verification', function(){
            it('applies block when two sources match', async function(){
                // Source 0 sends block
                await sync._handleBlock(blockEvent, 0);
                assert.strictEqual(applier.applyBlock.called, false); // waiting for source 1

                // Source 1 sends same block with same hashes
                await sync._handleBlock(blockEvent, 1);
                assert.strictEqual(applier.applyBlock.calledOnce, true);
            });

            it('does not apply block when sources have mismatched hashes', async function(){
                await sync._handleBlock(blockEvent, 0);

                let mismatchedEvent = { ...blockEvent, ledger_hash: 'DIFFERENT' };
                await sync._handleBlock(mismatchedEvent, 1);

                assert.strictEqual(applier.applyBlock.called, false);
            });

            it('applies from primary after timeout when only one source responds', async function(){
                let clock = sinon.useFakeTimers();

                await sync._handleBlock(blockEvent, 0);
                assert.strictEqual(applier.applyBlock.called, false);

                // Advance past timeout
                await clock.tickAsync(config.HASH_CONFIRM_TIMEOUT + 100);
                assert.strictEqual(applier.applyBlock.calledOnce, true);

                clock.restore();
            });

            it('arms a timeout when only the non-primary source arrives first', async function(){
                let clock = sinon.useFakeTimers();

                // Source 1 (non-primary) delivers first; before the fix no timer was
                // armed and the block would stall until the next block forced catch-up.
                await sync._handleBlock(blockEvent, 1);
                assert.strictEqual(applier.applyBlock.called, false);
                assert.strictEqual(sync._applyTimers.has(blockEvent.block_index), true,
                    '_applyTimers must be armed when the non-primary source arrives first');

                // After the timeout the block is applied from the available source.
                await clock.tickAsync(config.HASH_CONFIRM_TIMEOUT + 100);
                assert.strictEqual(applier.applyBlock.calledOnce, true);

                clock.restore();
            });

            it('does not double-arm the timer when both sources arrive before expiry', async function(){
                let clock = sinon.useFakeTimers();

                // Source 1 arrives first: timer armed
                await sync._handleBlock(blockEvent, 1);
                assert.strictEqual(sync._applyTimers.has(blockEvent.block_index), true);

                // Source 0 arrives before timeout: block applied immediately, timer NOT re-armed
                await sync._handleBlock(blockEvent, 0);
                assert.strictEqual(applier.applyBlock.calledOnce, true);

                // Ensure no delayed second apply fires after the original timer would have expired
                await clock.tickAsync(config.HASH_CONFIRM_TIMEOUT + 100);
                assert.strictEqual(applier.applyBlock.calledOnce, true); // still only called once

                clock.restore();
            });
        });

        describe('single source mode', function(){
            beforeEach(function(){
                config.SYNC_SOURCES = 'http://source1:3006';
                sync = new ClientSync('bitcoin', 'mainnet', db, applier, rollback, hashVerifier, config, util);
                sync.lastAppliedBlock = 100;
                sync.lastHashes = { ledger_hash: 'lh100', actions_hash: 'ah100', contract_hash: 'ch100' };
            });

            it('applies block immediately without waiting', async function(){
                await sync._handleBlock(blockEvent, 0);
                assert.strictEqual(applier.applyBlock.calledOnce, true);
            });
        });

        describe('verification disabled', function(){
            beforeEach(function(){
                config.VERIFY_HASHES = false;
                sync = new ClientSync('bitcoin', 'mainnet', db, applier, rollback, hashVerifier, config, util);
                sync.lastAppliedBlock = 100;
                sync.lastHashes = { ledger_hash: 'lh100', actions_hash: 'ah100', contract_hash: 'ch100' };
            });

            it('applies block immediately', async function(){
                await sync._handleBlock(blockEvent, 0);
                assert.strictEqual(applier.applyBlock.calledOnce, true);
            });
        });

        describe('decoder fork guard', function(){
            // The decoder path stores only the block's own block_hash (no replicated
            // previous-hash link). A block re-delivered at the committed tip with a
            // DIFFERENT block_hash signals a short reorg the client never observed
            // live; it must trigger catch-up rather than being silently dropped by the
            // already-applied skip. Mirrors the indexer's hash-continuity guard.
            function decoderSync(){
                let decoderDb = createMockDb();
                decoderDb.dbType = 'decoder';
                let s = new ClientSync('bitcoin', 'mainnet', decoderDb, applier, rollback, hashVerifier, config, util);
                s.lastAppliedBlock = 100;
                s.lastHashes = { block_hash: 'hash100' };
                return s;
            }

            it('triggers catch-up when the head block is re-delivered with a different hash', async function(){
                let s = decoderSync();
                sinon.stub(s, '_incrementalCatchUp').resolves();

                await s._handleBlock({ type: 'block', block_index: 100, block_hash: 'FORKED' }, 0);

                assert.strictEqual(s._incrementalCatchUp.calledOnce, true);
                assert.strictEqual(s._incrementalCatchUp.firstCall.args[0], 101);
                // The forked head must NOT be silently applied.
                assert.strictEqual(applier.applyBlock.called, false);
            });

            it('does not trigger catch-up when the head block re-arrives with the same hash', async function(){
                let s = decoderSync();
                sinon.stub(s, '_incrementalCatchUp').resolves();

                // Normal multi-source duplicate of the current tip: plain skip, no catch-up.
                await s._handleBlock({ type: 'block', block_index: 100, block_hash: 'hash100' }, 0);

                assert.strictEqual(s._incrementalCatchUp.called, false);
                assert.strictEqual(applier.applyBlock.called, false);
            });

            it('does not false-trigger before any block_hash is stored (fresh boot)', async function(){
                let s = decoderSync();
                s.lastHashes = null;
                sinon.stub(s, '_incrementalCatchUp').resolves();

                await s._handleBlock({ type: 'block', block_index: 100, block_hash: 'whatever' }, 0);

                assert.strictEqual(s._incrementalCatchUp.called, false);
            });
        });
    });

    describe('_applyBlockEvent', function(){
        it('calls applier.applyBlock', async function(){
            let event = { block_index: 10, ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c' };
            await sync._applyBlockEvent(event);
            assert.strictEqual(applier.applyBlock.calledOnce, true);
            assert.strictEqual(applier.applyBlock.firstCall.args[0], event);
        });

        it('updates lastAppliedBlock and lastHashes', async function(){
            await sync._applyBlockEvent({ block_index: 10, ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c' });
            assert.strictEqual(sync.lastAppliedBlock, 10);
            assert.strictEqual(sync.lastHashes.ledger_hash, 'l');
        });

        it('cleans up old pendingHashes entries', async function(){
            sync.pendingHashes.set(5, { 0: {} });
            sync.pendingHashes.set(15, { 0: {} });
            await sync._applyBlockEvent({ block_index: 10, ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c' });
            assert.strictEqual(sync.pendingHashes.has(5), false);
            assert.strictEqual(sync.pendingHashes.has(15), true);
        });

        it('handles apply error gracefully', async function(){
            applier.applyBlock.rejects(new Error('apply fail'));
            await sync._applyBlockEvent({ block_index: 10, ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c' });
            // Should not throw; error is caught and logged
            assert.strictEqual(console.error.called, true);
        });

        it('re-applies the source schema when the apply hits a missing table', async function(){
            applier.applyBlock.rejects(Object.assign(new Error('no table'), { errno: 1146 }));
            let heal = sinon.stub(sync, '_fetchAndApplySchema').resolves();
            await sync._applyBlockEvent({ block_index: 10, ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c' });
            assert.strictEqual(heal.calledOnce, true);
            assert.strictEqual(heal.firstCall.args[0], 'http://source1:3006');
        });
    });

    describe('_healSchemaIfStale', function(){
        let heal;
        beforeEach(function(){
            heal = sinon.stub(sync, '_fetchAndApplySchema').resolves();
        });

        it('heals on missing table (1146) and missing column (1054)', async function(){
            assert.strictEqual(await sync._healSchemaIfStale({ errno: 1146 }), true);
            sync._lastSchemaHeal = null; // reset the debounce between cases
            assert.strictEqual(await sync._healSchemaIfStale({ errno: 1054 }), true);
            assert.strictEqual(heal.callCount, 2);
        });

        it('ignores non-schema errors and null errors', async function(){
            assert.strictEqual(await sync._healSchemaIfStale({ errno: 1062 }), false);
            assert.strictEqual(await sync._healSchemaIfStale(new Error('plain')), false);
            assert.strictEqual(await sync._healSchemaIfStale(null), false);
            assert.strictEqual(heal.called, false);
        });

        it('debounces to one heal per minute', async function(){
            assert.strictEqual(await sync._healSchemaIfStale({ errno: 1146 }), true);
            assert.strictEqual(await sync._healSchemaIfStale({ errno: 1146 }), false);
            assert.strictEqual(heal.callCount, 1);
        });
    });

    describe('_runIncrementalCatchUp schema self-heal', function(){
        it('heals and retries ONCE when the catch-up apply hits a missing table', async function(){
            db.getLastBlock.resolves(5);
            let snapshot = { schema_version: 'x', block_height: 9, tables: {} };
            let gz = require('zlib').gzipSync(JSON.stringify(snapshot));
            sinon.stub(axios, 'get').resolves({ data: gz });
            // First apply fails on the schema gap, the post-heal retry succeeds.
            applier.applyIncrementalSnapshot
                .onFirstCall().rejects(Object.assign(new Error('no table'), { errno: 1146 }))
                .onSecondCall().resolves();
            let heal = sinon.stub(sync, '_fetchAndApplySchema').resolves();

            await sync._runIncrementalCatchUp();

            assert.strictEqual(heal.calledOnce, true);
            assert.strictEqual(applier.applyIncrementalSnapshot.callCount, 2);
            assert.strictEqual(sync.lastAppliedBlock, 9, 'retry applied the snapshot');
        });

        it('does not retry when the retry would hit the heal debounce', async function(){
            db.getLastBlock.resolves(5);
            let snapshot = { schema_version: 'x', block_height: 9, tables: {} };
            let gz = require('zlib').gzipSync(JSON.stringify(snapshot));
            sinon.stub(axios, 'get').resolves({ data: gz });
            // Persistent schema-gap failure (e.g. the DDL was rejected): the
            // first failure heals + retries, the second failure is debounced:
            // exactly two apply attempts, no spin.
            applier.applyIncrementalSnapshot.rejects(Object.assign(new Error('no table'), { errno: 1146 }));
            sinon.stub(sync, '_fetchAndApplySchema').resolves();

            await sync._runIncrementalCatchUp();

            assert.strictEqual(applier.applyIncrementalSnapshot.callCount, 2);
        });
    });

    describe('_handleReorg', function(){
        // A live reorg only ever arrives once the replica holds a committed tip (the WS
        // is opened after start()'s non-null guard), so these exercise the reachable
        // below-tip rollback path with a real tip.
        beforeEach(function(){ sync.lastAppliedBlock = 100; });

        it('calls rollback with the event block_index', async function(){
            let event = { type: 'reorg', chain: 'bitcoin', network: 'mainnet', block_index: 50 };
            await sync._handleReorg(event);
            assert.strictEqual(rollback.rollback.calledOnce, true);
            assert.strictEqual(rollback.rollback.firstCall.args[0], 50);
        });

        it('resets lastAppliedBlock to block_index - 1', async function(){
            db.getBlockHashRow.resolves({ ledger_hash: 'l49', actions_hash: 'a49', contract_hash: 'c49' });
            await sync._handleReorg({ block_index: 50 });
            assert.strictEqual(sync.lastAppliedBlock, 49);
        });

        it('loads new lastHashes from DB', async function(){
            let hashes = { ledger_hash: 'l49', actions_hash: 'a49', contract_hash: 'c49' };
            db.getBlockHashRow.resolves(hashes);
            await sync._handleReorg({ block_index: 50 });
            assert.strictEqual(sync.lastHashes, hashes);
        });

        it('sets lastHashes to null when rolling back to block 0', async function(){
            await sync._handleReorg({ block_index: 0 });
            assert.strictEqual(sync.lastHashes, null);
        });

        it('handles rollback error gracefully', async function(){
            rollback.rollback.rejects(new Error('rollback fail'));
            await sync._handleReorg({ block_index: 50 });
            assert.strictEqual(console.error.called, true);
        });

        // Defense-in-depth (2026-07-08 re-sweep): a reorg with NO committed tip must be
        // a no-op, never a cursor advance. With a null tip the DB is empty, so there is
        // nothing to roll back, and setting lastAppliedBlock = block_index - 1 from
        // server-supplied data would inflate the tip past an empty DB and wedge the
        // replica (the same shape as the above-tip case). Unreachable on the live path
        // but guarded so a future WS-ordering change cannot re-open it.
        it('null tip: ignores the reorg entirely (no rollback, no cursor advance)', async function(){
            sync.lastAppliedBlock = null;
            await sync._handleReorg({ block_index: 5000 });
            assert.strictEqual(rollback.rollback.called, false, 'nothing to roll back with no committed tip');
            assert.strictEqual(sync.lastAppliedBlock, null, 'the cursor must NOT be inflated from server data');
            assert.strictEqual(sync.isHalted(), false, 'a null-tip reorg is a benign no-op, not a halt');
        });
    });

    describe('stop', function(){
        it('sets running to false', function(){
            sync.running = true;
            sync.stop();
            assert.strictEqual(sync.running, false);
        });
    });

    describe('_verifyTableCounts (replica-completeness)', function(){
        it('flags a table the source has rows in but the follower has zeroed', async function(){
            // The core gap this guards: ledger/actions/contract hashes still agree,
            // yet contract_stakes never replicated to the follower.
            db.getTableCount = async (t) => ({ blocks: 100, actions: 5000, contract_stakes: 0 })[t];
            let mismatches = await sync._verifyTableCounts({ blocks: 100, actions: 5000, contract_stakes: 7 });
            assert.strictEqual(mismatches.length, 1);
            assert.strictEqual(mismatches[0].table, 'contract_stakes');
            assert.strictEqual(mismatches[0].sourceCount, 7);
            assert.strictEqual(mismatches[0].localCount, 0);
            assert.strictEqual(mismatches[0].delta, 7);
        });

        it('reports a table missing entirely from the follower as a full shortfall', async function(){
            // getTableCount throws (table absent in this replica's schema) → treated as 0.
            db.getTableCount = async () => { throw new Error('no such table'); };
            let mismatches = await sync._verifyTableCounts({ attests: 3 });
            assert.strictEqual(mismatches.length, 1);
            assert.strictEqual(mismatches[0].table, 'attests');
            assert.strictEqual(mismatches[0].localCount, 0);
            assert.strictEqual(mismatches[0].delta, 3);
        });

        it('heals the schema when a replicated table is missing locally (errno 1146)', async function(){
            // The live bug: a table the source ADDED but has never written a row to
            // (polls / poll_results / vote_delegations on the BTC replicas) streams
            // nothing, so the apply-path heal never fires. The completeness check is
            // the only place that touches it, and it used to swallow the 1146 -- and
            // because source count is 0 too, no mismatch was raised either. Result:
            // ER_NO_SUCH_TABLE logged forever, table never created.
            let healed = [];
            sync._healSchemaIfStale = async (e) => { healed.push(e.errno); return true; };
            let err = new Error("Table 'X.polls' doesn't exist"); err.errno = 1146;
            db.getTableCount = async () => { throw err; };

            let mismatches = await sync._verifyTableCounts({ polls: 0 });
            assert.deepStrictEqual(healed, [1146], 'missing table must trigger the schema heal');
            // Source has 0 rows, so it is correctly NOT a count mismatch.
            assert.strictEqual(mismatches.length, 0);
        });

        it('does not fault the completeness check when the schema heal itself throws', async function(){
            sync._healSchemaIfStale = async () => { throw new Error('DDL rejected'); };
            let err = new Error('missing'); err.errno = 1146;
            db.getTableCount = async () => { throw err; };
            // Advisory path: still returns, still reports the shortfall.
            let mismatches = await sync._verifyTableCounts({ polls: 4 });
            assert.strictEqual(mismatches.length, 1);
            assert.strictEqual(mismatches[0].localCount, 0);
        });

        it('returns no mismatches when the follower is complete (local >= source)', async function(){
            db.getTableCount = async (t) => ({ blocks: 100, actions: 5000, deposits: 12 })[t];
            // A follower legitimately ahead on a table is not flagged.
            let mismatches = await sync._verifyTableCounts({ blocks: 100, actions: 4999, deposits: 12 });
            assert.strictEqual(mismatches.length, 0);
        });

        it('treats absent/invalid table_counts as nothing to check (older source builds)', async function(){
            db.getTableCount = async () => 100;
            assert.deepStrictEqual(await sync._verifyTableCounts(undefined), []);
            assert.deepStrictEqual(await sync._verifyTableCounts(null), []);
            assert.deepStrictEqual(await sync._verifyTableCounts({ blocks: 'not-a-number' }), []);
        });

        it('skips a malicious table name without passing it to getTableCount', async function(){
            // A hostile/MITM'd source could put a backtick-injection payload in a
            // table_counts key. It must be rejected at the boundary, never reaching
            // the identifier interpolation in getTableCount, and must not manufacture
            // a false mismatch.
            let queried = [];
            db.getTableCount = async (t) => { queried.push(t); return 0; };
            let evilKey = 'blocks` WHERE 1=1 UNION SELECT password FROM mysql.user -- ';
            let mismatches = await sync._verifyTableCounts({ [evilKey]: 999, blocks: 100 });

            assert.ok(!queried.includes(evilKey), 'malicious key must never reach getTableCount');
            assert.deepStrictEqual(queried, ['blocks'], 'only the valid identifier is queried');
            assert.ok(!mismatches.some(m => m.table === evilKey), 'malicious key produces no mismatch');
        });
    });

    describe('decoder bootstrap completeness', function(){
        // Regression guard: a truncated/stale decoder full snapshot must not be
        // accepted silently. The indexer-only hash path (_verifyAgainstSource)
        // short-circuits for decoder, so bootstrap must run a row-count cross-check
        // against the second source independent of the VERIFY_HASHES flag.

        function decoderSync(cfg){
            let decoderDb = createMockDb();
            decoderDb.dbType = 'decoder';
            let s = new ClientSync('bitcoin', 'mainnet', decoderDb, applier, rollback, hashVerifier, cfg, util);
            return { s, decoderDb };
        }

        describe('_verifyDecoderCompleteness', function(){
            it('flags a truncated snapshot loudly when the source has more rows', async function(){
                let { s, decoderDb } = decoderSync(config);
                decoderDb.getTableCount = async (t) => ({ blocks: 100, transactions: 0 })[t];
                sinon.stub(axios, 'get').resolves({ data: { table_counts: { blocks: 100, transactions: 4200 } } });

                await s._verifyDecoderCompleteness('http://source2:3006', 500);

                // The shortfall must surface as a loud TABLE_COUNT_MISMATCH, not be swallowed.
                let logged = console.error.getCalls().some(c =>
                    typeof c.args[0] === 'string' && c.args[0].indexOf('TABLE_COUNT_MISMATCH') !== -1);
                assert.strictEqual(logged, true);
            });

            it('passes quietly when the follower is complete', async function(){
                let { s, decoderDb } = decoderSync(config);
                decoderDb.getTableCount = async (t) => ({ blocks: 100, transactions: 4200 })[t];
                sinon.stub(axios, 'get').resolves({ data: { table_counts: { blocks: 100, transactions: 4200 } } });

                await s._verifyDecoderCompleteness('http://source2:3006', 500);

                let mismatch = console.error.getCalls().some(c =>
                    typeof c.args[0] === 'string' && c.args[0].indexOf('TABLE_COUNT_MISMATCH') !== -1);
                assert.strictEqual(mismatch, false);
            });

            it('is a no-op for non-decoder dbType', async function(){
                // sync is the default indexer instance from the outer beforeEach.
                sinon.stub(axios, 'get').resolves({ data: { table_counts: { blocks: 9 } } });
                await sync._verifyDecoderCompleteness('http://source2:3006', 500);
                assert.strictEqual(axios.get.called, false);
            });
        });

        describe('_bootstrapFromSnapshot wiring', function(){
            it('runs the decoder completeness check even when VERIFY_HASHES is false', async function(){
                let cfg = Object.assign({}, config, { VERIFY_HASHES: false });
                let { s } = decoderSync(cfg);
                sinon.stub(s, '_fetchAndApplySchema').resolves();
                sinon.stub(s, '_verifyDecoderCompleteness').resolves();
                sinon.stub(s, '_verifyAgainstSource').resolves();
                sinon.stub(axios, 'get').resolves({ data: Buffer.from(JSON.stringify({ block_height: 500 })) });

                await s._bootstrapFromSnapshot();

                assert.strictEqual(s._verifyDecoderCompleteness.calledOnce, true,
                    'decoder completeness check must run regardless of VERIFY_HASHES');
                assert.strictEqual(s._verifyDecoderCompleteness.firstCall.args[0], 'http://source2:3006');
                assert.strictEqual(s._verifyDecoderCompleteness.firstCall.args[1], 500);
                // The indexer-only hash path must never run for decoder.
                assert.strictEqual(s._verifyAgainstSource.called, false);
            });

            it('does not run the decoder check in single-source mode', async function(){
                let cfg = Object.assign({}, config, { SYNC_SOURCES: 'http://source1:3006' });
                let { s } = decoderSync(cfg);
                sinon.stub(s, '_fetchAndApplySchema').resolves();
                sinon.stub(s, '_verifyDecoderCompleteness').resolves();
                sinon.stub(axios, 'get').resolves({ data: Buffer.from(JSON.stringify({ block_height: 500 })) });

                await s._bootstrapFromSnapshot();

                assert.strictEqual(s._verifyDecoderCompleteness.called, false);
            });

            it('takes the indexer hash path (not the decoder check) for indexer dbType', async function(){
                sinon.stub(sync, '_fetchAndApplySchema').resolves();
                sinon.stub(sync, '_verifyAgainstSource').resolves();
                sinon.stub(sync, '_verifyDecoderCompleteness').resolves();
                sinon.stub(axios, 'get').resolves({ data: Buffer.from(JSON.stringify({ block_height: 500 })) });

                await sync._bootstrapFromSnapshot();

                assert.strictEqual(sync._verifyAgainstSource.calledOnce, true);
                assert.strictEqual(sync._verifyDecoderCompleteness.called, false);
            });
        });
    });
});

describe('ClientSync._shouldReconcileDispensers (decoder resume cadence)', function(){
    // Pure decision over (config, _catchUpCount, _lastDispenserReconcileAt); exercise it
    // in isolation via prototype.call with a hand-built context.
    function decide(ctx, now){
        return ClientSync.prototype._shouldReconcileDispensers.call(ctx, now);
    }

    it('reconciles on the first cycle after a resume that skipped bootstrap', function(){
        let ctx = { config: {}, _lastDispenserReconcileAt: null };
        assert.strictEqual(decide(ctx, 1000), true);   // firstResume
        assert.strictEqual(ctx._catchUpCount, 1);
    });

    it('does not force a reconcile on the first cycle when bootstrap already reconciled', function(){
        let ctx = { config: {}, _lastDispenserReconcileAt: 1000 };  // bootstrap stamped it
        assert.strictEqual(decide(ctx, 1100), false);
    });

    it('reconciles every Nth catch-up in steady state', function(){
        let ctx = { config: { DISPENSERS_RECONCILE_EVERY: '5' }, _lastDispenserReconcileAt: 1000, _catchUpCount: 4 };
        assert.strictEqual(decide(ctx, 1100), true);   // 4 -> 5, 5 % 5 == 0
    });

    it('reconciles when the last reconcile is older than the max interval', function(){
        let ctx = { config: { DISPENSERS_RECONCILE_EVERY: '1000', DISPENSERS_RECONCILE_MAX_INTERVAL_MS: '60000' },
                    _lastDispenserReconcileAt: 1000, _catchUpCount: 1 };
        assert.strictEqual(decide(ctx, 1000 + 60000), true);   // exactly 60s elapsed
    });

    it('skips reconcile within the interval and off the periodic cycle', function(){
        let ctx = { config: { DISPENSERS_RECONCILE_EVERY: '1000', DISPENSERS_RECONCILE_MAX_INTERVAL_MS: '60000' },
                    _lastDispenserReconcileAt: 1000, _catchUpCount: 1 };
        assert.strictEqual(decide(ctx, 1000 + 59999), false);
    });

    it('treats a max interval of 0 as disabling the time trigger', function(){
        let ctx = { config: { DISPENSERS_RECONCILE_EVERY: '1000', DISPENSERS_RECONCILE_MAX_INTERVAL_MS: '0' },
                    _lastDispenserReconcileAt: 1000, _catchUpCount: 1 };
        assert.strictEqual(decide(ctx, 1000 + 99999999), false);
    });
});
