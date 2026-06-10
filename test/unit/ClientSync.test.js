// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available —
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

    describe('_logGap throttling', function(){
        // On a fast chain (e.g. Dogecoin testnet) the replica trails the tip and
        // would log a gap line per block — thousands/min. _logGap collapses that
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
            sinon.stub(sync, '_bootstrapFromSnapshot').resolves();
            sinon.stub(sync, '_incrementalCatchUp').resolves();
            sinon.stub(sync, '_connectWebSockets').callsFake(() => { sync.running = false; });

            await sync.start();

            assert.strictEqual(sync._bootstrapFromSnapshot.calledOnce, true);
            assert.strictEqual(sync._incrementalCatchUp.called, false);
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

                // Normal multi-source duplicate of the current tip — plain skip, no catch-up.
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
            // Should not throw — error is caught and logged
            assert.strictEqual(console.error.called, true);
        });
    });

    describe('_handleReorg', function(){
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

        it('returns no mismatches when the follower is complete (local >= source)', async function(){
            db.getTableCount = async (t) => ({ blocks: 100, actions: 5000, contract_balances: 12 })[t];
            // A follower legitimately ahead on a table is not flagged.
            let mismatches = await sync._verifyTableCounts({ blocks: 100, actions: 4999, contract_balances: 12 });
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
