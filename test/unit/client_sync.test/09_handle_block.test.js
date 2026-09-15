// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

const {
    assert, sinon, axios, ClientSync, createMockDb, registerClientSyncHooks
} = require('./support');

let sync, db, applier, rollback, hashVerifier, config, util;

function assignState(state){
    ({ sync, db, applier, rollback, hashVerifier, config, util } = state);
}

let blockEvent;

function registerBlockEventHook(){
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
}

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

function registerHandleBlockGroup1Tests(){
    describe('handleBlock', function(){
        registerBlockEventHook();

        it('skips blocks already applied', async function(){
            sync.lastAppliedBlock = 101;
            await sync.handleBlock(blockEvent, 0);
            assert.strictEqual(applier.applyBlock.called, false);
        });

        it('verifies chain continuity', async function(){
            sinon.spy(hashVerifier, 'verifyChainContinuity');
            // Single source mode to skip cross-source verification
            config.VERIFY_HASHES = false;
            sync = new ClientSync('bitcoin', 'mainnet', db, applier, rollback, hashVerifier, config, util);
            sync.lastAppliedBlock = 100;
            sync.lastHashes = { ledger_hash: 'lh100' };

            await sync.handleBlock(blockEvent, 0);
            assert.strictEqual(hashVerifier.verifyChainContinuity.calledOnce, true);
        });

        it('triggers catch-up on chain continuity failure', async function(){
            sinon.stub(hashVerifier, 'verifyChainContinuity').returns({ valid: false, reason: 'Block gap' });
            sinon.stub(sync, 'incrementalCatchUp').resolves();

            await sync.handleBlock(blockEvent, 0);
            assert.strictEqual(sync.incrementalCatchUp.calledOnce, true);
            assert.strictEqual(applier.applyBlock.called, false);
        });
    });
}

function registerHandleBlockGroup2Tests(){
    describe('handleBlock', function(){
        registerBlockEventHook();

        describe('cross-source verification', function(){
            it('applies block when two sources match', async function(){
                // Source 0 sends block
                await sync.handleBlock(blockEvent, 0);
                assert.strictEqual(applier.applyBlock.called, false); // waiting for source 1

                // Source 1 sends same block with same hashes
                await sync.handleBlock(blockEvent, 1);
                assert.strictEqual(applier.applyBlock.calledOnce, true);
            });

            it('does not apply block when sources have mismatched hashes', async function(){
                await sync.handleBlock(blockEvent, 0);

                let mismatchedEvent = { ...blockEvent, ledger_hash: 'DIFFERENT' };
                await sync.handleBlock(mismatchedEvent, 1);

                assert.strictEqual(applier.applyBlock.called, false);
            });
        });
    });
}

function registerHandleBlockGroup3Tests(){
    describe('handleBlock', function(){
        registerBlockEventHook();

        describe('cross-source verification', function(){
            it('applies from primary after timeout when only one source responds', async function(){
                let clock = sinon.useFakeTimers();

                await sync.handleBlock(blockEvent, 0);
                assert.strictEqual(applier.applyBlock.called, false);

                // Advance past timeout
                await clock.tickAsync(config.HASH_CONFIRM_TIMEOUT + 100);
                assert.strictEqual(applier.applyBlock.calledOnce, true);

                clock.restore();
            });

            it('arms a timeout when only the non-primary source arrives first', async function(){
                let clock = sinon.useFakeTimers();

                // Source 1 (non-primary) delivers first; without a timer the block
                // stalls until the next block forces catch-up.
                await sync.handleBlock(blockEvent, 1);
                assert.strictEqual(applier.applyBlock.called, false);
                assert.strictEqual(sync._applyTimers.has(blockEvent.block_index), true,
                    '_applyTimers must be armed when the non-primary source arrives first');

                // After the timeout the block is applied from the available source.
                await clock.tickAsync(config.HASH_CONFIRM_TIMEOUT + 100);
                assert.strictEqual(applier.applyBlock.calledOnce, true);

                clock.restore();
            });
        });
    });
}

function registerHandleBlockGroup4Tests(){
    describe('handleBlock', function(){
        registerBlockEventHook();

        describe('cross-source verification', function(){
            it('does not double-arm the timer when both sources arrive before expiry', async function(){
                let clock = sinon.useFakeTimers();

                // Source 1 arrives first: timer armed
                await sync.handleBlock(blockEvent, 1);
                assert.strictEqual(sync._applyTimers.has(blockEvent.block_index), true);

                // Source 0 arrives before timeout: block applied immediately, timer NOT re-armed
                await sync.handleBlock(blockEvent, 0);
                assert.strictEqual(applier.applyBlock.calledOnce, true);

                // Ensure no delayed second apply fires after the original timer would have expired
                await clock.tickAsync(config.HASH_CONFIRM_TIMEOUT + 100);
                assert.strictEqual(applier.applyBlock.calledOnce, true); // still only called once

                clock.restore();
            });
        });
    });
}

function registerHandleBlockGroup5Tests(){
    describe('handleBlock', function(){
        registerBlockEventHook();

        describe('single source mode', function(){
            beforeEach(function(){
                config.SYNC_SOURCES = 'http://source1:3006';
                sync = new ClientSync('bitcoin', 'mainnet', db, applier, rollback, hashVerifier, config, util);
                sync.lastAppliedBlock = 100;
                sync.lastHashes = { ledger_hash: 'lh100', actions_hash: 'ah100', contract_hash: 'ch100' };
            });

            it('applies block immediately without waiting', async function(){
                await sync.handleBlock(blockEvent, 0);
                assert.strictEqual(applier.applyBlock.calledOnce, true);
            });
        });
    });
}

function registerHandleBlockGroup6Tests(){
    describe('handleBlock', function(){
        registerBlockEventHook();

        describe('verification disabled', function(){
            beforeEach(function(){
                config.VERIFY_HASHES = false;
                sync = new ClientSync('bitcoin', 'mainnet', db, applier, rollback, hashVerifier, config, util);
                sync.lastAppliedBlock = 100;
                sync.lastHashes = { ledger_hash: 'lh100', actions_hash: 'ah100', contract_hash: 'ch100' };
            });

            it('applies block immediately', async function(){
                await sync.handleBlock(blockEvent, 0);
                assert.strictEqual(applier.applyBlock.calledOnce, true);
            });
        });
    });
}

function registerHandleBlockGroup7Tests(){
    describe('handleBlock', function(){
        registerBlockEventHook();

        describe('decoder fork guard', function(){
            it('rewinds the orphaned head and catches up from the forked height', async function(){
                // A bare catch-up cannot reach the fork: runIncrementalCatchUp resolves
                // `since` from the DB tip, not from the argument, so with the orphan
                // still committed the client asks the source for /since/101 and either
                // 404s at the source's own tip or stacks later blocks on the orphan.
                // The rewind is what moves the tip.
                let s = decoderSync();
                sinon.stub(s, 'incrementalCatchUp').resolves();

                await s.handleBlock({ type: 'block', block_index: 100, block_hash: 'FORKED' }, 0);

                assert.strictEqual(s.rollback.rollback.calledOnceWith(100), true,
                    'the orphaned tip is actually unwound');
                assert.strictEqual(s.lastAppliedBlock, 99, 'the committed tip moves below the fork');
                assert.strictEqual(s.incrementalCatchUp.calledOnce, true);
                assert.strictEqual(s.incrementalCatchUp.firstCall.args[0], 100,
                    'and the replacement block is re-fetched, not skipped over');
                // The forked head must NOT be silently applied.
                assert.strictEqual(applier.applyBlock.called, false);
            });
        });
    });
}

function registerHandleBlockGroup8Tests(){
    describe('handleBlock', function(){
        registerBlockEventHook();

        describe('decoder fork guard', function(){
            it('does not trigger catch-up when the head block re-arrives with the same hash', async function(){
                let s = decoderSync();
                sinon.stub(s, 'incrementalCatchUp').resolves();

                // Normal multi-source duplicate of the current tip: plain skip, no catch-up.
                await s.handleBlock({ type: 'block', block_index: 100, block_hash: 'hash100' }, 0);

                assert.strictEqual(s.incrementalCatchUp.called, false);
                assert.strictEqual(applier.applyBlock.called, false);
            });

            it('does not false-trigger before any block_hash is stored (fresh boot)', async function(){
                let s = decoderSync();
                s.lastHashes = null;
                sinon.stub(s, 'incrementalCatchUp').resolves();

                await s.handleBlock({ type: 'block', block_index: 100, block_hash: 'whatever' }, 0);

                assert.strictEqual(s.incrementalCatchUp.called, false);
            });
        });
    });
}

describe('ClientSync', function(){
    registerClientSyncHooks(assignState);
    registerHandleBlockGroup1Tests();
    registerHandleBlockGroup2Tests();
    registerHandleBlockGroup3Tests();
    registerHandleBlockGroup4Tests();
    registerHandleBlockGroup5Tests();
    registerHandleBlockGroup6Tests();
    registerHandleBlockGroup7Tests();
    registerHandleBlockGroup8Tests();
});
