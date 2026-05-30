const assert = require('assert');
const sinon  = require('sinon');
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
            let mismatches = await sync._verifyTableCounts({ attestation_requests: 3 });
            assert.strictEqual(mismatches.length, 1);
            assert.strictEqual(mismatches[0].table, 'attestation_requests');
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
    });
});
