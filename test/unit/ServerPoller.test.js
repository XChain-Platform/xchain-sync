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
const ServerPoller = require('../../src/ServerPoller');
const Utility = require('../../src/utility');

function createMockDb(){
    return {
        getLastBlock: sinon.stub().resolves(null),
        getBlockHashRow: sinon.stub().resolves(null),
        getBlockScopedRows: sinon.stub().resolves([]),
        getTxScopedRows: sinon.stub().resolves([]),
        getActionScopedRows: sinon.stub().resolves([]),
        getEmissionRowsForBlock: sinon.stub().resolves([]),
        getStateRootsRow: sinon.stub().resolves({
            balances_root: 'br', block_merkle_root: 'bmr', state_root: 'sr'
        }),
        getTransactions: sinon.stub().resolves([]),
        getActions: sinon.stub().resolves([]),
        // Used by collectMaturedCooldownCredits; null short-circuits it to no credits.
        getStatusId: sinon.stub().resolves(null),
        doQuery: sinon.stub().resolves([]),
        // The forward batch is pinned to a REPEATABLE READ snapshot (H-P2).
        beginReadSnapshot: sinon.stub().resolves({ mockSnapshotConn: true }),
        commitReadSnapshot: sinon.stub().resolves(),
        rollbackReadSnapshot: sinon.stub().resolves()
    };
}

function createMockBroadcaster(){
    return {
        broadcast: sinon.stub(),
        updateStatus: sinon.stub(),
        getSubscribers: sinon.stub().returns([]),
        getSubscriberCount: sinon.stub().returns(0)
    };
}

function createMockLog(){
    return {
        epochSize:        100,
        recordBlock:      sinon.stub().resolves(),
        pruneFrom:        sinon.stub().resolves(),
        getHighWaterMark: sinon.stub().resolves(null),
        getRecordedHash:  sinon.stub().resolves(null),
        findGaps:         sinon.stub().resolves([]),
        recommitEpoch:    sinon.stub().resolves()
    };
}

describe('ServerPoller', function(){

    let poller, db, broadcaster, log, config, util;

    beforeEach(function(){
        db = createMockDb();
        broadcaster = createMockBroadcaster();
        log = createMockLog();
        config = { BLOCK_POLL_INTERVAL: 3000 };
        util = new Utility();
        poller = new ServerPoller('bitcoin', 'mainnet', db, broadcaster, log, config, util);
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });

    afterEach(function(){
        sinon.restore();
    });

    describe('table lists', function(){
        it('has block-scoped tables', function(){
            assert.ok(poller.blockScopedTables.includes('blocks'));
            assert.ok(poller.blockScopedTables.includes('transactions'));
            assert.ok(poller.blockScopedTables.includes('slash_events'));
            assert.ok(poller.blockScopedTables.includes('contract_slash_debits'));
            // WI-2 bump 2 capability-stake equivocation slashing (committed 8e95482).
            assert.ok(poller.blockScopedTables.includes('capability_slash_events'));
            assert.ok(poller.blockScopedTables.includes('capability_slash_debits'));
            // RB-ANCHOR: pre-image log of reconcile-deleted validator_rewards losers (stream:block).
            assert.ok(poller.blockScopedTables.includes('anchor_reward_reconcile_log'));
            // Per-(address,tick) locked totals (SPV sub-tree spec Stage B). It is
            // REPLICATED rather than recomputed on the follower, which is the whole
            // point: four families' open-remaining logic then exists once, on the
            // source, instead of having to agree across the twins.
            assert.ok(poller.blockScopedTables.includes('escrow_leaf_journal'));
            // Pre-rotation signing keys journaled by the DELEGATE v1 materialization sweep
            // (#4366). Replicated because the follower reaches the mutated (surviving)
            // contract_stakes row through it, in both directions: forward via updatedRows,
            // backward via the ClientRollback key restore.
            assert.ok(poller.blockScopedTables.includes('contract_delegation_rotations'));
            assert.strictEqual(poller.blockScopedTables.length, 11);
        });

        it('has action-scoped tables', function(){
            assert.ok(poller.actionScopedTables.length > 40);
            assert.ok(poller.actionScopedTables.includes('actions'));
            assert.ok(poller.actionScopedTables.includes('attests'));
            assert.ok(poller.actionScopedTables.includes('gated_files'));
            assert.ok(poller.actionScopedTables.includes('contract_stakes'));
            assert.ok(poller.actionScopedTables.includes('contract_unstakes'));
            assert.ok(poller.actionScopedTables.includes('contract_delegations'));
        });

        it('has index tables', function(){
            assert.strictEqual(poller.indexTables.length, 10);
            assert.ok(poller.indexTables.includes('index_transactions'));
            assert.ok(poller.indexTables.includes('index_addresses'));
        });
    });

    describe('_poll', function(){
        it('returns early when no blocks in DB', async function(){
            db.getLastBlock.resolves(null);
            await poller._poll();
            assert.strictEqual(broadcaster.broadcast.called, false);
        });

        it('initializes lastPolledBlock on first poll', async function(){
            db.getLastBlock.resolves(100);
            poller.lastPolledBlock = null;
            await poller._poll();
            assert.strictEqual(poller.lastPolledBlock, 100);
            assert.strictEqual(broadcaster.updateStatus.calledOnce, true);
            assert.strictEqual(broadcaster.broadcast.called, false);
        });

        it('processes new blocks when currentBlock > lastPolledBlock', async function(){
            poller.lastPolledBlock = 99;
            db.getLastBlock.resolves(100);
            db.getBlockHashRow.resolves({
                block_index: 100, block_time: 1700000000,
                ledger_hash: 'lh', actions_hash: 'ah', contract_hash: 'ch'
            });

            await poller._poll();

            assert.strictEqual(log.recordBlock.calledOnce, true);
            assert.strictEqual(broadcaster.broadcast.calledOnce, true);
            let event = broadcaster.broadcast.firstCall.args[2];
            assert.strictEqual(event.type, 'block');
            assert.strictEqual(event.block_index, 100);
            assert.strictEqual(event.chain, 'bitcoin');
            assert.strictEqual(event.network, 'mainnet');
            assert.strictEqual(poller.lastPolledBlock, 100);
        });

        it('does nothing when currentBlock equals lastPolledBlock', async function(){
            poller.lastPolledBlock = 100;
            db.getLastBlock.resolves(100);
            await poller._poll();
            assert.strictEqual(broadcaster.broadcast.called, false);
            assert.strictEqual(log.recordBlock.called, false);
        });

        it('limits to 100 blocks per poll', async function(){
            poller.lastPolledBlock = 0;
            db.getLastBlock.resolves(200);
            db.getBlockHashRow.resolves({
                block_index: 1, block_time: 100,
                ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c'
            });

            await poller._poll();

            assert.strictEqual(broadcaster.broadcast.callCount, 100);
            assert.strictEqual(poller.lastPolledBlock, 100);
        });

        it('detects reorg and broadcasts reorg event', async function(){
            poller.lastPolledBlock = 100;
            db.getLastBlock.resolves(95);
            db.getBlockHashRow.resolves({
                block_index: 95, block_time: 100,
                ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c'
            });

            await poller._poll();

            assert.strictEqual(broadcaster.broadcast.calledOnce, true);
            let event = broadcaster.broadcast.firstCall.args[2];
            assert.strictEqual(event.type, 'reorg');
            assert.strictEqual(event.block_index, 96); // currentBlock + 1
            assert.strictEqual(poller.lastPolledBlock, 95);
        });

        it('detects a net-forward reorg via a changed same-height hash', async function(){
            poller.lastPolledBlock = 100;
            poller.lastPolledBlockHash = 'old-ledger-hash';
            db.getLastBlock.resolves(100); // height unchanged: rollback + readvance within one interval
            db.getBlockHashRow.resolves({
                block_index: 100, block_time: 100,
                ledger_hash: 'new-ledger-hash', actions_hash: 'a', contract_hash: 'c'
            });

            await poller._poll();

            assert.strictEqual(broadcaster.broadcast.calledOnce, true);
            let event = broadcaster.broadcast.firstCall.args[2];
            assert.strictEqual(event.type, 'reorg');
            assert.strictEqual(event.block_index, 100); // the divergent block itself
            assert.strictEqual(poller.lastPolledBlock, 99); // rolled back one for the next-poll walk-back
        });

        it('resolves a multi-block net-forward reorg to the true fork in a single poll', async function(){
            // Pre-reorg the poller had broadcast blocks 98, 99, 100; their hashes
            // are recorded. A net-forward reorg rewrote BOTH 99 and 100 within one
            // interval; 98 is the unchanged true fork point.
            poller.lastPolledBlock = 100;
            poller.lastPolledBlockHash = 'h100-old';
            poller.recentBroadcastHashes.set(98, 'h98-old');
            poller.recentBroadcastHashes.set(99, 'h99-old');
            poller.recentBroadcastHashes.set(100, 'h100-old');
            db.getLastBlock.resolves(101); // net-forward: height stays >= lastPolledBlock

            // Source serves the POST-reorg hashes at the rewritten heights; 98 unchanged.
            db.getBlockHashRow.withArgs(100).resolves({
                block_index: 100, block_time: 100,
                ledger_hash: 'h100-new', actions_hash: 'a', contract_hash: 'c'
            });
            db.getBlockHashRow.withArgs(99).resolves({
                block_index: 99, block_time: 99,
                ledger_hash: 'h99-new', actions_hash: 'a', contract_hash: 'c'
            });
            db.getBlockHashRow.withArgs(98).resolves({
                block_index: 98, block_time: 98,
                ledger_hash: 'h98-old', actions_hash: 'a', contract_hash: 'c'
            });

            // A SINGLE poll walks down over the recorded pre-reorg hashes (100 and 99
            // both changed; 98 is unchanged -> the true fork) and broadcasts ONE deep
            // reorg at 99, not a shallow reorg@100 that would leave block 99 orphaned on
            // followers across subsequent polls. lastPolledBlock drops below the fork so
            // the forward loop re-streams 99..101 fresh.
            await poller._poll();
            assert.strictEqual(broadcaster.broadcast.callCount, 1);
            assert.strictEqual(broadcaster.broadcast.getCall(0).args[2].type, 'reorg');
            assert.strictEqual(broadcaster.broadcast.getCall(0).args[2].block_index, 99);
            assert.strictEqual(poller.lastPolledBlock, 98);
            assert.strictEqual(poller.lastPolledBlockHash, 'h98-old');
        });

        it('does not flag a net-forward reorg when the same-height hash is unchanged', async function(){
            poller.lastPolledBlock = 100;
            poller.lastPolledBlockHash = 'lh';
            db.getLastBlock.resolves(100);
            db.getBlockHashRow.resolves({
                block_index: 100, block_time: 100,
                ledger_hash: 'lh', actions_hash: 'a', contract_hash: 'c'
            });

            await poller._poll();

            assert.strictEqual(broadcaster.broadcast.called, false);
        });

        it('prunes the source transparency log on reorg (to currentBlock + 1)', async function(){
            poller.lastPolledBlock = 100;
            db.getLastBlock.resolves(95);
            db.getBlockHashRow.resolves({
                block_index: 95, block_time: 100,
                ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c'
            });

            await poller._poll();

            assert.strictEqual(log.pruneFrom.calledOnce, true);
            assert.strictEqual(log.pruneFrom.firstCall.args[0], 96); // orphaned suffix starts here
        });

        it('does not attempt a transparency prune on reorg for the decoder (no log)', async function(){
            let decoderDb = createMockDb();
            decoderDb.dbType = 'decoder';
            decoderDb.getLastBlock.resolves(95);
            decoderDb.getBlockHashRow.resolves({ block_index: 95, block_time: 100, block_hash: 'bh' });
            let decoderPoller = new ServerPoller('bitcoin', 'mainnet', decoderDb, broadcaster, null, config, util);
            decoderPoller.lastPolledBlock = 100;

            await decoderPoller._poll();  // must not throw despite transparencyLog === null

            // The reorg is still broadcast to subscribers.
            assert.strictEqual(broadcaster.broadcast.calledOnce, true);
            assert.strictEqual(broadcaster.broadcast.firstCall.args[2].type, 'reorg');
        });

        it('processes multiple sequential blocks', async function(){
            poller.lastPolledBlock = 97;
            db.getLastBlock.resolves(100);
            db.getBlockHashRow.callsFake(async (idx) => ({
                block_index: idx, block_time: idx * 10,
                ledger_hash: 'l' + idx, actions_hash: 'a' + idx, contract_hash: 'c' + idx
            }));

            await poller._poll();

            assert.strictEqual(broadcaster.broadcast.callCount, 3); // blocks 98, 99, 100
            assert.strictEqual(poller.lastPolledBlock, 100);
        });
    });

    describe('_resumeCursor (restart resume) @regression', function(){
        it('indexer resumes from the transparency-log high-water mark, not the source tip', async function(){
            // sync_meta recorded up to 100; source DB has since advanced to 250 (e.g.
            // the indexer ran on while the sync server was down).
            log.getHighWaterMark.resolves(100);
            db.getLastBlock.resolves(250);

            let cursor = await poller._resumeCursor();

            assert.strictEqual(cursor, 100, 'must resume from sync_meta high-water mark');
            assert.strictEqual(db.getLastBlock.called, false, 'must not seed from the source tip');
        });

        it('indexer resume is null on a fresh node (empty sync_meta)', async function(){
            log.getHighWaterMark.resolves(null);
            let cursor = await poller._resumeCursor();
            assert.strictEqual(cursor, null, 'null lets _poll initialise from the current tip');
        });

        it('decoder resumes from the source tip (no transparency log)', async function(){
            let decoderDb = createMockDb();
            decoderDb.dbType = 'decoder';
            decoderDb.getLastBlock.resolves(777);
            let decoderPoller = new ServerPoller('bitcoin', 'mainnet', decoderDb, broadcaster, null, config, util);

            let cursor = await decoderPoller._resumeCursor();
            assert.strictEqual(cursor, 777);
        });

        it('does not skip blocks advanced during downtime when polling resumes', async function(){
            // Restart-mid-advance: recorded through block 100, source now at 105.
            // After seeding the cursor from the high-water mark, _poll must record
            // every block in [101, 105]: none may be skipped.
            log.getHighWaterMark.resolves(100);
            db.getLastBlock.resolves(105);
            db.getBlockHashRow.callsFake(async (idx) => ({
                block_index: idx, block_time: idx * 10,
                ledger_hash: 'l' + idx, actions_hash: 'a' + idx, contract_hash: 'c' + idx
            }));

            poller.lastPolledBlock = await poller._resumeCursor();
            await poller._poll();

            let recorded = log.recordBlock.getCalls().map(c => c.args[0]);
            assert.deepStrictEqual(recorded, [101, 102, 103, 104, 105],
                'every downtime block must be recorded in the transparency log');
            assert.strictEqual(poller.lastPolledBlock, 105);
        });
    });

    // Fix #4 (MED): on restart the reorg guard seeded lastPolledBlockHash from a LIVE
    // source read, so a reorg that completed entirely during downtime was masked
    // (live == live) and never detected - stale sync_meta/merkle_epochs served
    // chain-wrong proofs forever. Seed from the DURABLE recorded hash instead so the
    // first poll compares recorded(pre-reorg) vs live(post-reorg) and fires.
    describe('_seedReorgGuardHash (durable reorg-guard seed) @regression', function(){
        it('indexer seeds from the recorded (pre-reorg) hash, NOT a live source read', async function(){
            log.getRecordedHash.withArgs(100).resolves('recorded-pre-reorg');
            // The live source at 100 is already post-reorg; seeding from it would mask
            // the reorg. getBlockHashRow (the live read) must not be consulted.
            db.getBlockHashRow.withArgs(100).resolves({ ledger_hash: 'live-post-reorg' });

            let seed = await poller._seedReorgGuardHash(100);

            assert.strictEqual(seed, 'recorded-pre-reorg', 'reorg guard seeds from the durable record');
            assert.ok(log.getRecordedHash.calledWith(100));
            assert.strictEqual(db.getBlockHashRow.called, false, 'must not seed from the live (post-reorg) source');
        });

        it('detects a during-downtime reorg on the first poll after restart', async function(){
            // Recorded up through 100 (pre-reorg ledger hash), no advance in height, but
            // the chain forked at 100 while the server was down: live hash differs.
            // Drive the seed + first poll directly (start() would enter its live poll
            // loop); this mirrors the _resumeCursor regression above.
            log.getHighWaterMark.resolves(100);
            log.getRecordedHash.withArgs(100).resolves('l100-pre');
            db.getLastBlock.resolves(100);
            db.getBlockHashRow.withArgs(100).resolves({ ledger_hash: 'l100-post' });

            poller.lastPolledBlock = await poller._resumeCursor();
            poller.lastPolledBlockHash = await poller._seedReorgGuardHash(poller.lastPolledBlock);
            assert.strictEqual(poller.lastPolledBlockHash, 'l100-pre', 'seeded from the recorded pre-reorg hash');

            await poller._poll();

            assert.ok(broadcaster.broadcast.calledWith('bitcoin', 'mainnet', sinon.match({ type: 'reorg', block_index: 100 })),
                'the during-downtime reorg is detected and broadcast');
            assert.ok(log.pruneFrom.calledWith(100), 'transparency log pruned from the fork point');
        });

        it('falls back to the live read for a fresh node (no recorded hash) and null cursor', async function(){
            assert.strictEqual(await poller._seedReorgGuardHash(null), null, 'null cursor -> no seed');
            log.getRecordedHash.withArgs(42).resolves(null);       // never recorded
            db.getBlockHashRow.withArgs(42).resolves({ ledger_hash: 'live-42' });
            assert.strictEqual(await poller._seedReorgGuardHash(42), 'live-42', 'live fallback on a recorded miss');
        });

        it('decoder (no transparency log) seeds from the live source read', async function(){
            let decoderDb = createMockDb();
            decoderDb.dbType = 'decoder';
            decoderDb.getBlockHashRow.withArgs(7).resolves({ block_hash: 'bh7' });
            let decoderPoller = new ServerPoller('bitcoin', 'mainnet', decoderDb, broadcaster, null, config, util);
            assert.strictEqual(await decoderPoller._seedReorgGuardHash(7), 'bh7');
        });
    });

    describe('backfillGaps @regression', function(){
        it('is a no-op for the decoder (no transparency log)', async function(){
            let decoderDb = createMockDb();
            decoderDb.dbType = 'decoder';
            let decoderPoller = new ServerPoller('bitcoin', 'mainnet', decoderDb, broadcaster, null, config, util);
            let n = await decoderPoller.backfillGaps();
            assert.strictEqual(n, 0);
        });

        it('is a no-op when the transparency log reports no gaps', async function(){
            log.findGaps.resolves([]);
            let n = await poller.backfillGaps();
            assert.strictEqual(n, 0);
            assert.strictEqual(log.recordBlock.called, false);
            assert.strictEqual(log.recommitEpoch.called, false);
        });

        it('replays recordBlock for each missing block and recomputes affected epochs', async function(){
            // Interior holes at 150 and 201 (epochs 2 and 3 with epochSize 100).
            log.findGaps.resolves([150, 201]);
            db.getBlockHashRow.callsFake(async (idx) => ({
                block_index: idx, block_time: idx * 10,
                ledger_hash: 'l' + idx, actions_hash: 'a' + idx, contract_hash: 'c' + idx
            }));

            let n = await poller.backfillGaps();

            assert.strictEqual(n, 2);
            let recorded = log.recordBlock.getCalls().map(c => c.args[0]);
            assert.deepStrictEqual(recorded, [150, 201]);
            let recomputed = log.recommitEpoch.getCalls().map(c => c.args[0]).sort();
            assert.deepStrictEqual(recomputed, [2, 3], 'epochs spanning the holes are rebuilt');
        });

        it('skips a gap whose source block has vanished (reorg) without recording it', async function(){
            log.findGaps.resolves([150]);
            db.getBlockHashRow.resolves(null);  // block no longer in source

            let n = await poller.backfillGaps();

            assert.strictEqual(n, 1);  // counted as detected
            assert.strictEqual(log.recordBlock.called, false);
            assert.strictEqual(log.recommitEpoch.called, false);
        });
    });

    describe('_buildBlockPayload', function(){
        it('returns null when block hash row is missing', async function(){
            db.getBlockHashRow.resolves(null);
            let result = await poller._buildBlockPayload(100);
            assert.strictEqual(result, null);
        });

        it('builds complete payload with correct structure', async function(){
            db.getBlockHashRow.resolves({
                block_index: 100, block_time: 1700000000,
                ledger_hash: 'lh', actions_hash: 'ah', contract_hash: 'ch'
            });
            db.getBlockScopedRows.resolves([{ block_index: 100 }]);
            db.getTransactions.resolves([{ tx_index: 1, block_index: 100, source_id: 10, tx_hash_id: 20 }]);
            db.getActions.resolves([{ action_index: 50, tx_index: 1 }]);

            let payload = await poller._buildBlockPayload(100);

            assert.strictEqual(payload.type, 'block');
            assert.strictEqual(payload.chain, 'bitcoin');
            assert.strictEqual(payload.network, 'mainnet');
            assert.strictEqual(payload.block_index, 100);
            assert.strictEqual(payload.block_time, 1700000000);
            assert.strictEqual(payload.ledger_hash, 'lh');
            assert.ok(payload.data);
            // Live block payloads must carry schema_version so ClientApplier
            // can enforce the version-pin gate the snapshot paths enforce.
            const { SCHEMA_VERSION } = require('../../src/schema-version');
            assert.strictEqual(payload.schema_version, SCHEMA_VERSION['indexer'],
                'live block payload must carry schema_version');
        });

        it('streams contract_emissions via getEmissionRowsForBlock, not getActionScopedRows', async function(){
            db.getBlockHashRow.resolves({
                block_index: 100, block_time: 1700000000,
                ledger_hash: 'lh', actions_hash: 'ah', contract_hash: 'ch'
            });
            // Internal SLASH emission: action_index is NULL; getActionScopedRows would drop it.
            db.getEmissionRowsForBlock.resolves([
                { execution_index: 10, emitted_action: 'SLASH', action_index: null, position: 0 }
            ]);

            let payload = await poller._buildBlockPayload(100);

            assert.ok(db.getEmissionRowsForBlock.calledOnceWith(100),
                'contract_emissions must be sourced via getEmissionRowsForBlock');
            assert.ok(!db.getActionScopedRows.getCalls().some(c => c.args[0] === 'contract_emissions'),
                'contract_emissions must NOT go through getActionScopedRows');
            assert.ok(payload.data['contract_emissions'], 'emission rows present in payload');
            assert.strictEqual(payload.data['contract_emissions'][0].action_index, null);
        });

        it('includes a sync_meta transparency row in the indexer payload', async function(){
            db.getBlockHashRow.resolves({
                block_index: 100, block_time: 1700000000,
                ledger_hash: 'lh', actions_hash: 'ah', contract_hash: 'ch'
            });
            let payload = await poller._buildBlockPayload(100);

            assert.ok(payload.data['sync_meta'], 'sync_meta present in indexer payload');
            assert.strictEqual(payload.data['sync_meta'].length, 1);
            let row = payload.data['sync_meta'][0];
            assert.strictEqual(row.block_index, 100);
            assert.strictEqual(row.block_time, 1700000000);
            assert.strictEqual(row.ledger_hash, 'lh');
            assert.strictEqual(row.actions_hash, 'ah');
            assert.strictEqual(row.contract_hash, 'ch');
        });

        it('ships state_hash NULL for burst-built blocks (viewTip ahead of B)', async function(){
            db.getBlockHashRow.resolves({
                block_index: 100, block_time: 1700000000,
                ledger_hash: 'lh', actions_hash: 'ah', contract_hash: 'ch', state_hash: 'sh'
            });
            // Catch-up burst: the pinned view's tip sits ahead of the block being
            // built, so updated_rows carry tip-state and the follower's apply-time
            // recompute at B must be skipped via the existing NULL gate.
            let payload = await poller._buildBlockPayload(100, null, 105);
            assert.strictEqual(payload.state_hash, null,
                'burst-built payload must ship state_hash NULL');
            assert.strictEqual(payload.ledger_hash, 'lh',
                'B-scoped hashes stay verified on the burst path');
        });

        it('keeps state_hash when the view tip IS the block (steady state) or is unknown', async function(){
            db.getBlockHashRow.resolves({
                block_index: 100, block_time: 1700000000,
                ledger_hash: 'lh', actions_hash: 'ah', contract_hash: 'ch', state_hash: 'sh'
            });
            let steady = await poller._buildBlockPayload(100, null, 100);
            assert.strictEqual(steady.state_hash, 'sh', 'steady-state payload keeps state_hash');
            let noTip = await poller._buildBlockPayload(100, null);
            assert.strictEqual(noTip.state_hash, 'sh', 'unknown view tip keeps state_hash');
        });

        it('ships state_root NULL for burst blocks but keeps balances/merkle roots (@regression)', async function(){
            // block 1000000 is past the state-commitment flag-day for bitcoin/mainnet.
            db.getBlockHashRow.resolves({
                block_index: 1000000, block_time: 1700000000,
                ledger_hash: 'lh', actions_hash: 'ah', contract_hash: 'ch', state_hash: 'sh'
            });
            // Catch-up burst: state_root folds the follower-recomputed stakes_root, which
            // is read from live tip-state stake tables during a burst, so it must be NULLed
            // exactly like state_hash. balances_root/block_merkle_root are B-scoped and stay.
            let burst = await poller._buildBlockPayload(1000000, null, 1000005);
            assert.strictEqual(burst.state_root, null,
                'burst-built payload must ship state_root NULL');
            assert.strictEqual(burst.balances_root, 'br',
                'balances_root stays verified on the burst path');
            assert.strictEqual(burst.block_merkle_root, 'bmr',
                'block_merkle_root stays verified on the burst path');

            let steady = await poller._buildBlockPayload(1000000, null, 1000000);
            assert.strictEqual(steady.state_root, 'sr',
                'steady-state payload keeps state_root');
        });

        it('omits sync_meta from the decoder payload', async function(){
            let decoderDb = createMockDb();
            decoderDb.dbType = 'decoder';
            decoderDb.getBlockHashRow.resolves({
                block_index: 100, block_time: 1700000000, block_hash: 'bh'
            });
            // Decoder has no transparency log.
            let decoderPoller = new ServerPoller('bitcoin', 'mainnet', decoderDb, broadcaster, null, config, util);

            let payload = await decoderPoller._buildBlockPayload(100);
            assert.strictEqual(payload.data['sync_meta'], undefined, 'decoder payload has no sync_meta');
            assert.strictEqual(payload.block_hash, 'bh');
        });

        it('includes block-scoped table rows in data', async function(){
            db.getBlockHashRow.resolves({
                block_index: 1, block_time: 100,
                ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c'
            });
            let blockRows = [{ block_index: 1, block_time: 100 }];
            db.getBlockScopedRows.resolves(blockRows);

            let payload = await poller._buildBlockPayload(1);
            assert.ok(payload.data['blocks']);
        });

        it('skips a per-table SCHEMA-GAP read error (errno 1146) and still builds the block @regression', async function(){
            db.getBlockHashRow.resolves({
                block_index: 1, block_time: 100,
                ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c'
            });
            let gap = new Error('table missing'); gap.errno = 1146;
            db.getBlockScopedRows.rejects(gap);
            db.getTransactions.resolves([]);
            db.getActions.resolves([]);

            let payload = await poller._buildBlockPayload(1);
            assert.ok(payload); // Should not be null
            assert.strictEqual(payload.type, 'block');
        });

        it('fails closed on a TRANSIENT per-table read error (deadlock 1213) so the block is retried, not broadcast incomplete @regression', async function(){
            db.getBlockHashRow.resolves({
                block_index: 1, block_time: 100,
                ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c'
            });
            let transient = new Error('Deadlock found when trying to get lock'); transient.errno = 1213;
            db.getBlockScopedRows.rejects(transient);
            db.getTransactions.resolves([]);
            db.getActions.resolves([]);

            let threw = false;
            try {
                await poller._buildBlockPayload(1);
            } catch(e){
                threw = true;
                assert.strictEqual(e.errno, 1213);
            }
            assert.ok(threw, 'a transient DB fault must propagate out of _buildBlockPayload');
        });

        it('fails closed on a TRANSIENT updated_rows collection error (deadlock 1213) so the block is retried, not broadcast without updated_rows @regression', async function(){
            db.getBlockHashRow.resolves({
                block_index: 1, block_time: 100,
                ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c'
            });
            db.getBlockScopedRows.resolves([]);
            db.getTransactions.resolves([]);
            db.getActions.resolves([]);
            // collectUpdatedRows runs its reads via db.doQuery and re-throws transients;
            // ServerPoller must NOT swallow that (a dropped in-place mutation forks followers).
            let transient = new Error('Deadlock found when trying to get lock'); transient.errno = 1213;
            db.doQuery.rejects(transient);

            let threw = false;
            try {
                await poller._buildBlockPayload(1);
            } catch(e){
                threw = true;
                assert.strictEqual(e.errno, 1213);
            }
            assert.ok(threw, 'a transient updated_rows fault must propagate out of _buildBlockPayload');
        });

        it('fetches index_transactions by referenced IDs', async function(){
            db.getBlockHashRow.resolves({
                block_index: 1, block_time: 100,
                ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c'
            });
            let blockRow = { block_index: 1, ledger_hash_id: 1, actions_hash_id: 2, contract_hash_id: 3 };
            db.getBlockScopedRows.callsFake(async (table) => {
                if(table === 'blocks') return [blockRow];
                return [];
            });
            db.getTransactions.resolves([{ tx_index: 1, source_id: 10, tx_hash_id: 5 }]);
            db.getActions.resolves([]);
            db.doQuery.resolves([{ id: 1, hash: 'abc' }]);

            let payload = await poller._buildBlockPayload(1);

            // Should have called doQuery for index_transactions with IDs 1,2,3,5
            let idxCall = db.doQuery.getCalls().find(c => c.args[0].includes('index_transactions'));
            assert.ok(idxCall);
        });

        it('fetches index_addresses by source_id from transactions', async function(){
            db.getBlockHashRow.resolves({
                block_index: 1, block_time: 100,
                ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c'
            });
            db.getBlockScopedRows.resolves([]);
            db.getTransactions.resolves([
                { tx_index: 1, source_id: 10 },
                { tx_index: 2, source_id: 10 }, // duplicate source_id
                { tx_index: 3, source_id: 20 }
            ]);
            db.getActions.resolves([]);
            db.doQuery.resolves([{ id: 10 }]);

            let payload = await poller._buildBlockPayload(1);

            let addrCall = db.doQuery.getCalls().find(c => c.args[0].includes('index_addresses'));
            assert.ok(addrCall);
            // Should deduplicate source_ids: [10, 20]
            assert.strictEqual(addrCall.args[1].length, 2);
        });

        it('extracts the remaining indexer index tables from referenced _id columns', async function(){
            db.getBlockHashRow.resolves({
                block_index: 1, block_time: 100,
                ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c'
            });
            db.getBlockScopedRows.resolves([]);
            db.getTransactions.resolves([{ tx_index: 1, source_id: 10 }]);
            // An action interning a brand-new action name (action_id) + status (status_id),
            // and a send interning a new ticker (tick_id): the mid-stream "new value" case.
            db.getActions.resolves([{ action_index: 1, action_id: 7, status_id: 3 }]);
            db.getActionScopedRows.callsFake(async (table) => {
                if(table === 'sends') return [{ action_index: 1, tick_id: 42, get_coin_id: 5 }];
                return [];
            });
            // Return a row only for the index tables actually queried by the generic pass.
            db.doQuery.callsFake(async (sql, params) => {
                if(sql.includes('index_actions'))   return [{ id: 7, action: 'NEWACTION' }];
                if(sql.includes('index_statuses'))  return [{ id: 3, status: 'valid' }];
                if(sql.includes('index_tickers'))   return [{ id: 42, tick: 'NEWTICK' }];
                if(sql.includes('index_coins'))     return [{ id: 5, coin: 'litecoin' }];
                return [];
            });

            let payload = await poller._buildBlockPayload(1);

            // The new interned rows ride the live block payload (previously snapshot-only).
            assert.deepStrictEqual(payload.data['index_actions'],  [{ id: 7, action: 'NEWACTION' }]);
            assert.deepStrictEqual(payload.data['index_statuses'], [{ id: 3, status: 'valid' }]);
            assert.deepStrictEqual(payload.data['index_tickers'],  [{ id: 42, tick: 'NEWTICK' }]);
            assert.deepStrictEqual(payload.data['index_coins'],    [{ id: 5, coin: 'litecoin' }]);

            // The generic pass must skip index_transactions (it carries block-hash/
            // tx-hash IDs the generic _id scan can't see, so it keeps its explicit
            // join). index_addresses, by contrast, IS intentionally re-fetched by the
            // generic pass over the full ref set, so a non-tx-interned address is
            // streamed at its intern block (without it the follower index map forks).
            let genericTables = db.doQuery.getCalls()
                .map(c => c.args[0])
                .filter(sql => /WHERE id IN/.test(sql));
            assert.ok(!genericTables.some(sql => sql.includes('`index_transactions`')),
                'generic pass should not re-query index_transactions');
            assert.ok(genericTables.some(sql => sql.includes('`index_addresses`')),
                'generic pass should re-fetch index_addresses over the full ref set');
        });

        it('does not run the generic index pass for the decoder', async function(){
            let decoderDb = createMockDb();
            decoderDb.dbType = 'decoder';
            decoderDb.getBlockHashRow.resolves({ block_index: 1, block_time: 100, block_hash: 'bh' });
            decoderDb.getTransactions.resolves([{ tx_index: 1, source_id: 10 }]);
            let decoderPoller = new ServerPoller('bitcoin', 'mainnet', decoderDb, broadcaster, null, config, util);

            await decoderPoller._buildBlockPayload(1);

            // Decoder must never query the indexer-only interning tables.
            let touchedIndexerOnly = decoderDb.doQuery.getCalls().some(c =>
                /index_(actions|statuses|tickers|fiats|coins|memos|mime_types|pubkeys)/.test(c.args[0]));
            assert.ok(!touchedIndexerOnly, 'decoder payload must not touch indexer-only index tables');
        });
    });

    describe('_updateStatus', function(){
        it('calls broadcaster.updateStatus with correct shape', async function(){
            poller.lastPolledBlock = 50;
            db.getBlockHashRow.resolves({
                block_index: 50, block_time: 1700000000,
                ledger_hash: 'lh', actions_hash: 'ah', contract_hash: 'ch'
            });

            await poller._updateStatus();

            assert.strictEqual(broadcaster.updateStatus.calledOnce, true);
            let args = broadcaster.updateStatus.firstCall.args;
            assert.strictEqual(args[0], 'bitcoin');
            assert.strictEqual(args[1], 'mainnet');
            assert.strictEqual(args[2].block_height, 50);
            assert.strictEqual(args[2].ledger_hash, 'lh');
        });

        it('handles null lastPolledBlock', async function(){
            poller.lastPolledBlock = null;
            await poller._updateStatus();
            let status = broadcaster.updateStatus.firstCall.args[2];
            assert.strictEqual(status.block_height, null);
            assert.strictEqual(status.block_time, null);
        });

        // Replication freshness. source_block_height falls back to
        // db.getLastBlock(), a MAX(block_index) against the SERVED database, so on
        // a node fronting a native SQL replica both heights freeze together when
        // replication stops applying and the derived lag reads 0. These pin the
        // fail-closed rules: only a confirmed primary or a confirmed in-window
        // replica may report fresh.
        describe('replication freshness', function(){
            function statusAfter(){ return broadcaster.updateStatus.firstCall.args[2]; }

            it('reports fresh on a primary (not a replica at all)', async function(){
                db.getReplicaStatus = sinon.stub().resolves({ isReplica: false, running: null, secondsBehind: null });
                await poller._updateStatus(100);
                assert.strictEqual(statusAfter().replica_stale, false);
                assert.strictEqual(statusAfter().replica_seconds_behind, null);
            });

            it('reports fresh on a replica inside the lag ceiling', async function(){
                config.SYNC_REPLICA_MAX_LAG_S = 120;
                db.getReplicaStatus = sinon.stub().resolves({ isReplica: true, running: true, secondsBehind: 5 });
                await poller._updateStatus(100);
                assert.strictEqual(statusAfter().replica_stale, false);
                assert.strictEqual(statusAfter().replica_seconds_behind, 5);
            });

            it('reports stale past the lag ceiling', async function(){
                config.SYNC_REPLICA_MAX_LAG_S = 120;
                db.getReplicaStatus = sinon.stub().resolves({ isReplica: true, running: true, secondsBehind: 900 });
                await poller._updateStatus(100);
                assert.strictEqual(statusAfter().replica_stale, true);
            });

            it('reports stale when the SQL thread stopped (Seconds_Behind NULL)', async function(){
                db.getReplicaStatus = sinon.stub().resolves({ isReplica: true, running: false, secondsBehind: null });
                await poller._updateStatus(100);
                assert.strictEqual(statusAfter().replica_stale, true,
                    'a stopped applier is unbounded lag; this is the failure that published lag 0');
            });

            it('fails closed when the replication status is unreadable', async function(){
                db.getReplicaStatus = sinon.stub().resolves({ isReplica: null, running: null, secondsBehind: null });
                await poller._updateStatus(100);
                assert.strictEqual(statusAfter().replica_stale, true);
            });

            it('fails closed when the read throws', async function(){
                db.getReplicaStatus = sinon.stub().rejects(new Error('boom'));
                await poller._updateStatus(100);
                assert.strictEqual(statusAfter().replica_stale, true);
            });
        });
    });

    describe('stop', function(){
        it('sets running to false', function(){
            poller.running = true;
            poller.stop();
            assert.strictEqual(poller.running, false);
        });
    });

    // Regression: deepdive H-P2. The live per-block payload used to read every
    // table (above all the updated_rows in-place-mutation channel) at the source's
    // CURRENT tip, so a surviving row mutated again right after block B streamed
    // its post-B state under B's payload and a strict follower's apply-time
    // recompute halted. The forward batch must be pinned to one REPEATABLE READ
    // snapshot and every payload read must observe that snapshot.
    describe('_poll snapshot pinning (H-P2)', function(){
        const HASH_ROW = {
            block_index: 100, block_time: 1700000000,
            ledger_hash: 'lh', actions_hash: 'ah', contract_hash: 'ch'
        };

        it('pins the forward batch to one read snapshot and threads it through every payload read', async function(){
            poller.lastPolledBlock = 99;
            db.getLastBlock.resolves(100);
            db.getBlockHashRow.resolves(HASH_ROW);
            const snap = { mockSnapshotConn: true };
            db.beginReadSnapshot.resolves(snap);

            await poller._poll();

            assert.strictEqual(db.beginReadSnapshot.calledOnce, true);
            assert.strictEqual(db.commitReadSnapshot.calledOnce, true);
            assert.strictEqual(db.commitReadSnapshot.firstCall.args[0], snap);
            // The batch tip is re-read INSIDE the snapshot (its view is the authority).
            assert.ok(db.getLastBlock.getCalls().some(c => c.args[0] === snap),
                'getLastBlock must be re-read on the snapshot connection');
            // Every payload read observes the pinned view. (The FIRST getBlockHashRow
            // call is the payload build; _updateStatus later re-reads it unpinned.)
            assert.strictEqual(db.getBlockHashRow.firstCall.args[1], snap);
            for(const call of db.getBlockScopedRows.getCalls())
                assert.strictEqual(call.args[2], snap);
            for(const call of db.getActionScopedRows.getCalls())
                assert.strictEqual(call.args[2], snap);
            for(const call of db.getTransactions.getCalls())
                assert.strictEqual(call.args[1], snap);
            for(const call of db.getActions.getCalls())
                assert.strictEqual(call.args[1], snap);
            // Raw queries too: this covers the updated_rows channel (collectUpdatedRows
            // passes its conn straight through as doQuery's third argument).
            assert.ok(db.doQuery.getCalls().length > 0, 'payload build must issue raw queries');
            for(const call of db.doQuery.getCalls())
                assert.strictEqual(call.args[2], snap);
        });

        it('releases the snapshot even when payload building throws', async function(){
            poller.lastPolledBlock = 99;
            db.getLastBlock.resolves(100);
            db.getBlockHashRow.rejects(new Error('boom'));

            await assert.rejects(() => poller._poll(), /boom/);

            assert.strictEqual(db.beginReadSnapshot.calledOnce, true);
            assert.strictEqual(db.commitReadSnapshot.calledOnce, true);
        });

        it('streams to the snapshot tip when a block landed between the outer read and the snapshot open', async function(){
            poller.lastPolledBlock = 99;
            db.getLastBlock.onFirstCall().resolves(100);   // pre-snapshot read
            db.getLastBlock.onSecondCall().resolves(101);  // view inside the snapshot
            db.getBlockHashRow.resolves(HASH_ROW);

            await poller._poll();

            assert.strictEqual(poller.lastPolledBlock, 101);
            assert.strictEqual(broadcaster.broadcast.callCount, 2);
        });

        it('never streams past the snapshot tip when it sits behind the outer read', async function(){
            poller.lastPolledBlock = 99;
            db.getLastBlock.onFirstCall().resolves(105);   // pre-snapshot read
            db.getLastBlock.onSecondCall().resolves(100);  // snapshot raced a reorg
            db.getBlockHashRow.resolves(HASH_ROW);

            await poller._poll();

            assert.strictEqual(poller.lastPolledBlock, 100,
                'a block the snapshot cannot see must wait for the next poll');
            assert.strictEqual(broadcaster.broadcast.callCount, 1);
        });
    });
});
