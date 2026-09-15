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
const ServerPoller = require('../../src/server/poller');
const Utility = require('../../src/util');
const { withDbMixins } = require('../helpers/db_mixins.js');

function createMockDb(){
    // Queries read through named Database methods. The real ones are installed for
    // any this fake does not stub, so they still reach doQuery below and every
    // doQuery call count these suites assert keeps counting them.
    return withDbMixins({
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
    });
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
            // ROLLCALL epoch closes and their pinned absences. Block-scoped for
            // REPLICATION, but keyed by close_block on BOTH dimensions, which is why the
            // rollback is bespoke and why the registry declares blockKey. Membership is
            // NOT delivery: while the reader assumed block_index these two raised errno
            // 1054 on every poll and were dropped from the payload in silence, so the
            // read the membership drives is asserted separately below.
            assert.ok(poller.blockScopedTables.includes('rollcalls'));
            assert.ok(poller.blockScopedTables.includes('rollcall_absences'));
            // The gates each verified signer re-signed at a rolled epoch: same close_block
            // key, same bespoke rollback, written by the same close.
            assert.ok(poller.blockScopedTables.includes('rollcall_gates'));
            assert.strictEqual(poller.blockScopedTables.length, 14);
            // The read those two names drive is asserted in db.test.js
            // (Database.getBlockScopedRows), because that is where it can fail.
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

    describe('poll', function(){
        it('returns early when no blocks in DB', async function(){
            db.getLastBlock.resolves(null);
            await poller.poll();
            assert.strictEqual(broadcaster.broadcast.called, false);
        });

        it('initializes lastPolledBlock on first poll', async function(){
            db.getLastBlock.resolves(100);
            poller.lastPolledBlock = null;
            await poller.poll();
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

            await poller.poll();

            assert.strictEqual(log.recordBlock.calledOnce, true);
            assert.strictEqual(broadcaster.broadcast.calledOnce, true);
            let event = broadcaster.broadcast.firstCall.args[2];
            assert.strictEqual(event.type, 'block');
            assert.strictEqual(event.block_index, 100);
            assert.strictEqual(event.chain, 'bitcoin');
            assert.strictEqual(event.network, 'mainnet');
            assert.strictEqual(poller.lastPolledBlock, 100);
        });

    });

    describe('poll', function(){
        it('does nothing when currentBlock equals lastPolledBlock', async function(){
            poller.lastPolledBlock = 100;
            db.getLastBlock.resolves(100);
            await poller.poll();
            assert.strictEqual(broadcaster.broadcast.called, false);
            assert.strictEqual(log.recordBlock.called, false);
        });

        // The failure the replication verdict exists to catch is exactly the one that
        // stops block advancement: a native SQL replica that stops applying freezes the
        // served tip, so a refresh gated on blocksProcessed > 0 never runs again and the
        // last healthy verdict is republished forever.
        it('re-evaluates the replica verdict on idle polls', async function(){
            poller.lastPolledBlock = 100;
            db.getLastBlock.resolves(100);
            db.getBlockHashRow.resolves({
                block_index: 100, block_time: 1700000000,
                ledger_hash: 'lh', actions_hash: 'ah', contract_hash: 'ch'
            });
            db.getReplicaStatus = sinon.stub().resolves({ isReplica: true, running: true, secondsBehind: 5 });

            await poller.poll();
            assert.strictEqual(broadcaster.updateStatus.callCount, 1);
            assert.strictEqual(broadcaster.updateStatus.lastCall.args[2].replica_stale, false);

            // Replication stops applying. The served tip is frozen from here on, so no
            // later poll ever processes a block.
            db.getReplicaStatus.resolves({ isReplica: true, running: false, secondsBehind: null });

            await poller.poll();
            assert.strictEqual(broadcaster.broadcast.called, false);
            assert.strictEqual(broadcaster.updateStatus.callCount, 2);
            assert.strictEqual(broadcaster.updateStatus.lastCall.args[2].replica_stale, true);
            assert.strictEqual(broadcaster.updateStatus.lastCall.args[2].block_height, 100);
        });

        it('limits to 100 blocks per poll', async function(){
            poller.lastPolledBlock = 0;
            db.getLastBlock.resolves(200);
            db.getBlockHashRow.resolves({
                block_index: 1, block_time: 100,
                ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c'
            });

            await poller.poll();

            assert.strictEqual(broadcaster.broadcast.callCount, 100);
            assert.strictEqual(poller.lastPolledBlock, 100);
        });

    });

    describe('poll', function(){
        it('detects reorg and broadcasts reorg event', async function(){
            poller.lastPolledBlock = 100;
            db.getLastBlock.resolves(95);
            db.getBlockHashRow.resolves({
                block_index: 95, block_time: 100,
                ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c'
            });

            await poller.poll();

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

            await poller.poll();

            assert.strictEqual(broadcaster.broadcast.calledOnce, true);
            let event = broadcaster.broadcast.firstCall.args[2];
            assert.strictEqual(event.type, 'reorg');
            assert.strictEqual(event.block_index, 100); // the divergent block itself
            assert.strictEqual(poller.lastPolledBlock, 99); // rolled back one for the next-poll walk-back
        });

    });

    describe('poll', function(){
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
            await poller.poll();
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

            await poller.poll();

            assert.strictEqual(broadcaster.broadcast.called, false);
        });

    });

    describe('poll', function(){
        it('prunes the source transparency log on reorg (to currentBlock + 1)', async function(){
            poller.lastPolledBlock = 100;
            db.getLastBlock.resolves(95);
            db.getBlockHashRow.resolves({
                block_index: 95, block_time: 100,
                ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c'
            });

            await poller.poll();

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

            await decoderPoller.poll();  // must not throw despite transparencyLog === null

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

            await poller.poll();

            assert.strictEqual(broadcaster.broadcast.callCount, 3); // blocks 98, 99, 100
            assert.strictEqual(poller.lastPolledBlock, 100);
        });
    });
});
