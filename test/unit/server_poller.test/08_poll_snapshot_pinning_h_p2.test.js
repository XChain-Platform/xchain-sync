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
const ServerPoller = require('../../../src/server/poller');
const Utility = require('../../../src/util');
const { withDbMixins } = require('../../helpers/db_mixins.js');

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

    // Reading every table in the live per-block payload at the source's CURRENT
    // tip can observe a surviving row after another mutation. Streaming that
    // post-B state under block B makes a strict follower's apply-time recompute
    // halt. The forward batch must be pinned to one REPEATABLE READ snapshot and
    // every payload read must observe that snapshot.
    describe('poll snapshot pinning (H-P2)', function(){
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

            await poller.poll();

            assert.strictEqual(db.beginReadSnapshot.calledOnce, true);
            assert.strictEqual(db.commitReadSnapshot.calledOnce, true);
            assert.strictEqual(db.commitReadSnapshot.firstCall.args[0], snap);
            // The batch tip is re-read INSIDE the snapshot (its view is the authority).
            assert.ok(db.getLastBlock.getCalls().some(c => c.args[0] === snap),
                'getLastBlock must be re-read on the snapshot connection');
            // Every payload read observes the pinned view. (The FIRST getBlockHashRow
            // call is the payload build; updateStatus later re-reads it unpinned.)
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
    });

    describe('poll snapshot pinning (H-P2)', function(){
        const HASH_ROW = {
            block_index: 100, block_time: 1700000000,
            ledger_hash: 'lh', actions_hash: 'ah', contract_hash: 'ch'
        };

        it('releases the snapshot even when payload building throws', async function(){
            poller.lastPolledBlock = 99;
            db.getLastBlock.resolves(100);
            db.getBlockHashRow.rejects(new Error('boom'));

            await assert.rejects(() => poller.poll(), /boom/);

            assert.strictEqual(db.beginReadSnapshot.calledOnce, true);
            assert.strictEqual(db.commitReadSnapshot.calledOnce, true);
        });

        it('streams to the snapshot tip when a block landed between the outer read and the snapshot open', async function(){
            poller.lastPolledBlock = 99;
            db.getLastBlock.onFirstCall().resolves(100);   // pre-snapshot read
            db.getLastBlock.onSecondCall().resolves(101);  // view inside the snapshot
            db.getBlockHashRow.resolves(HASH_ROW);

            await poller.poll();

            assert.strictEqual(poller.lastPolledBlock, 101);
            assert.strictEqual(broadcaster.broadcast.callCount, 2);
        });

        it('never streams past the snapshot tip when it sits behind the outer read', async function(){
            poller.lastPolledBlock = 99;
            db.getLastBlock.onFirstCall().resolves(105);   // pre-snapshot read
            db.getLastBlock.onSecondCall().resolves(100);  // snapshot raced a reorg
            db.getBlockHashRow.resolves(HASH_ROW);

            await poller.poll();

            assert.strictEqual(poller.lastPolledBlock, 100,
                'a block the snapshot cannot see must wait for the next poll');
            assert.strictEqual(broadcaster.broadcast.callCount, 1);
        });
    });
});
