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
const ServerPoller = require('../../../src/ServerPoller');
const Utility = require('../../../src/utility');

function createMockDb(){
    return {
        getLastBlock: sinon.stub().resolves(null),
        getBlockHashRow: sinon.stub().resolves(null),
        getBlockScopedRows: sinon.stub().resolves([]),
        getTxScopedRows: sinon.stub().resolves([]),
        getActionScopedRows: sinon.stub().resolves([]),
        getEmissionRowsForBlock: sinon.stub().resolves([]),
        getTransactions: sinon.stub().resolves([]),
        getActions: sinon.stub().resolves([]),
        getStatusId: sinon.stub().resolves(null),
        doQuery: sinon.stub().resolves([]),
        beginReadSnapshot: sinon.stub().resolves({ mockSnapshotConn: true }),
        commitReadSnapshot: sinon.stub().resolves(),
        rollbackReadSnapshot: sinon.stub().resolves()
    };
}

describe('Boundary: Reorg Detection', function(){

    let poller, db, broadcaster;

    beforeEach(function(){
        db = createMockDb();
        broadcaster = { broadcast: sinon.stub(), updateStatus: sinon.stub(), getSubscribers: sinon.stub().returns([]), getSubscriberCount: sinon.stub().returns(0) };
        let log = { recordBlock: sinon.stub().resolves(), pruneFrom: sinon.stub().resolves() };
        poller = new ServerPoller('bitcoin', 'mainnet', db, broadcaster, log, { BLOCK_POLL_INTERVAL: 100 }, new Utility());
        db.getBlockHashRow.callsFake(async (idx) => ({
            block_index: idx, block_time: idx * 10,
            ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c'
        }));
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });

    afterEach(function(){ sinon.restore(); });

    it('no change (currentBlock === lastPolledBlock): no-op', async function(){
        poller.lastPolledBlock = 10;
        db.getLastBlock.resolves(10);
        await poller._poll();
        assert.strictEqual(broadcaster.broadcast.called, false);
        assert.strictEqual(poller.lastPolledBlock, 10);
    });

    it('one new block: no reorg', async function(){
        poller.lastPolledBlock = 10;
        db.getLastBlock.resolves(11);
        await poller._poll();
        let event = broadcaster.broadcast.firstCall.args[2];
        assert.strictEqual(event.type, 'block');
        assert.strictEqual(poller.lastPolledBlock, 11);
    });

    it('one block rollback: reorg detected', async function(){
        poller.lastPolledBlock = 10;
        db.getLastBlock.resolves(9);
        await poller._poll();
        let event = broadcaster.broadcast.firstCall.args[2];
        assert.strictEqual(event.type, 'reorg');
        assert.strictEqual(event.block_index, 10); // currentBlock + 1
        assert.strictEqual(poller.lastPolledBlock, 9);
    });

    it('deep rollback (10 blocks): reorg at correct index', async function(){
        poller.lastPolledBlock = 100;
        db.getLastBlock.resolves(90);
        await poller._poll();
        let event = broadcaster.broadcast.firstCall.args[2];
        assert.strictEqual(event.type, 'reorg');
        assert.strictEqual(event.block_index, 91); // currentBlock + 1
        assert.strictEqual(poller.lastPolledBlock, 90);
    });

    it('mid-rewrite height drop: walks back to the true fork point', async function(){
        // The source is observed mid-rewrite: the tip dropped from 10 to 8, but the rewrite
        // actually forked at 5, so blocks 5-8 are already replacements. A height-only check
        // would broadcast reorg@9 (too shallow), leaving followers on stale blocks 5-8 until a
        // later poll caught the deeper rewrite, so the walk-back must resolve fork=5 in this poll.
        poller.lastPolledBlock = 10;
        db.getLastBlock.resolves(8);
        // Recorded broadcast hashes: 4 matches the live source ('l'), 5..10 were broadcast pre-reorg with a different content hash.
        poller.recentBroadcastHashes.set(4, 'l');
        for(let bi = 5; bi <= 10; bi++) poller.recentBroadcastHashes.set(bi, 'pre-reorg');
        await poller._poll();
        let event = broadcaster.broadcast.firstCall.args[2];
        assert.strictEqual(event.type, 'reorg');
        assert.strictEqual(event.block_index, 5);
        assert.strictEqual(poller.lastPolledBlock, 4);
        // Guard re-seeded from the recorded (still matching) hash at the fork parent.
        assert.strictEqual(poller.lastPolledBlockHash, 'l');
        assert.strictEqual(poller.transparencyLog.pruneFrom.calledOnceWithExactly(5), true);
    });

    it('currentBlock = null (all blocks deleted): early return', async function(){
        poller.lastPolledBlock = 10;
        db.getLastBlock.resolves(null);
        await poller._poll();
        assert.strictEqual(broadcaster.broadcast.called, false);
        assert.strictEqual(poller.lastPolledBlock, 10); // unchanged
    });

    it('first poll (lastPolledBlock = null): initializes without processing', async function(){
        poller.lastPolledBlock = null;
        db.getLastBlock.resolves(50);
        await poller._poll();
        assert.strictEqual(poller.lastPolledBlock, 50);
        assert.strictEqual(broadcaster.broadcast.called, false); // no block broadcasts
        assert.strictEqual(broadcaster.updateStatus.calledOnce, true);
    });

    it('first poll with empty DB: remains null', async function(){
        poller.lastPolledBlock = null;
        db.getLastBlock.resolves(null);
        await poller._poll();
        assert.strictEqual(poller.lastPolledBlock, null);
        assert.strictEqual(broadcaster.broadcast.called, false);
    });

    it('same-height non-detection: no event when data changes at same block', async function(){
        poller.lastPolledBlock = 10;
        db.getLastBlock.resolves(10);
        // Even if underlying data changed at block 10, poller does not detect it
        await poller._poll();
        assert.strictEqual(broadcaster.broadcast.called, false);
    });
});
