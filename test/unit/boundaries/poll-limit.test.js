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

function createMockBroadcaster(){
    return { broadcast: sinon.stub(), updateStatus: sinon.stub(), getSubscribers: sinon.stub().returns([]), getSubscriberCount: sinon.stub().returns(0) };
}

function createMockLog(){
    return { recordBlock: sinon.stub().resolves(), pruneFrom: sinon.stub().resolves() };
}

describe('Boundary: Poll Loop Limit (100 blocks)', function(){

    let poller, db, broadcaster, log;

    beforeEach(function(){
        db = createMockDb();
        broadcaster = createMockBroadcaster();
        log = createMockLog();
        let util = new Utility();
        poller = new ServerPoller('bitcoin', 'mainnet', db, broadcaster, log, { BLOCK_POLL_INTERVAL: 100 }, util);
        db.getBlockHashRow.callsFake(async (idx) => ({
            block_index: idx, block_time: idx * 10,
            ledger_hash: 'l' + idx, actions_hash: 'a' + idx, contract_hash: 'c' + idx
        }));
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });

    afterEach(function(){ sinon.restore(); });

    it('processes 0 blocks when at current (no-op)', async function(){
        poller.lastPolledBlock = 50;
        db.getLastBlock.resolves(50);
        await poller._poll();
        assert.strictEqual(broadcaster.broadcast.callCount, 0);
        assert.strictEqual(poller.lastPolledBlock, 50);
    });

    it('processes 1 new block', async function(){
        poller.lastPolledBlock = 49;
        db.getLastBlock.resolves(50);
        await poller._poll();
        assert.strictEqual(broadcaster.broadcast.callCount, 1);
        assert.strictEqual(poller.lastPolledBlock, 50);
    });

    it('processes exactly 99 blocks in one poll', async function(){
        poller.lastPolledBlock = 1;
        db.getLastBlock.resolves(100);
        await poller._poll();
        assert.strictEqual(broadcaster.broadcast.callCount, 99);
        assert.strictEqual(poller.lastPolledBlock, 100);
    });

    it('processes exactly 100 blocks in one poll (at limit)', async function(){
        poller.lastPolledBlock = 0;
        db.getLastBlock.resolves(100);
        await poller._poll();
        assert.strictEqual(broadcaster.broadcast.callCount, 100);
        assert.strictEqual(poller.lastPolledBlock, 100);
    });

    it('caps at 100 blocks when 101 available', async function(){
        poller.lastPolledBlock = 0;
        db.getLastBlock.resolves(101);
        await poller._poll();
        assert.strictEqual(broadcaster.broadcast.callCount, 100);
        assert.strictEqual(poller.lastPolledBlock, 100);
    });

    it('processes remaining 1 block on second poll after cap', async function(){
        poller.lastPolledBlock = 0;
        db.getLastBlock.resolves(101);
        await poller._poll(); // processes 1–100
        assert.strictEqual(poller.lastPolledBlock, 100);

        broadcaster.broadcast.resetHistory();
        await poller._poll(); // processes 101
        assert.strictEqual(broadcaster.broadcast.callCount, 1);
        assert.strictEqual(poller.lastPolledBlock, 101);
    });

    it('caps at 100 blocks when 200 available', async function(){
        poller.lastPolledBlock = 0;
        db.getLastBlock.resolves(200);
        await poller._poll();
        assert.strictEqual(broadcaster.broadcast.callCount, 100);
        assert.strictEqual(poller.lastPolledBlock, 100);
    });
});
