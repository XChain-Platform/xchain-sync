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
const sinon = require('sinon');
const ServerPoller = require('../../../src/server/poller');
const seedReorgWindow = require('../../../src/server/poller/reorg_window_seed');

function createBroadcaster(){
    return {
        broadcast: sinon.stub(),
        updateStatus: sinon.stub(),
        getSubscriberCount: sinon.stub().returns(0)
    };
}

function createLog(){
    return {
        getHighWaterMark: sinon.stub().resolves(null),
        getRecordedHash: sinon.stub().resolves(null),
        pruneFrom: sinon.stub().resolves()
    };
}

function createPoller(dbType = 'indexer'){
    const db = {
        dbType,
        getLastBlock: sinon.stub().resolves(null),
        getBlockHashRow: sinon.stub().resolves(null)
    };
    const broadcaster = createBroadcaster();
    const log = dbType === 'decoder' ? null : createLog();
    const util = { sleep: sinon.stub().resolves(), logError: sinon.stub() };
    const poller = new ServerPoller('bitcoin', 'mainnet', db, broadcaster, log,
        { BLOCK_POLL_INTERVAL: 0 }, util);
    return { poller, db, broadcaster, log };
}

async function startOnce(poller){
    sinon.stub(poller, 'backfillGaps').resolves(0);
    sinon.stub(poller, 'updateStatus').resolves();
    const poll = sinon.stub(poller, 'poll').callsFake(async () => {
        poller.stop();
        return 0;
    });
    await poller.start();
    poll.restore();
}

async function testRestartReorg(){
    const { poller, db, broadcaster, log } = createPoller();
    const cursor = 20;
    poller.recentHashCap = 6;
    log.getHighWaterMark.resolves(cursor);
    // Recorded identities are ledger|actions|contract, the form getRecordedHash returns.
    log.getRecordedHash.callsFake(async blockIndex => 'old-' + blockIndex + '|a|c');

    await startOnce(poller);

    assert.deepStrictEqual([...poller.recentBroadcastHashes.entries()], [
        [15, 'old-15|a|c'], [16, 'old-16|a|c'], [17, 'old-17|a|c'],
        [18, 'old-18|a|c'], [19, 'old-19|a|c'], [20, 'old-20|a|c']
    ]);

    db.getLastBlock.resolves(cursor);
    db.getBlockHashRow.callsFake(async blockIndex => ({
        block_index: blockIndex,
        ledger_hash: blockIndex >= 18 ? 'new-' + blockIndex : 'old-' + blockIndex,
        actions_hash: 'a', contract_hash: 'c'
    }));

    await poller.poll();

    assert.strictEqual(broadcaster.broadcast.calledOnce, true);
    assert.strictEqual(broadcaster.broadcast.firstCall.args[2].type, 'reorg');
    assert.strictEqual(broadcaster.broadcast.firstCall.args[2].block_index, 18);
    assert.strictEqual(poller.lastPolledBlock, 17);
}

async function testMissingHash(){
    const { poller, log } = createPoller();
    poller.lastPolledBlock = 20;
    poller.recentHashCap = 6;
    log.getRecordedHash.callsFake(async blockIndex => blockIndex === 17 ? null : 'old-' + blockIndex);
    const logger = { warn: sinon.spy() };

    await seedReorgWindow(poller, logger);

    assert.deepStrictEqual([...poller.recentBroadcastHashes.entries()], [
        [18, 'old-18'], [19, 'old-19'], [20, 'old-20']
    ]);
    assert.deepStrictEqual(log.getRecordedHash.getCalls().map(call => call.args[0]), [20, 19, 18, 17]);
    assert.strictEqual(logger.warn.calledOnce, true);
}

async function testDecoderHashes(){
    const { poller, db } = createPoller('decoder');
    poller.lastPolledBlock = 8;
    poller.recentHashCap = 3;
    db.getBlockHashRow.callsFake(async blockIndex => ({ block_hash: 'block-' + blockIndex }));

    await seedReorgWindow(poller, { warn: sinon.spy() });

    assert.deepStrictEqual([...poller.recentBroadcastHashes.entries()], [
        [6, 'block-6'], [7, 'block-7'], [8, 'block-8']
    ]);
}

describe('ServerPoller restart reorg-window seed @regression', function(){
    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'warn');
        sinon.stub(console, 'error');
    });

    afterEach(function(){
        sinon.restore();
    });

    it('seeds the durable window on restart and resolves a three-block reorg at the true fork', testRestartReorg);
    it('keeps only the newest contiguous suffix when a durable hash is missing', testMissingHash);
    it('seeds decoder hashes from sourceBlockHash without a transparency log', testDecoderHashes);
});
