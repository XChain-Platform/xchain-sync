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

function hashRow(blockIndex){
    return {
        block_index: blockIndex,
        block_time: blockIndex,
        ledger_hash: 'ledger',
        actions_hash: 'actions',
        contract_hash: 'contract'
    };
}

function createPoller(markers){
    let db = {
        getLastBlock: sinon.stub().resolves(100),
        getBlockHashRow: sinon.stub().callsFake(async blockIndex => hashRow(blockIndex)),
        doQuery: sinon.stub().callsFake(async sql => /FROM events/.test(sql) ? markers : [])
    };
    let broadcaster = {
        broadcast: sinon.stub(),
        updateStatus: sinon.stub(),
        getSubscriberCount: sinon.stub().returns(0)
    };
    let transparencyLog = { pruneFrom: sinon.stub().resolves() };
    let poller = new ServerPoller('bitcoin', 'mainnet', db, broadcaster,
        transparencyLog, { BLOCK_POLL_INTERVAL: 100 }, { sleep: sinon.stub().resolves() });
    poller.lastPolledBlock = 100;
    poller.lastPolledBlockHash = 'ledger|actions|contract';
    poller.reorgMarkerCursorInitialized = true;
    return { poller, db, broadcaster, transparencyLog };
}

describe('ServerPoller indexer REORG marker @regression', function(){
    afterEach(function(){ sinon.restore(); });

    it('rewinds when all three block hashes are unchanged', async function(){
        let context = createPoller([{ id: 7, data: JSON.stringify({ block_index: 100 }) }]);

        await context.poller.poll();

        assert.ok(context.transparencyLog.pruneFrom.calledOnceWithExactly(100));
        assert.ok(context.broadcaster.broadcast.calledWith('bitcoin', 'mainnet', sinon.match({
            type: 'reorg', block_index: 100
        })));
        assert.strictEqual(context.poller.lastPolledBlock, 99);
        assert.strictEqual(context.poller.lastReorgMarkerId, 7);
    });

    it('does not process the same marker twice', async function(){
        let context = createPoller([{ id: 7, data: JSON.stringify({ block_index: 100 }) }]);
        await context.poller.handleReorgMarkers();
        context.db.doQuery.resolves([]);

        assert.strictEqual(await context.poller.handleReorgMarkers(), false);
        assert.strictEqual(context.transparencyLog.pruneFrom.callCount, 1);
    });

    it('does not acknowledge a marker when transparency pruning fails', async function(){
        let context = createPoller([{ id: 7, data: JSON.stringify({ block_index: 100 }) }]);
        context.poller.lastReorgMarkerId = 6;
        context.transparencyLog.pruneFrom.rejects(new Error('prune failed'));

        await assert.rejects(() => context.poller.handleReorgMarkers(), /prune failed/);

        assert.strictEqual(context.poller.lastReorgMarkerId, 6);
        assert.strictEqual(context.poller.lastPolledBlock, 100);
        assert.strictEqual(context.broadcaster.broadcast.called, false);
    });

    it('uses the deepest fork when more than one marker arrives between polls', async function(){
        let context = createPoller([
            { id: 8, data: JSON.stringify({ block_index: 99 }) },
            { id: 9, data: JSON.stringify({ block_index: 97 }) }
        ]);
        context.poller.lastReorgMarkerId = 7;

        await context.poller.handleReorgMarkers();

        assert.ok(context.transparencyLog.pruneFrom.calledOnceWithExactly(97));
        assert.strictEqual(context.poller.lastPolledBlock, 96);
        assert.strictEqual(context.poller.lastReorgMarkerId, 9);
        assert.deepStrictEqual(context.db.doQuery.firstCall.args[1], [7]);
    });

    it('never probes indexer markers for a decoder poller', async function(){
        let context = createPoller([]);
        context.poller.dbType = 'decoder';

        assert.deepStrictEqual(await context.poller.readNewReorgMarkers(), []);
        assert.strictEqual(context.db.doQuery.called, false);
    });

    it('seeds the marker cursor without replaying history at startup', async function(){
        let context = createPoller([{ id: 12 }]);
        context.poller.reorgMarkerCursorInitialized = false;

        await context.poller.initializeReorgMarkerCursor();

        assert.strictEqual(context.poller.lastReorgMarkerId, 12);
        assert.strictEqual(context.poller.reorgMarkerCursorInitialized, true);
        assert.match(context.db.doQuery.firstCall.args[0], /ORDER BY id DESC LIMIT 1/);
        assert.strictEqual(context.broadcaster.broadcast.called, false);
    });
});
