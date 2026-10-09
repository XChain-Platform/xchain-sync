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
    markers = markers.map(row => ({ code: 'REORG', ...row }));
    let newestEventId = markers.length === 0 ? null :
        Math.max(...markers.map(row => Number(row.id)));
    let db = {
        getLastBlock: sinon.stub().resolves(100),
        getBlockHashRow: sinon.stub().callsFake(async blockIndex => hashRow(blockIndex)),
        getMaxRowId: sinon.stub().resolves(newestEventId),
        getContentIdWindowRows: sinon.stub().resolves(markers)
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

describe('ServerPoller indexer REORG marker rewind @regression', function(){
    afterEach(function(){ sinon.restore(); });

    it('rewinds when all three block hashes are unchanged', async function(){
        let context = createPoller([{ id: 7, data: JSON.stringify({ block_index: 100 }) }]);

        await context.poller.poll();

        assert.ok(context.transparencyLog.pruneFrom.calledOnceWithExactly(100));
        assert.ok(context.broadcaster.broadcast.calledWith('bitcoin', 'mainnet', sinon.match({
            type: 'reorg', block_index: 100
        })));
        assert.strictEqual(context.poller.lastPolledBlock, 99);
        assert.strictEqual(context.poller.lastEventId, 7);
    });

    it('does not process the same marker twice', async function(){
        let context = createPoller([{ id: 7, data: JSON.stringify({ block_index: 100 }) }]);
        await context.poller.handleReorgMarkers();

        assert.strictEqual(await context.poller.handleReorgMarkers(), false);
        assert.strictEqual(context.transparencyLog.pruneFrom.callCount, 1);
    });

    it('does not acknowledge a marker when transparency pruning fails', async function(){
        let context = createPoller([{ id: 7, data: JSON.stringify({ block_index: 100 }) }]);
        context.poller.lastEventId = 6;
        context.transparencyLog.pruneFrom.rejects(new Error('prune failed'));

        await assert.rejects(() => context.poller.handleReorgMarkers(), /prune failed/);

        assert.strictEqual(context.poller.lastEventId, 6);
        assert.strictEqual(context.poller.lastPolledBlock, 100);
        assert.strictEqual(context.broadcaster.broadcast.called, false);
    });
});

describe('ServerPoller indexer REORG marker cursor @regression', function(){
    afterEach(function(){ sinon.restore(); });

    it('uses the deepest fork when more than one marker arrives between polls', async function(){
        let context = createPoller([
            { id: 8, data: JSON.stringify({ block_index: 99 }) },
            { id: 9, data: JSON.stringify({ block_index: 97 }) }
        ]);
        context.poller.lastEventId = 7;

        await context.poller.handleReorgMarkers();

        assert.ok(context.transparencyLog.pruneFrom.calledOnceWithExactly(97));
        assert.strictEqual(context.poller.lastPolledBlock, 96);
        assert.strictEqual(context.poller.lastEventId, 9);
        assert.ok(context.db.getContentIdWindowRows.calledOnceWithExactly('events', 7, 9));
    });

    it('never probes indexer markers for a decoder poller', async function(){
        let context = createPoller([]);
        context.poller.dbType = 'decoder';

        assert.deepStrictEqual(await context.poller.readNewReorgMarkers(), {
            markers: [], newestEventId: null
        });
        assert.strictEqual(context.db.getMaxRowId.called, false);
        assert.strictEqual(context.db.getContentIdWindowRows.called, false);
    });

    it('seeds the marker cursor without replaying history at startup', async function(){
        let context = createPoller([{ id: 12 }]);
        context.poller.reorgMarkerCursorInitialized = false;

        await context.poller.initializeReorgMarkerCursor();

        assert.strictEqual(context.poller.lastEventId, 12);
        assert.strictEqual(context.poller.reorgMarkerCursorInitialized, true);
        assert.ok(context.db.getMaxRowId.calledOnceWithExactly('events'));
        assert.strictEqual(context.broadcaster.broadcast.called, false);
    });

    it('advances past unrelated events without broadcasting a reorg', async function(){
        let context = createPoller([{ id: 13, code: 'NOTICE', data: '{}' }]);
        context.poller.lastEventId = 12;

        assert.strictEqual(await context.poller.handleReorgMarkers(), false);

        assert.strictEqual(context.poller.lastEventId, 13);
        assert.strictEqual(context.transparencyLog.pruneFrom.called, false);
        assert.strictEqual(context.broadcaster.broadcast.called, false);
    });
});
