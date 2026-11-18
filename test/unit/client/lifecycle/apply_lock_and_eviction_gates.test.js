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
const EventEmitter = require('events');
const sinon = require('sinon');
const ClientSync = require('../../../../src/client/sync');
const Utility = require('../../../../src/util');
const HashVerifier = require('../../../../src/client/hash_verifier');

function makeSync(sources){
    const syncState = new Map();
    const db = {
        dbName: 'test_db', dbType: 'indexer',
        getLastBlock: sinon.stub().resolves(10),
        getBlockHashRow: sinon.stub().resolves({ block_hash: 'h' }),
        getActiveHalt: sinon.stub().resolves(null),
        doQuery: sinon.stub().resolves([]),
        getSyncState: sinon.stub().callsFake(async key => (syncState.has(key) ? syncState.get(key) : null)),
        setSyncState: sinon.stub().callsFake(async (key, value) => { syncState.set(key, value); }),
        recordHalt: sinon.stub().resolves({ block_index: 0 })
    };
    const applier = { applyBlock: sinon.stub().resolves(true), applyFullSnapshot: sinon.stub().resolves() };
    const config = {
        SYNC_SOURCES: sources || 'http://a:3006', VERIFY_HASHES: false,
        VERIFY_RECOMPUTE: false, VERIFY_STATE_HASH: false, VERIFY_STATE_COMMITMENT: false,
        CLIENT_RECONNECT_DELAY: 100, HASH_CONFIRM_TIMEOUT: 5000,
        MAX_ROLLBACK_DEPTH: 10, WS_MAX_PAYLOAD: 1024, SNAPSHOT_MAX_CONTENT: 1024
    };
    return new ClientSync('bitcoin', 'mainnet', db, applier,
        { rollback: sinon.stub().resolves() }, new HashVerifier(), config, new Utility());
}

function socket(){
    const ws = new EventEmitter();
    ws.close = sinon.stub();
    return ws;
}

const FOUR = 'http://a:3006,http://b:3006,http://c:3006,http://d:3006';

function stubStatusWork(sync){
    sinon.stub(sync, 'incrementalCatchUp').resolves();
    sinon.stub(sync, 'maybeVerifyCompleteness').resolves();
    sinon.stub(sync, 'reconcileDispensers').resolves(true);
    sinon.stub(sync, 'handleBlock').resolves();
    sinon.stub(sync, 'shouldReconcileDispensers').returns(true);
}

function registerEvictedEventTests(){
    it('a status event from an evicted source drives no catch-up, repair or height record', async function(){
        const sync = makeSync(FOUR);
        stubStatusWork(sync);
        sync.lastAppliedBlock = 10;
        sync._evictedSources.add(3);

        await sync.handleEvent({ type: 'status', block_height: 50 }, 3);

        assert.strictEqual(sync.incrementalCatchUp.called, false);
        assert.strictEqual(sync.maybeVerifyCompleteness.called, false);
        assert.strictEqual(sync.reconcileDispensers.called, false);
        assert.strictEqual(sync.lastKnownServerBlock, null);
    });

    it('a block event from an evicted source does not record its height', async function(){
        const sync = makeSync(FOUR);
        stubStatusWork(sync);
        sync._evictedSources.add(3);

        await sync.handleEvent({ type: 'block', block_index: 11 }, 3);

        assert.strictEqual(sync.handleBlock.called, false);
        assert.strictEqual(sync.lastKnownServerBlock, null);
    });

    it('the same status event from an active source still reaches the sweep', async function(){
        const sync = makeSync(FOUR);
        stubStatusWork(sync);
        sync.lastAppliedBlock = 10;
        sync._evictedSources.add(3);

        await sync.handleEvent({ type: 'status', block_height: 50 }, 2);

        assert.strictEqual(sync.incrementalCatchUp.calledOnce, true);
        assert.strictEqual(sync.maybeVerifyCompleteness.calledOnceWith('http://c:3006', 50), true);
        assert.strictEqual(sync.lastKnownServerBlock, 50);
    });
}

function registerEvictedSocketTests(){
    it('a frame on a closing evicted socket refreshes no liveness and queues no event', function(){
        const sync = makeSync(FOUR);
        const ws = socket();
        sync.running = true;
        sync.wsConns[3] = ws;
        sync.registerWebSocketHandlers(ws, 'http://d:3006', 3);
        sinon.stub(sync, 'handleEvent').resolves();

        sync.evictSource(3);
        ws.emit('message', Buffer.from(JSON.stringify({ type: 'status', block_height: 50 })));

        assert.strictEqual(sync.handleEvent.called, false);
        assert.strictEqual(sync._sourceLastWsEventAt.has(3), false);
        assert.strictEqual(sync._sourceLastReportedTip.has(3), false);
        assert.strictEqual(sync._streamWatchdogs.has(3), false);
        assert.strictEqual(sync._lastWsEventAt, null);
    });

    it('eviction clears the evicted source stream watchdog', function(){
        const sync = makeSync(FOUR);
        const clock = sinon.useFakeTimers();
        sync.running = true;
        sync.config['CLIENT_SOURCE_STALE_MS'] = 1000;
        sync.armStreamWatchdog('http://d:3006', 3, socket());
        assert.strictEqual(sync._streamWatchdogs.has(3), true);

        sync.evictSource(3);

        assert.strictEqual(sync._streamWatchdogs.has(3), false);
        clock.restore();
    });
}

describe('ClientSync events from an evicted source @regression', function(){
    beforeEach(function(){
        sinon.stub(console, 'log'); sinon.stub(console, 'error'); sinon.stub(console, 'warn');
    });
    afterEach(function(){ sinon.restore(); });

    registerEvictedEventTests();
    registerEvictedSocketTests();
});

function liveSync(){
    const sync = makeSync();
    sync.lastAppliedBlock = 10;
    sinon.stub(sync, 'checkTrainActivation').resolves(false);
    sinon.stub(sync, 'liveBlockBlockedByUnverifiedTail').resolves(false);
    sinon.stub(sync, 'haltOnLiveStateHashDivergence').resolves(false);
    sinon.stub(sync, 'finishAppliedBlock');
    sinon.stub(sync, 'verifyCheckpointQuorumIfDue').resolves();
    return sync;
}

function holdLock(sync){
    let release;
    sync._applyLock = new Promise(r => { release = r; });
    return release;
}

function registerLiveApplyTests(){
    it('does not commit a block queued on the apply lock when a halt lands first', async function(){
        const sync = liveSync();
        const release = holdLock(sync);
        const pending = sync.applyAndVerifyBlock({ block_index: 11, block_time: 1 });
        await new Promise(resolve => setImmediate(resolve));

        sync._halted = { blockIndex: 10, reason: 'max-rollback-depth-exceeded', mismatches: [], sources: [], at: null };
        release();
        await pending;

        assert.strictEqual(sync.applier.applyBlock.called, false);
        assert.strictEqual(sync.finishAppliedBlock.called, false);
        assert.strictEqual(sync.lastAppliedBlock, 10);
    });

    it('commits the same queued block when no halt lands', async function(){
        const sync = liveSync();
        const release = holdLock(sync);
        const pending = sync.applyAndVerifyBlock({ block_index: 11, block_time: 1 });
        await new Promise(resolve => setImmediate(resolve));

        release();
        await pending;

        assert.strictEqual(sync.applier.applyBlock.calledOnce, true);
        assert.strictEqual(sync.finishAppliedBlock.calledOnce, true);
    });

}

function trackHaltAtRelease(sync){
    const seen = { halted: undefined };
    const original = sync.withApplyLock.bind(sync);
    sync.withApplyLock = fn => original(async () => {
        try { return await fn(); } finally { if(seen.halted === undefined) seen.halted = sync._halted; }
    });
    return seen;
}

function registerFailedRollbackTests(){
    it('a failed rollback is halted before the apply lock releases', async function(){
        const sync = makeSync();
        sync.lastAppliedBlock = 10;
        sync.rollback.rollback = sinon.stub().rejects(new Error('row removal failed'));
        const seen = trackHaltAtRelease(sync);

        await sync.handleReorg({ block_index: 10 }, undefined, 10);

        assert.ok(seen.halted, 'the hold is set while the rollback still owns the lock');
        assert.strictEqual(seen.halted.reason, 'reorg-rollback-failed');
        assert.strictEqual(sync.lastAppliedBlock, 10);
    });

    it('a failed rollback is halted and logged once, and the prunes still run', async function(){
        const sync = makeSync();
        sync.lastAppliedBlock = 10;
        sync.rollback.rollback = sinon.stub().rejects(new Error('row removal failed'));
        sinon.spy(sync, 'haltForReorgRollbackFailure');
        sinon.spy(sync, 'prunePendingReorgs');
        sinon.spy(sync, 'pruneBlockWorkAboveTip');

        await sync.handleReorg({ block_index: 10 }, undefined, 10);

        assert.strictEqual(sync.haltForReorgRollbackFailure.callCount, 1);
        assert.strictEqual(sync.getHaltInfo().reason, 'reorg-rollback-failed');
        assert.strictEqual(sync.prunePendingReorgs.calledOnce, true);
        assert.strictEqual(sync.pruneBlockWorkAboveTip.calledOnce, true);
    });

    it('a block queued behind a failed rollback is refused', async function(){
        const sync = liveSync();
        sync.rollback.rollback = sinon.stub().rejects(new Error('row removal failed'));

        const reorg = sync.handleReorg({ block_index: 10 }, undefined, 10);
        const block = sync.applyAndVerifyBlock({ block_index: 11, block_time: 1 });
        await Promise.all([reorg, block]);

        assert.strictEqual(sync.applier.applyBlock.called, false);
        assert.strictEqual(sync.getHaltInfo().reason, 'reorg-rollback-failed');
    });
}

describe('ClientSync live apply under a halt @regression', function(){
    beforeEach(function(){
        sinon.stub(console, 'log'); sinon.stub(console, 'error'); sinon.stub(console, 'warn');
    });
    afterEach(function(){ sinon.restore(); });

    registerLiveApplyTests();
    registerFailedRollbackTests();
});

describe('ClientSync catch-up 404 from a source that is behind @regression', function(){
    beforeEach(function(){
        sinon.stub(console, 'log'); sinon.stub(console, 'error'); sinon.stub(console, 'warn');
    });
    afterEach(function(){ sinon.restore(); });

    function notFound(){
        const err = new Error('Request failed with status code 404');
        err.response = { status: 404 };
        return err;
    }

    function warnLines(){
        return console.warn.getCalls().map(c => c.args.join(' ')).filter(l => l.includes('catch-up'));
    }

    it('names the peer that proves the replica is behind instead of calling it current', async function(){
        const sync = makeSync('http://a:3006,http://b:3006');
        sync._sourceLastReportedTip.set(1, 150);
        console.error.resetHistory();

        await sync.handleIncrementalCatchUpFailure('http://a:3006', 101, notFound(), 100);

        const lines = warnLines();
        assert.strictEqual(lines.length, 1);
        assert.ok(lines[0].includes('http://b:3006 reports tip 150 above the committed tip 100'));
        assert.ok(lines[0].includes('asked only http://a:3006 and does not fail over'));
        assert.strictEqual(lines[0].includes('replica is current'), false);
        assert.strictEqual(lines[0].includes('is behind'), false);
        assert.deepStrictEqual(console.error.getCalls().map(c => c.args.join(' ')), []);
    });

    it('keeps the current-or-behind wording when no peer reports a higher tip', async function(){
        const sync = makeSync('http://a:3006,http://b:3006');
        sync._sourceLastReportedTip.set(1, 100);

        await sync.handleIncrementalCatchUpFailure('http://a:3006', 101, notFound(), 100);

        const lines = warnLines();
        assert.strictEqual(lines.length, 1);
        assert.ok(lines[0].includes('replica is current or source is behind'));
    });

    it('ignores a peer tip reported by an evicted source', async function(){
        const sync = makeSync('http://a:3006,http://b:3006,http://c:3006');
        sync._sourceLastReportedTip.set(2, 150);
        sync._evictedSources.add(2);

        await sync.handleIncrementalCatchUpFailure('http://a:3006', 101, notFound(), 100);

        assert.ok(warnLines()[0].includes('replica is current or source is behind'));
    });
});
