// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// A halt whose sync_halt write fails stays in force, reports itself as not durable and
// retries the write until it lands; a clear abandons the retry. stop() hands back a
// promise for in-flight work, and SyncService closes the pools only after it settles.
const assert = require('assert');
const sinon  = require('sinon');
const SyncService = require('../../../src/sync_service');
const { buildClient, hashesAt } = require('../../helpers/verified_tip_client');

function quiet(){
    beforeEach(function(){ sinon.stub(console, 'log'); sinon.stub(console, 'error'); sinon.stub(console, 'warn'); });
    afterEach(function(){ sinon.restore(); });
}

describe('ClientSync: a halt write that fails is retried until durable @regression', function(){
    quiet();

    it('retries on a timer and reports durable once the write lands', async function(){
        let clock = sinon.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout'] });
        let { sync, db } = buildClient({ tip: 200 });
        db.recordHalt = sinon.stub();
        db.recordHalt.onFirstCall().rejects(new Error('db down'));
        db.recordHalt.onSecondCall().rejects(new Error('db down'));
        db.recordHalt.resolves({ block_index: 200 });
        await sync.haltOnDivergence(200, [{ field: 'ledger_hash' }], ['http://a:3006'], 'local-recompute-divergence');
        assert.strictEqual(sync.isHalted(), true);
        assert.strictEqual(sync.isHaltDurable(), false);
        await clock.tickAsync(5000);
        assert.strictEqual(sync.isHaltDurable(), false);
        await clock.tickAsync(10000);
        assert.strictEqual(sync.isHaltDurable(), true);
        assert.strictEqual(db.recordHalt.callCount, 3);
        assert.deepStrictEqual(db.recordHalt.thirdCall.args.slice(0, 3), ['indexer', 200, 'local-recompute-divergence']);
        clock.restore();
    });

    it('a clear abandons the pending write, so it cannot bring the halt back', async function(){
        let clock = sinon.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout'] });
        let { sync, db } = buildClient({ tip: 200 });
        db.recordHalt = sinon.stub().rejects(new Error('db down'));
        await sync.haltOnDivergence(200, [], [], 'local-recompute-divergence');
        await sync.clearHalt();
        db.recordHalt.resetHistory();
        await clock.tickAsync(120000);
        assert.strictEqual(db.recordHalt.called, false);
        assert.strictEqual(sync.isHalted(), false);
        clock.restore();
    });
});

describe('ClientSync: stop() drains in-flight work @regression', function(){
    quiet();

    it('the stop promise settles only after the in-flight apply and its verification', async function(){
        let { sync, applier, db } = buildClient({ tip: 199 });
        sync.lastAppliedBlock = 199;
        let release;
        applier.applyBlock = sinon.stub().callsFake(() => new Promise(r => { release = () => { db.tip = 200; r(); }; }));
        let applying = sync.applyBlockEvent(Object.assign({ block_index: 200, block_time: 1 }, hashesAt(200)));
        await new Promise(r => setImmediate(r));
        let stopped = false;
        let stopping = sync.stop().then(() => { stopped = true; });
        await new Promise(r => setImmediate(r));
        assert.strictEqual(stopped, false, 'stop settled while a block was mid-apply');
        release();
        await Promise.all([applying, stopping]);
        assert.strictEqual(stopped, true);
        assert.strictEqual(sync.lastAppliedBlock, 200);
    });

    it('SyncService closes pools only after every client drain settles', async function(){
        let order = [];
        let svc = Object.create(SyncService.prototype);
        svc.ready = true;
        svc.pollers = new Map();
        let release;
        let client = { stop(){ order.push('sync.stop'); return new Promise(r => { release = () => { order.push('drained'); r(); }; }); } };
        svc.clientSyncs = new Map([['btc:regtest:indexer', client]]);
        svc.databases = new Map([['btc:regtest:indexer', { db: { async close(){ order.push('close'); } }, config: {}, dbType: 'indexer' }]]);
        let stopping = svc.stop();
        await new Promise(r => setImmediate(r));
        assert.deepStrictEqual(order, ['sync.stop']);
        release();
        await stopping;
        assert.deepStrictEqual(order, ['sync.stop', 'drained', 'close']);
    });
});

function queueWs(sync, ev){
    sync.handleWebSocketMessage(Buffer.from(JSON.stringify(ev)), 'http://a:3006', 0);
}

function held(){
    const h = {};
    h.promise = new Promise(r => { h.release = r; });
    return h;
}

describe('ClientSync: stop() drains WS events already on the event chain @regression', function(){
    quiet();

    it('drops a reorg and a block queued behind a running catch-up', async function(){
        const { sync, applier, rollback } = buildClient({ tip: 200 });
        sync.lastAppliedBlock = 200;
        sync.lastHashes = hashesAt(200);
        const catchUp = held();
        sinon.stub(sync, 'incrementalCatchUp').callsFake(() => catchUp.promise);
        const completeness = sinon.stub(sync, 'maybeVerifyCompleteness').resolves();
        const reorgs = sinon.spy(sync, 'handleSourceReorg');
        const blocks = sinon.spy(sync, 'handleBlock');
        queueWs(sync, { type: 'status', block_height: 205 });
        queueWs(sync, { type: 'reorg', block_index: 198 });
        queueWs(sync, Object.assign({ type: 'block', block_index: 201, block_time: 1 }, hashesAt(201)));
        await new Promise(r => setImmediate(r));
        assert.strictEqual(sync.incrementalCatchUp.calledOnce, true, 'the status event did not reach its catch-up');
        const stopping = sync.stop();
        catchUp.release();
        await stopping;
        await sync._wsEventChain;
        assert.strictEqual(reorgs.called, false, 'a reorg queued before stop() still ran');
        assert.strictEqual(blocks.called, false, 'a block queued before stop() still ran');
        assert.strictEqual(rollback.rollback.called, false);
        assert.strictEqual(applier.applyBlock.called, false);
        assert.strictEqual(completeness.called, false, 'the status event kept working after stop()');
        assert.strictEqual(sync.isHalted(), false);
    });

    it('the stop promise settles only after the event the chain is running', async function(){
        const { sync } = buildClient({ tip: 200 });
        sync.lastAppliedBlock = 200;
        const sweep = held();
        sinon.stub(sync, 'maybeVerifyCompleteness').callsFake(() => sweep.promise);
        queueWs(sync, { type: 'status', block_height: 200 });
        await new Promise(r => setImmediate(r));
        assert.strictEqual(sync.maybeVerifyCompleteness.calledOnce, true, 'the status event did not reach its sweep');
        let stopped = false;
        const stopping = sync.stop().then(() => { stopped = true; });
        await new Promise(r => setImmediate(r));
        assert.strictEqual(stopped, false, 'stop settled while a WS event was mid-run');
        sweep.release();
        await stopping;
        assert.strictEqual(stopped, true);
    });
});
