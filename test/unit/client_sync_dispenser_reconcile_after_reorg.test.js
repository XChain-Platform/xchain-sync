/*********************************************************************
 *
 * Copyright © 2025–2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC – https://dankest.llc
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This file is part of XChain Platform. Licensed under the GNU Affero
 * General Public License v3.0 or later; see LICENSE.md. A commercial
 * license (without AGPL source-disclosure terms) is available -
 * contact legal@dankest.llc.
 *
 **********************************************************************
 * ClientSync: a decoder reorg rollback arms a dispensers reconcile.
 *
 * The source decoder rewrites dispensers off-stream when it rolls a block
 * back (un-expires rows, rewinds extensions, deletes the orphaned block's
 * rows), and the follower's rollback leaves the table alone. The next status
 * tick or catch-up must therefore reconcile it at once, even with the
 * wall-clock interval disabled, instead of waiting out that interval while
 * replacement blocks reuse the orphaned tx_index values.
 ********************************************************************/

const assert = require('assert');
const sinon  = require('sinon');
const zlib   = require('zlib');
const axios  = require('axios');
const ClientSync = require('../../src/client/sync');
const { SCHEMA_VERSION } = require('../../src/schema/version');

const SOURCE = 'http://source1:3006';

// A hand-built context carrying what applyReorgRollback reads.
function rollbackCtx(dbType){
    return {
        dbType, withApplyLock: (fn) => fn(),
        rollback: { rollback: sinon.stub().resolves() },
        db: { getBlockHashRow: sinon.stub().resolves({ block_hash: 'aa' }) },
    };
}

// A live-following decoder context whose wall-clock interval is disabled.
function tickCtx(over){
    return Object.assign({
        dbType: 'decoder', config: { DISPENSERS_RECONCILE_MAX_INTERVAL_MS: '0' },
        sources: [SOURCE], lastKnownServerBlock: 500, lastAppliedBlock: 500, _halted: null,
        _lastDispenserReconcileAt: Date.now(), _catchUpCount: 3, _dispenserReconcileInFlight: false,
        recordUpstreamStatus: sinon.stub(), logGap: sinon.stub(),
        incrementalCatchUp: sinon.stub().resolves(), maybeVerifyCompleteness: sinon.stub().resolves(),
        reconcileDispensers: sinon.stub().resolves(true),
        dispenserReconcileIntervalDue: ClientSync.prototype.dispenserReconcileIntervalDue,
    }, over || {});
}

function tick(ctx){
    return ClientSync.prototype.handleEvent.call(ctx, { type: 'status', block_height: 500 }, 0);
}

describe('ClientSync reorg rollback arms the dispensers reconcile', function(){
    beforeEach(function(){ sinon.stub(console, 'log'); });
    afterEach(function(){ sinon.restore(); });

    it('a decoder rollback arms the reconcile after the rollback commits', async function(){
        let ctx = rollbackCtx('decoder');
        await ClientSync.prototype.applyReorgRollback.call(ctx, { block_index: 100 });
        assert.strictEqual(ctx.rollback.rollback.calledOnceWith(100), true);
        assert.strictEqual(ctx._dispenserReconcileAfterReorg, true);
        assert.strictEqual(ctx.lastAppliedBlock, 99);
    });

    it('an indexer rollback does not arm it', async function(){
        let ctx = rollbackCtx('indexer');
        await ClientSync.prototype.applyReorgRollback.call(ctx, { block_index: 100 });
        assert.strictEqual(ctx._dispenserReconcileAfterReorg, undefined);
    });

    it('a failed rollback does not arm it', async function(){
        let ctx = rollbackCtx('decoder');
        ctx.rollback.rollback = sinon.stub().rejects(new Error('lock wait'));
        await assert.rejects(ClientSync.prototype.applyReorgRollback.call(ctx, { block_index: 100 }), /lock wait/);
        assert.strictEqual(ctx._dispenserReconcileAfterReorg, undefined);
    });
});

describe('ClientSync status tick after a reorg rollback', function(){
    beforeEach(function(){ sinon.stub(console, 'log'); });
    afterEach(function(){ sinon.restore(); });

    it('reconciles on the next tick even with the wall-clock interval disabled', async function(){
        let ctx = tickCtx({ _dispenserReconcileAfterReorg: true });
        await tick(ctx);
        assert.strictEqual(ctx.reconcileDispensers.calledOnceWith(SOURCE), true);
        assert.strictEqual(ctx._dispenserReconcileInFlight, false);
    });

    it('does not reconcile on a tick with no reorg and the interval disabled', async function(){
        let ctx = tickCtx();
        await tick(ctx);
        assert.strictEqual(ctx.reconcileDispensers.called, false);
    });

    it('waits while a reconcile is in flight, then fires on the following tick', async function(){
        let ctx = tickCtx({ _dispenserReconcileAfterReorg: true, _dispenserReconcileInFlight: true });
        await tick(ctx);
        assert.strictEqual(ctx.reconcileDispensers.called, false);
        ctx._dispenserReconcileInFlight = false;
        await tick(ctx);
        assert.strictEqual(ctx.reconcileDispensers.calledOnce, true);
    });

    it('does not reconcile while halted', async function(){
        let ctx = tickCtx({ _dispenserReconcileAfterReorg: true, _halted: { reason: 'divergence' } });
        await tick(ctx);
        assert.strictEqual(ctx.reconcileDispensers.called, false);
    });
});

describe('ClientSync catch-up after a reorg rollback', function(){
    it('reconciles on the next catch-up cycle off the periodic cadence', function(){
        let ctx = { config: { DISPENSERS_RECONCILE_MAX_INTERVAL_MS: '0', DISPENSERS_RECONCILE_EVERY: '20' },
                    _lastDispenserReconcileAt: 1000, _catchUpCount: 0, _dispenserReconcileAfterReorg: true };
        assert.strictEqual(ClientSync.prototype.shouldReconcileDispensers.call(ctx, 2000), true);
        ctx._dispenserReconcileAfterReorg = false;
        assert.strictEqual(ClientSync.prototype.shouldReconcileDispensers.call(ctx, 2000), false);
    });
});

describe('ClientSync.reconcileDispensers clears the post-reorg trigger on attempt', function(){
    beforeEach(function(){ sinon.stub(console, 'log'); sinon.stub(console, 'error'); });
    afterEach(function(){ sinon.restore(); });

    function decoderCtx(){
        return { dbType: 'decoder', chain: 'bitcoin', network: 'mainnet', config: {},
                 _lastDispenserReconcileAt: null, _dispenserReconcileAfterReorg: true,
                 upstreamHeaders: () => ({}), withApplyLock: (fn) => fn(),
                 applier: { applyDispensersReplace: sinon.stub().resolves() } };
    }

    it('clears it when the reconcile succeeds', async function(){
        let ctx = decoderCtx();
        let page = { schema_version: SCHEMA_VERSION.decoder, has_more: false, rows: [{ tx_index: 1 }] };
        sinon.stub(axios, 'get').resolves({ data: zlib.gzipSync(Buffer.from(JSON.stringify(page))) });
        assert.strictEqual(await ClientSync.prototype.reconcileDispensers.call(ctx, SOURCE), true);
        assert.strictEqual(ctx._dispenserReconcileAfterReorg, false);
    });

    it('clears it when the fetch fails, so a failing source is not re-fetched every tick', async function(){
        let ctx = decoderCtx();
        sinon.stub(axios, 'get').rejects(new Error('ECONNREFUSED'));
        assert.strictEqual(await ClientSync.prototype.reconcileDispensers.call(ctx, SOURCE), false);
        assert.strictEqual(ctx._dispenserReconcileAfterReorg, false);
        assert.strictEqual(ctx.applier.applyDispensersReplace.called, false);
    });

    it('leaves it armed when there is no source to attempt', async function(){
        let ctx = decoderCtx();
        assert.strictEqual(await ClientSync.prototype.reconcileDispensers.call(ctx, undefined), false);
        assert.strictEqual(ctx._dispenserReconcileAfterReorg, true);
    });
});
