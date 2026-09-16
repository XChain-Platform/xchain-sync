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
 * ClientSync: the dispensers wall-clock reconcile is reachable while a
 * decoder replica follows live.
 *
 * DISPENSERS_RECONCILE_MAX_INTERVAL_MS used to be sampled only from inside
 * incrementalCatchUp, and every caller of that method is an exceptional path
 * (resume, block gap, empty-replica refusal, head fork). A decoder replica that
 * bootstrapped and then followed cleanly never evaluated the bound at all, so
 * the one cadence it claims to protect against (no catch-ups) was the one it
 * could not reach, and the replica kept serving rows the source had soft-expired
 * or hard-purged. The recurring status tick now samples the same bound, using a
 * side-effect-free predicate so the every-Nth catch-up counter is untouched.
 ********************************************************************/

const assert = require('assert');
const sinon  = require('sinon');
const ClientSync = require('../../src/client/sync');

describe('ClientSync.dispenserReconcileIntervalDue (wall-clock term)', function(){
    function due(ctx, now){
        return ClientSync.prototype.dispenserReconcileIntervalDue.call(ctx, now);
    }

    it('is due once the last reconcile is older than the interval', function(){
        let ctx = { config: { DISPENSERS_RECONCILE_MAX_INTERVAL_MS: '60000' },
                    _lastDispenserReconcileAt: 1000 };
        assert.strictEqual(due(ctx, 1000 + 60000), true);   // exactly at the bound
    });

    it('is not due inside the interval', function(){
        let ctx = { config: { DISPENSERS_RECONCILE_MAX_INTERVAL_MS: '60000' },
                    _lastDispenserReconcileAt: 1000 };
        assert.strictEqual(due(ctx, 1000 + 59999), false);
    });

    it('falls back to the 30 minute default when the config value is not a number', function(){
        let ctx = { config: { DISPENSERS_RECONCILE_MAX_INTERVAL_MS: 'later' },
                    _lastDispenserReconcileAt: 0 };
        assert.strictEqual(due(ctx, 1799999), false);
        assert.strictEqual(due(ctx, 1800000), true);
    });

    it('treats 0 as disabling the wall-clock trigger', function(){
        let ctx = { config: { DISPENSERS_RECONCILE_MAX_INTERVAL_MS: '0' },
                    _lastDispenserReconcileAt: 1000 };
        assert.strictEqual(due(ctx, 1000 + 99999999), false);
    });

    it('is not due before any reconcile has stamped a time', function(){
        // firstResume belongs to the catch-up path: firing it from a recurring tick
        // would retry a failing re-dump on every tick instead of once per interval.
        let ctx = { config: { DISPENSERS_RECONCILE_MAX_INTERVAL_MS: '60000' },
                    _lastDispenserReconcileAt: null };
        assert.strictEqual(due(ctx, 99999999), false);
    });

    it('leaves the catch-up cycle counter alone', function(){
        let ctx = { config: {}, _lastDispenserReconcileAt: 1000, _catchUpCount: 7 };
        due(ctx, 1000);
        assert.strictEqual(ctx._catchUpCount, 7);
    });
});

describe('ClientSync status tick fires the stale dispensers reconcile', function(){
    let logStub;

    function tickCtx(over){
        let ctx = {
            dbType: 'decoder',
            config: { DISPENSERS_RECONCILE_MAX_INTERVAL_MS: '60000' },
            sources: ['http://source1:3006'],
            lastKnownServerBlock: 500,
            lastAppliedBlock: 500,
            _halted: null,
            _lastDispenserReconcileAt: 1000,
            _catchUpCount: 3,
            _dispenserReconcileInFlight: false,
            recordUpstreamStatus: sinon.stub(),
            logGap: sinon.stub(),
            incrementalCatchUp: sinon.stub().resolves(),
            maybeVerifyCompleteness: sinon.stub().resolves(),
            reconcileDispensers: sinon.stub().resolves(),
            dispenserReconcileIntervalDue: ClientSync.prototype.dispenserReconcileIntervalDue
        };
        return Object.assign(ctx, over || {});
    }

    function tick(ctx, nowMs){
        let clock = sinon.useFakeTimers({ now: nowMs, toFake: ['Date'] });
        try {
            return ClientSync.prototype.handleEvent.call(ctx,
                { type: 'status', block_height: 500 }, 0);
        } finally {
            clock.restore();
        }
    }

    beforeEach(function(){ logStub = sinon.stub(console, 'log'); });
    afterEach(function(){ sinon.restore(); logStub = null; });

    it('reconciles on a tick when the stamp is older than the interval', async function(){
        let ctx = tickCtx();
        await tick(ctx, 1000 + 60000);
        assert.strictEqual(ctx.reconcileDispensers.calledOnce, true);
        assert.strictEqual(ctx.reconcileDispensers.firstCall.args[0], 'http://source1:3006');
        // The completeness sweep still runs afterwards, and the catch-up path is untouched.
        assert.strictEqual(ctx.maybeVerifyCompleteness.calledOnce, true);
        assert.strictEqual(ctx.incrementalCatchUp.called, false);
    });

    it('does not advance the every-Nth catch-up counter', async function(){
        let ctx = tickCtx();
        await tick(ctx, 1000 + 60000);
        assert.strictEqual(ctx._catchUpCount, 3);
    });

    it('does not reconcile inside the interval', async function(){
        let ctx = tickCtx();
        await tick(ctx, 1000 + 59999);
        assert.strictEqual(ctx.reconcileDispensers.called, false);
    });

    it('does not reconcile for an indexer replica', async function(){
        let ctx = tickCtx({ dbType: 'indexer' });
        await tick(ctx, 1000 + 60000);
        assert.strictEqual(ctx.reconcileDispensers.called, false);
    });

    it('does not reconcile while halted', async function(){
        let ctx = tickCtx({ _halted: { reason: 'divergence' } });
        await tick(ctx, 1000 + 60000);
        assert.strictEqual(ctx.reconcileDispensers.called, false);
    });

    it('does not reconcile before bootstrap has applied a block', async function(){
        let ctx = tickCtx({ lastAppliedBlock: null });
        await tick(ctx, 1000 + 60000);
        assert.strictEqual(ctx.reconcileDispensers.called, false);
    });

    it('does not reconcile while one is already in flight', async function(){
        let ctx = tickCtx({ _dispenserReconcileInFlight: true });
        await tick(ctx, 1000 + 60000);
        assert.strictEqual(ctx.reconcileDispensers.called, false);
    });

    it('clears the in-flight flag even when the reconcile rejects', async function(){
        let ctx = tickCtx({ reconcileDispensers: sinon.stub().rejects(new Error('boom')) });
        await assert.rejects(() => tick(ctx, 1000 + 60000), /boom/);
        assert.strictEqual(ctx._dispenserReconcileInFlight, false);
    });

    it('stops firing once the reconcile stamps a fresh time', async function(){
        let ctx = tickCtx({
            reconcileDispensers: sinon.stub().callsFake(async function(){
                ctx._lastDispenserReconcileAt = 61000;
            })
        });
        await tick(ctx, 61000);
        assert.strictEqual(ctx.reconcileDispensers.calledOnce, true);
        await tick(ctx, 61001);
        assert.strictEqual(ctx.reconcileDispensers.calledOnce, true);
    });
});
