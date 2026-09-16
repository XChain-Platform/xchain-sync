// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

const { assert, ClientSync } = require('./support');

describe('ClientSync.shouldReconcileDispensers (decoder resume cadence)', function(){
    // Pure decision over (config, _catchUpCount, _lastDispenserReconcileAt); exercise it
    // in isolation via prototype.call with a hand-built context.
    function decide(ctx, now){
        return ClientSync.prototype.shouldReconcileDispensers.call(ctx, now);
    }

    it('reconciles on the first cycle after a resume that skipped bootstrap', function(){
        let ctx = { config: {}, _lastDispenserReconcileAt: null };
        assert.strictEqual(decide(ctx, 1000), true);   // firstResume
        assert.strictEqual(ctx._catchUpCount, 1);
    });

    it('does not force a reconcile on the first cycle when bootstrap already reconciled', function(){
        let ctx = { config: {}, _lastDispenserReconcileAt: 1000 };  // bootstrap stamped it
        assert.strictEqual(decide(ctx, 1100), false);
    });

    it('reconciles every Nth catch-up in steady state', function(){
        let ctx = { config: { DISPENSERS_RECONCILE_EVERY: '5' }, _lastDispenserReconcileAt: 1000, _catchUpCount: 4 };
        assert.strictEqual(decide(ctx, 1100), true);   // 4 -> 5, 5 % 5 == 0
    });

    it('reconciles when the last reconcile is older than the max interval', function(){
        let ctx = { config: { DISPENSERS_RECONCILE_EVERY: '1000', DISPENSERS_RECONCILE_MAX_INTERVAL_MS: '60000' },
                    _lastDispenserReconcileAt: 1000, _catchUpCount: 1 };
        assert.strictEqual(decide(ctx, 1000 + 60000), true);   // exactly 60s elapsed
    });

    it('skips reconcile within the interval and off the periodic cycle', function(){
        let ctx = { config: { DISPENSERS_RECONCILE_EVERY: '1000', DISPENSERS_RECONCILE_MAX_INTERVAL_MS: '60000' },
                    _lastDispenserReconcileAt: 1000, _catchUpCount: 1 };
        assert.strictEqual(decide(ctx, 1000 + 59999), false);
    });

    it('treats a max interval of 0 as disabling the time trigger', function(){
        let ctx = { config: { DISPENSERS_RECONCILE_EVERY: '1000', DISPENSERS_RECONCILE_MAX_INTERVAL_MS: '0' },
                    _lastDispenserReconcileAt: 1000, _catchUpCount: 1 };
        assert.strictEqual(decide(ctx, 1000 + 99999999), false);
    });
});
