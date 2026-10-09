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
const RollbackGuard = require('../../../src/client/rollback_guard');

describe('RollbackGuard', function(){
    it('measures a single event from the tip', function(){
        let g = new RollbackGuard(5);
        assert.strictEqual(g.depthFor(100, 96), 5);
        assert.strictEqual(g.exceeds(100, 96), false);
        assert.strictEqual(g.exceeds(100, 95), true);
    });

    it('accumulates a rewind split across events', function(){
        let g = new RollbackGuard(5);
        g.record(100, 98);
        let tip = 97;
        assert.strictEqual(g.depthFor(tip, 96), 5);
        assert.strictEqual(g.exceeds(tip, 96), false);
        g.record(tip, 96);
        tip = 95;
        assert.strictEqual(g.exceeds(tip, 95), true);
    });

    it('ends the streak once the tip recovers to the peak', function(){
        let g = new RollbackGuard(5);
        g.record(100, 98);
        assert.strictEqual(g.depthFor(100, 98), 3);
        assert.strictEqual(g.peak, null);
    });

    it('ends the streak once the tip climbs a full limit above the low target', function(){
        let g = new RollbackGuard(5);
        g.record(100, 98);
        assert.strictEqual(g.depthFor(103, 102), 2);
        assert.strictEqual(g.peak, null);
    });

    it('round-trips an active streak through state', function(){
        const g = new RollbackGuard(5);
        g.record(100, 98);

        const restored = new RollbackGuard(5);
        restored.restoreState(g.toState());

        assert.deepStrictEqual(restored.toState(), {peak: 100, low: 98});
        assert.strictEqual(restored.depthFor(97, 96), 5);
    });

    it('round-trips reset state', function(){
        const restored = new RollbackGuard(5);
        restored.record(100, 98);
        restored.restoreState(new RollbackGuard(5).toState());

        assert.deepStrictEqual(restored.toState(), {peak: null, low: null});
    });

    it('stays reset when restored state is malformed', function(){
        const malformedStates = [
            null,
            {},
            {peak: 100},
            {peak: '100', low: 98},
            {peak: 100, low: null},
            {peak: 98, low: 100},
            {peak: -1, low: 0},
        ];

        for(const state of malformedStates){
            const g = new RollbackGuard(5);
            g.record(100, 98);
            g.restoreState(state);
            assert.deepStrictEqual(g.toState(), {peak: null, low: null});
        }
    });
});
