// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

'use strict';

// Covers the pure time cap and source-quorum rules for retroactive rewinds.
const assert = require('assert');
const {
    RETROACTIVE_BLOCK_SECONDS,
    retroactiveCapBlocks,
    admitRetroactiveRewind,
} = require('../../../src/client/retroactive_admission');

describe('Retroactive admission rules @regression', function(){
    describe('retroactiveCapBlocks', function(){
        it('freezes the block intervals used by the cap', function(){
            assert.deepStrictEqual(RETROACTIVE_BLOCK_SECONDS, {BTC: 600, LTC: 150, DOGE: 60});
            assert.strictEqual(Object.isFrozen(RETROACTIVE_BLOCK_SECONDS), true);
        });

        it('calculates the default 72-hour caps', function(){
            assert.strictEqual(retroactiveCapBlocks('BTC'), 432);
            assert.strictEqual(retroactiveCapBlocks('LTC'), 1728);
            assert.strictEqual(retroactiveCapBlocks('DOGE'), 4320);
        });

        it('accepts a positive finite hours override', function(){
            assert.strictEqual(retroactiveCapBlocks('BTC', 24), 144);
        });

        it('throws for an unknown coin', function(){
            assert.throws(() => retroactiveCapBlocks('XYZ'), /Unknown coin/);
        });

        it('throws for hours that are not finite and positive', function(){
            assert.throws(() => retroactiveCapBlocks('BTC', 0), /finite positive/);
            assert.throws(() => retroactiveCapBlocks('BTC', Infinity), /finite positive/);
        });
    });

    describe('admitRetroactiveRewind', function(){
        const retroactive = {target: 500, rules_to: 'rules-1'};
        const report = (source, target = 500, rulesTo = 'rules-1') => ({
            source,
            target,
            rules_to: rulesTo,
        });
        const admit = (overrides = {}) => admitRetroactiveRewind(Object.assign({
            depth: 432,
            capBlocks: 432,
            retroactive,
            reports: [report('source-a'), report('source-b')],
            quorum: 2,
        }, overrides));

        it('refuses a request that is not retroactive', function(){
            assert.deepStrictEqual(admit({retroactive: null}), {admit: false, reason: 'not_retroactive'});
        });

        it('admits depth exactly at the cap and refuses one block over', function(){
            assert.deepStrictEqual(admit(), {admit: true, reason: 'quorum'});
            assert.deepStrictEqual(admit({depth: 433}), {admit: false, reason: 'over_cap'});
        });

        it('counts a duplicate source once', function(){
            assert.deepStrictEqual(
                admit({reports: [report('source-a'), report('source-a')]}),
                {admit: false, reason: 'no_quorum'}
            );
        });

        it('does not count reports for a different target or rules_to', function(){
            assert.deepStrictEqual(
                admit({reports: [report('source-a'), report('source-b', 500, 'rules-2')]}),
                {admit: false, reason: 'no_quorum'}
            );
            assert.deepStrictEqual(
                admit({reports: [report('source-a'), report('source-b', 499)]}),
                {admit: false, reason: 'no_quorum'}
            );
        });
    });
});
