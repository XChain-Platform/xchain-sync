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

// Covers the pure follow-forward rules for superseding checkpoint ranges.
const assert = require('assert');
const {
    supersedingSeqFor,
    isNonBindingStep,
    bindingSteps,
} = require('../../../src/client/supersede_follow');

const row = (checkpointSeq, blockIndex, supersedesFrom = null) => ({
    checkpoint_seq: checkpointSeq,
    block_index: blockIndex,
    supersedes_from: supersedesFrom,
});

function registerSupersedingSeqTests(){
    it('supersedes steps at and above supersedes_from but not below it', function(){
        const below = row(10, 104);
        const at = row(11, 105);
        const above = row(12, 110);
        const superseder = row(13, 120, 105);
        const chain = [below, at, above, superseder];

        assert.strictEqual(supersedingSeqFor(below, chain), null);
        assert.strictEqual(supersedingSeqFor(at, chain), 13);
        assert.strictEqual(supersedingSeqFor(above, chain), 13);
        assert.strictEqual(isNonBindingStep(below, chain), false);
        assert.strictEqual(isNonBindingStep(at, chain), true);
    });

    it('returns the lowest checkpoint_seq of two later superseders', function(){
        const step = row(10, 110);
        const later = row(13, 130, 100);
        const earlier = row(12, 120, 105);

        assert.strictEqual(supersedingSeqFor(step, [step, later, earlier]), 12);
    });

    it('ignores a superseder whose checkpoint_seq is lower than the step', function(){
        const lower = row(9, 90, 50);
        const step = row(10, 100);

        assert.strictEqual(supersedingSeqFor(step, [lower, step]), null);
    });

    it('compares JSON string sequence and height fields as numbers', function(){
        const step = row('11', '110');
        const superseder = row('12', '120', '105');

        assert.strictEqual(supersedingSeqFor(step, [step, superseder]), 12);
    });
}

function registerBindingStepTests(){
    it('returns a plain range whole, in order, in a new array', function(){
        const first = row(10, 100);
        const second = row(11, 110);
        const chain = [first, second];
        const result = bindingSteps(chain);

        assert.notStrictEqual(result, chain);
        assert.deepStrictEqual(result, [first, second]);
        assert.strictEqual(result[0], first);
        assert.strictEqual(result[1], second);
    });

    it('drops a superseded step while retaining its superseder', function(){
        const binding = row(10, 100);
        const superseded = row(11, 110);
        const superseder = row(12, 120, 105);
        const result = bindingSteps([binding, superseded, superseder]);

        assert.deepStrictEqual(result, [binding, superseder]);
        assert.strictEqual(result[0], binding);
        assert.strictEqual(result[1], superseder);
    });

    it('does not mutate the input range', function(){
        const chain = [row(10, 100), row(11, 110), row(12, 120, 105)];
        const snapshot = JSON.stringify(chain);

        bindingSteps(chain);

        assert.strictEqual(JSON.stringify(chain), snapshot);
    });

    it('throws a TypeError when bindingSteps receives a non-array', function(){
        assert.throws(() => bindingSteps(null), TypeError);
    });
}

describe('Supersede follow rules @regression', function(){
    registerSupersedingSeqTests();
    registerBindingStepTests();
});
