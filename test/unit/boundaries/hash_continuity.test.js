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
const { hashes, registerHooks } = require('./hash_continuity.test/helpers/hash_continuity_suite');

describe('Boundary: Hash Continuity Check', function(){
    let verifier;
    registerHooks(function(value){ verifier = value; });

    describe('block index continuity (exact +1 requirement)', function(){
        it('valid: 10 → 11 (sequential)', function(){
            let result = verifier.verifyChainContinuity(10, hashes, { block_index: 11 });
            assert.strictEqual(result.valid, true);
        });
        it('invalid: 10 → 12 (gap of 1)', function(){
            let result = verifier.verifyChainContinuity(10, hashes, { block_index: 12 });
            assert.strictEqual(result.valid, false);
            assert.ok(result.reason.includes('expected 11'));
            assert.ok(result.reason.includes('got 12'));
        });
        it('invalid: 10 → 10 (same block)', function(){
            let result = verifier.verifyChainContinuity(10, hashes, { block_index: 10 });
            assert.strictEqual(result.valid, false);
        });
        it('invalid: 10 → 9 (backward)', function(){
            let result = verifier.verifyChainContinuity(10, hashes, { block_index: 9 });
            assert.strictEqual(result.valid, false);
        });
        it('valid: 0 → 1 (zero-based chain)', function(){
            let result = verifier.verifyChainContinuity(0, hashes, { block_index: 1 });
            assert.strictEqual(result.valid, true);
        });
        it('invalid: 0 → 2 (skip from zero)', function(){
            let result = verifier.verifyChainContinuity(0, hashes, { block_index: 2 });
            assert.strictEqual(result.valid, false);
        });
        it('invalid: 0 → 0 (repeat at zero)', function(){
            let result = verifier.verifyChainContinuity(0, hashes, { block_index: 0 });
            assert.strictEqual(result.valid, false);
        });
    });
});
