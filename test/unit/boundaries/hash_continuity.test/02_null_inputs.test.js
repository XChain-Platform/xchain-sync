// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// Covers null continuity inputs. One part of hash_continuity.test.js.
const assert = require('assert');
const { registerHooks } = require('./helpers/hash_continuity_suite');

describe('Boundary: Hash Continuity Check', function(){
    let verifier;
    registerHooks(function(value){ verifier = value; });
    describe('null prevBlockIndex (bootstrap)', function(){
        it('valid: null → 1 (first block)', function(){
            let result = verifier.verifyChainContinuity(null, null, { block_index: 1 });
            assert.strictEqual(result.valid, true);
            assert.strictEqual(result.reason, null);
        });
        it('valid: null → 0 (first block at zero)', function(){
            let result = verifier.verifyChainContinuity(null, null, { block_index: 0 });
            assert.strictEqual(result.valid, true);
        });
        it('valid: null → 999 (any block after bootstrap)', function(){
            let result = verifier.verifyChainContinuity(null, null, { block_index: 999 });
            assert.strictEqual(result.valid, true);
        });
    });
    describe('null prevHashes', function(){
        it('valid: prevBlockIndex=5, prevHashes=null → skips check', function(){
            let result = verifier.verifyChainContinuity(5, null, { block_index: 6 });
            assert.strictEqual(result.valid, true);
            assert.strictEqual(result.reason, null);
        });
        it('invalid: prevBlockIndex=5, prevHashes=null, block 9 is still a gap', function(){
            let result = verifier.verifyChainContinuity(5, null, { block_index: 9 });
            assert.strictEqual(result.valid, false);
            assert.ok(result.reason.includes('Block gap'), result.reason);
            assert.ok(result.reason.includes('6'), result.reason);
            assert.ok(result.reason.includes('9'), result.reason);
        });
        it('invalid: prevBlockIndex=5, prevHashes=null, block 5 repeats the tip', function(){
            let result = verifier.verifyChainContinuity(5, null, { block_index: 5 });
            assert.strictEqual(result.valid, false);
        });
    });
});
