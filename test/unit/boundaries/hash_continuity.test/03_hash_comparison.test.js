// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// Covers hash comparisons. One part of hash_continuity.test.js.
const assert = require('assert');
const { hashes, registerHooks } = require('./helpers/hash_continuity_suite');

describe('Boundary: Hash Continuity Check', function(){
    let verifier;
    registerHooks(function(value){ verifier = value; });
    describe('hash comparison boundaries', function(){
        it('match: all three hashes identical', function(){
            let result = verifier.compareBlockHashes(1, hashes, { ...hashes });
            assert.strictEqual(result.match, true);
            assert.strictEqual(result.mismatches.length, 0);
        });
        it('mismatch: single field different', function(){
            let result = verifier.compareBlockHashes(1, hashes, { ...hashes, ledger_hash: 'zzz' });
            assert.strictEqual(result.match, false);
            assert.strictEqual(result.mismatches.length, 1);
        });
        it('mismatch: all three fields different', function(){
            let result = verifier.compareBlockHashes(1, hashes, { ledger_hash: 'x', actions_hash: 'y', contract_hash: 'z' });
            assert.strictEqual(result.match, false);
            assert.strictEqual(result.mismatches.length, 3);
        });
        it('null vs string is mismatch', function(){
            let result = verifier.compareBlockHashes(1, hashes, { ledger_hash: null, actions_hash: 'bbb', contract_hash: 'ccc' });
            assert.strictEqual(result.match, false);
        });
        it('null vs null is match', function(){
            let n = { ledger_hash: null, actions_hash: null, contract_hash: null };
            let result = verifier.compareBlockHashes(1, n, { ...n });
            assert.strictEqual(result.match, true);
        });
        it('empty string vs empty string is match', function(){
            let e = { ledger_hash: '', actions_hash: '', contract_hash: '' };
            let result = verifier.compareBlockHashes(1, e, { ...e });
            assert.strictEqual(result.match, true);
        });
        it('empty string vs null is mismatch', function(){
            let a = { ledger_hash: '', actions_hash: '', contract_hash: '' };
            let b = { ledger_hash: null, actions_hash: null, contract_hash: null };
            let result = verifier.compareBlockHashes(1, a, b);
            assert.strictEqual(result.match, false);
            assert.strictEqual(result.mismatches.length, 3);
        });
    });
});
