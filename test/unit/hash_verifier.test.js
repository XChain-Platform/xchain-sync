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
const HashVerifier = require('../../src/client/hash_verifier');

describe('HashVerifier', function(){

    let verifier;

    beforeEach(function(){
        verifier = new HashVerifier();
    });

    describe('compareBlockHashes', function(){
        const hashA = { ledger_hash: 'aaa', actions_hash: 'bbb', contract_hash: 'ccc' };

        it('returns match when all hashes are identical', function(){
            let result = verifier.compareBlockHashes(100, hashA, { ...hashA });
            assert.strictEqual(result.match, true);
            assert.strictEqual(result.blockHeight, 100);
            assert.strictEqual(result.mismatches.length, 0);
        });

        it('detects single field mismatch', function(){
            let hashB = { ...hashA, ledger_hash: 'zzz' };
            let result = verifier.compareBlockHashes(100, hashA, hashB);
            assert.strictEqual(result.match, false);
            assert.strictEqual(result.mismatches.length, 1);
            assert.strictEqual(result.mismatches[0].field, 'ledger_hash');
            assert.strictEqual(result.mismatches[0].a, 'aaa');
            assert.strictEqual(result.mismatches[0].b, 'zzz');
        });

        it('detects all three fields mismatched', function(){
            let hashB = { ledger_hash: 'x', actions_hash: 'y', contract_hash: 'z' };
            let result = verifier.compareBlockHashes(50, hashA, hashB);
            assert.strictEqual(result.match, false);
            assert.strictEqual(result.mismatches.length, 3);
        });

        it('treats null vs string as mismatch', function(){
            let hashB = { ledger_hash: null, actions_hash: 'bbb', contract_hash: 'ccc' };
            let result = verifier.compareBlockHashes(1, hashA, hashB);
            assert.strictEqual(result.match, false);
            assert.strictEqual(result.mismatches.length, 1);
        });

        it('treats null vs null as match', function(){
            let h = { ledger_hash: null, actions_hash: null, contract_hash: null };
            let result = verifier.compareBlockHashes(1, h, { ...h });
            assert.strictEqual(result.match, true);
        });
    });
});

describe('HashVerifier', function(){

    let verifier;

    beforeEach(function(){
        verifier = new HashVerifier();
    });

    describe('compareBlockHashes state_hash', function(){
        const hashA = { ledger_hash: 'aaa', actions_hash: 'bbb', contract_hash: 'ccc' };

        it('detects a state_hash-only mismatch when both sides carry one', function(){
            let result = verifier.compareBlockHashes(7, { ...hashA, state_hash: 's1' }, { ...hashA, state_hash: 's2' });
            assert.strictEqual(result.match, false);
            assert.deepStrictEqual(result.mismatches, [{ field: 'state_hash', a: 's1', b: 's2' }]);
            assert.deepStrictEqual(result.skipped, []);
        });

        it('skips state_hash, still matching, when only one side carries it', function(){
            let local = verifier.compareBlockHashes(7, { ...hashA, state_hash: 's1' }, { ...hashA });
            assert.strictEqual(local.match, true);
            assert.deepStrictEqual(local.skipped, [{ field: 'state_hash', reason: 'absent-remote' }]);
            let remote = verifier.compareBlockHashes(7, { ...hashA, state_hash: null }, { ...hashA, state_hash: 's2' });
            assert.strictEqual(remote.match, true);
            assert.deepStrictEqual(remote.skipped, [{ field: 'state_hash', reason: 'absent-local' }]);
        });

        it('matches with nothing skipped when neither side carries a state_hash', function(){
            let result = verifier.compareBlockHashes(7, { ...hashA, state_hash: null }, { ...hashA });
            assert.strictEqual(result.match, true);
            assert.deepStrictEqual(result.skipped, []);
        });
    });
});

describe('HashVerifier', function(){

    let verifier;

    beforeEach(function(){
        verifier = new HashVerifier();
    });

    describe('verifyChainContinuity', function(){
        it('returns valid when prevBlockIndex is null (bootstrap)', function(){
            let result = verifier.verifyChainContinuity(null, null, { block_index: 1 });
            assert.strictEqual(result.valid, true);
            assert.strictEqual(result.reason, null);
        });

        it('returns valid for sequential block', function(){
            let result = verifier.verifyChainContinuity(99, { ledger_hash: 'a' }, { block_index: 100 });
            assert.strictEqual(result.valid, true);
        });

        it('returns invalid for block gap', function(){
            let result = verifier.verifyChainContinuity(99, { ledger_hash: 'a' }, { block_index: 105 });
            assert.strictEqual(result.valid, false);
            assert.ok(result.reason.includes('Block gap'));
            assert.ok(result.reason.includes('100'));
            assert.ok(result.reason.includes('105'));
        });

        it('returns invalid for duplicate block', function(){
            let result = verifier.verifyChainContinuity(99, { ledger_hash: 'a' }, { block_index: 99 });
            assert.strictEqual(result.valid, false);
        });

        it('returns valid when prevHashes is null', function(){
            let result = verifier.verifyChainContinuity(5, null, { block_index: 6 });
            assert.strictEqual(result.valid, true);
        });

        it('returns invalid for a block gap when prevHashes is null', function(){
            let result = verifier.verifyChainContinuity(4, null, { block_index: 8 });
            assert.strictEqual(result.valid, false);
            assert.ok(result.reason.includes('Block gap'), result.reason);
            assert.ok(result.reason.includes('5'), result.reason);
            assert.ok(result.reason.includes('8'), result.reason);
        });
    });
});
