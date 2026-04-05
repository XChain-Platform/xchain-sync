const assert = require('assert');
const HashVerifier = require('../../../src/HashVerifier');

describe('Boundary: Hash Continuity Check', function(){

    let verifier;
    const hashes = { ledger_hash: 'aaa', actions_hash: 'bbb', contract_hash: 'ccc' };

    beforeEach(function(){
        verifier = new HashVerifier();
    });

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
            let result = verifier.verifyChainContinuity(null, null, { block_index: 6 });
            assert.strictEqual(result.valid, true);
        });
    });

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
