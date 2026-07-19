//  doctrine test-coverage program: regression coverage for
// src/consensus-constants.js.
//
// [regression:p1] The follower's frozen consensus constants are DERIVED from
// the canonical coin registry precisely so they can never drift from the source
// indexer (a drift soft-forks the federation). This locks the derivation
// invariants that a careless registry edit could silently break: every allowed
// coin must resolve a numeric activation-delay, and the GAS tick / validator
// query cap must stay at their frozen values.

const assert = require('assert');
const C = require('../../src/consensus-constants.js');
const coins = require('../../src/coins');

describe('consensus-constants derivation [regression:p1]', function () {
    it('every allowed coin resolves a non-negative integer activation-delay', function () {
        for (const tick of coins.ALLOWED_COINS) {
            const d = C.activationDelayBlocks(tick);
            assert.ok(Number.isSafeInteger(d) && d >= 0,
                `ACTIVATION_DELAY_BLOCKS drifted or missing for ${tick}`);
        }
    });

    it('the derived per-coin delay map covers exactly the allowed coins', function () {
        const keys = Object.keys(C.ACTIVATION_DELAY_BLOCKS_BY_COIN).sort();
        assert.deepStrictEqual(keys, [...coins.ALLOWED_COINS].sort());
    });

    it('GAS tick stays frozen at XCHAIN', function () {
        assert.strictEqual(C.gasTickSymbol(), 'XCHAIN');
    });

    it('VALIDATOR_QUERY_LIMIT stays a positive integer', function () {
        assert.ok(Number.isSafeInteger(C.VALIDATOR_QUERY_LIMIT) && C.VALIDATOR_QUERY_LIMIT > 0);
    });
});
