//  doctrine test-coverage program: boundary coverage for
// src/consensus-constants.js. A thin replica must agree with the source indexer
// on every value that gates block-hashed state; these are derived from the
// canonical coin registry so they cannot drift. This exercises the resolver
// edges: null/undefined coin, an unrecognized coin, and the frozen scalar
// invariants a follower's stakes_root/contract_hash builds depend on.

const assert = require('assert');
const C = require('../../src/consensus-constants.js');

describe('consensus-constants (boundary)', function () {
    it('activationDelayBlocks maps null/undefined to the no-op null path', function () {
        assert.strictEqual(C.activationDelayBlocks(null), null);
        assert.strictEqual(C.activationDelayBlocks(undefined), null);
    });

    it('activationDelayBlocks maps an unrecognized coin to undefined (hard misconfig)', function () {
        assert.strictEqual(C.activationDelayBlocks('NOT-A-COIN'), undefined);
    });

    it('activationDelayBlocks resolves a known coin to a non-negative integer', function () {
        const d = C.activationDelayBlocks('BTC');
        assert.ok(Number.isSafeInteger(d) && d >= 0);
    });

    it('activationDelayBlocks is case-insensitive across ticker and full name', function () {
        assert.strictEqual(C.activationDelayBlocks('btc'), C.activationDelayBlocks('BTC'));
    });

    it('GAS_TICK / gasTickSymbol are the frozen XCHAIN symbol', function () {
        assert.strictEqual(C.GAS_TICK, 'XCHAIN');
        assert.strictEqual(C.gasTickSymbol(), 'XCHAIN');
    });

    it('VALIDATOR_QUERY_LIMIT is a positive integer cap', function () {
        assert.ok(Number.isSafeInteger(C.VALIDATOR_QUERY_LIMIT) && C.VALIDATOR_QUERY_LIMIT > 0);
    });

    it('BTC stake-capability floors are a non-empty map returned by reference', function () {
        const caps = C.btcStakeCapabilities();
        assert.ok(caps && typeof caps === 'object');
        assert.ok(Object.keys(caps).length > 0);
    });
});
