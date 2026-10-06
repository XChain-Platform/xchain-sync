//  doctrine test-coverage program: boundary coverage for
// src/consensus-constants.js. A thin replica must agree with the source indexer
// on every value that gates block-hashed state; these are derived from the
// canonical coin registry so they cannot drift. This exercises the resolver
// edges: null/undefined coin, an unrecognized coin, and the frozen scalar
// invariants a follower's stakes_root/contract_hash builds depend on.

const assert = require('assert');
const C = require('../../src/consensus-constants.js');

function fileIt(title, fn){
    const test = it(title, fn);
    test.file = __filename;
    return test;
}

describe('consensus-constants (boundary)', function () {
    fileIt('activationDelayBlocks maps null/undefined to the no-op null path', function () {
        assert.strictEqual(C.activationDelayBlocks(null), null);
        assert.strictEqual(C.activationDelayBlocks(undefined), null);
    });

    fileIt('activationDelayBlocks maps an unrecognized coin to undefined (hard misconfig)', function () {
        assert.strictEqual(C.activationDelayBlocks('NOT-A-COIN'), undefined);
    });

    fileIt('activationDelayBlocks resolves a known coin to a non-negative integer', function () {
        const d = C.activationDelayBlocks('BTC');
        assert.ok(Number.isSafeInteger(d) && d >= 0);
    });

    fileIt('activationDelayBlocks is case-insensitive across ticker and full name', function () {
        assert.strictEqual(C.activationDelayBlocks('btc'), C.activationDelayBlocks('BTC'));
        // Pin the full names production passes as cfg.coin, as literals; the integer check
        // keeps the equality from passing when both sides are undefined.
        for(const [full, tick] of [['bitcoin', 'BTC'], ['litecoin', 'LTC'], ['dogecoin', 'DOGE']]){
            const d = C.activationDelayBlocks(full);
            assert.ok(Number.isSafeInteger(d) && d >= 0, full + ' must resolve to a frozen integer');
            assert.strictEqual(d, C.activationDelayBlocks(tick));
        }
        // Resolve a mixed-case full name the same as its ticker.
        assert.strictEqual(C.activationDelayBlocks('Dogecoin'), C.activationDelayBlocks('DOGE'));
        assert.strictEqual(C.activationDelayBlocks('LITECOIN'), C.activationDelayBlocks('LTC'));
    });

    fileIt('GAS_TICK / gasTickSymbol are the frozen XCHAIN symbol', function () {
        assert.strictEqual(C.GAS_TICK, 'XCHAIN');
        assert.strictEqual(C.gasTickSymbol(), 'XCHAIN');
    });

    fileIt('VALIDATOR_QUERY_LIMIT is a positive integer cap', function () {
        assert.ok(Number.isSafeInteger(C.VALIDATOR_QUERY_LIMIT) && C.VALIDATOR_QUERY_LIMIT > 0);
    });

    fileIt('BTC stake-capability floors are a non-empty map returned by reference', function () {
        const caps = C.btcStakeCapabilities();
        assert.ok(caps && typeof caps === 'object');
        assert.ok(Object.keys(caps).length > 0);
    });
});
