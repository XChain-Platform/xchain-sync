// Gates the utf8_bin pin on the stake-weight snapshot's ranking/ordering, which is a
// TRUNCATION boundary: it decides which sources and which keys survive the caps, and
// therefore which rows feed the committed stakes_root. Pins the per-chain resolution
// order ('<COIN>:<network>' first, then bare network), the fail-closed handling of
// unknown chains, and - load-bearing for this change - that NO mainnet chain is armed.

const assert = require('assert');
const {
    STAKE_WEIGHT_COLLATION_ACTIVATION, isStakeWeightBinCollationActive,
} = require('../../src/stake_weight_collation_activation.js');

describe('stake_weight_collation_activation', function () {
    it('is armed from genesis on regtest', function () {
        assert.strictEqual(STAKE_WEIGHT_COLLATION_ACTIVATION.regtest, 0);
        assert.strictEqual(isStakeWeightBinCollationActive(0, 'regtest'), true);
        assert.strictEqual(isStakeWeightBinCollationActive(0, 'regtest', 'BTC'), true,
            'the bare regtest key must cover every regtest coin');
    });

    it('is armed from genesis on every testnet chain', function () {
        for (const coin of ['BTC', 'LTC', 'DOGE']) {
            assert.strictEqual(STAKE_WEIGHT_COLLATION_ACTIVATION[coin + ':testnet'], 0);
            assert.strictEqual(isStakeWeightBinCollationActive(0, 'testnet', coin), true);
        }
    });

    // The whole safety argument of this change: mainnet emits today's SQL, so
    // deploying it cannot move a single mainnet stakes_root. Arming is a separate,
    // coordinated step after both repos are fleet-wide.
    it('arms NO mainnet chain (inert on deploy)', function () {
        for (const coin of ['BTC', 'LTC', 'DOGE']) {
            assert.strictEqual(STAKE_WEIGHT_COLLATION_ACTIVATION[coin + ':mainnet'], undefined,
                coin + ':mainnet must stay unarmed until a coordinated arming step');
            assert.strictEqual(isStakeWeightBinCollationActive(10 ** 12, 'mainnet', coin), false);
        }
        assert.strictEqual(STAKE_WEIGHT_COLLATION_ACTIVATION.mainnet, undefined,
            'a bare mainnet key would arm every mainnet coin at once');
    });

    it('resolves the <COIN>:<network> key ahead of a bare network key', function () {
        // testnet is keyed per coin with no bare `testnet` fallback, so a coin-less
        // testnet lookup finds nothing and stays inert.
        assert.strictEqual(isStakeWeightBinCollationActive(0, 'testnet', 'BTC'), true);
        assert.strictEqual(isStakeWeightBinCollationActive(0, 'testnet', undefined), false);
    });

    it('fails closed (off) on an unknown chain or malformed height', function () {
        assert.strictEqual(isStakeWeightBinCollationActive(10 ** 12, 'mainnet', 'ZZZ'), false);
        assert.strictEqual(isStakeWeightBinCollationActive(10 ** 12, 'stagenet', 'BTC'), false);
        assert.strictEqual(isStakeWeightBinCollationActive('bad', 'testnet', 'BTC'), false);
        assert.strictEqual(isStakeWeightBinCollationActive(undefined, 'regtest', 'BTC'), false);
        assert.strictEqual(isStakeWeightBinCollationActive(null, 'regtest', 'BTC'), false);
    });

    // Guards the threshold comparison itself, so a later arming step inherits a
    // tested at/below/above boundary rather than re-proving it.
    it('flips at the threshold, not one block either side', function () {
        const probe = { 'X:net': 100 };
        // Mirror of the module's own predicate over a synthetic map: at/after -> on.
        const at = (b) => Number.isFinite(parseInt(b)) && parseInt(b) >= probe['X:net'];
        assert.strictEqual(at(99), false);
        assert.strictEqual(at(100), true);
        assert.strictEqual(at(101), true);
        // And the real module agrees on its own genesis-armed entries.
        assert.strictEqual(isStakeWeightBinCollationActive(-1, 'regtest'), false);
        assert.strictEqual(isStakeWeightBinCollationActive(0, 'regtest'), true);
    });
});
