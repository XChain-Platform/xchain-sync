// Gates the contract-state state_key binary-collation flip that changes the
// contract_hash preimage, so an ungated or mis-resolved threshold forks
// against deployed nodes. Pins the per-chain resolution order
// ('<COIN>:<network>' first, then bare network) and the fail-closed handling
// of unknown chains.

const assert = require('assert');
const {
    STATE_KEY_COLLATION_ACTIVATION, isStateKeyBinCollationActive,
} = require('../../src/state_key_collation_activation.js');

describe('state_key_collation_activation', function () {
    it('is armed from genesis on regtest', function () {
        assert.strictEqual(STATE_KEY_COLLATION_ACTIVATION.regtest, 0);
        assert.strictEqual(isStateKeyBinCollationActive(0, 'regtest'), true);
    });

    it('resolves the <COIN>:<network> key ahead of a bare network key', function () {
        const t = STATE_KEY_COLLATION_ACTIVATION['BTC:mainnet'];
        assert.ok(Number.isSafeInteger(t) && t > 0);
        assert.strictEqual(isStateKeyBinCollationActive(t, 'mainnet', 'BTC'), true);
        assert.strictEqual(isStateKeyBinCollationActive(t - 1, 'mainnet', 'BTC'), false);
    });

    it('uses per-coin thresholds (LTC and DOGE differ from BTC)', function () {
        assert.ok(STATE_KEY_COLLATION_ACTIVATION['LTC:mainnet'] > 0);
        assert.ok(STATE_KEY_COLLATION_ACTIVATION['DOGE:mainnet'] > 0);
    });

    it('fails closed (off) on an unknown chain or malformed height', function () {
        assert.strictEqual(isStateKeyBinCollationActive(10 ** 12, 'mainnet', 'ZZZ'), false);
        assert.strictEqual(isStateKeyBinCollationActive('bad', 'mainnet', 'BTC'), false);
    });
});
