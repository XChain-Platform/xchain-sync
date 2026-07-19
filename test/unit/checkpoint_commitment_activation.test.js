//  doctrine test-coverage program: unit coverage for
// src/checkpoint_commitment_activation.js. This is a byte-identical twin of the
// hub/indexer/sdk/explorer copies; it gates the SIGNED checkpoint preimage on
// the BTC-anchored snapshot_block, so the threshold map and the gate function
// must stay pinned or federation quorum verification forks.

const assert = require('assert');
const {
    CHECKPOINT_COMMITMENT_ACTIVATION, isCheckpointCommitmentActive,
} = require('../../src/checkpoint_commitment_activation.js');

describe('checkpoint_commitment_activation', function () {
    it('exposes a per-network threshold map with regtest/testnet armed from genesis', function () {
        assert.strictEqual(CHECKPOINT_COMMITMENT_ACTIVATION.regtest, 0);
        assert.strictEqual(CHECKPOINT_COMMITMENT_ACTIVATION.testnet, 0);
        assert.ok(Number.isSafeInteger(CHECKPOINT_COMMITMENT_ACTIVATION.mainnet));
        assert.ok(CHECKPOINT_COMMITMENT_ACTIVATION.mainnet > 0);
    });

    it('activates at/above the mainnet threshold and is off below it', function () {
        const t = CHECKPOINT_COMMITMENT_ACTIVATION.mainnet;
        assert.strictEqual(isCheckpointCommitmentActive(t, 'mainnet'), true);
        assert.strictEqual(isCheckpointCommitmentActive(t - 1, 'mainnet'), false);
    });

    it('fails closed on malformed input and unknown networks', function () {
        assert.strictEqual(isCheckpointCommitmentActive('nope', 'mainnet'), false);
        assert.strictEqual(isCheckpointCommitmentActive(999999999, 'no-such-net'), false);
    });
});
