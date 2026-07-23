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
    it('exposes a per-network threshold map with regtest armed from genesis', function () {
        assert.strictEqual(CHECKPOINT_COMMITMENT_ACTIVATION.regtest, 0);
        //  lead 0e418c8c: testnet was 0, which made the hub commit the SPV
        // root suffix from testnet genesis before the indexer had roots to sign,
        // so it refused to sign every testnet checkpoint. Now armed at the first
        // BTC-testnet anchor past all three STATE_COMMITMENT testnet thresholds.
        assert.strictEqual(CHECKPOINT_COMMITMENT_ACTIVATION.testnet, 146000);
        assert.ok(Number.isSafeInteger(CHECKPOINT_COMMITMENT_ACTIVATION.mainnet));
        assert.ok(CHECKPOINT_COMMITMENT_ACTIVATION.mainnet > 0);
    });

    it('activates at/above the mainnet threshold and is off below it', function () {
        const t = CHECKPOINT_COMMITMENT_ACTIVATION.mainnet;
        assert.strictEqual(isCheckpointCommitmentActive(t, 'mainnet'), true);
        assert.strictEqual(isCheckpointCommitmentActive(t - 1, 'mainnet'), false);
    });

    it('testnet arms at 146000, off one block below ( keying-skew fix)', function () {
        assert.strictEqual(isCheckpointCommitmentActive(146000, 'testnet'), true);
        assert.strictEqual(isCheckpointCommitmentActive(145999, 'testnet'), false);
    });

    it('fails closed on malformed input and unknown networks', function () {
        assert.strictEqual(isCheckpointCommitmentActive('nope', 'mainnet'), false);
        assert.strictEqual(isCheckpointCommitmentActive(999999999, 'no-such-net'), false);
    });
});
