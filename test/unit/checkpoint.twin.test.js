/*********************************************************************
 *
 * Copyright © 2025–2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC – https://dankest.llc
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This file is part of XChain Platform. Licensed under the GNU Affero
 * General Public License v3.0 or later; see LICENSE.md.
 *
 **********************************************************************
 * Vendored checkpoint verifier conformance. src/checkpoint.js (+ its
 * stake_weighted_quorum / equivocation_header / checkpoint_commitment_activation
 * siblings) is a byte-identical TWIN of the xchain-sdk copies. This guards that
 * the vendored copy actually verifies a real federation-signed checkpoint and
 * rejects a tampered one, so drift from the SDK is caught here rather than in
 * production (mirrors how merkle.js is golden-vector guarded).
 ********************************************************************/

const assert = require('assert');
const crypto = require('crypto');
const checkpoint = require('../../src/checkpoint');

// Raw 32-byte Ed25519 pubkey + a signer, matching the federation's key encoding.
function makeSigner(){
    const { publicKey, privateKey } = crypto.generateKeyPairSync('ed25519');
    const spki = publicKey.export({ format: 'der', type: 'spki' });
    return { privateKey, pubkeyHex: spki.subarray(spki.length - 32).toString('hex') };
}
function sign(privateKey, canonical){
    return crypto.sign(null, Buffer.from(canonical, 'utf8'), privateKey).toString('hex');
}
// A checkpoint signed by `signer`. regtest => CHECKPOINT_COMMITMENT + stake-weighted
// quorum are active (activation height 0), so the canonical commits the roots and
// the quorum is weighted, the realistic launch-epoch shape.
function signedCheckpoint(signer, overrides){
    const cp = Object.assign({
        chain: 'BTC', network: 'regtest', block_index: 100, block_hash: 'c0'.repeat(32),
        ledger_hash: 'a1'.repeat(32), actions_hash: 'b2'.repeat(32), contract_hash: 'c3'.repeat(32),
        checkpoint_seq: 0, snapshot_block: 100, state_root: 'd4'.repeat(32), state_root_version: 1,
        block_merkle_root: 'e5'.repeat(32), block_merkle_version: 1, validator_signatures: []
    }, overrides || {});
    cp.validator_signatures = [{ pubkey: signer.pubkeyHex, sig: sign(signer.privateKey, checkpoint.canonicalCheckpoint(cp)) }];
    return cp;
}

describe('vendored checkpoint verifier (twin conformance) @regression', function(){
    it('verifies a real Ed25519 quorum-signed checkpoint against its pinned set', function(){
        const s = makeSigner();
        const cp = signedCheckpoint(s);
        const validators = [{ pubkey: s.pubkeyHex, source: s.pubkeyHex, weight: '100' }];
        const r = checkpoint.verifyCheckpoint(cp, validators);
        assert.strictEqual(r.valid, true);
        assert.strictEqual(r.weighted, true, 'regtest activates the stake-weighted regime');
    });

    it('rejects a checkpoint with a tampered field (signature no longer matches the canonical)', function(){
        const s = makeSigner();
        const cp = signedCheckpoint(s);
        cp.state_root = 'ff'.repeat(32);                         // mutate after signing
        const validators = [{ pubkey: s.pubkeyHex, source: s.pubkeyHex, weight: '100' }];
        assert.strictEqual(checkpoint.verifyCheckpoint(cp, validators).valid, false);
    });

    it('rejects a checkpoint whose signer is not in the pinned set', function(){
        const real = makeSigner(), rogue = makeSigner();
        const cp = signedCheckpoint(rogue);                      // signed by a non-pinned key
        const validators = [{ pubkey: real.pubkeyHex, source: real.pubkeyHex, weight: '100' }];
        assert.strictEqual(checkpoint.verifyCheckpoint(cp, validators).valid, false);
    });

    it('an empty validator set can never verify', function(){
        const s = makeSigner();
        assert.strictEqual(checkpoint.verifyCheckpoint(signedCheckpoint(s), []).valid, false);
    });
});
