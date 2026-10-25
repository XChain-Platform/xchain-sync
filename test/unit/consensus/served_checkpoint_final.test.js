'use strict';

// Copyright © 2025-2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC - https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md.

// The explorer verify endpoint says whether a checkpointed height sits inside an
// open retroactive replay window (`final`). This vendored copy of the SDK verifier
// must carry that answer through to its caller as the SDK does, so a replica can tell
// a checkpoint that may still be superseded from one that is settled. Signature
// validity and finality are independent: a well-signed checkpoint inside the window
// is valid and not final.

const assert = require('assert');
const crypto = require('crypto');
const Checkpoint = require('../../../src/checkpoint.js');

function makeKeypair() {
    let { publicKey, privateKey } = crypto.generateKeyPairSync('ed25519');
    let pubDer = publicKey.export({ format: 'der', type: 'spki' });
    return { pubkeyHex: pubDer.slice(12).toString('hex'), privateKey };
}

// One validator signs a pre-commitment mainnet checkpoint, so the count regime
// decides and a single signature is quorate.
function signedBody(extra) {
    let key = makeKeypair();
    let cp = {
        chain: 'BTC', network: 'mainnet', block_index: 900123,
        block_hash: 'ab'.repeat(32), ledger_hash: 'cd'.repeat(32),
        actions_hash: 'ef'.repeat(32), contract_hash: '01'.repeat(32),
        checkpoint_seq: 417, snapshot_block: 900120
    };
    let sig = crypto.sign(null, Buffer.from(Checkpoint.canonicalCheckpoint(cp), 'utf8'), key.privateKey).toString('hex');
    cp.validator_signatures = JSON.stringify([{ pubkey: key.pubkeyHex, sig: sig }]);
    return Object.assign({ checkpoint: cp, validators: [key.pubkeyHex], snapshot_available: true }, extra);
}

function stubFetch(body) {
    return async () => ({ ok: true, json: async () => body });
}

describe('served checkpoint final (sync twin)', function () {

    it('passes final false through for a height inside an open replay window', function () {
        let result = Checkpoint.verifyServedCheckpoint(signedBody({ final: false }), null);
        assert.strictEqual(result.valid, true);
        assert.strictEqual(result.final, false);
    });

    it('passes final true through once the window has replayed', function () {
        let result = Checkpoint.verifyServedCheckpoint(signedBody({ final: true }), null);
        assert.strictEqual(result.valid, true);
        assert.strictEqual(result.final, true);
    });

    it('reads an absent final as true, the answer of an explorer that predates the field', function () {
        let result = Checkpoint.verifyServedCheckpoint(signedBody({}), null);
        assert.strictEqual(result.final, true);
    });

    it('reads any present value other than true as not final', function () {
        for (let v of [null, 'true', 'false', 0, 1, {}]) {
            let result = Checkpoint.verifyServedCheckpoint(signedBody({ final: v }), null);
            assert.strictEqual(result.final, false, 'final ' + JSON.stringify(v));
        }
    });

    it('keeps final when the verdict is judged against a supplied set', function () {
        let body = signedBody({ final: false });
        let result = Checkpoint.verifyServedCheckpoint(body, body.validators);
        assert.strictEqual(result.validatorSource, 'supplied');
        assert.strictEqual(result.final, false);
    });

    it('fetchAndVerifyCheckpoint reports final false inside the window and true once replayed', async function () {
        let open = await Checkpoint.fetchAndVerifyCheckpoint('https://explorer.xchain.io', 'BTC', 900123,
            stubFetch(signedBody({ final: false })));
        assert.strictEqual(open.valid, true);
        assert.strictEqual(open.final, false);
        let replayed = await Checkpoint.fetchAndVerifyCheckpoint('https://explorer.xchain.io', 'BTC', 900123,
            stubFetch(signedBody({ final: true })));
        assert.strictEqual(replayed.final, true);
    });
});
