/*********************************************************************
 *
 * Copyright © 2025-2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC - https://dankest.llc
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This file is part of XChain Platform. Licensed under the GNU Affero
 * General Public License v3.0 or later; see LICENSE.md.
 *
 ********************************************************************/

function assertMissingCommitments({ assert, checkpoint, signedCheckpoint, signer, validators }) {
    for (const field of ['state_root', 'block_merkle_root']) {
        const cp = signedCheckpoint(signer, { [field]: '' });
        assert.strictEqual(checkpoint.commitmentMissing(cp), true, 'empty ' + field + ' is missing');
        assert.strictEqual(checkpoint.verifyCheckpoint(cp, validators).valid, false);
    }
    for (const field of ['state_root_version', 'block_merkle_version']) {
        for (const value of [null, undefined]) {
            const cp = signedCheckpoint(signer, { [field]: value });
            assert.strictEqual(checkpoint.commitmentMissing(cp), true,
                field + '=' + String(value) + ' is missing');
            assert.strictEqual(checkpoint.verifyCheckpoint(cp, validators).valid, false,
                field + '=' + String(value) + ' must not verify');
        }
    }
    const zero = signedCheckpoint(signer, { state_root_version: 0, block_merkle_version: 0 });
    assert.strictEqual(checkpoint.commitmentMissing(zero), false, 'version 0 is present');
    assert.strictEqual(checkpoint.verifyCheckpoint(zero, validators).valid, true, 'version 0 must verify');
}

module.exports = { assertMissingCommitments };
