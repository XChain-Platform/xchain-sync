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
 * Standalone byte vector for the ANCHOR v3 fold-wrapper branch of the vendored
 * canonicalCheckpoint. The twin loop in ../checkpoint_twin.test.js skips without the
 * SDK checkout and passes on an edit made to both copies, so this must hold alone.
 ********************************************************************/

const assert = require('assert');
const checkpoint = require('../../../src/checkpoint');

describe('vendored checkpoint verifier fold-wrapper canonical @regression', function(){
    // Pin the fold-wrapper bytes on regtest, where CHECKPOINT_COMMITMENT and EQUIV arm at genesis:
    // the batch fields follow the roots, the CRC is lowercased and the round id gains the seq.
    // The expected string is written from the spec parts, the same literal as the SDK's vector.
    it('canonicalCheckpoint matches the ANCHOR v3 fold-wrapper spec byte-for-byte (archive suffix, round-id seq)', function(){
        const base = { chain: 'BTC', network: 'regtest', block_index: 100,
            block_hash: 'c0'.repeat(32), ledger_hash: 'a1'.repeat(32),
            actions_hash: 'b2'.repeat(32), contract_hash: 'c3'.repeat(32),
            checkpoint_seq: 7, snapshot_block: 100,
            state_root: 'd4'.repeat(32), state_root_version: 1,
            block_merkle_root: 'e5'.repeat(32), block_merkle_version: 1 };
        const fold = { match_batch_seq: 42, match_count: 17, batch_crc32: '9C4E1B22', total_chunks: 1 };
        const plainRaw = ['XCHECKPOINT', 'BTC', 'regtest', '100', 'c0'.repeat(32), 'a1'.repeat(32), 'b2'.repeat(32),
            'c3'.repeat(32), '7', '100', 'd4'.repeat(32), '1', 'e5'.repeat(32), '1'].join('|');
        assert.strictEqual(
            checkpoint.canonicalCheckpoint(Object.assign({}, base, { fold_archive: fold })),
            'EQUIV|XCHECKPOINT|BTC|regtest|100|7|42|0||' + plainRaw + '|42|17|9c4e1b22|1',
            'sync fold-wrapper canonical drifted from the hub foldArchiveCanonical and the SDK copy; re-align the vendored twin');
        assert.strictEqual(
            checkpoint.canonicalCheckpoint(Object.assign({}, base, { fold_archive: null })),
            'EQUIV|XCHECKPOINT|BTC|regtest|100|7|0||' + plainRaw,
            'a null fold_archive must add neither the archive suffix nor the round-id seq');
    });
});
