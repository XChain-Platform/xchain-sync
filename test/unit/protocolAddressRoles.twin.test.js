// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md.

// CONSENSUS guard: cross-repo byte-identity of the special-address role map.
//
// The block-hash preimage substitutes a protocol special address (BURN / GAS /
// DONATE1 / DONATE2 / REWARD) for its chain-independent role token so identical
// actions hash identically on every chain. The indexer DERIVES that map from
// src/configs/*.js; this replica has no per-coin config, so it vendors a FROZEN
// snapshot in src/protocolAddressRoles.js. If the snapshot ever drifts from the
// indexer's derived map (a config edit adds/changes a special address and the
// snapshot is not updated), BlockHasher recomputes a mismatching hash and the
// divergence breaker halts the follower. The local indexer-coverage test cannot
// catch this: it only sees its own derived map, never this snapshot.
//
// This suite closes that gap. It loads the sibling xchain-indexer's derived map
// and asserts deep equality with the local snapshot. Skipped (not failed) when
// the sibling repo is absent, so a sync-only checkout / CI clone still runs
// green; point XCHAIN_INDEXER_DIR at the repo to force it. Mirrors the
// sibling-present guard pattern in ConsensusPrimitiveConformance.test.js.

const assert = require('assert');
const fs     = require('fs');
const path   = require('path');

const { ROLE_BY_ADDRESS, canonicalizeHashAddress } = require('../../src/protocolAddressRoles');

const INDEXER_DIR = process.env.XCHAIN_INDEXER_DIR ||
    path.join(__dirname, '..', '..', '..', 'xchain-indexer');
const INDEXER_MODULE = path.join(INDEXER_DIR, 'src', 'protocolAddressRoles.js');
const INDEXER_PRESENT = fs.existsSync(INDEXER_MODULE);

let indexerRoles = null;
let indexerCanonicalize = null;
if (INDEXER_PRESENT) {
    // The indexer module derives its map from xchain-indexer/src/configs/*.js at
    // require time, so this loads the source-of-truth exactly as the indexer runs it.
    const mod = require(INDEXER_MODULE);
    indexerRoles = mod.ROLE_BY_ADDRESS;
    indexerCanonicalize = mod.canonicalizeHashAddress;
}

describe('protocolAddressRoles cross-repo byte-identity (consensus) @regression', function () {

    before(function () {
        if (!INDEXER_PRESENT) this.skip();
    });

    it('snapshot equals the indexer config-derived ROLE_BY_ADDRESS', function () {
        // deepStrictEqual on address-keyed objects checks both key set and role
        // values, so a missing, extra, or re-pointed address all fail loudly.
        assert.deepStrictEqual(ROLE_BY_ADDRESS, indexerRoles,
            'xchain-sync frozen snapshot has drifted from xchain-indexer\'s config-derived ' +
            'map; regenerate src/protocolAddressRoles.js from the indexer before deploying.');
    });

    it('canonicalizes every special address identically to the indexer', function () {
        for (const addr of Object.keys(indexerRoles)) {
            assert.strictEqual(canonicalizeHashAddress(addr), indexerCanonicalize(addr),
                `canonicalization of ${addr} differs between sync and indexer`);
        }
    });

    it('passes a non-special address through unchanged on both sides', function () {
        const plain = 'mq7tVfobimRUPxPNnyd5mKn11SVmTiLxtu';
        assert.strictEqual(canonicalizeHashAddress(plain), plain);
        assert.strictEqual(indexerCanonicalize(plain), plain);
    });
});
