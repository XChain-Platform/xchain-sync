/*********************************************************************
 *
 * Copyright © 2025-2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC - https://dankest.llc
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This file is part of XChain Platform. Licensed under the GNU Affero
 * General Public License v3.0 or later; see LICENSE.md. A commercial
 * license (without AGPL source-disclosure terms) is available -
 * contact legal@dankest.llc.
 *
 *********************************************************************/

// Cross-repo guard: the follower's stakes_root inputs must match the indexer's.
//
// In the active light-client class (STATE_COMMITMENT regtest/testnet = block 0),
// ClientApplier.computeFollowerRoots recomputes stakes_root every block via
// gatherStakeEntries, and ClientSync halts on any state_root mismatch. The leaf
// encoders (merkle.js) and _stakeWeightsSql are byte-guarded, but the *inputs*
// to gatherStakeEntries were not: BTC_STAKE_CAPABILITIES (the capability set and
// per-cap MIN_STAKE floors) and VALIDATOR_QUERY_LIMIT are hand-mirrored from the
// indexer's src/configs/BTC.js. A future indexer edit (a sixth capability, a
// floor change, or a limit bump) would silently fork the regtest/testnet
// follower stakes_root into a hard halt with nothing catching it in CI.
//
// This is the stakesValidatorSetParity guard that prior commit messages already
// referenced. It loads the sibling xchain-indexer config and asserts the inputs
// match; when the sibling repo is not checked out (standalone deploy), it skips
// rather than failing, mirroring the consensus-primitive conformance tests.

const assert = require('assert');
const fs     = require('fs');
const path   = require('path');

const consts = require('../../src/consensus-constants.js');

const INDEXER_DIR = process.env.XCHAIN_INDEXER_DIR ||
    path.join(__dirname, '..', '..', '..', 'xchain-indexer');
const BTC_CONFIG_PATH = path.join(INDEXER_DIR, 'src', 'configs', 'BTC.js');
const INDEXER_PRESENT = fs.existsSync(BTC_CONFIG_PATH);

let indexerCaps = null;     // { capability: MIN_STAKE string }
let indexerLimit = null;
if (INDEXER_PRESENT) {
    try {
        const cfg = require(BTC_CONFIG_PATH).getConfig('mainnet');
        indexerCaps = {};
        for (const k of Object.keys(cfg.STAKING.CAPABILITIES)) {
            indexerCaps[k] = cfg.STAKING.CAPABILITIES[k].MIN_STAKE;
        }
        indexerLimit = cfg.VALIDATOR_QUERY_LIMIT;
    } catch (_e) {
        indexerCaps = null;
    }
}

describe('stakes_root validator-set parity: xchain-sync == xchain-indexer @regression', function () {
    before(function () {
        if (!INDEXER_PRESENT || !indexerCaps) this.skip();
    });

    it('BTC_STAKE_CAPABILITIES has the identical capability set as the indexer', function () {
        const syncKeys    = Object.keys(consts.BTC_STAKE_CAPABILITIES).sort();
        const indexerKeys = Object.keys(indexerCaps).sort();
        assert.deepStrictEqual(
            syncKeys, indexerKeys,
            'capability set drift: a capability added/removed in xchain-indexer ' +
            'src/configs/BTC.js STAKING.CAPABILITIES must be mirrored in xchain-sync ' +
            'consensus-constants.js BTC_STAKE_CAPABILITIES, or the follower stakes_root forks'
        );
    });

    it('each capability MIN_STAKE floor matches the indexer', function () {
        for (const cap of Object.keys(indexerCaps)) {
            assert.strictEqual(
                consts.BTC_STAKE_CAPABILITIES[cap], indexerCaps[cap],
                `MIN_STAKE floor drift for "${cap}": xchain-sync ` +
                `${consts.BTC_STAKE_CAPABILITIES[cap]} != xchain-indexer ${indexerCaps[cap]}`
            );
        }
    });

    it('VALIDATOR_QUERY_LIMIT matches the indexer', function () {
        assert.strictEqual(
            consts.VALIDATOR_QUERY_LIMIT, indexerLimit,
            `VALIDATOR_QUERY_LIMIT drift: xchain-sync ${consts.VALIDATOR_QUERY_LIMIT} ` +
            `!= xchain-indexer ${indexerLimit}; the result cap bounds the stake set and ` +
            `is consensus-critical on the follower apply path`
        );
    });
});
