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
 *
 * Generator for test/fixtures/state-hash-vectors.json — the state_hash
 * conformance lock (sibling of gen-block-hash-vectors.js). It runs the shared
 * buildStateHashData over canned RESOLVED rows but computes the final hash with
 * the REAL xchain-indexer getDataHash, so the expected hash is authentically the
 * indexer's. The committed JSON then locks sync's stateHash.js assembly +
 * utility.getDataHash with NO runtime cross-repo dependency in the unit test.
 * The xchain-e2e-test recompute scenario is the live cross-repo drift guard.
 *
 * Run manually (sibling xchain-indexer must be present) and commit the output:
 *   INDEXER_COIN=BTC INDEXER_NETWORK=regtest \
 *     node test/fixtures/gen-state-hash-vectors.js
 *
 ********************************************************************/

const fs   = require('fs');
const path = require('path');

process.env.INDEXER_COIN    = process.env.INDEXER_COIN    || 'BTC';
process.env.INDEXER_NETWORK = process.env.INDEXER_NETWORK || 'regtest';

const { buildStateHashData } = require('../../src/stateHash');
const IndexerUtil = require('../../../xchain-indexer/src/utility.js');

const BLOCK_INDEX     = 1000;
const ACTIVATION_DELAY = 6;       // BTC regtest ACTIVATION_DELAY_BLOCKS — deactivation stamp = 1000 + 6
const GAS_TICK        = 'XCHAIN';
const COMPLETED_ID    = 7;

// Canned RESOLVED rows the buildStateHashData queries return, IN CALL ORDER:
//   deactivations: stakes, delegations, contract_stakes, contract_delegations
//   slashes:       stakes, unstakes, contract_stakes, contract_unstakes
//   request_status:attests, xcalls
//   cooldown:      unstakes, contract_unstakes
//   credits:       (single UNION ALL result)
// Surrogate ids are already resolved to canonical strings by the live JOINs, so
// these model the resolved column set exactly.
const results = [
    // --- deactivation_block stamps (value = BLOCK_INDEX + ACTIVATION_DELAY = 1006) ---
    [ { action_index: 10, deactivation_block: 1006 } ],   // stakes
    [],                                                   // delegations
    [ { action_index: 11, deactivation_block: 1006 } ],   // contract_stakes
    [],                                                   // contract_delegations
    // --- SLASH amount cuts ---
    [ { action_index: 12, amount: '900' } ],              // stakes
    [],                                                   // unstakes
    [],                                                   // contract_stakes
    [ { action_index: 13, amount: '50' } ],               // contract_unstakes
    // --- v0 request_status flips ---
    [ { action_index: 14, request_status: 'fulfilled', resolved_block: 1000 } ], // attests
    [ { action_index: 15, request_status: 'expired',   resolved_block: 1000 } ], // xcalls
    // --- cooldown-maturity status flips (status_id resolved) ---
    [ { action_index: 16, status: 'completed' } ],        // unstakes
    [ { action_index: 17, status: 'completed' } ],        // contract_unstakes
    // --- backdated refund credits (capability GAS + contract own-tick, UNION ALL) ---
    [ { action_index: 16, address: '1JDogZS6tQcSxwfxhv6XKKjcyicYA4Feev', tick: 'XCHAIN', amount: '1000' },
      { action_index: 17, address: '1FuckButtZ6tQcSxwfxhv6XKKjcyiabcde', tick: 'JDOG',   amount: '500'  } ]
];

async function main(){
    let call = 0;
    const mockDb = {
        doQuery:     async () => results[call++],
        getStatusId: async () => COMPLETED_ID
    };
    const stateData = await buildStateHashData(mockDb, BLOCK_INDEX, {
        activationDelay: ACTIVATION_DELAY,
        gasTick:         GAS_TICK
    });
    // Hash with the INDEXER's getDataHash so the expected value is authentic.
    const expected = new IndexerUtil().getDataHash(stateData);

    const vector = {
        block_index:      BLOCK_INDEX,
        activation_delay: ACTIVATION_DELAY,
        gas_tick:         GAS_TICK,
        completed_id:     COMPLETED_ID,
        results,
        expected_state_hash: expected
    };
    const out = path.join(__dirname, 'state-hash-vectors.json');
    fs.writeFileSync(out, JSON.stringify(vector, null, 2) + '\n');
    console.log('wrote ' + out);
    console.log('expected_state_hash: ' + expected);
}

main().catch(e => { console.error(e); process.exit(1); });
