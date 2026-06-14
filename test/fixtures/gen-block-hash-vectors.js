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
 * Generator for test/fixtures/block-hash-vectors.json — the BlockHasher
 * conformance lock. It runs BlockHasher's assembly over canned replica rows
 * but computes the final hash with the REAL xchain-indexer getDataHash, so the
 * expected hashes are authentically the indexer's (not a sync-side echo). The
 * committed JSON then locks sync's BlockHasher + utility.getDataHash against
 * regression with NO runtime cross-repo dependency in the unit test. The
 * xchain-e2e-test recompute scenario is the live cross-repo drift guard.
 *
 * Run manually (sibling xchain-indexer must be present) and commit the output:
 *   INDEXER_COIN=BTC INDEXER_NETWORK=regtest \
 *     node test/fixtures/gen-block-hash-vectors.js
 *
 ********************************************************************/

const fs   = require('fs');
const path = require('path');

process.env.INDEXER_COIN    = process.env.INDEXER_COIN    || 'BTC';
process.env.INDEXER_NETWORK = process.env.INDEXER_NETWORK || 'regtest';

const BlockHasher  = require('../../src/BlockHasher');
const IndexerUtil  = require('../../../xchain-indexer/src/utility.js');

// Canned rows the 11 BlockHasher queries return, IN CALL ORDER:
// credits, debits, escrows, actions, contracts, state, executions,
// emissions, deposits, withdrawals, previous-block-hash-row.
// As of BLOCK_HASH_VERSION 2 the consensus projections carry the RESOLVED canonical
// strings (address/tick/action/status) the JOINs produce, never the raw lookup ids — so
// these canned rows model the resolved column set exactly as the live queries return it.
const BLOCK_INDEX = 1000;
const results = [
    // credits
    [ { action_index: 1, address: '1JDogZS6tQcSxwfxhv6XKKjcyicYA4Feev', tick: 'JDOG', amount: '1000' },
      { action_index: 3, address: '1FuckButtZ6tQcSxwfxhv6XKKjcyiabcde', tick: 'JDOG', amount: '500'  } ],
    // debits
    [ { action_index: 1, address: '1AdminZS6tQcSxwfxhv6XKKjcyicYA4Fee', tick: 'JDOG', amount: '1500' } ],
    // escrows
    [],
    // actions  (note: assembled as an array WITH block_index/previous_hash props)
    [ { action_index: 1, tx_index: 1, action: 'SEND' },
      { action_index: 2, tx_index: 1, action: 'DEPLOY' },
      { action_index: 3, tx_index: 2, action: 'SEND' } ],
    // contracts
    [ { action_index: 2, source_address: '1AdminZS6tQcSxwfxhv6XKKjcyicYA4Fee', code_hash: 'deadbeefcafe', status: 'deployed' } ],
    // contract state
    [ { contract_index: 2, state_key: 'counter', state_value: '42' },
      { contract_index: 2, state_key: 'owner',   state_value: 'abc' } ],
    // executions
    [ { action_index: 3, contract_index: 2, caller_address: '1FuckButtZ6tQcSxwfxhv6XKKjcyiabcde', gas_used: '12345', status: 'success', emitted_count: 1 } ],
    // emissions
    [ { execution_index: 3, emitted_action: 'EXECUTE', action_index: 3, position: 0 } ],
    // deposits
    [ { action_index: 1, contract_index: 2, source_address: '1JDogZS6tQcSxwfxhv6XKKjcyicYA4Feev', tick: 'JDOG', amount: '1000', status: 'success' } ],
    // withdrawals
    [],
    // previous-block hash row (chaining)
    [ { ledger: 'prevLedgerHash', actions: 'prevActionsHash', contracts: 'prevContractsHash' } ]
];

async function main(){
    let call = 0;
    const mockDb = { doQuery: async () => results[call++] };
    // Hash with the INDEXER's getDataHash so the expected values are authentic.
    const hasher = new BlockHasher(mockDb, new IndexerUtil());
    const expected = await hasher.computeBlockHashes(BLOCK_INDEX);

    const vector = { block_index: BLOCK_INDEX, results, expected };
    const out = path.join(__dirname, 'block-hash-vectors.json');
    fs.writeFileSync(out, JSON.stringify(vector, null, 2) + '\n');
    console.log('wrote ' + out);
    console.log('expected: ' + JSON.stringify(expected, null, 2));
}

main().catch(e => { console.error(e); process.exit(1); });
