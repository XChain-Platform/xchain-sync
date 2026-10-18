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
 * Index-map state_hash class (id-determinism P4) - FOLLOWER twin.
 *
 * xchain-sync/src/stateHash.js is the byte-identical copy the follower recomputes
 * with (ClientSync HALTS on mismatch). This mirrors the indexer's
 * test/unit/state_commitment/state_hash_index_map.test.js to lock the follower's copy to the same
 * folding behavior: every preimage includes the map, and a divergent id->address
 * pair changes state_hash so the follower halts.
 *
 ********************************************************************/
'use strict';

const assert  = require('assert');
const Utility = require('../../src/util');
const {
    buildStateHashData,
    POLL_FINALIZE_STATE_HASH_ACTIVATION, TOKEN_SUPPLY_STATE_HASH_ACTIVATION,
    BET_STATUS_STATE_HASH_ACTIVATION, ATTEST_BATCH_HEAD_STATE_HASH_ACTIVATION,
} = require('../../src/consensus/state_hash');

const util = new Utility();
function dbFor(results, completedId){
    let i = 0;
    return { doQuery: async () => results[i++], getStatusId: async () => (completedId === undefined ? null : completedId) };
}
function baseResults(){ return [[], [], [], [], [], [], [], [], []]; }

async function build(opts, results){
    const data = await buildStateHashData(dbFor(results || baseResults(), null), 7, opts);
    return { data, hash: util.getDataHash(data) };
}
describe('state_hash index-map class (id-determinism P4) - follower twin @regression', function(){

    // Isolate this suite from the poll_finalize / token_supply classes (armed on
    // regtest since 2026-07-07): their query slots would shift the canned
    // call-order mock and their preimage keys would break the shape assertions.
    let pollPrev, tokenPrev, betPrev, attestBatchHeadPrev;
    before(function(){
        pollPrev  = POLL_FINALIZE_STATE_HASH_ACTIVATION.regtest;
        tokenPrev = TOKEN_SUPPLY_STATE_HASH_ACTIVATION.regtest;
        betPrev   = BET_STATUS_STATE_HASH_ACTIVATION.regtest;
        attestBatchHeadPrev = ATTEST_BATCH_HEAD_STATE_HASH_ACTIVATION.regtest;
        POLL_FINALIZE_STATE_HASH_ACTIVATION.regtest = 999999999;
        TOKEN_SUPPLY_STATE_HASH_ACTIVATION.regtest  = 999999999;
        BET_STATUS_STATE_HASH_ACTIVATION.regtest    = 999999999;
        ATTEST_BATCH_HEAD_STATE_HASH_ACTIVATION.regtest = 999999999;
    });
    after(function(){
        POLL_FINALIZE_STATE_HASH_ACTIVATION.regtest = pollPrev;
        TOKEN_SUPPLY_STATE_HASH_ACTIVATION.regtest  = tokenPrev;
        BET_STATUS_STATE_HASH_ACTIVATION.regtest    = betPrev;
        ATTEST_BATCH_HEAD_STATE_HASH_ACTIVATION.regtest = attestBatchHeadPrev;
    });

    // Index-map rows are now part of every preimage, so each fixture supplies both
    // query results directly. Changing only the address id proves that persistent
    // identifier drift changes the follower hash while the ticker rows remain fixed.
    it('armed: a divergent id->address pair changes state_hash (follower would HALT)', async function(){
        const fromGenesis = baseResults().concat([[{ id: 1, address: 'bc1qSrc' }], [{ id: 2, tick: 'TOKA' }]]);
        const recovered   = baseResults().concat([[{ id: 3, address: 'bc1qSrc' }], [{ id: 2, tick: 'TOKA' }]]);
        const g = await build({ activationDelay: null, gasTick: 'XCHAIN', network: 'regtest' }, fromGenesis);
        const r = await build({ activationDelay: null, gasTick: 'XCHAIN', network: 'regtest' }, recovered);
        assert.deepStrictEqual(Object.keys(g.data),
            ['deactivations', 'slashes', 'request_status', 'cooldown', 'credits', 'anchor_invalid',
             'index_addresses_new', 'index_tickers_new', 'block_index', 'state_hash_version']);
        assert.notStrictEqual(r.hash, g.hash);
    });
});
