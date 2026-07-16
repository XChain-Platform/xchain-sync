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
 * test/unit/state-hash-index-map.test.js to lock the follower's copy to the same
 * gating + folding behavior: inert by default (no preimage change), and when armed
 * a divergent id->address pair changes state_hash so the follower halts.
 *
 ********************************************************************/
'use strict';

const assert  = require('assert');
const Utility = require('../../src/utility');
const {
    buildStateHashData, isIndexMapStateHashActive, INDEX_MAP_STATE_HASH_ACTIVATION,
    POLL_FINALIZE_STATE_HASH_ACTIVATION, TOKEN_SUPPLY_STATE_HASH_ACTIVATION,
} = require('../../src/stateHash');

const util = new Utility();
const PREFEATURE_KEYS = ['deactivations', 'slashes', 'request_status', 'cooldown', 'credits', 'anchor_invalid', 'block_index', 'state_hash_version'];

function dbFor(results, completedId){
    let i = 0;
    return { doQuery: async () => results[i++], getStatusId: async () => (completedId === undefined ? null : completedId) };
}
function baseResults(){ return [[], [], [], [], [], [], [], [], []]; }

async function build(opts, results){
    const data = await buildStateHashData(dbFor(results || baseResults(), null), 7, opts);
    return { data, hash: util.getDataHash(data) };
}
async function withArmed(height, fn){
    const prev = INDEX_MAP_STATE_HASH_ACTIVATION.regtest;
    INDEX_MAP_STATE_HASH_ACTIVATION.regtest = height;
    try { return await fn(); } finally { INDEX_MAP_STATE_HASH_ACTIVATION.regtest = prev; }
}

describe('state_hash index-map class (id-determinism P4) - follower twin @regression', function(){

    // Isolate this suite from the poll_finalize / token_supply classes (armed on
    // regtest since 2026-07-07): their query slots would shift the canned
    // call-order mock and their preimage keys would break the shape assertions.
    // The index-map class itself is also disarmed suite-locally (armed on regtest
    // since 2026-07-16, ) so the inert-shape tests below still exercise the
    // below-threshold preimage; withArmed() re-arms per test.
    let pollPrev, tokenPrev, indexPrev;
    before(function(){
        pollPrev  = POLL_FINALIZE_STATE_HASH_ACTIVATION.regtest;
        tokenPrev = TOKEN_SUPPLY_STATE_HASH_ACTIVATION.regtest;
        indexPrev = INDEX_MAP_STATE_HASH_ACTIVATION.regtest;
        POLL_FINALIZE_STATE_HASH_ACTIVATION.regtest = 999999999;
        TOKEN_SUPPLY_STATE_HASH_ACTIVATION.regtest  = 999999999;
        INDEX_MAP_STATE_HASH_ACTIVATION.regtest     = 999999999;
    });
    after(function(){
        POLL_FINALIZE_STATE_HASH_ACTIVATION.regtest = pollPrev;
        TOKEN_SUPPLY_STATE_HASH_ACTIVATION.regtest  = tokenPrev;
        INDEX_MAP_STATE_HASH_ACTIVATION.regtest     = indexPrev;
    });

    it('gate: regtest armed from genesis , arms at/after the per-chain height', function(){
        assert.strictEqual(indexPrev, 0, 'regtest armed from genesis ');
        assert.strictEqual(isIndexMapStateHashActive(7, 'regtest'), false, 'suite-local disarm holds');
        assert.strictEqual(isIndexMapStateHashActive(7, null), false);
        return withArmed(5, () => {
            assert.strictEqual(isIndexMapStateHashActive(4, 'regtest'), false);
            assert.strictEqual(isIndexMapStateHashActive(5, 'regtest'), true);
        });
    });

    it('inert: follower preimage is byte-identical to the pre-feature shape', async function(){
        const { data } = await build({ activationDelay: null, gasTick: 'XCHAIN', network: 'regtest' });
        assert.deepStrictEqual(Object.keys(data), PREFEATURE_KEYS);
    });

    it('armed: a divergent id->address pair changes state_hash (follower would HALT)', async function(){
        await withArmed(0, async () => {
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
});
