/*********************************************************************
 *
 * Copyright © 2025–2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC – https://dankest.llc
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This file is part of XChain Platform. Licensed under the GNU Affero
 * General Public License v3.0 or later; see LICENSE.md. A commercial
 * license (without AGPL source-disclosure terms) is available -
 * contact legal@dankest.llc.
 *
 **********************************************************************
 * test/unit/blockhash_conformance_twin.test/02_stakes_root_twin_parity.test.js
 *
 * Cross-repo ROOT parity for buildStakesRoot, the second declared divergence in
 * the header of src/state_commitment/index.js. The indexer patches its
 * predecessor's stakes tree when the stake set changes; this copy rebuilds it.
 * The shape of that difference is pinned statically in
 * test/unit/blockhash_conformance_twin.test.js; this case runs BOTH functions over the
 * same changing stake set, block by block, and asserts they commit the same
 * root every block. A patch that mishandled a removed or changed key would fork
 * the follower's state_root and halt it, so this is a consensus guard.
 *
 * No DB required: both sides build on their own MemoryNodeStore. Skips when the
 * xchain-indexer sibling is absent or is a lane symlink into a live main checkout,
 * and fails in either case where XCHAIN_REQUIRE_SIBLINGS=1.
 */

'use strict';

const assert = require('assert');
const path   = require('path');
const { siblingCheckout, skipOrFail } = require('../../helpers/sibling_checkout.js');

const SYNC = require('../../../src/state_commitment/index.js');
const M    = require('../../../src/merkle.js');

const INDEXER_ROOT = process.env.XCHAIN_INDEXER_SQL_PATH
    ? path.resolve(process.env.XCHAIN_INDEXER_SQL_PATH, '..', '..')
    : path.join(__dirname, '../../../../xchain-indexer');
const INDEXER_STAKES = path.join(INDEXER_ROOT, 'src/state_commitment/stakes_root.js');
const INDEXER_SMT    = path.join(INDEXER_ROOT, 'src/state_commitment/persistent_smt.js');

// Deterministic stake-set walk, so any failure reproduces exactly.
function makeRng(seed){
    let s = seed;
    return function(n){ s = (s * 1103515245 + 12345) & 0x7fffffff; return s % n; };
}

// One member leaf per staker plus the total leaf, the shape gatherStakeEntries emits.
function entriesOf(stakers){
    const out = [];
    for(const [pk, w] of stakers)
        out.push([ M.toHex(M.stakeKey(pk, 'BTC_VALIDATOR')), M.toHex(M.stakeMemberLeaf('src-' + pk, w)) ]);
    if(stakers.size)
        out.push([ M.toHex(M.stakeKey(M.STAKE_TOTAL_PUBKEY, 'BTC_VALIDATOR')),
                   M.toHex(M.stakeTotalLeaf(String(stakers.size))) ]);
    return out;
}

// Add or re-weight a staker, drop one, or leave the set unchanged; empty it once
// at step 120 so every key is deleted back to the empty root.
function stepStakers(stakers, rnd, step){
    const op = rnd(10);
    if(op < 3) stakers.set('pk' + rnd(32), String(1 + rnd(1000)));
    else if(op < 5 && stakers.size) stakers.delete(Array.from(stakers.keys())[rnd(stakers.size)]);
    else if(op === 5 && stakers.size)
        stakers.set(Array.from(stakers.keys())[rnd(stakers.size)], String(1 + rnd(1000)));
    if(step === 120) stakers.clear();
}

// Count the indexer's patch deletes and its full rebuilds, so a run that never
// reached the incremental path cannot pass as parity (buildFull never deletes).
function countPatchPath(smt){
    const counts = { deletes: 0, fullBuilds: 0 };
    const update = smt.update.bind(smt);
    smt.update = function(rootHex, keyBuf, leaf){
        if(leaf == null) counts.deletes++;
        return update(rootHex, keyBuf, leaf);
    };
    const buildFull = smt.buildFull.bind(smt);
    smt.buildFull = function(entries){
        counts.fullBuilds++;
        return buildFull(entries);
    };
    return counts;
}

describe('buildStakesRoot twin root parity: xchain-sync == xchain-indexer @regression', function(){
    let IDX_STAKES, IDX_SMT;

    before(function(){
        // Refuse an absent sibling and a lane symlink into a live main checkout alike.
        const verdict = siblingCheckout(__dirname, INDEXER_STAKES);
        if(!skipOrFail(this, verdict, 'the buildStakesRoot twin root parity guard')) return;
        IDX_STAKES = require(INDEXER_STAKES);
        IDX_SMT    = require(INDEXER_SMT);
    });

    beforeEach(function(){
        if(SYNC.resetStakesMemo) SYNC.resetStakesMemo();
        if(IDX_STAKES) IDX_STAKES.resetStakesMemo();
    });

    it('commits the same stakes root every block while the stake set changes', async function(){
        const rnd     = makeRng(424242);
        const syncSmt = new SYNC.PersistentSMT(new SYNC.MemoryNodeStore());
        const idxSmt  = new IDX_SMT.PersistentSMT(new IDX_SMT.MemoryNodeStore());
        const counts  = countPatchPath(idxSmt);
        const stakers = new Map();
        let blockIndex = 0, prevDigest = null, changedBlocks = 0;
        for(let step = 1; step <= 240; step++){
            stepStakers(stakers, rnd, step);
            // Skip heights once, so both sides must fall back to a full rebuild.
            blockIndex += (step === 180) ? 5 : 1;

            const entries  = entriesOf(stakers);
            const reversed = entries.slice().reverse();
            const digest   = entries.map(e => e.join(':')).sort().join('|');
            if(prevDigest !== null && digest !== prevDigest) changedBlocks++;
            prevDigest = digest;
            const syncRoot = await SYNC.buildStakesRoot(syncSmt, 'BTC', 'regtest', blockIndex, entries);
            const idxRoot  = await IDX_STAKES.buildStakesRoot(idxSmt, 'BTC', 'regtest', blockIndex, reversed);
            assert.strictEqual(idxRoot, syncRoot,
                'stakes root forked at step ' + step + ' (block ' + blockIndex + ', ' + stakers.size +
                ' stakers): the indexer incremental patch in xchain-indexer/src/state_commitment/' +
                'stakes_root.js no longer equals the follower full rebuild');
        }
        // buildFull never deletes, and a set change the patch handled needs no rebuild.
        assert.ok(counts.deletes > 0 && counts.fullBuilds < changedBlocks,
            'the walk never reached the indexer incremental patch (deletes=' + counts.deletes +
            ', fullBuilds=' + counts.fullBuilds + ', changedBlocks=' + changedBlocks +
            '), so this parity run proved nothing');
    });
});
