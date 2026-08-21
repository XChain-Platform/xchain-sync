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
 **********************************************************************
 *
 * The FOLLOWER twin of the source's "no prior state_tree_roots row" guard.
 *
 * The indexer twin (xchain-indexer/src/stateCommitment.js) full-recomputes when
 * block-1 has no roots row, and its comment names the alternative as the thing
 * not to do: "Do NOT substitute the empty-tree root: that silently emits a
 * balances_root forked from a from-genesis node." The follower threaded from
 * EMPTY_ROOT_HEX in exactly that case and applied only THIS block's touched keys
 * on top, so it committed a root covering one block's deltas and nothing else.
 * A follower reaches that state after a snapshot bootstrap, a rollback that took
 * the row below this height, or a resume across a data gap.
 *
 * LTC:mainnet is used because no sub-tree is armed there, so the balances branch
 * is the only thing under test.
 *
 ********************************************************************/

'use strict';

const assert = require('assert');
const SC = require('../../src/stateCommitment.js');

const CHAIN = 'LTC', NETWORK = 'mainnet', BLOCK = 500000;
const BALANCES = [{ address: 'ltc1holder', tick: 'XCP', net: '250.000000000000000000' }];

// Mock over exactly the reads computeFollowerRoots issues on a chain with no
// armed sub-tree. The full-balance set is non-empty, so a full rebuild and the
// empty-root fallback are distinguishable roots rather than both EMPTY.
function makeFollowerDb({ priorRow }){
    const nodes = new Map();
    const persisted = {};
    const route = async (sql, params) => {
        if(/FROM state_tree_nodes/.test(sql)){
            const v = nodes.get(params[0]);
            return v ? [v] : [];
        }
        if(/INSERT IGNORE INTO state_tree_nodes/.test(sql)){
            if(!nodes.has(params[0]))
                nodes.set(params[0], { left_hash: params[1], right_hash: params[2] });
            return [];
        }
        if(/UNION ALL/.test(sql) && /credits/.test(sql)) return BALANCES;
        if(/INSERT INTO\s+state_tree_roots/.test(sql)){
            persisted.balances_root = params[3];
            return [];
        }
        return [];
    };
    const db = {
        doQuery: route,
        doQueryStrict: route,
        getStateRootsRow: async () => (priorRow || null),
        getNetBalance: async () => null,
        getStakeWeightsByCapability: async () => [],
        getBlockLeafRows: async () => ({ ledger: { credits: [], debits: [], escrows: [] },
                                         actions: [], contracts: {} })
    };
    return { db, persisted };
}

describe('follower: a missing prior roots row full-rebuilds, it does not thread from EMPTY @regression', function(){

    it('commits the full-rebuild root when block-1 has no state_tree_roots row', async function(){
        const { db, persisted } = makeFollowerDb({ priorRow: null });
        const out = await SC.computeFollowerRoots(db, CHAIN, NETWORK, BLOCK, [], false);

        const { db: refDb } = makeFollowerDb({ priorRow: null });
        const expected = await SC.buildFullBalancesRoot(refDb, CHAIN, NETWORK, BLOCK);

        assert.strictEqual(out.balances_root, expected,
            'the follower must land on the root the source full-recomputes');
        assert.strictEqual(persisted.balances_root, expected);
        assert.notStrictEqual(out.balances_root, SC.EMPTY_ROOT_HEX,
            'threading from the empty root commits only this block deltas, which is the fork');
    });

    it('an empty touched set does not hide it: EMPTY was the exact old answer', async function(){
        // With no prior row AND no touched keys the old code returned EMPTY_ROOT_HEX
        // verbatim, so this is the sharpest form of the regression.
        const { db } = makeFollowerDb({ priorRow: null });
        const out = await SC.computeFollowerRoots(db, CHAIN, NETWORK, BLOCK, [], false);
        assert.notStrictEqual(out.balances_root, SC.EMPTY_ROOT_HEX);
    });

    it('a present prior row still threads incrementally, unchanged', async function(){
        // The guard must fire only on the missing-row case; a normal block keeps the
        // cheap incremental thread rather than rebuilding the whole tree every block.
        const { db } = makeFollowerDb({ priorRow: { balances_root: SC.EMPTY_ROOT_HEX } });
        const out = await SC.computeFollowerRoots(db, CHAIN, NETWORK, BLOCK, [], false);
        assert.strictEqual(out.balances_root, SC.EMPTY_ROOT_HEX,
            'an empty touched set over a prior EMPTY root threads to EMPTY');
    });
});
