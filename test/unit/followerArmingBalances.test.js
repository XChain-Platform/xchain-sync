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
 * The FOLLOWER twin of the escrow arming-block full build.
 *
 * The source twin's gate lives in xchain-indexer/src/stateCommitment.js and is
 * pinned by its own vectors in that repo's test/unit/stateCommitment.test.js. This
 * file exists because fixing only the source RELOCATES the divergence instead of
 * removing it: the arming replay writes no journal row for a key whose locked total
 * is unchanged, so touched-set application at the arming height misses it, and the
 * prior committed root holds no escrow leaves at all (block-1 is below the armed
 * height). A source that full-builds and a live follower that still threads
 * incrementally commit two different balances_roots for one block.
 *
 * BTC:regtest is the only chain the escrow leaf is armed on
 * (state_subtree_activation.js, block 11200), so the boundary is 11200.
 *
 ********************************************************************/

'use strict';

const assert = require('assert');
const SC = require('../../src/stateCommitment.js');

const ARM = 11200;
const LIVE = [{ address: 'escAddr1', tick: 'XCP', locked_amount: '250.000000000000000000' }];

// Mock over exactly the reads computeFollowerRoots issues on BTC:regtest. The
// unchanged live lock is the whole scenario, so the arming-height touched read is
// EMPTY while the live (MAX(id)-per-key) read returns the lock.
function makeFollowerDb({ priorRow, liveJournalRows }){
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
        // touchedEscrowKeys: rows stamped at THIS block only. Empty is the point.
        if(/escrow_leaf_journal/.test(sql) && /j\.block_index = \?/.test(sql)) return [];
        // liveEscrowLeaves: the MAX(id)-per-key winner set, what a snapshot node sees.
        if(/escrow_leaf_journal/.test(sql) && /MAX\(id\)/.test(sql)) return liveJournalRows;
        if(/UNION ALL/.test(sql) && /credits/.test(sql)) return [];   // no spendable balances
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
        getStakeWeightsByCapability: async () => [],
        getBlockLeafRows: async () => ({ ledger: { credits: [], debits: [], escrows: [] },
                                         actions: [], contracts: {} })
    };
    return { db, persisted };
}

describe('follower: escrow arming block full-builds balances @regression', function(){

    it('commits the live escrow leaf that has no arming-height journal row', async function(){
        const { db, persisted } = makeFollowerDb({
            priorRow: { balances_root: SC.EMPTY_ROOT_HEX }, liveJournalRows: LIVE });
        const out = await SC.computeFollowerRoots(db, 'BTC', 'regtest', ARM, [], false);

        // Ground truth: the snapshot build over the same live escrow set.
        const { db: refDb } = makeFollowerDb({ priorRow: null, liveJournalRows: LIVE });
        const expected = await SC.buildFullBalancesRoot(refDb, 'BTC', 'regtest', ARM);

        assert.strictEqual(out.balances_root, expected,
            'the follower must commit the same root the source and a snapshot node commit');
        assert.strictEqual(persisted.balances_root, expected);
        assert.notStrictEqual(out.balances_root, SC.EMPTY_ROOT_HEX,
            'threading the prior root drops the unchanged live lock');
    });

    it('the block AFTER arming still threads incrementally', async function(){
        // 11201 is armed but not the boundary, so the incremental branch owns it and
        // the fix does not turn every armed block into a full rebuild on the follower.
        const { db } = makeFollowerDb({
            priorRow: { balances_root: SC.EMPTY_ROOT_HEX }, liveJournalRows: LIVE });
        const out = await SC.computeFollowerRoots(db, 'BTC', 'regtest', ARM + 1, [], false);
        assert.strictEqual(out.balances_root, SC.EMPTY_ROOT_HEX);
    });
});
