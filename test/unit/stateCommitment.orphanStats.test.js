/*********************************************************************
 *
 * Copyright (c) 2025-2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC, https://dankest.llc
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This file is part of XChain Platform. Licensed under the GNU Affero
 * General Public License v3.0 or later; see LICENSE.md. A commercial
 * license (without AGPL source-disclosure terms) is available; contact
 * legal@dankest.llc.
 *
 **********************************************************************
 * test/unit/stateCommitment.orphanStats.test.js
 *
 * Sync-follower twin of the indexer reportOrphanStats test. Kept IDENTICAL in
 * cases to guard against the two stateCommitment.js copies drifting. Read-only
 * orphan-count observability over the COW state_tree_nodes store; marks from the
 * UNION of all RETAINED state_tree_roots rows; never deletes.
 */

'use strict';

const assert = require('assert');
const M      = require('../../src/merkle.js');
const SC     = require('../../src/stateCommitment.js');

function makeQuery(store, liveRoots) {
    return async (sql) => {
        if (/COUNT\(\*\)/.test(sql)) return [{ c: store.map.size }];
        if (/SELECT node_hash, left_hash, right_hash/.test(sql)) {
            return Array.from(store.map.entries()).map(([h, v]) => ({
                node_hash: h, left_hash: v.left_hash, right_hash: v.right_hash
            }));
        }
        if (/FROM state_tree_roots/.test(sql)) return liveRoots.map(r => ({ r }));
        return [];
    };
}

function leaf(n) { return M.toHex(M.leafHash(M.canonicalAmount(String(n)))); }
function key(hexByte) { return M.toBuf(hexByte.repeat(64)); }

describe('stateCommitment.reportOrphanStats (sync twin) @regression @tier2', function () {

    it('reports zero orphans when every node is reachable from the single retained root', async function () {
        const store = new SC.MemoryNodeStore();
        const smt   = new SC.PersistentSMT(store);
        const rootA = await smt.update(SC.EMPTY_ROOT_HEX, key('1'), leaf(5));

        const stats = await SC.reportOrphanStats(makeQuery(store, [rootA, SC.EMPTY_ROOT_HEX]), 'BTC', 'regtest');
        assert.strictEqual(stats.totalNodes, store.map.size);
        assert.strictEqual(stats.reachableNodes, store.map.size);
        assert.strictEqual(stats.orphanCount, 0);
        assert.strictEqual(stats.reachabilitySkipped, false);
    });

    it('counts nodes reachable ONLY from a dropped (reorged) root as orphans', async function () {
        const store = new SC.MemoryNodeStore();
        const smt   = new SC.PersistentSMT(store);
        const rootA = await smt.update(SC.EMPTY_ROOT_HEX, key('a'), leaf(5));
        const sizeA = store.map.size;
        const rootB = await smt.update(SC.EMPTY_ROOT_HEX, key('b'), leaf(9));
        const sizeB = store.map.size - sizeA;
        assert.notStrictEqual(rootA, rootB);

        const stats = await SC.reportOrphanStats(makeQuery(store, [rootB, SC.EMPTY_ROOT_HEX]), 'BTC', 'regtest');
        assert.strictEqual(stats.totalNodes, sizeA + sizeB);
        assert.strictEqual(stats.reachableNodes, sizeB);
        assert.strictEqual(stats.orphanCount, sizeA);
    });

    it('keeps historical-root nodes live (proof safety): retaining BOTH roots yields zero orphans', async function () {
        const store = new SC.MemoryNodeStore();
        const smt   = new SC.PersistentSMT(store);
        const rootA = await smt.update(SC.EMPTY_ROOT_HEX, key('a'), leaf(5));
        const rootB = await smt.update(SC.EMPTY_ROOT_HEX, key('b'), leaf(9));

        const stats = await SC.reportOrphanStats(makeQuery(store, [rootA, rootB]), 'BTC', 'regtest');
        assert.strictEqual(stats.orphanCount, 0);
        assert.strictEqual(stats.reachableNodes, store.map.size);
    });

    it('returns all-zero for an empty store', async function () {
        const store = new SC.MemoryNodeStore();
        const stats = await SC.reportOrphanStats(makeQuery(store, []), 'BTC', 'regtest');
        assert.deepStrictEqual(stats, { totalNodes: 0, reachableNodes: 0, orphanCount: 0, reachabilitySkipped: false });
    });

    it('skips the in-memory mark when the store exceeds maxNodes', async function () {
        const store = new SC.MemoryNodeStore();
        const smt   = new SC.PersistentSMT(store);
        await smt.update(SC.EMPTY_ROOT_HEX, key('a'), leaf(5));

        const stats = await SC.reportOrphanStats(makeQuery(store, []), 'BTC', 'regtest', { maxNodes: 1 });
        assert.ok(stats.totalNodes > 1);
        assert.strictEqual(stats.reachableNodes, null);
        assert.strictEqual(stats.orphanCount, null);
        assert.strictEqual(stats.reachabilitySkipped, true);
    });
});
