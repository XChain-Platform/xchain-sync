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
 *
 * PersistentSMT node writes are BATCHED, and the batching changes
 * nothing but the statement count.
 *
 * The defect this pins was a throughput one with a correctness-shaped symptom.
 * One key update writes SMT_DEPTH internal nodes, and a value leaf's ancestors
 * are never an all-empty subtree, so the write loop is always a full 256 rows.
 * Issued one statement at a time that is 256 sequential DB round trips per key.
 * On the BTC regtest venue the stakes subtree is rebuilt from an empty root
 * EVERY block over 49 stake keys, which is 12,544 round trips and 25-66s of wall
 * clock per block, against LTC's 3-5s for the same code (LTC commits the empty
 * stakes root, so it never pays it). Five standing envelope tests failed as
 * timeouts and read as correctness failures.
 *
 * Two properties, and the second is the one with teeth:
 *   1. EQUIVALENCE - the batched engine emits the identical root and the
 *      identical node set as a per-node writer. This is the consensus guard.
 *   2. ROUND-TRIP COUNT - a key update costs a BOUNDED number of store writes,
 *      not one per level. Restoring the per-node loop reddens this and only
 *      this, which is what makes it a regression gate rather than a restatement
 *      of the conformance suite. The existing golden/fuzz tests all pass
 *      against the slow engine, which is exactly why they never caught it.
 *
 * No DB required: the counting store implements the same interface as
 * DbNodeStore. The DB-shaped half (chunking, SQL arity) is asserted against a
 * recording fake of doQueryStrict rather than a live MariaDB.
 *
 ********************************************************************/

'use strict';

const assert = require('assert');
const M  = require('../../../src/merkle.js');
const SC = require('../../../src/state_commitment/index.js');

// Records what would go to MariaDB without needing one.
function fakeDb(){
    const calls = [];
    return {
        calls,
        async doQueryStrict(sql, args){ calls.push({ sql, args }); return []; }
    };
}

describe("stateCommitment: DbNodeStore.putMany SQL shape @regression", function(){

    it('writes one multi-row INSERT IGNORE with three bound params per row', async function(){
        const db = fakeDb();
        const store = new SC.DbNodeStore(db);
        const nodes = [];
        for(let i = 0; i < 10; i++)
            nodes.push({ hash: 'h' + i, left: 'l' + i, right: 'r' + i });
        await store.putMany(nodes);

        assert.strictEqual(db.calls.length, 1, 'ten nodes must be one statement, not ten');
        const { sql, args } = db.calls[0];
        assert.ok(/^INSERT IGNORE INTO state_tree_nodes \(node_hash, left_hash, right_hash\) VALUES /.test(sql),
            'statement must stay an INSERT IGNORE on state_tree_nodes: ' + sql);
        assert.strictEqual((sql.match(/\(\?, \?, \?\)/g) || []).length, 10, 'one value tuple per node');
        assert.strictEqual(args.length, 30, 'three bound params per node');
        assert.deepStrictEqual(args.slice(0, 6), ['h0', 'l0', 'r0', 'h1', 'l1', 'r1'],
            'params must be flattened in hash/left/right order');
    });

    it('chunks a full-depth path so no single statement grows unbounded', async function(){
        const db = fakeDb();
        const store = new SC.DbNodeStore(db);
        const nodes = [];
        for(let i = 0; i < M.SMT_DEPTH; i++)
            nodes.push({ hash: 'h' + i, left: 'l' + i, right: 'r' + i });
        await store.putMany(nodes);

        assert.ok(db.calls.length >= 2 && db.calls.length <= 4,
            '256 nodes should chunk into a small handful of statements, got ' + db.calls.length);
        let rows = 0;
        for(const c of db.calls){
            const tuples = (c.sql.match(/\(\?, \?, \?\)/g) || []).length;
            assert.ok(tuples <= 128, 'a chunk exceeded the declared 128-row bound: ' + tuples);
            assert.strictEqual(c.args.length, tuples * 3, 'chunk arity must match its tuple count');
            rows += tuples;
        }
        assert.strictEqual(rows, M.SMT_DEPTH, 'chunking must not drop or duplicate a node');
    });

});

describe("stateCommitment: DbNodeStore.putMany SQL shape @regression", function(){

    it('an empty batch issues no statement at all', async function(){
        const db = fakeDb();
        await new SC.DbNodeStore(db).putMany([]);
        assert.strictEqual(db.calls.length, 0, 'an empty batch must not send an INSERT with no VALUES');
    });

    it('duplicate hashes inside one batch are left to INSERT IGNORE, not pre-filtered away', async function(){
        // INSERT IGNORE already makes a repeated key a no-op WITHIN a single
        // multi-row statement, exactly as it did across the old single-row calls.
        // Asserted so nobody "fixes" it later with a dedup pass that would have to
        // be kept correct for no gain.
        const db = fakeDb();
        const store = new SC.DbNodeStore(db);
        await store.putMany([
            { hash: 'dup', left: 'a', right: 'b' },
            { hash: 'dup', left: 'a', right: 'b' }
        ]);
        assert.strictEqual(db.calls.length, 1);
        assert.strictEqual(db.calls[0].args.length, 6, 'both rows must reach the statement');
    });
});
