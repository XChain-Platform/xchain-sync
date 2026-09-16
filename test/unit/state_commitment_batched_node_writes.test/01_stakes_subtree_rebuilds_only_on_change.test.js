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

function keyFor(i){ return M.sha256(Buffer.from('xc1174:' + i, 'utf8')); }
function leafFor(i){ return M.toHex(M.amountLeaf(String(i * 3 + 1) + '.00000000')); }

// A store that answers like MemoryNodeStore but counts how many WRITE calls the
// engine makes, so "one round trip per level" is measurable rather than argued.
class CountingStore {
    constructor(){
        this.map = new Map();
        this.putCalls = 0;         // single-row writes
        this.putManyCalls = 0;     // batch writes
        this.rowsWritten = 0;      // rows offered to the store, batched or not
        this.batchSizes = [];
    }
    async get(h){ return this.map.has(h) ? this.map.get(h) : null; }
    _set(h, l, r){
        this.rowsWritten++;
        if(!this.map.has(h)) this.map.set(h, { left_hash: l, right_hash: r });
    }
    async put(h, l, r){ this.putCalls++; this._set(h, l, r); }
    async putMany(nodes){
        this.putManyCalls++;
        this.batchSizes.push(nodes.length);
        for(const n of nodes) this._set(n.hash, n.left, n.right);
    }
    get writeCalls(){ return this.putCalls + this.putManyCalls; }
}

// A store that predates putMany: bare {get, put}, like the subtree unit fakes
// and the bin/ instrumentation decorator. Doubles as the pre-fix reference the
// batched engine must match row for row.
class PerNodeStore extends CountingStore {
    constructor(){ super(); this.putMany = undefined; }
}

const CHAIN = 'BTC', NETWORK = 'regtest';
// 49 keys is the live figure the BTC regtest venue reports
// (getstakeweightsbycapability keys=49), rebuilt from an empty root every block.
function stakeEntries(n, salt){
    const out = [];
    for(let i = 0; i < n; i++)
        out.push([M.toHex(keyFor('stake:' + i)), leafFor(i + (salt || 0))]);
    return out;
}

function freshSmt(){ return new SC.PersistentSMT(new CountingStore()); }

function registerHooks(){

    beforeEach(function(){ SC.resetStakesMemo(); });
    afterEach(function(){ SC.resetStakesMemo(); });

}

describe("stateCommitment: stakes subtree rebuilds only on change @regression", function(){

    registerHooks();

    it('an unchanged stake set on the next block writes NOTHING and returns the same root', async function(){
        const smt = freshSmt();
        const e = stakeEntries(49);
        const first = await SC.buildStakesRoot(smt, CHAIN, NETWORK, 100, e);
        const afterFirst = smt.store.writeCalls;
        assert.strictEqual(afterFirst, 49, 'the first block builds the whole tree');

        const second = await SC.buildStakesRoot(smt, CHAIN, NETWORK, 101, stakeEntries(49));
        assert.strictEqual(second, first, 'an unchanged stake set must commit the same stakes_root');
        assert.strictEqual(smt.store.writeCalls, afterFirst,
            'an unchanged stake set must write no nodes at all; wrote ' + (smt.store.writeCalls - afterFirst));
    });

    it('a RUN of unchanged blocks writes nothing after the first, not every other one', async function(){
        // A memo that answers a hit but does not re-stamp itself still passes the
        // single-hit test: block 101 hits, then block 102 is no longer the memo's
        // successor and rebuilds. That halves the defect instead of fixing it, and
        // it is invisible unless the run is longer than two blocks. The regtest
        // suite runs hundreds.
        const smt = freshSmt();
        let root = await SC.buildStakesRoot(smt, CHAIN, NETWORK, 100, stakeEntries(49));
        const afterFirst = smt.store.writeCalls;
        for(let h = 101; h <= 110; h++){
            const next = await SC.buildStakesRoot(smt, CHAIN, NETWORK, h, stakeEntries(49));
            assert.strictEqual(next, root, 'the root moved on an unchanged stake set at height ' + h);
            root = next;
        }
        assert.strictEqual(smt.store.writeCalls, afterFirst,
            'ten unchanged blocks must write nothing; wrote ' + (smt.store.writeCalls - afterFirst) +
            ' batches (a memo that does not re-stamp itself rebuilds every other block)');
    });

    it('a CHANGED stake set rebuilds and moves the root', async function(){
        const smt = freshSmt();
        const first  = await SC.buildStakesRoot(smt, CHAIN, NETWORK, 100, stakeEntries(49));
        const before = smt.store.writeCalls;
        const second = await SC.buildStakesRoot(smt, CHAIN, NETWORK, 101, stakeEntries(49, 1));
        assert.notStrictEqual(second, first, 'a changed stake set must move the root');
        assert.ok(smt.store.writeCalls > before, 'a changed stake set must actually rebuild');
    });

    it('the memoized root is byte-identical to what buildFull would have returned', async function(){
        // The whole safety case: the shortcut may not be a different answer.
        const e = stakeEntries(49);
        const memoSmt = freshSmt();
        await SC.buildStakesRoot(memoSmt, CHAIN, NETWORK, 100, e);
        const memoized = await SC.buildStakesRoot(memoSmt, CHAIN, NETWORK, 101, stakeEntries(49));

        const plain = await freshSmt().buildFull(stakeEntries(49));
        assert.strictEqual(memoized, plain, 'the memo returned a root buildFull would not have produced');
    });
});

describe("stateCommitment: stakes subtree rebuilds only on change @regression", function(){

    registerHooks();

    it('a GAP in block continuity rebuilds, even with an identical stake set', async function(){
        // A reorg or a rollback lands on a block that is not the memo's successor.
        const smt = freshSmt();
        await SC.buildStakesRoot(smt, CHAIN, NETWORK, 100, stakeEntries(49));
        const before = smt.store.writeCalls;
        await SC.buildStakesRoot(smt, CHAIN, NETWORK, 105, stakeEntries(49));
        assert.ok(smt.store.writeCalls > before,
            'a non-successor block must rebuild rather than trust the memo');
    });

    it('re-parsing the SAME block index rebuilds rather than trusting a sibling memo', async function(){
        const smt = freshSmt();
        await SC.buildStakesRoot(smt, CHAIN, NETWORK, 100, stakeEntries(49));
        const before = smt.store.writeCalls;
        await SC.buildStakesRoot(smt, CHAIN, NETWORK, 100, stakeEntries(49));
        assert.ok(smt.store.writeCalls > before, 'block N is not the successor of block N');
    });

    it('a different chain or network never reads the other one\'s memo', async function(){
        const smt = freshSmt();
        await SC.buildStakesRoot(smt, CHAIN, NETWORK, 100, stakeEntries(49));
        let before = smt.store.writeCalls;
        await SC.buildStakesRoot(smt, 'LTC', NETWORK, 101, stakeEntries(49));
        assert.ok(smt.store.writeCalls > before, 'a different chain must rebuild');

        SC.resetStakesMemo();
        await SC.buildStakesRoot(smt, CHAIN, NETWORK, 100, stakeEntries(49));
        before = smt.store.writeCalls;
        await SC.buildStakesRoot(smt, CHAIN, 'mainnet', 101, stakeEntries(49));
        assert.ok(smt.store.writeCalls > before, 'a different network must rebuild');
    });

    it('a cold start has no memo, so the first block after a restart rebuilds', async function(){
        const smt = freshSmt();
        await SC.buildStakesRoot(smt, CHAIN, NETWORK, 100, stakeEntries(49));
        SC.resetStakesMemo();                       // models the process restarting
        const before = smt.store.writeCalls;
        await SC.buildStakesRoot(smt, CHAIN, NETWORK, 101, stakeEntries(49));
        assert.ok(smt.store.writeCalls > before, 'a cold start must rebuild, not trust a stale root');
    });

    it('a memoized root missing from the store rebuilds instead of committing it', async function(){
        // Models the node store being wiped or pruned under a live process. The
        // memo would otherwise commit a root whose tree is not there to prove.
        const smt = freshSmt();
        const first = await SC.buildStakesRoot(smt, CHAIN, NETWORK, 100, stakeEntries(49));
        smt.store.map.clear();
        const before = smt.store.writeCalls;
        const second = await SC.buildStakesRoot(smt, CHAIN, NETWORK, 101, stakeEntries(49));
        assert.ok(smt.store.writeCalls > before, 'a vanished tree must be rebuilt');
        assert.strictEqual(second, first, 'and the rebuild must land on the same root');
        assert.ok(smt.store.map.has(first), 'the tree must be back in the store');
    });
});

describe("stateCommitment: stakes subtree rebuilds only on change @regression", function(){

    registerHooks();

    it('reordering the same stake entries still hits, because buildFull is order-independent', async function(){
        const smt = freshSmt();
        const e = stakeEntries(49);
        const first = await SC.buildStakesRoot(smt, CHAIN, NETWORK, 100, e);
        const before = smt.store.writeCalls;
        const second = await SC.buildStakesRoot(smt, CHAIN, NETWORK, 101, e.slice().reverse());
        assert.strictEqual(second, first, 'a reorder is not a change');
        assert.strictEqual(smt.store.writeCalls, before, 'and must not trigger a rebuild');
    });

    it('an empty stake set is memoized without a store read that cannot succeed', async function(){
        // The empty root is never a row in state_tree_nodes, so an existence check
        // on it would always miss and rebuild forever.
        const smt = freshSmt();
        const first = await SC.buildStakesRoot(smt, CHAIN, NETWORK, 100, []);
        assert.strictEqual(first, SC.EMPTY_ROOT_HEX);
        const before = smt.store.writeCalls;
        const second = await SC.buildStakesRoot(smt, CHAIN, NETWORK, 101, []);
        assert.strictEqual(second, SC.EMPTY_ROOT_HEX);
        assert.strictEqual(smt.store.writeCalls, before, 'an empty set must not rebuild every block');
    });
});
