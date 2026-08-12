// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// The FOLLOWER at an ARMED height, and a reorg across it.
//
// WHAT IS UNTESTED HERE AND WHY IT MATTERS. Stage A's arming was verified on a live
// chain, but only on the SOURCE: the indexer committed the slot, the root moved, the
// version flipped. The follower half of the same boundary has never run anywhere,
// because no indexer-plus-follower venue exists. That absence has already cost
// something real once (a follower could not replicate contract_state at
// all under strict mode, live for weeks, found the first time a follower saw such a
// block), which is the argument for spending the harness on the armed path too rather
// than only on the shadow window.
//
// The reorg vector is the one the spec singles out as hardest: "the hardest vector is
// a reorg ACROSS the armed height: rolling back below it must recommit the slot EMPTY
// (the v1 assembly), and re-advancing across it repeats the arming-block full build."
// A1 answered it structurally, by arguing that reorg deletes contract_state and
// state_tree_roots together while state_tree_nodes is copy-on-write and
// rollback-exempt, so the next block threads from the surviving row. That argument is
// correct and it is still an argument; this runs it through the real ClientRollback.
//
// Scoped to LTC:regtest, arming maps snapshot-and-restored per test, mainnet asserted
// untouched. BTC:regtest carries a REAL armed height for contract_state_root, which is
// why nothing here deletes a map key.

const assert   = require('assert');
const sinon    = require('sinon');
const setup    = require('./helpers/setup');
const testDb   = require('./helpers/testDb');
const fixtures = require('./helpers/fixtures');

const ServerPoller   = require('../../src/ServerPoller');
const ClientApplier  = require('../../src/ClientApplier');
const ClientRollback = require('../../src/ClientRollback');
const SUB = require('../../src/state_subtree_activation');
const SC  = require('../../src/stateCommitment');

const CHAIN   = 'litecoin';
const TICKER  = 'LTC';
const NETWORK = 'regtest';
const KEY     = TICKER + ':' + NETWORK;

const TIP = 6;            // the ARMED height
const CONTRACT = 41;

describe('Integration: the follower at an armed height, and a reorg across it', function() {

    let sourceDb, replicaDb, poller;

    before(async function() {
        this.timeout(60000);
        await setup.globalSetup();
        sourceDb  = setup.getSourceDb();
        replicaDb = setup.getReplicaDb();
        const broadcaster = { broadcast(){}, addSubscription(){} };
        const log = { recordBlock: async () => {} };
        poller = new ServerPoller(CHAIN, NETWORK, sourceDb, broadcaster, log,
                                  { BLOCK_POLL_INTERVAL: 100 }, testDb.util);
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });

    after(async function() {
        this.timeout(60000);
        sinon.restore();
        await setup.globalTeardown();
    });

    // Arm BOTH stages at `height` for LTC:regtest for the duration of fn, then restore
    // what was there. RESTORE rather than delete: a real armed height lives in the
    // contract_state_root map, and a delete-based helper is how a green suite once
    // tested a disarmed gate.
    async function withArmed(height, fn) {
        const maps = [SUB.STATE_SUBTREE_ACTIVATION.contract_state_root, SUB.ESCROW_LOCKED_LEAF_ACTIVATION];
        const prior = maps.map(m => ({
            had: Object.prototype.hasOwnProperty.call(m, KEY), was: m[KEY] }));
        maps.forEach(m => { m[KEY] = height; });
        try { return await fn(); }
        finally {
            maps.forEach((m, i) => { if(prior[i].had) m[KEY] = prior[i].was; else delete m[KEY]; });
        }
    }

    async function seedSubtreeRows(b) {
        await sourceDb.doQuery(
            'INSERT INTO contract_state (contract_index, state_key, state_value, block_index, action_index) ' +
            'VALUES (?, ?, ?, ?, ?)', [CONTRACT, 'padlen', '"7000"', b, b * 100 + 1]);
        const addr = await sourceDb.doQuery('SELECT id FROM index_addresses WHERE address=?', ['addr_1']);
        const tick = await sourceDb.doQuery('SELECT id FROM index_tickers WHERE tick=?', ['TOKEN']);
        assert.ok(addr.length && tick.length, 'seedBlocks must have interned addr_1/TOKEN');
        await sourceDb.doQuery(
            'INSERT INTO escrow_leaf_journal (address_id, tick_id, locked_amount, block_index, action_index) ' +
            'VALUES (?, ?, ?, ?, ?)',
            [Number(addr[0].id), Number(tick[0].id), '250', b, b * 100 + 2]);
    }

    async function rootsRow(db, b) {
        const rows = await db.doQuery(
            'SELECT balances_root, stakes_root, state_root, contract_state_root ' +
            'FROM state_tree_roots WHERE chain=? AND network=? AND block_index=?',
            [TICKER, NETWORK, b]);
        return rows.length ? rows[0] : null;
    }

    // The replica is established through TIP-1 with a real roots row, which is what a
    // snapshot-bootstrapped follower has, and TIP-1 is below the armed height either
    // way so it commits the v1 leaf set.
    async function stage() {
        await testDb.truncateAll(sourceDb);
        await testDb.truncateAll(replicaDb);
        await fixtures.seedBlocks(sourceDb, 1, TIP);
        await fixtures.seedBlocks(replicaDb, 1, TIP - 1);
        await seedSubtreeRows(TIP);
        await SC.seedSnapshotRoots(replicaDb, TICKER, NETWORK, TIP - 1);
    }

    it('commits the slot at the armed height, and the stored state_root reassembles from it',
       async function() {
        this.timeout(40000);
        await withArmed(TIP, async () => {
            await stage();
            const payload = await poller._buildBlockPayload(TIP);
            await new ClientApplier(replicaDb, testDb.util, CHAIN, NETWORK).applyBlock(payload);

            const armed = await rootsRow(replicaDb, TIP);
            const below = await rootsRow(replicaDb, TIP - 1);
            assert.ok(armed, 'no roots row at the armed height');
            assert.ok(below, 'no roots row below the armed height');

            // The boundary is exactly at TIP: the slot is real at TIP and absent below.
            assert.ok(armed.contract_state_root, 'the armed height committed no contract_state_root');
            assert.strictEqual(below.contract_state_root, null,
                               'the slot is populated BELOW the armed height');

            // The committed state_root is the three-sub-root assembly, not the v1 one.
            // Both directions are asserted: the 3-arg assembly must reproduce it, and
            // the 2-arg (v1) assembly must NOT, or the slot never entered the root and
            // every other assertion here would pass over a no-op.
            assert.strictEqual(
                SC.assembleStateRoot(armed.balances_root, armed.stakes_root,
                                     { contract_state_root: armed.contract_state_root }),
                armed.state_root, 'the armed row does not reassemble from its own sub-roots');
            assert.notStrictEqual(
                SC.assembleStateRoot(armed.balances_root, armed.stakes_root),
                armed.state_root, 'the v1 assembly reproduces the armed root, so the slot is inert');

            // And below the boundary the v1 assembly IS the committed root.
            assert.strictEqual(SC.assembleStateRoot(below.balances_root, below.stakes_root),
                               below.state_root,
                               'the row below the armed height is not a v1 assembly');
        });
    });

    it('reports version 2 at the armed height and 1 immediately below it', async function() {
        await withArmed(TIP, async () => {
            assert.strictEqual(SUB.stateRootVersion(TIP, NETWORK, TICKER), 2);
            assert.strictEqual(SUB.stateRootVersion(TIP - 1, NETWORK, TICKER), 1);
        });
        // And with nothing armed the same heights are version 1, so the assertion above
        // is about the arming rather than about the chain.
        assert.strictEqual(SUB.stateRootVersion(TIP, NETWORK, TICKER), 1);
    });

    it('commits the locked leaf inside balances_root at the armed height', async function() {
        this.timeout(40000);
        await withArmed(TIP, async () => {
            await stage();
            const payload = await poller._buildBlockPayload(TIP);
            await new ClientApplier(replicaDb, testDb.util, CHAIN, NETWORK).applyBlock(payload);
            const armed = await rootsRow(replicaDb, TIP);

            // Stage B's arming claim, on the follower: balances_root now carries the
            // XCHAIN_ESC leaf, so it differs from the same rebuild with the v1 leaf set.
            // buildFullBalancesRoot omitting the height degrades to v1 deliberately (work
            // item 2), which is what makes this comparison available at all.
            const spendableOnly = await SC.buildFullBalancesRoot(replicaDb, TICKER, NETWORK);
            assert.notStrictEqual(armed.balances_root, spendableOnly,
                'balances_root equals the spendable-only rebuild, so no locked leaf was committed');

            // ... and the armed rebuild reproduces exactly what was committed, which is
            // the property the arming block itself depends on.
            const withLocked = await SC.buildFullBalancesRoot(replicaDb, TICKER, NETWORK, TIP);
            assert.strictEqual(armed.balances_root, withLocked,
                'the committed balances_root is not what a height-aware full rebuild produces');
        });
    });

    it('a reorg across the armed height reverts it, and re-advancing reproduces it exactly',
       async function() {
        this.timeout(60000);
        await withArmed(TIP, async () => {
            await stage();
            const payload = await poller._buildBlockPayload(TIP);
            const applier = new ClientApplier(replicaDb, testDb.util, CHAIN, NETWORK);

            await applier.applyBlock(payload);
            const first = await rootsRow(replicaDb, TIP);
            const belowBefore = await rootsRow(replicaDb, TIP - 1);
            assert.ok(first && first.contract_state_root, 'setup: the armed block did not commit');

            // The reorg. Real ClientRollback, real generic table lists from the
            // lifecycle registry.
            await new ClientRollback(replicaDb, testDb.util, TICKER).rollback(TIP);

            const afterRollback = await rootsRow(replicaDb, TIP);
            const belowAfter    = await rootsRow(replicaDb, TIP - 1);
            assert.strictEqual(afterRollback, null,
                'the armed height kept its roots row through a rollback, so a re-advance ' +
                'would thread from an orphaned root');
            assert.deepStrictEqual(belowAfter, belowBefore,
                'the rollback disturbed the surviving row BELOW the armed height');
            // The orphaned write is gone from the journal side too, or the re-advance
            // would rebuild over rows the source no longer has.
            const orphanState = await replicaDb.doQuery(
                'SELECT COUNT(*) AS c FROM contract_state WHERE block_index >= ?', [TIP]);
            assert.strictEqual(Number(orphanState[0].c), 0, 'orphaned contract_state survived');

            // Re-advance across the boundary. state_tree_nodes is copy-on-write and
            // rollback-exempt, so this re-threads from the surviving row rather than a
            // clean tree, which is exactly the case A1 reasoned about and never ran.
            await applier.applyBlock(payload);
            const second = await rootsRow(replicaDb, TIP);
            assert.ok(second, 'the re-advance wrote no roots row');
            assert.deepStrictEqual(
                { b: second.balances_root, s: second.stakes_root,
                  c: second.contract_state_root, r: second.state_root },
                { b: first.balances_root, s: first.stakes_root,
                  c: first.contract_state_root, r: first.state_root },
                'the arming block is not deterministic across a reorg at the boundary');
        });
    });

    it('a snapshot bootstrap AT the armed height commits the same roots as threading across it',
       async function() {
        this.timeout(60000);
        await withArmed(TIP, async () => {
            await stage();
            const payload = await poller._buildBlockPayload(TIP);
            await new ClientApplier(replicaDb, testDb.util, CHAIN, NETWORK).applyBlock(payload);
            const threaded = await rootsRow(replicaDb, TIP);
            assert.ok(threaded && threaded.contract_state_root, 'setup: the armed block did not commit');

            // Stage A work item 2's hazard, stated there as: "a follower seeding from a
            // snapshot taken at or after an armed height would otherwise commit an EMPTY
            // slot against a source that committed a real one and halt on its first
            // recomputed block." A1 answered it by routing all three block paths through
            // one seam, and covered it with a unit vector. This runs the seam itself.
            //
            // A fresh bootstrap has the DATA a snapshot carried and no derived tree at
            // all, so both derived tables go, not just the roots. That also makes the
            // full build prove it does not lean on nodes left behind by the threaded run:
            // state_tree_nodes is content-addressed, so leftovers would be silently
            // reused and this vector would be weaker than it looks.
            await replicaDb.doQuery('DELETE FROM state_tree_roots', []);
            await replicaDb.doQuery('DELETE FROM state_tree_nodes', []);

            await SC.seedSnapshotRoots(replicaDb, TICKER, NETWORK, TIP);
            const seeded = await rootsRow(replicaDb, TIP);
            assert.ok(seeded, 'seedSnapshotRoots wrote no row at the armed height');

            assert.deepStrictEqual(
                { b: seeded.balances_root, s: seeded.stakes_root,
                  c: seeded.contract_state_root, r: seeded.state_root },
                { b: threaded.balances_root, s: threaded.stakes_root,
                  c: threaded.contract_state_root, r: threaded.state_root },
                'a bootstrapped follower and one that threaded across the boundary disagree, ' +
                'which is the halt-on-first-block hazard work item 2 exists to prevent');

            // And the slot is genuinely non-empty in the bootstrapped row, so the
            // agreement above is not two nodes agreeing on EMPTY.
            assert.notStrictEqual(seeded.contract_state_root, SC.EMPTY_ROOT_HEX,
                                  'the bootstrapped slot is the empty-SMT root');
        });
    });

    it('leaves both activation maps exactly as it found them', function() {
        assert.strictEqual(Object.prototype.hasOwnProperty.call(
            SUB.STATE_SUBTREE_ACTIVATION.contract_state_root, KEY), false);
        assert.strictEqual(Object.prototype.hasOwnProperty.call(
            SUB.ESCROW_LOCKED_LEAF_ACTIVATION, KEY), false);
        // The real armed chain must still be armed: restoring must not have wiped it.
        assert.deepStrictEqual(SUB.STATE_SUBTREE_ACTIVATION.contract_state_root,
                               { 'BTC:regtest': 10000 },
                               'the real armed set was not restored intact');
        for(const map of [SUB.STATE_SUBTREE_ACTIVATION.contract_state_root, SUB.ESCROW_LOCKED_LEAF_ACTIVATION])
            for(const k of Object.keys(map))
                assert.ok(!/mainnet/.test(k), 'an activation map is armed on mainnet (' + k + ')');
    });
});
