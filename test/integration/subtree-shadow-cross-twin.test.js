// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// SPV sub-tree shadow window, the CROSS-TWIN leg (spec §7 step 1).
//
// WHAT WAS MISSING AND WHY A LIVE VENUE WAS NOT ACTUALLY THE ONLY WAY TO GET IT.
// The spec's shadow window asks for two things. Its SOURCE-SIDE half ran on three
// real regtest ledgers and is green. Its cross-twin half was recorded as blocked on
// "an indexer plus a follower on one chain", a venue that does not exist in this
// environment, and the spec then narrowed honestly what that venue would actually
// prove: the reader is a byte-identical twin and cannot diverge, and the journal is
// source-authored and REPLICATED rather than recomputed, so its VALUES cannot
// diverge either. What is genuinely unproven is **replication TIMING**: that the
// follower holds THIS block's `contract_state` and `escrow_leaf_journal` rows at the
// moment it computes that block's roots.
//
// Timing is exactly what this tier can test, and it tests it better than a venue
// would, because a venue can only show the happy path. Here the real ServerPoller
// builds the real block payload from a real source database, the real ClientApplier
// applies it into a real replica, and the real commitment hook runs inside the real
// transaction. Then the same scenario runs again with the journal rows WITHHELD from
// the payload, which is what a replication lag would look like, and the assertion is
// that the follower's shadow value CHANGES. Without that second half the suite would
// pass just as happily against a follower that never read the journal at all.
//
// Everything is scoped to LTC:regtest, and only the two SHADOW maps are armed, for
// the duration of each test, snapshot-and-restore. The committed roots are untouched:
// a shadow value is written to its own column and is never handed to gateSubRoots.
// BTC:regtest carries a REAL armed height for `contract_state_root` now, which is
// why nothing here deletes a map key. See the arming note in the spec: a helper that
// cleaned up with `delete` silently disarmed a real chain for every later test.

const assert  = require('assert');
const sinon   = require('sinon');
const setup   = require('./helpers/setup');
const testDb  = require('./helpers/testDb');
const fixtures = require('./helpers/fixtures');

const ServerPoller  = require('../../src/ServerPoller');
const ClientApplier = require('../../src/ClientApplier');
const SUB = require('../../src/state_subtree_activation');
const CST = require('../../src/contractStateSubtree');
const SC  = require('../../src/stateCommitment');

const CHAIN   = 'litecoin';     // full-name form, as the services pass it
const TICKER  = 'LTC';          // what the activation maps and state_tree_roots key on
const NETWORK = 'regtest';      // state_commitment is armed from genesis here
const KEY     = TICKER + ':' + NETWORK;

const TIP = 6;                  // the block whose payload is under test
const CONTRACT = 41;

describe('Integration: SPV sub-tree shadow columns, cross-twin replication timing', function() {

    let sourceDb, replicaDb, poller;

    before(async function() {
        this.timeout(60000);
        await setup.globalSetup();
        sourceDb  = setup.getSourceDb();
        replicaDb = setup.getReplicaDb();

        // A poller needs a broadcaster and a transparency log only for _poll; the
        // payload build reads the database, so stubs keep this test to one moving part.
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

    // Arm both SHADOW maps for LTC:regtest for the duration of fn, then restore what
    // was there. RESTORE rather than delete: a real armed height lives in the sibling
    // ACTIVATION map, and a delete-based helper is how a green suite once tested a
    // disarmed gate.
    async function withShadowArmed(height, fn) {
        const maps = [SUB.STATE_SUBTREE_SHADOW.contract_state_root, SUB.ESCROW_LOCKED_LEAF_SHADOW];
        const prior = maps.map(m => ({
            had: Object.prototype.hasOwnProperty.call(m, KEY), was: m[KEY] }));
        maps.forEach(m => { m[KEY] = height; });
        try { return await fn(); }
        finally {
            maps.forEach((m, i) => { if(prior[i].had) m[KEY] = prior[i].was; else delete m[KEY]; });
        }
    }

    // Source-side rows for block `b`: one live contract-state key and one locked
    // escrow position for an address that seedBlocks already credited, so both
    // derivations have something real to commit rather than an empty tree.
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
            'SELECT balances_root, contract_state_root, contract_state_root_shadow, ' +
            'balances_root_escrow_shadow FROM state_tree_roots ' +
            'WHERE chain=? AND network=? AND block_index=?', [TICKER, NETWORK, b]);
        return rows.length ? rows[0] : null;
    }

    // Source + replica agreed through TIP-1; TIP exists on the source only.
    //
    // The replica is given a real state_tree_roots row at TIP-1 through the follower's
    // own seedSnapshotRoots, which is what a snapshot-bootstrapped follower has. It
    // matters for more than realism: without a prior row the committed thread starts
    // from EMPTY and commits a root over only the LAST block's touched keys, while the
    // shadow full-builds over every key, and the two are then incomparable for reasons
    // that have nothing to do with the escrow leaf.
    async function stageBlocks() {
        await testDb.truncateAll(sourceDb);
        await testDb.truncateAll(replicaDb);
        await fixtures.seedBlocks(sourceDb, 1, TIP);
        await fixtures.seedBlocks(replicaDb, 1, TIP - 1);
        await seedSubtreeRows(TIP);
        await SC.seedSnapshotRoots(replicaDb, TICKER, NETWORK, TIP - 1);
    }

    it('the block payload carries both sub-tree tables, which is the whole timing premise',
       async function() {
        this.timeout(30000);
        await stageBlocks();

        const payload = await poller._buildBlockPayload(TIP);
        assert.ok(payload, 'no payload built for block ' + TIP);

        // Both tables are registered `stream:block` in tableLifecycle, so they ride
        // the block payload by construction. "By construction" is precisely the claim
        // the spec declined to accept on trust, so it is asserted here: if either
        // table ever loses its stream:block classification, the follower would
        // compute this block's roots without this block's rows and HALT, and this is
        // the assertion that fails first and names the table.
        assert.ok(Array.isArray(payload.data.contract_state),
                  'contract_state absent from the block payload');
        assert.strictEqual(payload.data.contract_state.length, 1);
        assert.ok(Array.isArray(payload.data.escrow_leaf_journal),
                  'escrow_leaf_journal absent from the block payload');
        assert.strictEqual(payload.data.escrow_leaf_journal.length, 1);
    });

    it('the follower computes both shadow values from rows that arrived in the same block',
       async function() {
        this.timeout(30000);
        await stageBlocks();

        const payload = await poller._buildBlockPayload(TIP);
        const applier = new ClientApplier(replicaDb, testDb.util, CHAIN, NETWORK);

        const row = await withShadowArmed(0, async () => {
            await applier.applyBlock(payload);
            return rootsRow(replicaDb, TIP);
        });

        assert.ok(row, 'follower wrote no state_tree_roots row for block ' + TIP);

        // Stage A: the shadow slot is populated while the committed slot stays EMPTY.
        // Both halves matter. A non-null shadow proves the derivation ran over
        // replicated rows; a null committed column proves the shadow cannot reach
        // state_root, which is the property that makes the window safe to run below
        // an armed height at all.
        assert.ok(row.contract_state_root_shadow, 'no contract_state_root_shadow computed');
        assert.strictEqual(row.contract_state_root, null,
                           'the shadow window must not populate the COMMITTED slot');

        // The follower's shadow equals an independent derivation over the SOURCE's own
        // contract_state, which is the cross-twin value comparison the spec asks for.
        const srcSmt = new SC.PersistentSMT(new SC.DbNodeStore(sourceDb));
        const srcContractRoot = await CST.resolveContractStateRoot(
            sourceDb, srcSmt, TICKER, NETWORK, TIP, true);
        assert.strictEqual(row.contract_state_root_shadow, srcContractRoot,
                           'follower and source disagree on contract_state_root');

        // Stage B: the escrow shadow is populated AND differs from the committed
        // balances_root, which is what proves the locked leaf actually entered the
        // shadow tree. Equality here would be a vacuous pass: a follower that never
        // read the journal would also produce a valid-looking root.
        assert.ok(row.balances_root_escrow_shadow, 'no balances_root_escrow_shadow computed');
        assert.notStrictEqual(row.balances_root_escrow_shadow, row.balances_root,
                              'the escrow shadow equals the spendable root, so no locked leaf entered it');
    });

    it('withholding the journal rows changes the escrow shadow, so a replication lag is detectable',
       async function() {
        this.timeout(30000);

        // Positive run, kept in this test so the two roots are produced from the same
        // staged data on the same MariaDB.
        await stageBlocks();
        let payload = await poller._buildBlockPayload(TIP);
        let applier = new ClientApplier(replicaDb, testDb.util, CHAIN, NETWORK);
        const withJournal = await withShadowArmed(0, async () => {
            await applier.applyBlock(payload);
            return rootsRow(replicaDb, TIP);
        });

        // Negative control: the same block, with escrow_leaf_journal stripped from the
        // payload. That is what the follower would see if the journal rows had not
        // replicated yet, which is the ONE failure mode the spec says a live venue
        // would have shown and nothing else could.
        await stageBlocks();
        payload = await poller._buildBlockPayload(TIP);
        delete payload.data.escrow_leaf_journal;
        applier = new ClientApplier(replicaDb, testDb.util, CHAIN, NETWORK);
        const withoutJournal = await withShadowArmed(0, async () => {
            await applier.applyBlock(payload);
            return rootsRow(replicaDb, TIP);
        });

        assert.ok(withJournal && withoutJournal, 'both runs must produce a roots row');
        assert.notStrictEqual(withoutJournal.balances_root_escrow_shadow,
                              withJournal.balances_root_escrow_shadow,
                              'the escrow shadow is blind to the journal, so this suite cannot ' +
                              'detect the replication-timing failure it exists to detect');

        // And with no locked leaf to apply, the escrow shadow collapses onto the
        // spendable root: the shadow tree is the balances tree plus XCHAIN_ESC keys,
        // so an empty journal makes the two identical. This pins WHICH way it moved
        // rather than just that it moved.
        assert.strictEqual(withoutJournal.balances_root_escrow_shadow,
                           withoutJournal.balances_root,
                           'with no journal rows the escrow shadow should equal balances_root');

        // The contract-state shadow is unaffected: its rows did replicate. A single
        // withheld table must not perturb the other derivation.
        assert.strictEqual(withoutJournal.contract_state_root_shadow,
                           withJournal.contract_state_root_shadow,
                           'withholding the journal moved the CONTRACT-STATE shadow too');
    });

    it('leaves both shadow maps exactly as it found them', function() {
        // The suite arms LTC:regtest inside every test. If a restore ever leaks, the
        // next suite in the process runs against an armed chain (or, worse, a
        // disarmed real one), so the invariant is asserted rather than assumed.
        assert.strictEqual(Object.prototype.hasOwnProperty.call(
            SUB.STATE_SUBTREE_SHADOW.contract_state_root, KEY), false);
        assert.strictEqual(Object.prototype.hasOwnProperty.call(
            SUB.ESCROW_LOCKED_LEAF_SHADOW, KEY), false);
        // And mainnet is untouched by anything here, the guard that must never relax.
        for(const map of [SUB.STATE_SUBTREE_SHADOW.contract_state_root, SUB.ESCROW_LOCKED_LEAF_SHADOW])
            for(const k of Object.keys(map))
                assert.ok(!/mainnet/.test(k), 'a shadow map is armed on mainnet (' + k + ')');
    });
});
