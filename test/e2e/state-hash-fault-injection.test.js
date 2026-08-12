// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// State_hash fault-injection e2e.
//
// The three consensus hashes (ledger/actions/contract) cover ONLY the new,
// block-scoped rows of a block. They structurally cannot see an in-place mutation
// the indexer makes to a SURVIVING (earlier-block) row: those ride the replication
// "updated_rows" channel, and a follower that silently drops that upsert diverges
// with NO consensus-hash mismatch to flag it. The fourth per-block state_hash
// (stateHash.js) exists to close exactly that gap: the follower recomputes it
// APPLY-TIME over its replica and HALTS on mismatch.
//
// This drives that halt end-to-end against the real MariaDB harness, using the
// always-hashed request_status class (a v0 attests request going terminal in
// place) as the surviving-row mutation:
//
//   1. seed blocks 1..N-1 plus one surviving v0 attests request row ('pending'),
//      then bootstrap the follower (it receives the pending row via the snapshot);
//   2. seed block N and flip the attests row terminal IN PLACE at block N
//      (resolved_block = N) -- the mutation the updated_rows REQUEST_STATUS channel
//      carries and the state_hash request_status class covers;
//   3. commit the source's fourth (state) hash for block N;
//   4. hand the follower the exact live block payload the poller broadcasts
//      (state_hash + updated_rows.attests) and observe the apply-time check.
//
// CONTROL applies the flip and stays clean; FAULT stubs the follower's
// updated_rows apply to a no-op (the "updated_rows-drop" class) and asserts a
// durable state-hash-divergence halt, with the three consensus hashes still
// matching (only the state_hash catches the drop).

const assert        = require('assert');
const sinon         = require('sinon');
const setup         = require('./helpers/setup');
const fixtures      = require('./helpers/fixtures');
const ServerProcess = require('./helpers/serverProcess');
const ClientProcess = require('./helpers/clientProcess');

const SERVER_PORT       = 29650;
const ATTEST_ACTION_IDX = 100;   // sits in block 1's action-index space (block N*100)
const MUTATION_BLOCK    = 6;     // the surviving-row flip lands here

// Seed a surviving v0 attests REQUEST row in 'pending' (resolved_block NULL). Its
// creating action is an earlier block, so it is invisible to any block-scoped hash;
// only the state_hash request_status class covers its later terminal flip.
async function seedPendingAttest(db) {
    await db.doQuery(
        "INSERT INTO attests (action_index, version, request_id, provider_id, request_status, resolved_block, block_index) " +
        "VALUES (?, 0, ?, 'http_get', 'pending', NULL, 1)",
        [ATTEST_ACTION_IDX, 'a'.repeat(64)]);
}

// Flip the surviving request terminal IN PLACE at `resolvedBlock` (the exact
// mutation markResolved does on the indexer: request_status terminal + resolved_block
// stamp). collectUpdatedRows keys on resolved_block BETWEEN B AND B, so this rides
// block B's updated_rows; the state_hash request_status class hashes it there too.
async function resolveAttestInPlace(db, resolvedBlock) {
    await db.doQuery(
        "UPDATE attests SET request_status='fulfilled', resolved_block=? WHERE action_index=?",
        [resolvedBlock, ATTEST_ACTION_IDX]);
}

async function getAttestRow(db) {
    let rows = await db.doQueryCommitted(
        "SELECT request_status, resolved_block FROM attests WHERE action_index=?", [ATTEST_ACTION_IDX]);
    return rows.length ? rows[0] : null;
}

describe('E2E: state_hash fault injection - updated_rows drop -> state-hash-divergence', function() {

    let sourceDb, replicaDb, server, client;

    before(async function() {
        await setup.globalSetup();
        sourceDb  = setup.getSourceDb();
        replicaDb = setup.getReplicaDb();
        if (!process.env.E2E_VERBOSE) { sinon.stub(console, 'log'); sinon.stub(console, 'error'); }
    });

    after(async function() {
        sinon.restore();
        if (client) await client.stop();
        if (server) await server.stop();
        await setup.globalTeardown();
    });

    beforeEach(async function() {
        if (client) { await client.stop(); client = null; }
        if (server) { await server.stop(); server = null; }
        await setup.resetDatabases();
    });

    // Bring the follower up to MUTATION_BLOCK-1 via bootstrap, seed the mutation
    // block and its in-place attests flip on the source, commit the source
    // state_hash, and return the exact live block payload the poller broadcasts
    // for it. Background polling is halted first so nothing races the manual writes.
    async function primeThroughMutationBlock() {
        await fixtures.seedBlocks(sourceDb, 1, MUTATION_BLOCK - 1);
        await seedPendingAttest(sourceDb);

        server = new ServerProcess(sourceDb, SERVER_PORT);
        await server.start();
        // Drive block delivery manually (no live socket); stop the background poll so
        // it can neither race the source writes below nor advance any shared state.
        clearInterval(server.pollInterval);
        clearInterval(server.statusInterval);

        client = new ClientProcess(replicaDb, server.getUrl());
        await client.bootstrap();
        assert.strictEqual(await replicaDb.getLastBlock(), MUTATION_BLOCK - 1,
            'follower bootstrapped to the pre-mutation tip');

        // The surviving request row must have reached the follower via the snapshot,
        // in its pre-flip ('pending', resolved_block NULL) state.
        let pre = await getAttestRow(replicaDb);
        assert.ok(pre, 'follower received the surviving attests row at bootstrap');
        assert.strictEqual(pre.request_status, 'pending');
        assert.strictEqual(pre.resolved_block, null);

        // Seed the mutation block, then flip the surviving row in place AT it.
        await fixtures.seedBlocks(sourceDb, MUTATION_BLOCK, MUTATION_BLOCK);
        await resolveAttestInPlace(sourceDb, MUTATION_BLOCK);
        // Commit the source's fourth (state) hash so the block ships state_hash != null.
        let sh = await fixtures.computeAndStoreStateHash(sourceDb, MUTATION_BLOCK);
        assert.ok(typeof sh === 'string' && sh.length === 64, 'state_hash computed and stored');

        // Build the real live payload (state_hash + updated_rows.attests carrying the flip).
        let payload = await server.poller._buildBlockPayload(MUTATION_BLOCK, null, MUTATION_BLOCK);
        assert.ok(payload, 'poller built a payload for the mutation block');
        assert.strictEqual(payload.state_hash, sh, 'payload carries the committed state_hash');
        assert.ok(payload.updated_rows && Array.isArray(payload.updated_rows.attests)
            && payload.updated_rows.attests.length === 1,
            'updated_rows carries the in-place attests flip');
        return { payload, sourceStateHash: sh };
    }

    it('CONTROL: applying the updated_rows flip recomputes an identical state_hash (no halt)', async function() {
        this.timeout(30000);
        let { payload } = await primeThroughMutationBlock();

        await client.sync._applyBlockEvent(payload);

        assert.strictEqual(client.sync._halted, null, 'no halt when the flip is applied');
        assert.strictEqual(await replicaDb.getActiveHalt('indexer'), null, 'no durable halt row');
        assert.strictEqual(client.sync.lastAppliedBlock, MUTATION_BLOCK, 'follower advanced past the block');

        // The flip landed on the replica: source and follower agree, hence no divergence.
        let post = await getAttestRow(replicaDb);
        assert.strictEqual(post.request_status, 'fulfilled');
        assert.strictEqual(Number(post.resolved_block), MUTATION_BLOCK);
    });

    it('FAULT: dropping the updated_rows apply diverges the state_hash and HALTS durably', async function() {
        this.timeout(30000);
        let { payload, sourceStateHash } = await primeThroughMutationBlock();

        // Inject the fault: the follower silently drops the updated_rows upsert -- the
        // exact class (a dropped in-place mutation on a surviving row) the state_hash
        // was built to catch. The block-scoped data still applies normally.
        let drop = sinon.stub(client.applier, '_applyUpdatedRows').resolves();

        await client.sync._applyBlockEvent(payload);

        assert.ok(drop.called, 'the updated_rows apply path was exercised (and dropped)');

        // The three consensus hashes still match (block-scoped rows applied fine); ONLY
        // the state_hash catches the dropped mutation, so the halt reason is precise.
        assert.ok(client.sync._halted, 'follower halted on the dropped in-place mutation');
        assert.strictEqual(client.sync._halted.reason, 'state-hash-divergence');
        assert.strictEqual(client.sync._halted.blockIndex, MUTATION_BLOCK);
        assert.strictEqual(client.sync.lastAppliedBlock, MUTATION_BLOCK - 1,
            'halted client did not advance its applied cursor');

        // The recorded mismatch pins the source hash against the follower recompute.
        let mm = client.sync._halted.mismatches[0];
        assert.strictEqual(mm.field, 'state_hash');
        assert.strictEqual(mm.a, sourceStateHash);
        assert.notStrictEqual(mm.b, sourceStateHash);

        // Durable halt so a restart comes up halted rather than resuming onto the fork.
        let halt = await replicaDb.getActiveHalt('indexer');
        assert.ok(halt, 'a durable halt row was written');
        assert.strictEqual(halt.reason, 'state-hash-divergence');
        assert.strictEqual(Number(halt.block_index), MUTATION_BLOCK);

        // The replica genuinely holds the stale (pre-flip) surviving row: the
        // divergence the state_hash caught is real, not a false positive.
        let post = await getAttestRow(replicaDb);
        assert.strictEqual(post.request_status, 'pending');
        assert.strictEqual(post.resolved_block, null);
    });
});
