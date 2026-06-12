// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available —
// contact legal@dankest.llc.
//
// E2E: Disconnect/resume PARITY (test-framework program P2, slice S2).
//
// The property under test everywhere here: no matter how a replica's
// connection history played out (clean resume, reorg during downtime, torn
// bootstrap, divergence halt, schema drift, flapping), it must end either
// BYTE-IDENTICAL to the source (assertReplicaByteIdentical: full replicated
// table set + per-block recompute conformance) or DURABLY HALTED — never
// silently diverged. June 2026 production background: five replicas halted
// holding orphaned pre-reorg blocks their origins had deleted; the remediation
// was a re-seed. 10.2 pins that whole lifecycle as a regression test.

'use strict';

const assert = require('assert');
const sinon  = require('sinon');
const setup    = require('./helpers/setup');
const fixtures = require('./helpers/fixtures');
const testDb   = require('./helpers/testDb');
const ServerProcess = require('./helpers/serverProcess');
const ClientProcess = require('./helpers/clientProcess');
const { waitFor, waitForReplicaBlock } = require('./helpers/waitFor');
const { assertReplicaByteIdentical } = require('./helpers/assertions');

const SERVER_PORT = 29500;

// Wait until a resumed client's WebSocket is actually OPEN — a block seeded
// before the subscription lands broadcasts to nobody, and nothing re-delivers
// it until the NEXT block triggers gap detection.
async function waitForConnected(c) {
    await waitFor(() => c.sync.wsConns && c.sync.wsConns[0] && c.sync.wsConns[0].readyState === 1, 10000);
}

// Blocks land before the rest of a snapshot's tables within one apply, so a
// tip check alone can observe a mid-apply replica. Quiesce on row-count
// parity of the busiest tables before running the byte oracle.
async function waitForQuiesce(src, rep, timeout) {
    await waitFor(async () => {
        for (let t of ['blocks', 'transactions', 'credits']) {
            if (await testDb.getRowCount(src, t) !== await testDb.getRowCount(rep, t)) return false;
        }
        return true;
    }, timeout || 15000);
}
const REPLICA_B_NAME = 'xchain_e2e_replica_b';

describe('E2E: Disconnect/Resume Parity', function() {

    let sourceDb, replicaDb, replicaBDb, server, client, clientB;

    before(async function() {
        this.timeout(60000);
        await setup.globalSetup();
        sourceDb  = setup.getSourceDb();
        replicaDb = setup.getReplicaDb();

        // Second replica (the "control" that never disconnects) — same MariaDB
        // endpoint as the primary replica, name-scoped.
        replicaBDb = await testDb.createDatabase(REPLICA_B_NAME,
            testDb.REPLICA_DB_HOST, testDb.REPLICA_DB_PORT,
            testDb.REPLICA_DB_USER, testDb.REPLICA_DB_PASS);
        await testDb.seedSchema(replicaBDb);

        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });

    after(async function() {
        sinon.restore();
        if (client)  await client.stop();
        if (clientB) await clientB.stop();
        if (server)  await server.stop();
        if (replicaBDb) await replicaBDb.close();
        await testDb.dropDatabase(REPLICA_B_NAME,
            testDb.REPLICA_DB_HOST, testDb.REPLICA_DB_PORT,
            testDb.REPLICA_DB_USER, testDb.REPLICA_DB_PASS);
        await setup.globalTeardown();
    });

    beforeEach(async function() {
        this.timeout(30000);
        if (client)  { await client.stop();  client = null; }
        if (clientB) { await clientB.stop(); clientB = null; }
        if (server)  { await server.stop(); server = null; }
        await setup.resetDatabases();
        await testDb.truncateAll(replicaBDb);
    });

    describe('10.1 Resume parity vs a control replica', function() {
        it('a resumed replica converges byte-identically to one that never disconnected', async function() {
            this.timeout(60000);

            await fixtures.seedBlocks(sourceDb, 1, 20);
            server = new ServerProcess(sourceDb, SERVER_PORT);
            await server.start();

            // Replica A (will disconnect) and replica B (control, stays live).
            client = new ClientProcess(replicaDb, server.getUrl());
            await client.start();
            clientB = new ClientProcess(replicaBDb, server.getUrl());
            await clientB.start();
            await waitForReplicaBlock(replicaDb, 20, 15000);
            await waitForReplicaBlock(replicaBDb, 20, 15000);

            // A disconnects; the chain advances 30 blocks; B follows live.
            await client.stop();
            await fixtures.seedBlocks(sourceDb, 21, 50);
            await waitForReplicaBlock(replicaBDb, 50, 30000);

            // A resumes: fresh client session over the same replica DB — the
            // production restart shape (gap detection + incremental catch-up).
            client = new ClientProcess(replicaDb, server.getUrl());
            await client.connectLive();
            await waitForConnected(client);
            // A new block must arrive for gap detection to notice the deficit.
            await fixtures.seedBlocks(sourceDb, 51, 51);
            await waitForReplicaBlock(replicaDb, 51, 30000);
            await waitForReplicaBlock(replicaBDb, 51, 30000);
            await waitForQuiesce(sourceDb, replicaDb);
            await waitForQuiesce(sourceDb, replicaBDb);

            // Both replicas must be byte-identical to the source — and thereby
            // to each other: resume path ≡ always-connected path.
            await assertReplicaByteIdentical(sourceDb, replicaDb);
            await assertReplicaByteIdentical(sourceDb, replicaBDb);
        });
    });

    describe('10.2 Reorg while disconnected (June 2026 production scenario)', function() {
        it('a replica holding orphaned blocks must not silently follow the new chain — and a re-seed converges it', async function() {
            this.timeout(90000);

            await fixtures.seedBlocks(sourceDb, 1, 25);
            server = new ServerProcess(sourceDb, SERVER_PORT);
            await server.start();

            client = new ClientProcess(replicaDb, server.getUrl());
            await client.start();
            await waitForReplicaBlock(replicaDb, 25, 15000);

            // Disconnect. The source reorgs below the replica's tip: blocks
            // 18-25 are orphaned and replaced by a different chain 18-28.
            await client.stop();
            await fixtures.deleteBlocksFrom(sourceDb, 18);
            // indexOffset: replacement transactions get FRESH global indexes,
            // as on a real chain — they must not collide with the orphans.
            await fixtures.seedBlocks(sourceDb, 18, 28, { creditAmount: '7777', indexOffset: 5000 });

            // Resume. The replica still holds the orphaned 18-25; the reorg
            // event is long gone (only live subscribers saw it).
            client = new ClientProcess(replicaDb, server.getUrl());
            await client.connectLive();
            await waitForConnected(client);
            await fixtures.seedBlocks(sourceDb, 29, 29, { indexOffset: 5000 });

            // Give the client ample time to react (gap catch-up + applies).
            await waitFor(async () => {
                if (client.sync.isHalted()) return true;
                let b = await replicaDb.getLastBlock();
                return b !== null && b >= 29;
            }, 30000).catch(() => {});

            // THE PROPERTY: never silently diverged. Either the client halted,
            // or the replica is byte-identical to the post-reorg source.
            if (!client.sync.isHalted()) {
                await assertReplicaByteIdentical(sourceDb, replicaDb);
            } else {
                // Halted: the halt must be durable, and remediation must work.
                let halt = await replicaDb.getActiveHalt('indexer');
                assert.ok(halt, 'an in-memory halt must also be recorded durably in sync_halt');
            }

            // Remediation (what production did): stop, wipe, re-bootstrap.
            await client.stop();
            await testDb.truncateAll(replicaDb);
            await replicaDb.clearHalt('indexer');
            client = new ClientProcess(replicaDb, server.getUrl());
            await client.start();
            await waitForReplicaBlock(replicaDb, 29, 30000);
            await assertReplicaByteIdentical(sourceDb, replicaDb);
        });
    });

    describe('10.3 Torn bootstrap state', function() {
        it('re-bootstrapping over a torn replica converges byte-identically', async function() {
            this.timeout(60000);

            await fixtures.seedBlocks(sourceDb, 1, 30);
            server = new ServerProcess(sourceDb, SERVER_PORT);
            await server.start();

            client = new ClientProcess(replicaDb, server.getUrl());
            await client.bootstrap();

            // Tear the replica mid-state: drop arbitrary rows from several
            // tables (simulates a bootstrap killed partway through an apply).
            await client.stop();
            await replicaDb.doQuery('DELETE FROM credits WHERE action_index > 1500');
            await replicaDb.doQuery('DELETE FROM transactions WHERE block_index > 22');
            await replicaDb.doQuery('DELETE FROM balances');

            // A fresh bootstrap over the torn state must produce a replica
            // indistinguishable from one bootstrapped cleanly.
            client = new ClientProcess(replicaDb, server.getUrl());
            await client.start();
            await waitForReplicaBlock(replicaDb, 30, 30000);
            await assertReplicaByteIdentical(sourceDb, replicaDb);
        });
    });

    describe('10.4 Divergence halt enforcement', function() {
        it('halts durably on recompute divergence; live AND catch-up refuse to advance; clearHalt resumes', async function() {
            this.timeout(60000);

            await fixtures.seedBlocks(sourceDb, 1, 10);
            server = new ServerProcess(sourceDb, SERVER_PORT);
            await server.start();
            // Take manual control of polling so the tampered block below
            // cannot be broadcast before the tamper lands.
            clearInterval(server.pollInterval);

            client = new ClientProcess(replicaDb, server.getUrl());
            await client.start();
            await server.poll();
            await waitForReplicaBlock(replicaDb, 10, 15000);

            // Seed block 11, then corrupt its RAW DATA on the source after its
            // hashes were committed (a silently corrupted origin — the same
            // class as the production DOUBLE-promotion bug): the streamed
            // block's rows can no longer reproduce the committed ledger hash.
            // The committed hash chain itself stays intact, so descendant
            // blocks remain internally consistent.
            await fixtures.seedBlocks(sourceDb, 11, 11);
            await sourceDb.doQuery(
                "UPDATE credits SET amount = '31337' WHERE action_index = 1100");
            await server.poll();

            await waitFor(async () => client.sync.isHalted(), 15000);

            // Durable halt record on the replica.
            let halt = await replicaDb.getActiveHalt('indexer');
            assert.ok(halt, 'sync_halt row must exist');
            assert.strictEqual(Number(halt.block_index), 11);
            assert.strictEqual(halt.reason, 'local-recompute-divergence');

            // The divergent block must not have advanced the cursor…
            assert.strictEqual(client.getLastAppliedBlock(), 10);

            // …and FURTHER blocks must be refused on BOTH paths (the June
            // half-enforced-halt regression: replicas kept advancing via
            // catch-up while "halted").
            await fixtures.seedBlocks(sourceDb, 12, 14);
            await server.poll();                          // live path
            await client.incrementalCatchUp(11).catch(() => {}); // catch-up path
            await new Promise(r => setTimeout(r, 1500));
            let replicaTip = await replicaDb.getLastBlock();
            assert.ok(replicaTip <= 11,
                'halted replica must not advance past the divergence (tip=' + replicaTip + ')');

            // Operator remediation. Two steps, BOTH required:
            // 1. Fix the source's corrupted row (the origin itself).
            // 2. Re-seed the replica — the live path APPLIES a block before
            //    recomputing, so the divergent block's rows are already in the
            //    replica even though the cursor never advanced.
            //    Clear-halt-alone is NOT enough — exactly the June production
            //    lesson.
            await sourceDb.doQuery(
                "UPDATE credits SET amount = '1000' WHERE action_index = 1100");
            // Later seeds rebuilt the source's balances while the corrupted
            // amount was live — rebuild them now that the ledger is honest.
            await require('../../src/balance-helpers').rebuildBalances(sourceDb);
            await client.stop();
            await testDb.truncateAll(replicaDb);
            client = new ClientProcess(replicaDb, server.getUrl());
            await client.start();
            await server.poll();
            await waitForReplicaBlock(replicaDb, 14, 15000);
            await assertReplicaByteIdentical(sourceDb, replicaDb);
        });
    });

    describe('10.5 Schema drift while disconnected (self-heal)', function() {
        it('a replica missing a replicated table re-applies the source schema and converges', async function() {
            this.timeout(60000);

            // Blocks 1-10 carry NO debits (default debitAmount '0'), so the
            // debits table is empty history — the replica losing it costs no
            // rows, exactly like production's anchor_actions wedge (a table
            // whose first-ever row arrives while the replica's schema predates
            // it; errno 1146 on apply).
            await fixtures.seedBlocks(sourceDb, 1, 10);
            server = new ServerProcess(sourceDb, SERVER_PORT);
            await server.start();

            client = new ClientProcess(replicaDb, server.getUrl());
            await client.start();
            await waitForReplicaBlock(replicaDb, 10, 15000);

            // Disconnect, then lose the (empty) replicated table.
            await client.stop();
            await replicaDb.doQuery('DROP TABLE debits');

            client = new ClientProcess(replicaDb, server.getUrl());
            await client.connectLive();
            await waitForConnected(client);
            // The FIRST debits rows in history arrive while the table is missing.
            await fixtures.seedBlocks(sourceDb, 11, 15, { debitAmount: '5' });

            // The apply hits 1146, self-heals the schema from the source's
            // /schema endpoint, retries, and converges.
            await waitForReplicaBlock(replicaDb, 15, 30000);
            await assertReplicaByteIdentical(sourceDb, replicaDb);
        });
    });

    describe('10.6 Connection flapping', function() {
        it('repeated disconnect/reconnect cycles end byte-identical', async function() {
            this.timeout(90000);

            await fixtures.seedBlocks(sourceDb, 1, 10);
            server = new ServerProcess(sourceDb, SERVER_PORT);
            await server.start();

            client = new ClientProcess(replicaDb, server.getUrl());
            await client.start();
            await waitForReplicaBlock(replicaDb, 10, 15000);

            let tip = 10;
            for (let cycle = 0; cycle < 5; cycle++) {
                await client.stop();
                // Advance the chain a deterministic-but-varied amount per cycle.
                let next = tip + 3 + cycle;
                await fixtures.seedBlocks(sourceDb, tip + 1, next);
                tip = next;

                client = new ClientProcess(replicaDb, server.getUrl());
                await client.connectLive();
                await waitForConnected(client);
                // One more block so gap detection fires on reconnect.
                tip += 1;
                await fixtures.seedBlocks(sourceDb, tip, tip);
                await waitForReplicaBlock(replicaDb, tip, 20000);
            }

            await assertReplicaByteIdentical(sourceDb, replicaDb);
        });
    });
});
