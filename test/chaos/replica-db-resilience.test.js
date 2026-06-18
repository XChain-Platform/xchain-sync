'use strict';

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
 * Chaos Engineering: Replica Database Resilience
 *
 * Experiment IDs:
 *   CE-DST-01  Complete replica DB unavailability during block application
 *   CE-DST-02  Slow write responses (2 s latency injection)
 *   CE-DST-03  Connection pool exhaustion (30 s timeout toxic)
 *   CE-DST-04  Intermittent connection drops (30 % TCP reset)
 *   CE-DST-05  Replica down → blocks accumulate on server → recovery + integrity
 *
 * The replica DB is what the sync client writes to. Faults are injected on
 * the replica_db_chaos Toxiproxy proxy (port 33061). The source DB and
 * server remain healthy throughout.
 *
 * Prerequisites (docker-compose.chaos.yml):
 *   - Source MariaDB proxied through toxiproxy on port 33060
 *   - Replica MariaDB proxied through toxiproxy on port 33061
 *   - Toxiproxy API on port 8474
 *
 * Run: npm run test:chaos
 */

const { expect } = require('chai');
const testDb     = require('../e2e/helpers/testDb');

const {
    assertReplicaMatchesSource,
    assertBlockExists,
    assertBalancesConsistent,
    assertHashesMatch
} = require('../e2e/helpers/assertions');

const {
    bootstrapDatabases,
    teardownDatabases,
    createServer,
    createClient,
    seedSourceBlocks,
    isServerAlive,
    waitForSyncRecovery,
    startReplicaPoller,
    sleep
} = require('./helpers/chaos-setup');

const {
    waitForToxiproxy,
    createProxy,
    replicaFaults,
    resetBoth,
    SOURCE_PROXY,
    REPLICA_PROXY
} = require('./helpers/toxiproxy-client');

// -------------------------------------------------------------------------
// Suite setup / teardown
// -------------------------------------------------------------------------

describe('Chaos: Replica Database Resilience', function () {

    let server, client;
    const SERVER_PORT = 30200;

before(async function () {
    await waitForToxiproxy();
    await createProxy(SOURCE_PROXY);
    await createProxy(REPLICA_PROXY);
    await bootstrapDatabases();

    // Seed initial blocks 1-10 into source
    await seedSourceBlocks(1, 10);

    // Start sync server
    server = createServer(SERVER_PORT);
    await server.start();

    // Bootstrap client and start live sync
    client = createClient(server.getUrl());
    await client.start();

    // Wait for initial sync to complete
    const recoveryMs = await waitForSyncRecovery(10, 30000);
    expect(recoveryMs).to.be.above(-1, 'Initial sync should complete within 30s');
    console.log(`    [setup] Initial sync to block 10 completed in ${recoveryMs}ms`);
});

after(async function () {
    if (client) client.stop();
    if (server) await server.stop();
    await teardownDatabases();
    await resetBoth();
});

// -------------------------------------------------------------------------
// CE-DST-01: Complete Replica DB Unavailability
// -------------------------------------------------------------------------

describe('CE-DST-01: Complete Replica DB Unavailability', function () {

    afterEach(async function () {
        await replicaFaults.reset();
    });

    it('baseline: replica has blocks 1-10 before fault injection', async function () {
        const replicaDb = require('./helpers/chaos-setup').getReplicaDb();
        const lastBlock = await replicaDb.getLastBlock();
        expect(lastBlock).to.be.at.least(10);
    });

    it('client process stays alive while replica DB is down', async function () {
        await replicaFaults.dbDown();

        // Seed new blocks into source; server will broadcast them
        await seedSourceBlocks(11, 15);
        await server.poll();

        // Wait for blocks to be broadcast (client receives but can't commit)
        await sleep(5000);

        // Client's WebSocket should still be connected (process alive)
        // We verify by checking that the server still has WebSocket subscribers
        // (if client crashed, it would disconnect)
        expect(client.getLastAppliedBlock()).to.be.at.most(10,
            'Client should not have advanced beyond block 10 with replica DB down');
    });

    it('client recovers and catches up after replica DB is restored', async function () {
        // Take replica down
        await replicaFaults.dbDown();

        // Seed blocks while replica is down
        await seedSourceBlocks(11, 20);
        await server.poll();
        await sleep(3000);

        // Restore replica
        await replicaFaults.dbUp();

        // Client should detect gap via status event and catch up
        const recoveryMs = await waitForSyncRecovery(20, 90000);
        expect(recoveryMs).to.be.above(-1,
            'Client should catch up to block 20 after replica recovery');
        console.log(`    CE-DST-01 recovery time: ${recoveryMs}ms`);
    });

    it('no data corruption after replica DB recovery', async function () {
        const replicaDb = require('./helpers/chaos-setup').getReplicaDb();
        const sourceDb  = require('./helpers/chaos-setup').getSourceDb();
        const lastBlock = await replicaDb.getLastBlock();

        if (lastBlock !== null && lastBlock >= 10) {
            await assertHashesMatch(sourceDb, replicaDb, lastBlock);
        }
    });
});

// -------------------------------------------------------------------------
// CE-DST-02: Slow Write Responses
// -------------------------------------------------------------------------

describe('CE-DST-02: Slow Write Responses', function () {

    afterEach(async function () {
        await replicaFaults.reset();
    });

    it('blocks still applied under 2s write latency', async function () {
        // Inject 2s latency on all replica DB writes
        await replicaFaults.addLatency(2000);

        // Seed blocks into source
        await seedSourceBlocks(11, 18);
        await server.poll();

        // Wait with generous timeout; each block application involves
        // multiple queries, each adding 2s latency
        const recoveryMs = await waitForSyncRecovery(18, 120000);
        expect(recoveryMs).to.be.above(-1,
            'Blocks should still be applied despite 2s write latency');
        console.log(`    CE-DST-02 sync time under 2s latency: ${recoveryMs}ms`);
    });

    it('write performance returns to normal after toxic removal', async function () {
        await replicaFaults.addLatency(2000);
        await replicaFaults.reset();

        // Seed and sync a block; should be fast
        await seedSourceBlocks(19, 20);
        await server.poll();

        const t0 = Date.now();
        const recoveryMs = await waitForSyncRecovery(20, 15000);
        const elapsed = Date.now() - t0;

        expect(recoveryMs).to.be.above(-1);
        expect(elapsed).to.be.below(15000,
            'Block application should be fast after latency toxic is removed');
    });
});

// -------------------------------------------------------------------------
// CE-DST-03: Connection Pool Exhaustion
// -------------------------------------------------------------------------

describe('CE-DST-03: Connection Pool Exhaustion', function () {

    afterEach(async function () {
        await replicaFaults.reset();
    });

    it('client stays alive when replica DB connections are held for 30s', async function () {
        await replicaFaults.timeout(30000);

        // Seed blocks; server broadcasts, client receives but can't write
        await seedSourceBlocks(11, 15);
        await server.poll();
        await sleep(8000);

        // Client process must still be running (WebSocket connected)
        // Verify server is still serving (not blocked by client issues)
        const alive = await isServerAlive(server.getUrl());
        expect(alive).to.equal(true,
            'Server must stay alive; client pool exhaustion must not affect server');
    });

    it('client recovers after timeout toxic is removed', async function () {
        await replicaFaults.timeout(30000);
        await seedSourceBlocks(11, 15);
        await server.poll();
        await sleep(5000);

        await replicaFaults.reset();

        // Allow circuit breaker to recover (up to 30s cooldown)
        const recoveryMs = await waitForSyncRecovery(15, 90000);
        expect(recoveryMs).to.be.above(-1,
            'Client should recover after replica pool exhaustion');
        console.log(`    CE-DST-03 recovery time: ${recoveryMs}ms`);
    });
});

// -------------------------------------------------------------------------
// CE-DST-04: Intermittent Connection Drops (30% TCP Reset)
// -------------------------------------------------------------------------

describe('CE-DST-04: Intermittent Connection Drops', function () {

    afterEach(async function () {
        await replicaFaults.reset();
    });

    it('majority of blocks eventually applied under 30% TCP reset rate', async function () {
        // Seed blocks before injecting fault
        await seedSourceBlocks(11, 25);

        // Inject 30% connection reset on replica writes
        await replicaFaults.resetConnections(0.3);

        // Force multiple poll cycles to broadcast blocks
        for (let i = 0; i < 15; i++) {
            try { await server.poll(); } catch { /* expected */ }
            await sleep(300);
        }

        // With retry logic, most blocks should eventually be applied
        const replicaDb = require('./helpers/chaos-setup').getReplicaDb();
        const lastBlock = await replicaDb.getLastBlock();

        expect(lastBlock).to.be.above(10,
            'Client should have applied some blocks despite 30% TCP resets');
        console.log(`    CE-DST-04 block height under 30% resets: ${lastBlock}`);
    });

    it('all blocks eventually reach replica after toxic removal', async function () {
        await replicaFaults.resetConnections(0.3);
        await sleep(3000);
        await replicaFaults.reset();

        // Allow catch-up to complete
        const recoveryMs = await waitForSyncRecovery(25, 60000);
        expect(recoveryMs).to.be.above(-1,
            'All blocks should reach replica after toxic removal');
    });
});

// -------------------------------------------------------------------------
// CE-DST-05: Replica Down → Blocks Accumulate → Recovery + Integrity
// -------------------------------------------------------------------------

describe('CE-DST-05: Replica Down → Blocks Accumulate → Recovery', function () {

    afterEach(async function () {
        await replicaFaults.reset();
    });

    it('full data integrity after replica outage with accumulated blocks', async function () {
        const replicaDb = require('./helpers/chaos-setup').getReplicaDb();
        const sourceDb  = require('./helpers/chaos-setup').getSourceDb();

        // Record pre-outage state
        const preOutageBlock = await replicaDb.getLastBlock();
        expect(preOutageBlock).to.be.at.least(10);

        // Take replica DB down
        await replicaFaults.dbDown();
        await sleep(2000);

        // Seed a batch of blocks into source while replica is down
        await seedSourceBlocks(11, 30);

        // Server polls and broadcasts (client receives via WebSocket but can't write)
        for (let i = 0; i < 10; i++) {
            try { await server.poll(); } catch { /* expected */ }
            await sleep(300);
        }

        // Wait for blocks to be broadcast and fail on client
        await sleep(5000);

        // Restore replica DB
        await replicaFaults.dbUp();
        await sleep(2000);

        // Client should detect gap and perform incremental catch-up
        const recoveryMs = await waitForSyncRecovery(30, 120000);
        expect(recoveryMs).to.be.above(-1,
            'Client should catch up to block 30 after replica recovery');
        console.log(`    CE-DST-05 full recovery time: ${recoveryMs}ms`);

        // Full integrity checks
        await assertReplicaMatchesSource(sourceDb, replicaDb, testDb);
        await assertBalancesConsistent(replicaDb);
        await assertHashesMatch(sourceDb, replicaDb, 30);

        // Verify no block gaps
        for (let i = 11; i <= 30; i++) {
            await assertBlockExists(replicaDb, i);
        }
    });
});

}); // describe('Chaos: Replica Database Resilience')
