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
 * Chaos Engineering: Source Database Resilience
 *
 * Experiment IDs:
 *   CE-SRC-01  Complete source DB unavailability (circuit breaker validation)
 *   CE-SRC-02  Slow query responses (3 s latency injection)
 *   CE-SRC-03  Connection pool exhaustion (30 s timeout toxic)
 *   CE-SRC-04  Intermittent connection drops (30 % TCP reset)
 *   CE-SRC-05  Source down → blocks accumulate → recovery + integrity check
 *
 * The source DB is what the sync server reads from (indexer DB). Faults are
 * injected on the source_db_chaos Toxiproxy proxy (port 33060). The replica
 * DB and WebSocket channel remain healthy throughout.
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
    resetDatabases,
    createServer,
    createClient,
    seedSourceBlocks,
    seedSourceDirect,
    httpGet,
    isServerAlive,
    waitForSyncRecovery,
    startReplicaPoller,
    sleep
} = require('./helpers/chaos-setup');

const {
    waitForToxiproxy,
    createProxy,
    sourceFaults,
    resetBoth,
    SOURCE_PROXY,
    REPLICA_PROXY
} = require('./helpers/toxiproxy-client');

// -------------------------------------------------------------------------
// Suite setup / teardown
// -------------------------------------------------------------------------

describe('Chaos: Source Database Resilience', function () {

    let server, client;
    const SERVER_PORT = 30100;

before(async function () {
    await waitForToxiproxy();
    await createProxy(SOURCE_PROXY);
    await createProxy(REPLICA_PROXY);
    await bootstrapDatabases();

    // Seed initial blocks 1-20 into source
    await seedSourceBlocks(1, 20);

    // Start sync server
    server = createServer(SERVER_PORT);
    await server.start();

    // Bootstrap client and start live sync
    client = createClient(server.getUrl());
    await client.start();

    // Wait for initial sync to complete
    const recoveryMs = await waitForSyncRecovery(20, 30000);
    expect(recoveryMs).to.be.above(-1, 'Initial sync should complete within 30s');
    console.log(`    [setup] Initial sync to block 20 completed in ${recoveryMs}ms`);
});

after(async function () {
    if (client) client.stop();
    if (server) await server.stop();
    await teardownDatabases();
    await resetBoth();
});

// -------------------------------------------------------------------------
// CE-SRC-01: Complete Source DB Unavailability
// -------------------------------------------------------------------------

describe('CE-SRC-01: Complete Source DB Unavailability', function () {

    afterEach(async function () {
        await sourceFaults.reset();
    });

    it('baseline: server /status returns 200 before fault injection', async function () {
        const res = await httpGet('/status', { baseUrl: server.getUrl() });
        expect(res.statusCode).to.equal(200);
        const status = JSON.parse(res.body);
        // /status nests per-dbType after the Phase-3 :dbType migration.
        expect(status.bitcoin.mainnet.indexer.block_height).to.equal(20);
    });

    it('server process remains alive while source DB is completely down', async function () {
        await sourceFaults.dbDown();

        // Wait long enough for the server to attempt several poll cycles
        await sleep(5000);

        // Server must still accept HTTP connections and not crash
        const alive = await isServerAlive(server.getUrl());
        expect(alive).to.equal(true, 'Server must stay alive during source DB outage');
    });

    it('server recovers and resumes sync after source DB is restored', async function () {
        // Take source DB down
        await sourceFaults.dbDown();
        await sleep(5000);

        // Restore source DB
        await sourceFaults.dbUp();

        // Seed new blocks into source (through the now-restored proxy)
        await sleep(2000); // allow pool to reconnect
        await seedSourceBlocks(21, 25);

        // Force poll to pick up new blocks
        await server.poll();

        // Wait for replica to receive the new blocks
        const recoveryMs = await waitForSyncRecovery(25, 60000);
        expect(recoveryMs).to.be.above(-1, 'Sync should recover within 60s');
        console.log(`    CE-SRC-01 recovery time: ${recoveryMs}ms`);
    });

    it('data integrity maintained after source DB recovery', async function () {
        // Verify hashes match between source and replica for the latest block
        const replicaDb = require('./helpers/chaos-setup').getReplicaDb();
        const sourceDb  = require('./helpers/chaos-setup').getSourceDb();
        const lastBlock = await replicaDb.getLastBlock();
        if (lastBlock !== null && lastBlock >= 20) {
            await assertHashesMatch(sourceDb, replicaDb, lastBlock);
        }
    });
});

// -------------------------------------------------------------------------
// CE-SRC-02: Slow Query Responses
// -------------------------------------------------------------------------

describe('CE-SRC-02: Slow Query Responses', function () {

    afterEach(async function () {
        await sourceFaults.reset();
    });

    it('server still advances block height under 3s DB latency', async function () {
        // Inject 3s latency on all source DB responses
        await sourceFaults.addLatency(3000);

        // Seed new blocks into source (through the latent proxy; will be slow)
        // Use direct connection for seeding to avoid the latency ourselves
        const sourceDbDirect = require('./helpers/chaos-setup').getSourceDbDirect();
        const fixtures = require('../e2e/helpers/fixtures');
        await fixtures.seedBlocks(sourceDbDirect, 21, 25);

        // Force poll; will be slow due to 3s latency per query
        await server.poll();

        // Wait for sync with generous timeout (3s latency × multiple queries per block)
        const recoveryMs = await waitForSyncRecovery(25, 90000);
        expect(recoveryMs).to.be.above(-1, 'Sync should complete despite 3s latency');
        console.log(`    CE-SRC-02 sync time under latency: ${recoveryMs}ms`);
    });

    it('latency returns to normal after toxic is removed', async function () {
        await sourceFaults.addLatency(3000);
        await sourceFaults.reset();

        // Measure a poll cycle; should be fast again
        const t0 = Date.now();
        await server.poll();
        const elapsed = Date.now() - t0;

        // Without the toxic, a poll should complete well under 3s
        expect(elapsed).to.be.below(3000,
            'Poll should be fast after latency toxic is removed');
    });
});

// -------------------------------------------------------------------------
// CE-SRC-03: Connection Pool Exhaustion
// -------------------------------------------------------------------------

describe('CE-SRC-03: Connection Pool Exhaustion', function () {

    afterEach(async function () {
        await sourceFaults.reset();
    });

    it('server stays alive when all DB connections are held for 30s', async function () {
        // Inject timeout toxic: holds connections without responding for 30s
        await sourceFaults.timeout(30000);

        // Wait for several poll cycles to fail (exhaust the 10-connection pool)
        await sleep(8000);

        // Server must still accept HTTP connections
        const alive = await isServerAlive(server.getUrl());
        expect(alive).to.equal(true,
            'Server must stay alive during connection pool exhaustion');
    });

    it('server recovers after timeout toxic is removed', async function () {
        await sourceFaults.timeout(30000);
        await sleep(5000);

        await sourceFaults.reset();

        // Seed blocks and force poll
        const sourceDbDirect = require('./helpers/chaos-setup').getSourceDbDirect();
        const fixtures = require('../e2e/helpers/fixtures');
        await fixtures.seedBlocks(sourceDbDirect, 21, 23);

        // Allow circuit breaker to recover (up to 30s cooldown + half-open attempt)
        await sleep(5000);
        await server.poll();

        const recoveryMs = await waitForSyncRecovery(23, 60000);
        expect(recoveryMs).to.be.above(-1, 'Sync should recover after pool exhaustion');
        console.log(`    CE-SRC-03 recovery time: ${recoveryMs}ms`);
    });
});

// -------------------------------------------------------------------------
// CE-SRC-04: Intermittent Connection Drops (30% TCP Reset)
// -------------------------------------------------------------------------

describe('CE-SRC-04: Intermittent Connection Drops', function () {

    afterEach(async function () {
        await sourceFaults.reset();
    });

    it('server continues advancing block height under 30% TCP reset rate', async function () {
        // Seed blocks first (before fault injection)
        const sourceDbDirect = require('./helpers/chaos-setup').getSourceDbDirect();
        const fixtures = require('../e2e/helpers/fixtures');
        await fixtures.seedBlocks(sourceDbDirect, 21, 30);

        // Inject 30% connection reset probability
        await sourceFaults.resetConnections(0.3);

        // Force multiple poll cycles; some will fail, some will succeed
        for (let i = 0; i < 20; i++) {
            try { await server.poll(); } catch { /* expected failures */ }
            await sleep(200);
        }

        // With 30% resets and retry logic, the server should have made progress
        const replicaDb = require('./helpers/chaos-setup').getReplicaDb();
        const lastBlock = await replicaDb.getLastBlock();

        // Server should have advanced beyond block 20 (at least some polls succeeded)
        expect(lastBlock).to.be.above(20,
            'Server should advance block height despite 30% connection resets');
        console.log(`    CE-SRC-04 block height reached under 30% resets: ${lastBlock}`);
    });

    it('server remains alive throughout intermittent drops', async function () {
        await sourceFaults.resetConnections(0.3);

        // Run for a while under drops
        for (let i = 0; i < 10; i++) {
            try { await server.poll(); } catch { /* expected */ }
            await sleep(300);
        }

        const alive = await isServerAlive(server.getUrl());
        expect(alive).to.equal(true,
            'Server must stay alive during intermittent connection drops');
    });

    it('success rate returns to 100% after toxic is removed', async function () {
        await sourceFaults.resetConnections(0.3);
        await sleep(2000);
        await sourceFaults.reset();

        // 5 consecutive polls should all succeed
        let successCount = 0;
        for (let i = 0; i < 5; i++) {
            try {
                await server.poll();
                successCount++;
            } catch { /* failure */ }
        }

        expect(successCount).to.equal(5,
            'All polls should succeed after TCP reset toxic is removed');
    });
});

// -------------------------------------------------------------------------
// CE-SRC-05: Source Down → Blocks Accumulate → Recovery + Integrity
// -------------------------------------------------------------------------

describe('CE-SRC-05: Source Down → Blocks Accumulate → Recovery', function () {

    afterEach(async function () {
        await sourceFaults.reset();
    });

    it('full data integrity after source DB outage with accumulated blocks', async function () {
        const replicaDb = require('./helpers/chaos-setup').getReplicaDb();
        const sourceDb  = require('./helpers/chaos-setup').getSourceDb();

        // Record pre-outage state
        const preOutageBlock = await replicaDb.getLastBlock();
        expect(preOutageBlock).to.be.at.least(20);

        // Take source DB down
        await sourceFaults.dbDown();
        await sleep(3000);

        // Seed blocks via DIRECT connection (bypasses disabled proxy)
        await seedSourceDirect(21, 35);

        // Wait a bit for poll failures to accumulate
        await sleep(5000);

        // Restore source DB
        await sourceFaults.dbUp();
        await sleep(2000);

        // Force polls to process accumulated blocks
        for (let i = 0; i < 10; i++) {
            try { await server.poll(); } catch { /* circuit may still be half-open */ }
            await sleep(500);
        }

        // Wait for full sync recovery
        const recoveryMs = await waitForSyncRecovery(35, 90000);
        expect(recoveryMs).to.be.above(-1,
            'Sync should recover and catch up to block 35');
        console.log(`    CE-SRC-05 full recovery time: ${recoveryMs}ms`);

        // Verify data integrity
        await assertReplicaMatchesSource(sourceDb, replicaDb, testDb);
        await assertBalancesConsistent(replicaDb);
        await assertHashesMatch(sourceDb, replicaDb, 35);

        // Verify no block gaps
        for (let i = 21; i <= 35; i++) {
            await assertBlockExists(replicaDb, i);
        }
    });
});

}); // describe('Chaos: Source Database Resilience')
