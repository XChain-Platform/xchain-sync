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

const { waitForClientEvents } = require('../e2e/helpers/waitFor');

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

describe('Chaos: Replica Database Resilience', function () {

    let server, client;
    const SERVER_PORT = 30200;

before(async function () {
    await waitForToxiproxy();
    await createProxy(SOURCE_PROXY);
    await createProxy(REPLICA_PROXY);
    await bootstrapDatabases();
    await seedSourceBlocks(1, 10);

    server = createServer(SERVER_PORT);
    await server.start();

    client = createClient(server.getUrl());
    await client.start();

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

        // Baseline before anything is broadcast, so the wait below counts only
        // the five blocks this step produces.
        const seenBlocks = client.getEventsHandled('block');

        await seedSourceBlocks(11, 15);
        await server.poll();

        // The claim is "the client received these blocks and could not commit
        // them", so wait for the receipt itself: five more block events fully
        // handled. The counter advances only after each event's handler settles,
        // so reaching it proves the client processed 11-15 and declined to
        // advance. A sleep here proved nothing - a client whose socket never
        // came up would sit at block 10 too and pass.
        await waitForClientEvents(client, 'block', seenBlocks + 5, 30000);

        expect(client.isConnected()).to.equal(true,
            'Client must still be connected to the server with only the replica DB down');
        expect(client.getLastAppliedBlock()).to.be.at.most(10,
            'Client should not have advanced beyond block 10 with replica DB down');
    });

    it('client recovers and catches up after replica DB is restored', async function () {
        await replicaFaults.dbDown();

        await seedSourceBlocks(11, 20);
        await server.poll();
        await sleep(3000);

        await replicaFaults.dbUp();

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

describe('CE-DST-02: Slow Write Responses', function () {

    afterEach(async function () {
        await replicaFaults.reset();
    });

    it('blocks still applied under 2s write latency', async function () {
        await replicaFaults.addLatency(2000);

        await seedSourceBlocks(11, 18);
        await server.poll();

        // Generous timeout: each block application involves multiple
        // queries, each adding 2s latency.
        const recoveryMs = await waitForSyncRecovery(18, 120000);
        expect(recoveryMs).to.be.above(-1,
            'Blocks should still be applied despite 2s write latency');
        console.log(`    CE-DST-02 sync time under 2s latency: ${recoveryMs}ms`);
    });

    it('write performance returns to normal after toxic removal', async function () {
        await replicaFaults.addLatency(2000);
        await replicaFaults.reset();

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

describe('CE-DST-03: Connection Pool Exhaustion', function () {

    afterEach(async function () {
        await replicaFaults.reset();
    });

    it('client stays alive when replica DB connections are held for 30s', async function () {
        await replicaFaults.timeout(30000);

        await seedSourceBlocks(11, 15);
        await server.poll();
        await sleep(8000);

        // Server must stay reachable even though the client is stuck
        // retrying against replica-write timeouts.
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

describe('CE-DST-04: Intermittent Connection Drops', function () {

    afterEach(async function () {
        await replicaFaults.reset();
    });

    it('majority of blocks eventually applied under 30% TCP reset rate', async function () {
        await seedSourceBlocks(11, 25);

        await replicaFaults.resetConnections(0.3);

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

        const recoveryMs = await waitForSyncRecovery(25, 60000);
        expect(recoveryMs).to.be.above(-1,
            'All blocks should reach replica after toxic removal');
    });
});

describe('CE-DST-05: Replica Down → Blocks Accumulate → Recovery', function () {

    afterEach(async function () {
        await replicaFaults.reset();
    });

    it('full data integrity after replica outage with accumulated blocks', async function () {
        const replicaDb = require('./helpers/chaos-setup').getReplicaDb();
        const sourceDb  = require('./helpers/chaos-setup').getSourceDb();

        const preOutageBlock = await replicaDb.getLastBlock();
        expect(preOutageBlock).to.be.at.least(10);

        await replicaFaults.dbDown();
        await sleep(2000);

        await seedSourceBlocks(11, 30);

        for (let i = 0; i < 10; i++) {
            try { await server.poll(); } catch { /* expected */ }
            await sleep(300);
        }

        await sleep(5000);

        await replicaFaults.dbUp();
        await sleep(2000);

        const recoveryMs = await waitForSyncRecovery(30, 120000);
        expect(recoveryMs).to.be.above(-1,
            'Client should catch up to block 30 after replica recovery');
        console.log(`    CE-DST-05 full recovery time: ${recoveryMs}ms`);

        await assertReplicaMatchesSource(sourceDb, replicaDb, testDb);
        await assertBalancesConsistent(replicaDb);
        await assertHashesMatch(sourceDb, replicaDb, 30);

        for (let i = 11; i <= 30; i++) {
            await assertBlockExists(replicaDb, i);
        }
    });
});

}); // describe('Chaos: Replica Database Resilience')
