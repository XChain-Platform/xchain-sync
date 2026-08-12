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
 * Chaos Engineering: Sync Resilience
 *
 * Experiment IDs:
 *   CE-SYNC-01  Server crash → WebSocket reconnect → gap healing
 *   CE-SYNC-02  Block gap detection → incremental catch-up
 *   CE-SYNC-03  Reorg during active sync (with source latency)
 *   CE-SYNC-04  Compound: source down + server crash + recovery → full integrity
 *
 * These are the highest-level chaos scenarios; they combine infrastructure
 * faults with specific sync state conditions (gaps, reorgs, compound failures).
 * Tests manage server/client lifecycle per-test (not shared across describe).
 *
 * Prerequisites (docker-compose.chaos.yml):
 *   - Source MariaDB proxied through toxiproxy on port 33060
 *   - Replica MariaDB proxied through toxiproxy on port 33061
 *   - Source MariaDB direct on port 33065
 *   - Toxiproxy API on port 8474
 *
 * Run: npm run test:chaos
 */

const { expect } = require('chai');
const testDb     = require('../e2e/helpers/testDb');
const fixtures   = require('../e2e/helpers/fixtures');

const {
    assertReplicaMatchesSource,
    assertBlockExists,
    assertBlockNotExists,
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
    deleteSourceBlocksFrom,
    waitForSyncRecovery,
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

describe('Chaos: Sync Resilience', function () {

    const SERVER_PORT = 30400;

before(async function () {
    await waitForToxiproxy();
    await createProxy(SOURCE_PROXY);
    await createProxy(REPLICA_PROXY);
    await bootstrapDatabases();
});

after(async function () {
    await teardownDatabases();
    await resetBoth();
});

describe('CE-SYNC-01: Server Crash → Reconnect → Gap Healing', function () {

    let server, client;

    afterEach(async function () {
        if (client) client.stop();
        if (server) await server.stop();
        await resetDatabases();
        await resetBoth();
    });

    it('client recovers from server crash and heals block gap', async function () {
        await seedSourceBlocks(1, 20);

        server = createServer(SERVER_PORT);
        await server.start();

        client = createClient(server.getUrl(), { reconnectDelay: 500 });
        await client.start();

        const initialSync = await waitForSyncRecovery(20, 30000);
        expect(initialSync).to.be.above(-1, 'Initial sync should complete');

        // Stop the server abruptly to simulate a crash, not a graceful shutdown.
        await server.stop();
        server = null;

        // Direct connection: the normal (server-fronted) seed path is down.
        await seedSourceDirect(21, 30);

        await sleep(3000);

        // Restart on the same port so the client's reconnect logic applies.
        server = createServer(SERVER_PORT);
        await server.start();

        // Client reconnects (500ms delay), receives a status event, detects
        // the gap (local=20, remote=30), and performs incremental catch-up.
        const recoveryMs = await waitForSyncRecovery(30, 60000);
        expect(recoveryMs).to.be.above(-1,
            'Client should reconnect and heal gap after server crash');
        console.log(`    CE-SYNC-01 recovery time: ${recoveryMs}ms`);

        const replicaDb = require('./helpers/chaos-setup').getReplicaDb();
        const sourceDb  = require('./helpers/chaos-setup').getSourceDb();

        for (let i = 21; i <= 30; i++) {
            await assertBlockExists(replicaDb, i);
        }
        await assertHashesMatch(sourceDb, replicaDb, 30);
    });
});

describe('CE-SYNC-02: Block Gap Detection → Incremental Catch-Up', function () {

    let server, client;

    afterEach(async function () {
        if (client) client.stop();
        if (server) await server.stop();
        await resetDatabases();
        await resetBoth();
    });

    it('client detects and heals a multi-block gap via status event', async function () {
        await seedSourceBlocks(1, 30);

        server = createServer(SERVER_PORT);
        await server.start();

        client = createClient(server.getUrl(), { reconnectDelay: 500 });
        await client.start();

        const initialSync = await waitForSyncRecovery(30, 30000);
        expect(initialSync).to.be.above(-1, 'Initial sync should complete');

        // Simulate a brief disconnect.
        client.stop();

        await seedSourceBlocks(31, 50);
        await server.poll();
        await sleep(1000);

        // Simulates resumption: the client's replica already has blocks 1-30.
        client = createClient(server.getUrl(), { reconnectDelay: 500 });

        // connectLive() skips the full bootstrap since the replica is already
        // populated; the server's status event (block_height=50) drives gap
        // detection (local=30, remote=50) and incrementalCatchUp(31).
        await client.connectLive();

        const recoveryMs = await waitForSyncRecovery(50, 60000);
        expect(recoveryMs).to.be.above(-1,
            'Client should detect gap and catch up to block 50');
        console.log(`    CE-SYNC-02 gap healing time: ${recoveryMs}ms`);

        const replicaDb = require('./helpers/chaos-setup').getReplicaDb();
        const sourceDb  = require('./helpers/chaos-setup').getSourceDb();

        await assertBlockExists(replicaDb, 31);
        await assertBlockExists(replicaDb, 50);
        await assertReplicaMatchesSource(sourceDb, replicaDb, testDb);
    });
});

describe('CE-SYNC-03: Reorg During Active Sync', function () {

    let server, client;

    afterEach(async function () {
        if (client) client.stop();
        if (server) await server.stop();
        await resetDatabases();
        await resetBoth();
    });

    it('client handles reorg while source DB has injected latency', async function () {
        await seedSourceBlocks(1, 20);

        server = createServer(SERVER_PORT);
        await server.start();

        client = createClient(server.getUrl(), { reconnectDelay: 500 });
        await client.start();

        const initialSync = await waitForSyncRecovery(20, 30000);
        expect(initialSync).to.be.above(-1);

        // Latency on source DB reads means reorg detection will be slow too.
        await sourceFaults.addLatency(1000);

        // Simulates a reorg by deleting blocks 18-20 from source.
        const sourceDbDirect = require('./helpers/chaos-setup').getSourceDbDirect();
        await fixtures.deleteBlocksFrom(sourceDbDirect, 18);

        // Poll should detect the reorg (currentBlock=17 < lastPolled=20).
        try { await server.poll(); } catch { /* may fail under latency */ }
        await sleep(2000);

        const replicaDb = require('./helpers/chaos-setup').getReplicaDb();
        await sleep(3000);

        // Different creditAmount so the assertions below can confirm this is
        // the re-seeded data, not the pre-reorg blocks.
        await fixtures.seedBlocks(sourceDbDirect, 18, 22, { creditAmount: '7777' });

        await sourceFaults.reset();
        await sleep(1000);

        for (let i = 0; i < 10; i++) {
            try { await server.poll(); } catch { /* recovery in progress */ }
            await sleep(500);
        }

        const recoveryMs = await waitForSyncRecovery(22, 60000);
        expect(recoveryMs).to.be.above(-1,
            'Client should apply re-seeded blocks after reorg');
        console.log(`    CE-SYNC-03 reorg recovery time: ${recoveryMs}ms`);

        const sourceDb = require('./helpers/chaos-setup').getSourceDb();
        await assertBalancesConsistent(replicaDb);

        await assertBlockExists(replicaDb, 22);
        await assertHashesMatch(sourceDb, replicaDb, 22);
    });
});

describe('CE-SYNC-04: Compound Failure (Source Down + Server Crash)', function () {

    let server, client;

    afterEach(async function () {
        if (client) client.stop();
        if (server) await server.stop();
        await resetDatabases();
        await resetBoth();
    });

    it('full data integrity after compound source outage + server crash', async function () {
        await seedSourceBlocks(1, 15);

        server = createServer(SERVER_PORT);
        await server.start();

        client = createClient(server.getUrl(), { reconnectDelay: 500 });
        await client.start();

        const initialSync = await waitForSyncRecovery(15, 30000);
        expect(initialSync).to.be.above(-1);

        await sourceFaults.dbDown();

        // Direct connection, since the source proxy is disabled.
        await seedSourceDirect(16, 25);

        // Let the server's circuit breaker start failing before crashing it.
        await sleep(5000);

        // Server crashes while source is still down.
        await server.stop();
        server = null;

        await sleep(3000);

        await sourceFaults.dbUp();
        await sleep(2000);

        server = createServer(SERVER_PORT);
        await server.start();

        for (let i = 0; i < 10; i++) {
            try { await server.poll(); } catch { /* circuit may be recovering */ }
            await sleep(500);
        }

        // Client reconnects (500ms delay), status event, gap detected, catch-up.
        const recoveryMs = await waitForSyncRecovery(25, 120000);
        expect(recoveryMs).to.be.above(-1,
            'Client should recover after compound failure (source down + server crash)');
        console.log(`    CE-SYNC-04 compound recovery time: ${recoveryMs}ms`);

        const replicaDb = require('./helpers/chaos-setup').getReplicaDb();
        const sourceDb  = require('./helpers/chaos-setup').getSourceDb();

        await assertReplicaMatchesSource(sourceDb, replicaDb, testDb);
        await assertBalancesConsistent(replicaDb);

        for (let i = 16; i <= 25; i++) {
            await assertBlockExists(replicaDb, i);
            await assertHashesMatch(sourceDb, replicaDb, i);
        }
    });
});

}); // describe('Chaos: Sync Resilience')
