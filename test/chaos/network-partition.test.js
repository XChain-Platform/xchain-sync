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
 * Chaos Engineering: Network Partition
 *
 * Experiment IDs:
 *   CE-NET-01  Full WebSocket partition → reconnection + gap healing
 *   CE-NET-02  High latency on client↔server WebSocket link
 *   CE-NET-03  Bandwidth throttling on WebSocket stream
 *   CE-NET-04  Packet slicing on WebSocket stream
 *
 * These tests fault the WebSocket channel between client and server using
 * a Toxiproxy WebSocket proxy (port 33062). The server listens on its normal
 * port, but the client connects through the proxy. Source and replica DBs
 * remain healthy.
 *
 * Prerequisites (docker-compose.chaos.yml):
 *   - Source MariaDB proxied through toxiproxy on port 33060
 *   - Replica MariaDB proxied through toxiproxy on port 33061
 *   - WebSocket proxy on port 33062
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
    waitForSyncRecovery,
    sleep
} = require('./helpers/chaos-setup');

const {
    waitForToxiproxy,
    createProxy,
    createWsProxy,
    deleteWsProxy,
    wsFaults,
    resetBoth,
    SOURCE_PROXY,
    REPLICA_PROXY
} = require('./helpers/toxiproxy-client');

describe('Chaos: Network Partition', function () {

    let server, client;
    const SERVER_PORT    = 30300;
    const WS_PROXY_PORT  = 33062;

before(async function () {
    await waitForToxiproxy();
    await createProxy(SOURCE_PROXY);
    await createProxy(REPLICA_PROXY);

    // Create WebSocket proxy: toxiproxy:33062 → host:30300 (server)
    await createWsProxy(SERVER_PORT);

    await bootstrapDatabases();
    await seedSourceBlocks(1, 20);

    server = createServer(SERVER_PORT);
    await server.start();

    const wsProxiedUrl = `http://127.0.0.1:${WS_PROXY_PORT}`;
    client = createClient(wsProxiedUrl);
    await client.start();

    const recoveryMs = await waitForSyncRecovery(20, 30000);
    expect(recoveryMs).to.be.above(-1, 'Initial sync should complete within 30s');
    console.log(`    [setup] Initial sync to block 20 completed in ${recoveryMs}ms`);
});

after(async function () {
    if (client) client.stop();
    if (server) await server.stop();
    await deleteWsProxy();
    await teardownDatabases();
    await resetBoth();
});

describe('CE-NET-01: Full WebSocket Partition', function () {

    afterEach(async function () {
        await wsFaults.reset();
    });

    it('baseline: client is synced to block 20 before partition', async function () {
        const lastBlock = client.getLastAppliedBlock();
        expect(lastBlock).to.be.at.least(20);
    });

    it('client detects disconnect and blocks stop flowing during partition', async function () {
        // dbDown()/dbUp() are the generic fault API; on the WS proxy they
        // sever/restore the WebSocket link rather than a database connection.
        await wsFaults.dbDown();

        await seedSourceBlocks(21, 30);
        await server.poll();

        // Wait for client to detect disconnect (ping timeout ~30s or close event)
        await sleep(5000);

        const replicaDb = require('./helpers/chaos-setup').getReplicaDb();
        const lastBlock = await replicaDb.getLastBlock();
        expect(lastBlock).to.be.at.most(20,
            'No blocks should arrive during full WebSocket partition');
    });

    it('client reconnects and heals block gap after partition is lifted', async function () {
        await wsFaults.dbDown();

        await seedSourceBlocks(21, 30);
        await server.poll();
        await sleep(3000);

        await wsFaults.dbUp();

        // Client reconnects (CLIENT_RECONNECT_DELAY=500ms in test config),
        // then detects the gap via a status event and triggers catch-up.
        const recoveryMs = await waitForSyncRecovery(30, 60000);
        expect(recoveryMs).to.be.above(-1,
            'Client should reconnect and heal gap within 60s');
        console.log(`    CE-NET-01 recovery time: ${recoveryMs}ms`);
    });

    it('data integrity maintained after partition recovery', async function () {
        const replicaDb = require('./helpers/chaos-setup').getReplicaDb();
        const sourceDb  = require('./helpers/chaos-setup').getSourceDb();
        const lastBlock = await replicaDb.getLastBlock();

        if (lastBlock >= 20) {
            await assertHashesMatch(sourceDb, replicaDb, lastBlock);
        }
    });
});

describe('CE-NET-02: High Latency on WebSocket Link', function () {

    afterEach(async function () {
        await wsFaults.reset();
    });

    it('blocks still delivered despite 500ms WebSocket latency', async function () {
        // 500ms latency, 100ms jitter
        await wsFaults.addLatency(500, 100);

        await seedSourceBlocks(21, 35);
        await server.poll();

        // Generous timeout: blocks arrive with ~500ms delay per message hop
        const recoveryMs = await waitForSyncRecovery(35, 60000);
        expect(recoveryMs).to.be.above(-1,
            'All blocks should be delivered despite 500ms WebSocket latency');
        console.log(`    CE-NET-02 sync time under 500ms WS latency: ${recoveryMs}ms`);
    });

    it('data integrity maintained under latency', async function () {
        const replicaDb = require('./helpers/chaos-setup').getReplicaDb();
        const sourceDb  = require('./helpers/chaos-setup').getSourceDb();
        const lastBlock = await replicaDb.getLastBlock();

        if (lastBlock >= 20) {
            await assertReplicaMatchesSource(sourceDb, replicaDb, testDb);
        }
    });
});

describe('CE-NET-03: Bandwidth Throttling', function () {

    afterEach(async function () {
        await wsFaults.reset();
    });

    it('blocks still delivered despite 4KB/s bandwidth throttle on WebSocket', async function () {
        // 4096 bytes/sec
        await wsFaults.limitBandwidth(4096);

        await seedSourceBlocks(21, 30);
        await server.poll();

        // With 4KB/s throttle, each block payload may take several seconds to arrive
        const recoveryMs = await waitForSyncRecovery(30, 120000);
        expect(recoveryMs).to.be.above(-1,
            'Blocks should eventually arrive despite bandwidth throttle');
        console.log(`    CE-NET-03 sync time under 4KB/s throttle: ${recoveryMs}ms`);
    });

    it('throughput returns to normal after throttle is removed', async function () {
        await wsFaults.limitBandwidth(4096);
        await wsFaults.reset();

        await seedSourceBlocks(31, 35);
        await server.poll();

        const t0 = Date.now();
        const recoveryMs = await waitForSyncRecovery(35, 15000);
        const elapsed = Date.now() - t0;

        expect(recoveryMs).to.be.above(-1);
        expect(elapsed).to.be.below(15000,
            'Sync should be fast after bandwidth throttle is removed');
    });
});

describe('CE-NET-04: Packet Slicing', function () {

    afterEach(async function () {
        await wsFaults.reset();
    });

    it('WebSocket reassembly handles sliced packets without data corruption', async function () {
        // 100-byte average chunk size, 50µs delay between slices
        await wsFaults.sliceData(100, 50);

        await seedSourceBlocks(21, 40);
        await server.poll();

        // Blocks should arrive intact; TCP reassembly handles sliced data
        const recoveryMs = await waitForSyncRecovery(40, 90000);
        expect(recoveryMs).to.be.above(-1,
            'All blocks should arrive despite packet slicing');
        console.log(`    CE-NET-04 sync time under packet slicing: ${recoveryMs}ms`);
    });

    it('no data corruption from sliced packets', async function () {
        const replicaDb = require('./helpers/chaos-setup').getReplicaDb();
        const sourceDb  = require('./helpers/chaos-setup').getSourceDb();

        await assertReplicaMatchesSource(sourceDb, replicaDb, testDb);
        await assertBalancesConsistent(replicaDb);

        const lastBlock = await replicaDb.getLastBlock();
        if (lastBlock !== null) {
            await assertHashesMatch(sourceDb, replicaDb, lastBlock);
        }
    });

    it('response integrity returns to normal after slicer toxic is removed', async function () {
        await wsFaults.sliceData(100, 50);
        await wsFaults.reset();

        await seedSourceBlocks(41, 45);
        await server.poll();

        const recoveryMs = await waitForSyncRecovery(45, 15000);
        expect(recoveryMs).to.be.above(-1,
            'Sync should complete normally after slicer toxic is removed');
    });
});

}); // describe('Chaos: Network Partition')
