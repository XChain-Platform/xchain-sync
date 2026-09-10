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
 *   CE-SRC-02  Slow query responses (per-query latency injection)
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

const { waitForServerPollFailures } = require('../e2e/helpers/waitFor');

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

describe('Chaos: Source Database Resilience', function () {

    let server, client;
    const SERVER_PORT = 30100;

before(async function () {
    await waitForToxiproxy();
    await createProxy(SOURCE_PROXY);
    await createProxy(REPLICA_PROXY);
    await bootstrapDatabases();
    await seedSourceBlocks(1, 20);

    server = createServer(SERVER_PORT);
    await server.start();

    client = createClient(server.getUrl());
    await client.start();

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

describe('CE-SRC-01: Complete Source DB Unavailability', function () {

    afterEach(async function () {
        await sourceFaults.reset();
    });

    it('baseline: server /status returns 200 before fault injection', async function () {
        const res = await httpGet('/status', { baseUrl: server.getUrl() });
        expect(res.statusCode).to.equal(200);
        const status = JSON.parse(res.body);
        // /status nests fields per-dbType (bitcoin.mainnet.*) since the
        // multi-chain, multi-network status API migration.
        expect(status.bitcoin.mainnet.indexer.block_height).to.equal(20);
    });

    it('server process remains alive while source DB is completely down', async function () {
        await sourceFaults.dbDown();

        // Wait for the outage to be FELT rather than for a duration to elapse:
        // three poll cycles that actually failed against the severed source. If
        // the fault never reached the poller the counter never moves and this
        // throws, instead of a sleep that would report a healthy server as proof
        // of outage survival.
        //
        // Budget: a poll against a severed source does not fail fast. The first
        // one trips on the pool's already-open socket (milliseconds), and every
        // one after it waits out the pool's acquire timeout, measured at 10.0s
        // per read against this fixture (the same 10s production sets in
        // poolSizing.js). Three failures therefore need upwards of 30s of
        // wall clock, which the old 30s budget could not contain: the wait
        // expired a fraction of a second before the third failure landed, and
        // did so deterministically. Sized to 3 x acquire timeout plus margin.
        await waitForServerPollFailures(server, 3, 60000);

        const alive = await isServerAlive(server.getUrl());
        expect(alive).to.equal(true, 'Server must stay alive during source DB outage');
    });

    it('server recovers and resumes sync after source DB is restored', async function () {
        await sourceFaults.dbDown();
        await sleep(5000);

        await sourceFaults.dbUp();
        await sleep(2000); // allow pool to reconnect
        await seedSourceBlocks(21, 25);

        await server.poll();

        const recoveryMs = await waitForSyncRecovery(25, 60000);
        expect(recoveryMs).to.be.above(-1, 'Sync should recover within 60s');
        console.log(`    CE-SRC-01 recovery time: ${recoveryMs}ms`);
    });

    it('data integrity maintained after source DB recovery', async function () {
        const replicaDb = require('./helpers/chaos-setup').getReplicaDb();
        const sourceDb  = require('./helpers/chaos-setup').getSourceDb();
        const lastBlock = await replicaDb.getLastBlock();
        if (lastBlock !== null && lastBlock >= 20) {
            await assertHashesMatch(sourceDb, replicaDb, lastBlock);
        }
    });
});

describe('CE-SRC-02: Slow Query Responses', function () {

    // Per-query latency for the advancement case below. A 3s toxic cannot measure
 // what the case claims to measure: the poller pays the toxic once per query and
 // issues ~86 action-scoped queries per block, so 3s costs ~260s for ONE block,
 // past this suite per-test cap, and the case can only ever expire, never
 // report. A 3s toxic also exceeds the fixture pool 1s connect timeout, so no
 // MariaDB handshake completes and every read fails with
 // ER_GET_CONNECTION_TIMEOUT, a source that is effectively DOWN, which is
 // CE-SRC-01 experiment, not this one. 100ms keeps a genuinely slow source (two
 // orders of magnitude off baseline, with connections still establishable) and
 // leaves the per-block cost inside a budget a failure can actually be reported
 // from.
 const QUERY_LATENCY_MS = 100;

    afterEach(async function () {
        await sourceFaults.reset();
    });

    it('server still advances block height under injected query latency', async function () {
        await sourceFaults.addLatency(QUERY_LATENCY_MS);

        // Seed via the direct connection so the latency just injected on the
        // proxy doesn't also slow down seeding itself. The range starts ABOVE
        // the tip CE-SRC-01 left behind (25) so the claim under test is real
        // advancement; re-seeding 21-25 leaves the height where it already was,
        // and the recovery wait below would be satisfied before the poller ran.
        const sourceDbDirect = require('./helpers/chaos-setup').getSourceDbDirect();
        const fixtures = require('../e2e/helpers/fixtures');
        await fixtures.seedBlocks(sourceDbDirect, 26, 30);

        await server.poll();

        // Budget: the latency is paid per query, and the poller issues ~86
        // action-scoped queries per block on top of its block-scoped reads
        // (sync_action_scoped_queries_per_block), so five blocks cost roughly
        // 5 x 100 x QUERY_LATENCY_MS. Sized with room to spare on top of that.
        const recoveryMs = await waitForSyncRecovery(30, 150000);
        expect(recoveryMs).to.be.above(-1, 'Sync should complete despite injected query latency');
        console.log(`    CE-SRC-02 sync time under latency: ${recoveryMs}ms`);
    });

    it('latency returns to normal after toxic is removed', async function () {
        await sourceFaults.addLatency(QUERY_LATENCY_MS);
        await sourceFaults.reset();

        const t0 = Date.now();
        await server.poll();
        const elapsed = Date.now() - t0;

        // Without the toxic, a poll should complete well under 3s
        expect(elapsed).to.be.below(3000,
            'Poll should be fast after latency toxic is removed');
    });
});

describe('CE-SRC-03: Connection Pool Exhaustion', function () {

    afterEach(async function () {
        await sourceFaults.reset();
    });

    it('server stays alive when all DB connections are held for 30s', async function () {
        await sourceFaults.timeout(30000);

        // Wait for several poll cycles to fail (exhaust the 10-connection pool)
        await sleep(8000);

        const alive = await isServerAlive(server.getUrl());
        expect(alive).to.equal(true,
            'Server must stay alive during connection pool exhaustion');
    });

    it('server recovers after timeout toxic is removed', async function () {
        await sourceFaults.timeout(30000);
        await sleep(5000);

        await sourceFaults.reset();

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

describe('CE-SRC-04: Intermittent Connection Drops', function () {

    afterEach(async function () {
        await sourceFaults.reset();
    });

    it('server continues advancing block height under 30% TCP reset rate', async function () {
        const sourceDbDirect = require('./helpers/chaos-setup').getSourceDbDirect();
        const fixtures = require('../e2e/helpers/fixtures');
        await fixtures.seedBlocks(sourceDbDirect, 21, 30);

        await sourceFaults.resetConnections(0.3);

        for (let i = 0; i < 20; i++) {
            try { await server.poll(); } catch { /* expected failures */ }
            await sleep(200);
        }

        // With 30% resets and retry logic, the server should have made progress
        const replicaDb = require('./helpers/chaos-setup').getReplicaDb();
        const lastBlock = await replicaDb.getLastBlock();

        expect(lastBlock).to.be.above(20,
            'Server should advance block height despite 30% connection resets');
        console.log(`    CE-SRC-04 block height reached under 30% resets: ${lastBlock}`);
    });

    it('server remains alive throughout intermittent drops', async function () {
        await sourceFaults.resetConnections(0.3);

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

describe('CE-SRC-05: Source Down → Blocks Accumulate → Recovery', function () {

    afterEach(async function () {
        await sourceFaults.reset();
    });

    it('full data integrity after source DB outage with accumulated blocks', async function () {
        const replicaDb = require('./helpers/chaos-setup').getReplicaDb();
        const sourceDb  = require('./helpers/chaos-setup').getSourceDb();

        const preOutageBlock = await replicaDb.getLastBlock();
        expect(preOutageBlock).to.be.at.least(20);

        await sourceFaults.dbDown();
        await sleep(3000);

        // Direct connection bypasses the now-disabled source proxy.
        await seedSourceDirect(21, 35);

        // Let poll failures accumulate against the circuit breaker.
        await sleep(5000);

        await sourceFaults.dbUp();
        await sleep(2000);

        for (let i = 0; i < 10; i++) {
            try { await server.poll(); } catch { /* circuit may still be half-open */ }
            await sleep(500);
        }

        const recoveryMs = await waitForSyncRecovery(35, 90000);
        expect(recoveryMs).to.be.above(-1,
            'Sync should recover and catch up to block 35');
        console.log(`    CE-SRC-05 full recovery time: ${recoveryMs}ms`);

        await assertReplicaMatchesSource(sourceDb, replicaDb, testDb);
        await assertBalancesConsistent(replicaDb);
        await assertHashesMatch(sourceDb, replicaDb, 35);

        for (let i = 21; i <= 35; i++) {
            await assertBlockExists(replicaDb, i);
        }
    });
});

}); // describe('Chaos: Source Database Resilience')
