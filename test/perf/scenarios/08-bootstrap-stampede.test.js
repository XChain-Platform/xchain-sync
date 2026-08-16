'use strict';

// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// N-concurrent-bootstrap load test against one sync server.
//
// The failure this exists to catch: ServerPoller and every /snapshot route
// share ONE Database, i.e. one mariadb pool, per chain:network:dbType. A full
// snapshot pins a pool connection for the entire duration of its stream. The
// HTTP rate limiter is per-IP, and a validator cohort bootstrapping on flag
// day is N DIFFERENT IPs, so nothing upstream of the pool bounds them. Enough
// simultaneous bootstraps and every connection is pinned, the poller's
// getLastBlock acquire starves, and live block broadcast stops for every
// already-synced follower on the network. That is a network-wide liveness
// outage triggered by ordinary, legitimate traffic.
//
// A per-Database semaphore fails fast with 503 SNAPSHOT_BUSY rather than
// queueing. It has unit coverage but has never been driven at cohort size
// against a real server, a real pool and a real poller. A validator cohort of
// this size is expected to bootstrap soon, so this harness proves three
// things together, under load, which is the only way any of them means
// anything:
//
//   1. the semaphore SHEDS: concurrent accepted streams never exceed the cap,
//      and the excess is refused immediately with a retryable answer, not
//      queued behind a connection that will not come free;
//   2. the poller NEVER STALLS: its poll cycle latency stays inside budget for
//      the whole stampede, so live broadcast keeps flowing;
//   3. a subscribed follower MISSES NOTHING: every block produced during the
//      stampede reaches it, in order.
//
// Assertion 1 alone would pass against a server that sheds by falling over.
// Assertions 2 and 3 are what make it a liveness proof.

const assert    = require('assert');
const http      = require('http');
const sinon     = require('sinon');
const axios     = require('axios');
const zlib      = require('zlib');
const WebSocket = require('ws');
const { bootEnvironment, teardownEnvironment, resetAll,
        createServer, createGenerator, SERVER_PORT } = require('../setup/perf-setup');
const ReportGenerator = require('../setup/report-generator');
const SnapshotBuilder = require('../../../src/SnapshotBuilder');
const poolSizing      = require('../../../src/poolSizing');
const { waitFor }     = require('../../e2e/helpers/waitFor');

// Explicit agent: the whole test rests on N requests being in flight AT ONCE.
// Whatever the ambient default maxSockets is, this pins it above the largest
// cohort here, so a serialized stampede can never be mistaken for a working
// semaphore.
const STAMPEDE_AGENT = new http.Agent({ keepAlive: false, maxSockets: 128 });

// The e2e TestDatabase pool is built with connectionLimit 10 (test/e2e/helpers/testDb.js).
const POOL_LIMIT = 10;

// Snapshot size. Big enough that a stream lasts long enough to genuinely
// overlap its siblings (a snapshot that completes in under a millisecond would
// let the stampede trickle through one at a time and prove nothing).
const STAMPEDE_BLOCKS  = parseInt(process.env.PERF_STAMPEDE_BLOCKS  || '150');
const STAMPEDE_ACTIONS = parseInt(process.env.PERF_STAMPEDE_ACTIONS || '10');

// Blocks produced DURING the stampede, one poll cycle each. This is the live
// traffic whose delivery must survive the bootstrap load.
const LIVE_BLOCKS = parseInt(process.env.PERF_STAMPEDE_LIVE_BLOCKS || '12');

// Budgets. The poller's own pool acquire is what the snapshot semaphore
// protects, so the ceiling is expressed against it: a cycle that takes longer
// than this is a stall by any operator's definition, whatever the cause.
const BUDGET_X = Number(process.env.PERF_BUDGET_MULTIPLIER) > 0
    ? Number(process.env.PERF_BUDGET_MULTIPLIER) : 1;
const POLL_MAX_BUDGET_MS      = 5000 * BUDGET_X;
const POLL_MEDIAN_BUDGET_MS   = 1500 * BUDGET_X;
const BROADCAST_GAP_BUDGET_MS = 8000 * BUDGET_X;

const SNAPSHOT_PATH = '/snapshot/indexer/bitcoin/mainnet';

function median(values) {
    if (values.length === 0) return 0;
    const sorted = [...values].sort((a, b) => a - b);
    const mid = Math.floor(sorted.length / 2);
    return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
}

describe('08 Bootstrap Stampede (N-concurrent-bootstrap load)', function () {
    this.timeout(900000);

    const reporter = new ReportGenerator();
    const allStats = {};
    let sourceDb, server;
    let savedDbType, savedPoolParams;

    before(async function () {
        const env = await bootEnvironment();
        sourceDb = env.sourceDb;

        // Fidelity fix, and the reason it matters: SnapshotBuilder._snapshotCap
        // derives the cap from `db.connectionPoolParams.connectionLimit`, falling
        // back to poolSizing's per-dbType DEFAULT when the field is absent. The
        // e2e TestDatabase carries neither field, so without this the semaphore
        // would size itself from the indexer default (12) against a pool that
        // actually holds 10, i.e. it would permit MORE concurrent streams than
        // the pool has connections and the harness would "prove" a cap that
        // cannot protect anything. Production Database sets both; mirror it.
        savedDbType     = sourceDb.dbType;
        savedPoolParams = sourceDb.connectionPoolParams;
        sourceDb.dbType = 'indexer';
        sourceDb.connectionPoolParams = { connectionLimit: POOL_LIMIT };

        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });

    after(async function () {
        sinon.restore();
        // Guard: a failed before-hook leaves sourceDb unset, and an after-hook
        // that throws on it buries the real error behind a TypeError.
        if (sourceDb) {
            sourceDb.dbType = savedDbType;
            sourceDb.connectionPoolParams = savedPoolParams;
        }
        if (Object.keys(allStats).length > 0) {
            reporter.writeJson({ allStats }, '08-stampede-combined');
        }
        await teardownEnvironment();
    });

    afterEach(async function () {
        if (server) { await server.stop(); server = null; }
        delete process.env.MAX_CONCURRENT_SNAPSHOTS;
    });

    function connectFollower(wsUrl) {
        return new Promise((resolve, reject) => {
            const ws = new WebSocket(wsUrl + '/subscribe/indexer/bitcoin/mainnet');
            const timer = setTimeout(() => reject(new Error('follower WS connect timeout')), 5000);
            ws.on('open', () => { clearTimeout(timer); resolve(ws); });
            ws.on('error', (e) => { clearTimeout(timer); reject(e); });
        });
    }

    // Wrap the live SnapshotBuilder's slot accounting so the test can observe
    // PEAK CONCURRENCY rather than infer it from response codes. Counting 200s
    // is not the same measurement: streams that start after an earlier one
    // finished are sequential, not concurrent, and would read as a cap breach.
    function instrumentSemaphore(builder) {
        const observed = { peak: 0, inflight: 0, acquired: 0, refused: 0 };
        const acquire = builder._acquireSnapshotSlot.bind(builder);
        const release = builder._releaseSnapshotSlot.bind(builder);

        builder._acquireSnapshotSlot = (db, res) => {
            const ok = acquire(db, res);
            if (ok) {
                observed.acquired++;
                observed.inflight++;
                if (observed.inflight > observed.peak) observed.peak = observed.inflight;
            } else {
                observed.refused++;
            }
            return ok;
        };
        builder._releaseSnapshotSlot = (db) => {
            observed.inflight--;
            return release(db);
        };
        return observed;
    }

    /**
     * Drive `concurrency` simultaneous full-snapshot downloads against one
     * server while blocks keep being produced and polled, with a subscribed
     * follower listening.
     */
    async function runStampede(concurrency, label, opts = {}) {
        await resetAll();

        const gen = createGenerator(sourceDb);
        await gen.seedBulk(STAMPEDE_BLOCKS, STAMPEDE_ACTIONS);

        server = createServer(sourceDb, SERVER_PORT);
        server.config.WS_MAX_PER_IP = 20;
        if (opts.maxConcurrentSnapshots !== undefined) {
            process.env.MAX_CONCURRENT_SNAPSHOTS = String(opts.maxConcurrentSnapshots);
        }
        await server.start();

        const cap = server.snapshotBuilder._snapshotCap(sourceDb);
        const observed = instrumentSemaphore(server.snapshotBuilder);

        // The poller starts at the seeded tip, so /snapshot has nothing recorded
        // for the pre-seeded range until it catches up. Do that BEFORE the
        // stampede so the measured poll latencies are live-block cycles, not
        // backfill.
        server.poller.lastPolledBlock = 0;
        await server.pollUntil(STAMPEDE_BLOCKS);

        const follower = await connectFollower(server.getWsUrl());
        const received = [];
        follower.on('message', (data) => {
            try {
                const event = JSON.parse(data);
                if (event.type === 'block' && event.block_index) {
                    received.push({ blockIndex: Number(event.block_index), at: Date.now() });
                }
            } catch (e) { /* status frames and other event types are not under test */ }
        });

        // The stampede: N validators asking for a full snapshot at once
        const startedAt = Date.now();
        const snapshotResults = [];
        const stampede = [];
        for (let i = 0; i < concurrency; i++) {
            stampede.push(
                axios.get(server.getUrl() + SNAPSHOT_PATH, {
                    responseType: 'arraybuffer',
                    timeout: 300000,
                    decompress: false,
                    httpAgent: STAMPEDE_AGENT,
                    // Read the 503 body instead of throwing, so a refusal can be
                    // inspected the way a real client's retry logic would.
                    validateStatus: () => true
                }).then((res) => snapshotResults.push({ i, status: res.status, headers: res.headers, data: res.data }))
                  .catch((err) => snapshotResults.push({ i, status: 0, error: err.message }))
            );
        }

        // Live traffic, concurrent with the stampede
        const pollLatencies = [];
        const liveBlocks = [];
        const liveLoop = (async () => {
            for (let n = 1; n <= LIVE_BLOCKS; n++) {
                const height = STAMPEDE_BLOCKS + n;
                await gen.seedBlockRange(height, height);
                const t = process.hrtime.bigint();
                await server.poll();
                pollLatencies.push(Number(process.hrtime.bigint() - t) / 1e6);
                liveBlocks.push(height);
                await new Promise(r => setTimeout(r, 50));
            }
        })();

        await Promise.all([...stampede, liveLoop]);
        const stampedeMs = Date.now() - startedAt;

        // Drain the frames the follower is still owed, bounded, instead of guessing
        // a flush window. A frame that arrives late is then judged by the
        // broadcast-gap budget below rather than silently counted as a miss; on
        // timeout the completeness assertion reports exactly what never arrived.
        try {
            await waitFor(() => {
                const seen = new Set(received.map(r => r.blockIndex));
                return liveBlocks.every(h => seen.has(h));
            }, 15000, 100);
        } catch (e) { /* reported by the completeness assertion below */ }
        try { follower.close(); } catch (e) { /* already closed */ }

        const accepted = snapshotResults.filter(r => r.status === 200);
        const refused  = snapshotResults.filter(r => r.status === 503);
        const other    = snapshotResults.filter(r => r.status !== 200 && r.status !== 503);

        const liveSet = new Set(liveBlocks);
        const receivedLive = received.filter(r => liveSet.has(r.blockIndex));
        const receivedHeights = new Set(receivedLive.map(r => r.blockIndex));

        // Largest gap between consecutive live-block deliveries: the number an
        // operator would call a broadcast stall.
        let maxGapMs = 0;
        for (let i = 1; i < receivedLive.length; i++) {
            maxGapMs = Math.max(maxGapMs, receivedLive[i].at - receivedLive[i - 1].at);
        }

        const stats = {
            concurrency,
            cap,
            poolLimit: POOL_LIMIT,
            peakConcurrentStreams: observed.peak,
            accepted: accepted.length,
            refused: refused.length,
            other: other.length,
            snapshotsRejectedCounter: server.snapshotBuilder.snapshotsRejected || 0,
            stampedeMs,
            pollMedianMs: +median(pollLatencies).toFixed(2),
            pollMaxMs: +Math.max(...pollLatencies, 0).toFixed(2),
            liveBlocksProduced: liveBlocks.length,
            liveBlocksDelivered: receivedHeights.size,
            maxBroadcastGapMs: maxGapMs
        };

        allStats[label] = stats;
        reporter.writeJson(stats, `08-${label}`);
        process.stdout.write(
            `      [${label}] cap=${cap} peak=${stats.peakConcurrentStreams} ` +
            `200=${stats.accepted} 503=${stats.refused} ` +
            `poll med/max=${stats.pollMedianMs}/${stats.pollMaxMs}ms ` +
            `live ${stats.liveBlocksDelivered}/${stats.liveBlocksProduced} gap=${maxGapMs}ms\n`
        );

        await server.stop();
        server = null;

        return { stats, accepted, refused, other, liveBlocks, receivedHeights };
    }

    // Shared assertions: whatever the concurrency, these must hold.
    function assertLivenessHeld(result) {
        const { stats, other, liveBlocks, receivedHeights } = result;

        assert.strictEqual(other.length, 0,
            `every snapshot request must answer 200 or 503; got ${other.length} other outcomes ` +
            `(${JSON.stringify(other.map(o => o.status + (o.error ? ':' + o.error : '')))})`);

        assert.ok(stats.peakConcurrentStreams <= stats.cap,
            `peak concurrent snapshot streams ${stats.peakConcurrentStreams} exceeded the cap ${stats.cap}: ` +
            `the semaphore did not hold and the pool can be pinned`);

        assert.ok(stats.cap <= POOL_LIMIT - 1,
            `cap ${stats.cap} leaves no connection for the poller in a pool of ${POOL_LIMIT}`);

        assert.ok(stats.pollMaxMs < POLL_MAX_BUDGET_MS,
            `slowest poll cycle during the stampede was ${stats.pollMaxMs}ms, over the ${POLL_MAX_BUDGET_MS}ms budget: ` +
            `live block broadcast stalled behind bootstrap traffic`);
        assert.ok(stats.pollMedianMs < POLL_MEDIAN_BUDGET_MS,
            `median poll cycle ${stats.pollMedianMs}ms over the ${POLL_MEDIAN_BUDGET_MS}ms budget`);

        assert.strictEqual(receivedHeights.size, liveBlocks.length,
            `follower received ${receivedHeights.size} of ${liveBlocks.length} live blocks during the stampede`);
        for (const h of liveBlocks) {
            assert.ok(receivedHeights.has(h), `follower never received live block ${h}`);
        }
        assert.ok(stats.maxBroadcastGapMs < BROADCAST_GAP_BUDGET_MS,
            `longest gap between live block deliveries was ${stats.maxBroadcastGapMs}ms, over ${BROADCAST_GAP_BUDGET_MS}ms`);
    }

    it('derives a cap that always leaves the poller a connection', function () {
        // The clamp is the load-bearing half of the snapshot semaphore: no
        // operator setting may hand the poller's last connection to a snapshot
        // stampede.
        const builder = new SnapshotBuilder(null);
        const db = { dbType: 'indexer', connectionPoolParams: { connectionLimit: POOL_LIMIT } };

        delete process.env.MAX_CONCURRENT_SNAPSHOTS;
        assert.strictEqual(builder._snapshotCap(db), POOL_LIMIT - 2,
            'default cap should reserve one connection for the poller and one for short reads');

        process.env.MAX_CONCURRENT_SNAPSHOTS = '1000';
        assert.strictEqual(builder._snapshotCap(db), POOL_LIMIT - 1,
            'an over-large override must clamp to poolSize - 1, never the whole pool');

        process.env.MAX_CONCURRENT_SNAPSHOTS = '0';
        assert.strictEqual(builder._snapshotCap(db), 1,
            'a zero/negative override must clamp to 1, not deadlock every bootstrap');

        delete process.env.MAX_CONCURRENT_SNAPSHOTS;
        assert.strictEqual(poolSizing.resolvePoolSize('indexer'), 12,
            'indexer pool default changed; the cohort budget in this harness assumes 12');
    });

    it('5 concurrent bootstraps (cohort floor) are all served, poller unaffected', async function () {
        const result = await runStampede(5, '05-concurrent');
        assertLivenessHeld(result);

        // Five is under the cap, so nothing should be shed: shedding when there
        // is capacity would turn a healthy cohort into a retry storm.
        assert.strictEqual(result.stats.refused, 0,
            `5 concurrent bootstraps are within the cap of ${result.stats.cap}; none should be refused`);
        assert.strictEqual(result.stats.accepted, 5, 'all five bootstraps should complete');

        for (const res of result.accepted) {
            const snapshot = JSON.parse(zlib.gunzipSync(res.data).toString());
            assert.ok(snapshot.block_height >= STAMPEDE_BLOCKS,
                `served snapshot is at height ${snapshot.block_height}, below the seeded tip ${STAMPEDE_BLOCKS}`);
            assert.ok(snapshot.tables && Object.keys(snapshot.tables).length > 0,
                'a snapshot served under load must still carry table data');
        }
    });

    it('10 concurrent bootstraps (cohort ceiling) shed the excess with a retryable 503', async function () {
        // Cap of 3 against 10 arrivals: the shed path is exercised deterministically
        // rather than depending on how fast this machine streams a snapshot.
        const result = await runStampede(10, '10-concurrent', { maxConcurrentSnapshots: 3 });
        assertLivenessHeld(result);

        assert.strictEqual(result.stats.cap, 3, 'MAX_CONCURRENT_SNAPSHOTS=3 should be honoured');
        assert.ok(result.stats.refused >= 10 - result.stats.cap,
            `expected at least ${10 - result.stats.cap} refusals at a cap of ${result.stats.cap}, got ${result.stats.refused}`);

        // A refusal has to be actionable, not just a failure: the client's retry
        // logic keys on the code, and the backoff on Retry-After.
        for (const res of result.refused) {
            const body = JSON.parse(Buffer.from(res.data).toString());
            assert.strictEqual(body.code, 'SNAPSHOT_BUSY',
                `a shed bootstrap must say why: got ${JSON.stringify(body)}`);
            assert.ok(res.headers['retry-after'],
                'a shed bootstrap must carry Retry-After so the validator backs off rather than hammering');
        }

        assert.strictEqual(result.stats.snapshotsRejectedCounter, result.stats.refused,
            'the snapshots_rejected counter must match the refusals, or operators cannot see a stampede');
    });

    it('a 25-validator flag-day cohort cannot pin the pool', async function () {
        // Cohort C is the scenario in the ledger item: more validators than the
        // pool has connections, arriving together, against a server that must
        // keep serving everyone already on the network.
        const result = await runStampede(25, '25-concurrent');
        assertLivenessHeld(result);

        assert.ok(result.stats.refused >= 25 - result.stats.cap,
            `expected at least ${25 - result.stats.cap} refusals at a cap of ${result.stats.cap}, got ${result.stats.refused}`);
        assert.ok(result.stats.accepted >= 1,
            'shedding must not degenerate into refusing everyone; some validators have to make progress');
        assert.strictEqual(result.stats.accepted + result.stats.refused, 25,
            'every request must be answered exactly once');
    });
});
