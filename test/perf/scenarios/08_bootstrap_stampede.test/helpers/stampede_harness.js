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
const WebSocket = require('ws');
const { bootEnvironment, teardownEnvironment, resetAll,
        createServer, createGenerator, SERVER_PORT } = require('../../../setup/perf-setup');
const ReportGenerator = require('../../../setup/report-generator');
const SnapshotBuilder = require('../../../../../src/server/snapshot_builder');
const poolSizing      = require('../../../../../src/db/pool_sizing');
const { waitFor }     = require('../../../../e2e/helpers/waitFor');

const reporter = new ReportGenerator();
const allStats = {};

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
    const acquire = builder.acquireSnapshotSlot.bind(builder);
    const release = builder.releaseSnapshotSlot.bind(builder);

    builder.acquireSnapshotSlot = (db, res) => {
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
    builder.releaseSnapshotSlot = (db) => {
        observed.inflight--;
        return release(db);
    };
    return observed;
}

function startSnapshotRequests(server, concurrency) {
    const snapshotResults = [];
    const requests = [];
    for (let i = 0; i < concurrency; i++) {
        requests.push(
            axios.get(server.getUrl() + SNAPSHOT_PATH, {
                responseType: 'arraybuffer',
                timeout: 300000,
                decompress: false,
                httpAgent: STAMPEDE_AGENT,
                // Keep the 503 body available so a refusal can be inspected
                // the way a real client's retry logic would inspect it.
                validateStatus: () => true
            }).then((res) => snapshotResults.push({ i, status: res.status, headers: res.headers, data: res.data }))
              .catch((err) => snapshotResults.push({ i, status: 0, error: err.message }))
        );
    }
    return { snapshotResults, requests };
}

async function produceLiveBlocks(gen, server) {
    const pollLatencies = [];
    const liveBlocks = [];
    for (let n = 1; n <= LIVE_BLOCKS; n++) {
        const height = STAMPEDE_BLOCKS + n;
        await gen.seedBlockRange(height, height);
        const t = process.hrtime.bigint();
        await server.poll();
        pollLatencies.push(Number(process.hrtime.bigint() - t) / 1e6);
        liveBlocks.push(height);
        await new Promise(r => setTimeout(r, 50));
    }
    return { pollLatencies, liveBlocks };
}

async function drainFollower(follower, received, liveBlocks) {
    // Drain the frames the follower is still owed within a bounded window.
    // A frame that arrives late is then judged by the
    // broadcast-gap budget below rather than silently counted as a miss; on
    // timeout the completeness assertion reports exactly what never arrived.
    try {
        await waitFor(() => {
            const seen = new Set(received.map(r => r.blockIndex));
            return liveBlocks.every(h => seen.has(h));
        }, 15000, 100);
    } catch (e) { /* reported by the completeness assertion below */ }
    try { follower.close(); } catch (e) { /* already closed */ }
}

function calculateStats(ctx) {
    const { concurrency, cap, observed, snapshotResults, stampedeMs,
            pollLatencies, liveBlocks, received } = ctx;
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
        snapshotsRejectedCounter: ctx.server.snapshotBuilder.snapshotsRejected || 0,
        stampedeMs,
        pollMedianMs: +median(pollLatencies).toFixed(2),
        pollMaxMs: +Math.max(...pollLatencies, 0).toFixed(2),
        liveBlocksProduced: liveBlocks.length,
        liveBlocksDelivered: receivedHeights.size,
        maxBroadcastGapMs: maxGapMs
    };
    return { stats, accepted, refused, other, liveBlocks, receivedHeights };
}

function reportStats(result, label, reporter, allStats) {
    const { stats } = result;
    allStats[label] = stats;
    reporter.writeJson(stats, `08-${label}`);
    process.stdout.write(
        `      [${label}] cap=${stats.cap} peak=${stats.peakConcurrentStreams} ` +
        `200=${stats.accepted} 503=${stats.refused} ` +
        `poll med/max=${stats.pollMedianMs}/${stats.pollMaxMs}ms ` +
        `live ${stats.liveBlocksDelivered}/${stats.liveBlocksProduced} gap=${stats.maxBroadcastGapMs}ms\n`
    );
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

class StampedeHarness {
    constructor() {
        this.reporter = reporter;
        this.allStats = allStats;
        this.sourceDb = null;
        this.server = null;
    }

    async boot() {
        const env = await bootEnvironment();
        this.sourceDb = env.sourceDb;

        // Fidelity fix, and the reason it matters: SnapshotBuilder.snapshotCap
        // derives the cap from `db.connectionPoolParams.connectionLimit`, falling
        // back to poolSizing's per-dbType DEFAULT when the field is absent. The
        // e2e TestDatabase carries neither field, so without this the semaphore
        // would size itself from the indexer default (12) against a pool that
        // actually holds 10, i.e. it would permit MORE concurrent streams than
        // the pool has connections and the harness would "prove" a cap that
        // cannot protect anything. Production Database sets both; mirror it.
        this.savedDbType = this.sourceDb.dbType;
        this.savedPoolParams = this.sourceDb.connectionPoolParams;
        this.sourceDb.dbType = 'indexer';
        this.sourceDb.connectionPoolParams = { connectionLimit: POOL_LIMIT };

        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    }

    async teardown() {
        sinon.restore();
        // Guard: a failed before-hook leaves sourceDb unset, and an after-hook
        // that throws on it buries the real error behind a TypeError.
        if (this.sourceDb) {
            this.sourceDb.dbType = this.savedDbType;
            this.sourceDb.connectionPoolParams = this.savedPoolParams;
        }
        if (Object.keys(this.allStats).length > 0) {
            this.reporter.writeJson({ allStats: this.allStats }, '08-stampede-combined');
        }
        await teardownEnvironment();
    }

    async cleanServer() {
        if (this.server) { await this.server.stop(); this.server = null; }
        delete process.env.MAX_CONCURRENT_SNAPSHOTS;
    }

    async prepareStampede(concurrency, opts) {
        await resetAll();
        const gen = createGenerator(this.sourceDb);
        await gen.seedBulk(STAMPEDE_BLOCKS, STAMPEDE_ACTIONS);

        this.server = createServer(this.sourceDb, SERVER_PORT);
        this.server.config.WS_MAX_PER_IP = 20;
        if (opts.maxConcurrentSnapshots !== undefined) {
            process.env.MAX_CONCURRENT_SNAPSHOTS = String(opts.maxConcurrentSnapshots);
        }
        await this.server.start();

        const cap = this.server.snapshotBuilder.snapshotCap(this.sourceDb);
        const observed = instrumentSemaphore(this.server.snapshotBuilder);

        // The poller starts at the seeded tip, so /snapshot has nothing recorded
        // for the pre-seeded range until it catches up. Do that BEFORE the
        // stampede so the measured poll latencies are live-block cycles, not
        // backfill.
        this.server.poller.lastPolledBlock = 0;
        await this.server.pollUntil(STAMPEDE_BLOCKS);
        const follower = await connectFollower(this.server.getWsUrl());
        const received = [];
        follower.on('message', (data) => {
            try {
                const event = JSON.parse(data);
                if (event.type === 'block' && event.block_index) {
                    received.push({ blockIndex: Number(event.block_index), at: Date.now() });
                }
            } catch (e) { /* status frames and other event types are not under test */ }
        });
        return { concurrency, gen, server: this.server, cap, observed, follower, received };
    }

    /**
     * Drive `concurrency` simultaneous full-snapshot downloads against one
     * server while blocks keep being produced and polled, with a subscribed
     * follower listening.
     */
    async runStampede(concurrency, label, opts = {}) {
        const ctx = await this.prepareStampede(concurrency, opts);

        // The stampede: N validators asking for a full snapshot at once
        ctx.startedAt = Date.now();
        const { snapshotResults, requests } = startSnapshotRequests(ctx.server, concurrency);
        ctx.snapshotResults = snapshotResults;

        // Live traffic, concurrent with the stampede
        const liveLoop = produceLiveBlocks(ctx.gen, ctx.server);
        await Promise.all([...requests, liveLoop]);
        ctx.stampedeMs = Date.now() - ctx.startedAt;
        Object.assign(ctx, await liveLoop);
        await drainFollower(ctx.follower, ctx.received, ctx.liveBlocks);

        const result = calculateStats(ctx);
        reportStats(result, label, this.reporter, this.allStats);
        await this.server.stop();
        this.server = null;
        return result;
    }
}

function createStampedeHarness() {
    const instance = new StampedeHarness();
    return {
        boot: instance.boot.bind(instance),
        teardown: instance.teardown.bind(instance),
        cleanServer: instance.cleanServer.bind(instance),
        runStampede: instance.runStampede.bind(instance),
        assertLivenessHeld,
        SnapshotBuilder, poolSizing, POOL_LIMIT, STAMPEDE_BLOCKS
    };
}

function defineStampedeSuite(registerTests) {
    describe('08 Bootstrap Stampede (N-concurrent-bootstrap load)', function () {
        this.timeout(900000);
        const harness = createStampedeHarness();

        before(async function () {
            await harness.boot();
        });

        after(async function () {
            await harness.teardown();
        });

        afterEach(async function () {
            await harness.cleanServer();
        });

        registerTests(harness);
    });
}

module.exports = { defineStampedeSuite };
