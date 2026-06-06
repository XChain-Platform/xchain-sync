'use strict';

// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available —
// contact legal@dankest.llc.

const assert = require('assert');
const sinon  = require('sinon');
const { bootEnvironment, teardownEnvironment, resetAll,
        createServer, createClient, createGenerator,
        SERVER_PORT, testDb } = require('../setup/perf-setup');
const MetricsCollector = require('../setup/metrics-collector');
const ReportGenerator  = require('../setup/report-generator');

const BLOCK_COUNT = parseInt(process.env.PERF_BLOCK_COUNT || '100');

describe('06 Incremental Catch-Up', function () {
    this.timeout(600000);

    const reporter = new ReportGenerator();
    const allStats = {};
    let sourceDb, replicaDb, server, client;

    before(async function () {
        const env = await bootEnvironment();
        sourceDb  = env.sourceDb;
        replicaDb = env.replicaDb;
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });

    after(async function () {
        sinon.restore();
        if (Object.keys(allStats).length > 0) {
            reporter.writeJson({ allStats }, '06-catchup-combined');
        }
        await teardownEnvironment();
    });

    afterEach(async function () {
        if (client) { client.stop(); client = null; }
        if (server) { await server.stop(); server = null; }
    });

    async function runCatchUp(gapSize, label) {
        await resetAll();

        const gen = createGenerator(sourceDb);
        const baseBlocks = 50; // Bootstrap to this point first
        const totalBlocks = baseBlocks + gapSize;

        // Seed all blocks into source
        await gen.seedBlockRange(1, totalBlocks);

        server = createServer(sourceDb, SERVER_PORT);
        await server.start();

        // Bootstrap client to baseBlocks only
        // First, temporarily reduce source to baseBlocks by remembering total
        // We bootstrap from snapshot which will include all blocks, so instead:
        // Seed base blocks, bootstrap, then seed remaining
        await resetAll();
        await gen.seedBlockRange(1, baseBlocks);

        server = createServer(sourceDb, SERVER_PORT);
        await server.start();

        client = createClient(replicaDb, server.getUrl());
        await client.bootstrap();
        assert.strictEqual(await replicaDb.getLastBlock(), baseBlocks);

        // Now add the gap blocks to source
        await gen.seedBlockRange(baseBlocks + 1, totalBlocks);

        // Restart server to pick up new blocks
        await server.stop();
        server = createServer(sourceDb, SERVER_PORT);
        await server.start();

        const collector = new MetricsCollector({ name: label });
        collector.start();

        // Measure incremental catch-up
        collector.beginOperation('catchUp');
        await client.incrementalCatchUp(baseBlocks + 1);
        const catchUpMs = collector.endOperation('catchUp');

        collector.stop();
        const stats = collector.getStats();

        stats.catchUpMetrics = {
            gapSize,
            baseBlocks,
            totalBlocks,
            catchUpMs: +catchUpMs.toFixed(2),
            blocksPerSecond: catchUpMs > 0 ? +(gapSize / (catchUpMs / 1000)).toFixed(2) : 0
        };

        reporter.generateAll(stats, `06-${label}`);

        allStats[label] = {
            gapSize,
            catchUpMs: stats.catchUpMetrics.catchUpMs,
            blocksPerSecond: stats.catchUpMetrics.blocksPerSecond
        };

        // Verify catch-up completed
        assert.strictEqual(await replicaDb.getLastBlock(), totalBlocks,
            `Replica should be at block ${totalBlocks} after catch-up`);

        // Verify data integrity
        const sourceBlockCount = await testDb.getRowCount(sourceDb, 'blocks');
        const replicaBlockCount = await testDb.getRowCount(replicaDb, 'blocks');
        assert.strictEqual(replicaBlockCount, sourceBlockCount,
            'Block count should match after catch-up');

        client.stop();
        client = null;
        await server.stop();
        server = null;
        return stats;
    }

    it('10-block gap', async function () {
        const stats = await runCatchUp(10, 'gap-10');
        assert.strictEqual(stats.errors.length, 0, 'No errors expected');
    });

    it('50-block gap', async function () {
        const stats = await runCatchUp(50, 'gap-50');
        assert.strictEqual(stats.errors.length, 0, 'No errors expected');
    });

    it('100-block gap', async function () {
        const stats = await runCatchUp(Math.min(BLOCK_COUNT, 100), 'gap-100');
        assert.strictEqual(stats.errors.length, 0, 'No errors expected');
    });

    it('500-block gap', async function () {
        const stats = await runCatchUp(Math.min(BLOCK_COUNT * 5, 500), 'gap-500');
        assert.strictEqual(stats.errors.length, 0, 'No errors expected');
    });

    it('catch-up rate scales linearly', async function () {
        // Compare blocks/second across gap sizes — should not degrade significantly
        const small = allStats['gap-10'];
        const large = allStats['gap-500'] || allStats['gap-100'];

        if (small && large) {
            // Large gap should not be slower per-block than small gap by more than 5x
            assert.ok(large.blocksPerSecond > small.blocksPerSecond / 5,
                `Large gap (${large.blocksPerSecond} blk/s) is much slower than small gap (${small.blocksPerSecond} blk/s)`);
        }
    });
});
