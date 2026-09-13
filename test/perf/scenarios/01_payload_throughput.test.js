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

const assert = require('assert');
const sinon  = require('sinon');
const { bootEnvironment, teardownEnvironment, resetAll,
        createServer, createGenerator, SERVER_PORT } = require('../setup/perf-setup');
const MetricsCollector = require('../setup/metrics-collector');
const ReportGenerator  = require('../setup/report-generator');

const BLOCK_COUNT = parseInt(process.env.PERF_BLOCK_COUNT || '100');

describe('01 Payload Throughput', function () {
    this.timeout(300000);

    const reporter = new ReportGenerator();
    let sourceDb, server;

    before(async function () {
        const env = await bootEnvironment();
        sourceDb = env.sourceDb;
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });

    after(async function () {
        sinon.restore();
        await teardownEnvironment();
    });

    afterEach(async function () {
        if (server) { await server.stop(); server = null; }
    });

    async function runScenario(label, blockCount, actionsPerBlock) {
        await resetAll();

        const gen = createGenerator(sourceDb);
        if (actionsPerBlock > 1) {
            await gen.seedBulk(blockCount, actionsPerBlock);
        } else {
            await gen.seedBlockRange(1, blockCount);
        }

        server = createServer(sourceDb, SERVER_PORT);
        // Override poll interval for immediate processing
        server.config.BLOCK_POLL_INTERVAL = 50;
        await server.start();

        // Reset poller to start from block 0 so it will process all blocks
        server.poller.lastPolledBlock = 0;

        const collector = new MetricsCollector({ name: label, warmupBlocks: 5 });
        collector.start();

        // Process all blocks via manual poll cycles
        let processed = 0;
        while (processed < blockCount) {
            let beforeBlock = await sourceDb.getLastBlock();
            let startBlock = server.poller.lastPolledBlock;

            collector.beginBlock(startBlock + 1);
            const t = process.hrtime.bigint();

            await server.poll();

            const pollMs = Number(process.hrtime.bigint() - t) / 1e6;
            let newBlock = server.poller.lastPolledBlock;
            let blocksThisPoll = newBlock - startBlock;

            if (blocksThisPoll > 0) {
                collector.endBlock(newBlock, { poll: pollMs });
                processed += blocksThisPoll;
            } else {
                // No progress, poll again
                collector.endBlock(startBlock, { poll: pollMs });
            }
        }

        collector.stop();
        const stats = collector.getStats();
        reporter.generateAll(stats, `01-${label}`);
        await server.stop();
        server = null;
        return stats;
    }

    it('light blocks (1 action/block)', async function () {
        const stats = await runScenario('light-1action', BLOCK_COUNT, 1);
        assert.ok(stats.throughput.blocksPerSecond > 0, 'Should process blocks');
        assert.strictEqual(stats.errors.length, 0, 'No errors expected');
    });

    it('medium blocks (10 actions/block)', async function () {
        const stats = await runScenario('medium-10action', BLOCK_COUNT, 10);
        assert.ok(stats.throughput.blocksPerSecond > 0, 'Should process blocks');
        assert.strictEqual(stats.errors.length, 0, 'No errors expected');
    });

    it('heavy blocks (50 actions/block)', async function () {
        const stats = await runScenario('heavy-50action', BLOCK_COUNT, 50);
        assert.ok(stats.throughput.blocksPerSecond > 0, 'Should process blocks');
        assert.strictEqual(stats.errors.length, 0, 'No errors expected');
    });

    it('dense blocks (200 actions/block)', async function () {
        const blockCount = Math.min(BLOCK_COUNT, 50); // Fewer blocks for dense scenario
        const stats = await runScenario('dense-200action', blockCount, 200);
        assert.ok(stats.throughput.blocksPerSecond > 0, 'Should process blocks');
        assert.strictEqual(stats.errors.length, 0, 'No errors expected');
    });
});
