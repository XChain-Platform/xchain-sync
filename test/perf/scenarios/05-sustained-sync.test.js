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
const { waitForReplicaBlock } = require('../../e2e/helpers/waitFor');
const MetricsCollector = require('../setup/metrics-collector');
const ReportGenerator  = require('../setup/report-generator');

const DURATION_MS       = parseInt(process.env.PERF_SUSTAINED_MS || '60000');
const BLOCKS_PER_BATCH  = 50;

describe('05 Sustained Sync', function () {
    this.timeout(0); // no timeout — duration controlled by PERF_SUSTAINED_MS

    const reporter = new ReportGenerator();
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
        await teardownEnvironment();
    });

    afterEach(async function () {
        if (client) { client.stop(); client = null; }
        if (server) { await server.stop(); server = null; }
    });

    it(`processes blocks continuously for ${DURATION_MS}ms without degradation`, async function () {
        await resetAll();

        const gen = createGenerator(sourceDb);

        // Seed initial blocks for bootstrap
        await gen.seedBlockRange(1, 10);

        server = createServer(sourceDb, SERVER_PORT);
        await server.start();

        client = createClient(replicaDb, server.getUrl());
        await client.start();
        await waitForReplicaBlock(replicaDb, 10);

        const collector = new MetricsCollector({ name: 'sustained-sync', warmupBlocks: 10 });
        collector.start();

        const endTime = Date.now() + DURATION_MS;
        let currentBlock = 11;
        let batchNumber = 0;

        while (Date.now() < endTime) {
            const batchEnd = currentBlock + BLOCKS_PER_BATCH - 1;

            // Seed a batch of blocks
            collector.beginOperation('seedBatch');
            await gen.seedBlockRange(currentBlock, batchEnd);
            collector.endOperation('seedBatch');

            // Wait for client to sync all blocks
            collector.beginBlock(currentBlock);
            const t = process.hrtime.bigint();

            // Poll server to detect new blocks
            for (let i = 0; i < BLOCKS_PER_BATCH; i++) {
                await server.poll();
            }

            // Wait for replica to catch up
            try {
                await waitForReplicaBlock(replicaDb, batchEnd, 30000);
            } catch (e) {
                collector.recordError(batchEnd, e);
                break;
            }

            const batchMs = Number(process.hrtime.bigint() - t) / 1e6;
            collector.endBlock(batchEnd, {
                syncBatch: batchMs,
                blocksInBatch: BLOCKS_PER_BATCH
            });

            currentBlock = batchEnd + 1;
            batchNumber++;
        }

        collector.stop();
        const stats = collector.getStats();
        reporter.generateAll(stats, '05-sustained-sync');

        // Must have processed enough batches for meaningful analysis
        const timings = stats._rawBlockTimings;
        assert.ok(timings.length >= 5, `Expected >= 5 batch measurements, got ${timings.length}`);
        assert.strictEqual(stats.errors.length, 0, 'No sync errors expected');

        // Degradation check: second-half p99 should not be >3x first-half p99
        if (timings.length >= 6) {
            const half = Math.floor(timings.length / 2);
            const firstHalf = [...timings.slice(0, half)].sort((a, b) => a - b);
            const secondHalf = [...timings.slice(half)].sort((a, b) => a - b);
            const p99First = firstHalf[Math.ceil(0.99 * firstHalf.length) - 1];
            const p99Second = secondHalf[Math.ceil(0.99 * secondHalf.length) - 1];
            assert.ok(p99Second < p99First * 3,
                `Degradation: second-half p99 (${p99Second.toFixed(1)}ms) > 3x first-half p99 (${p99First.toFixed(1)}ms)`);
        }

        // Memory check: heap growth should be bounded
        const heapGrowth = parseFloat(stats.memory.heapGrowthMb);
        assert.ok(heapGrowth < 50,
            `Heap grew by ${heapGrowth}MB during sustained sync — possible memory leak`);

        // Verify replica is consistent
        const replicaBlock = await replicaDb.getLastBlock();
        assert.ok(replicaBlock >= currentBlock - BLOCKS_PER_BATCH,
            'Replica should have kept up with source');

        client.stop();
        client = null;
        await server.stop();
        server = null;
    });
});
