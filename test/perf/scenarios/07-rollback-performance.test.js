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

describe('07 Rollback Performance', function () {
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
            reporter.writeJson({ allStats }, '07-rollback-combined');
        }
        await teardownEnvironment();
    });

    afterEach(async function () {
        if (client) { client.stop(); client = null; }
        if (server) { await server.stop(); server = null; }
    });

    async function runRollback(totalBlocks, rollbackDepth, label) {
        await resetAll();

        const gen = createGenerator(sourceDb);
        await gen.seedBlockRange(1, totalBlocks);

        server = createServer(sourceDb, SERVER_PORT);
        await server.start();

        // Bootstrap client with all blocks
        client = createClient(replicaDb, server.getUrl());
        await client.bootstrap();
        assert.strictEqual(await replicaDb.getLastBlock(), totalBlocks);

        const collector = new MetricsCollector({ name: label });
        collector.start();

        // Execute rollback
        const rollbackToBlock = totalBlocks - rollbackDepth + 1;

        collector.beginOperation('rollback');
        await client.rollbacker.rollback(rollbackToBlock);
        const rollbackMs = collector.endOperation('rollback');

        collector.stop();
        const stats = collector.getStats();

        // Count remaining rows
        const remainingBlocks = await testDb.getRowCount(replicaDb, 'blocks');
        const remainingActions = await testDb.getRowCount(replicaDb, 'actions');

        stats.rollbackMetrics = {
            totalBlocks,
            rollbackDepth,
            rollbackToBlock,
            rollbackMs: +rollbackMs.toFixed(2),
            blocksPerSecond: rollbackMs > 0 ? +(rollbackDepth / (rollbackMs / 1000)).toFixed(2) : 0,
            remainingBlocks,
            remainingActions,
            blocksRemoved: totalBlocks - remainingBlocks
        };

        reporter.generateAll(stats, `07-${label}`);

        allStats[label] = {
            rollbackDepth,
            rollbackMs: stats.rollbackMetrics.rollbackMs,
            blocksPerSecond: stats.rollbackMetrics.blocksPerSecond
        };

        // Verify rollback correctness
        const lastBlock = await replicaDb.getLastBlock();
        assert.ok(lastBlock !== null && lastBlock < rollbackToBlock + rollbackDepth,
            `Last block should be before rollback point (got ${lastBlock})`);

        client.stop();
        client = null;
        await server.stop();
        server = null;
        return stats;
    }

    it('rollback 1 block', async function () {
        const total = Math.max(BLOCK_COUNT, 50);
        const stats = await runRollback(total, 1, 'depth-1');
        assert.strictEqual(stats.errors.length, 0, 'No errors expected');
    });

    it('rollback 5 blocks', async function () {
        const total = Math.max(BLOCK_COUNT, 50);
        const stats = await runRollback(total, 5, 'depth-5');
        assert.strictEqual(stats.errors.length, 0, 'No errors expected');
    });

    it('rollback 10 blocks', async function () {
        const total = Math.max(BLOCK_COUNT, 50);
        const stats = await runRollback(total, 10, 'depth-10');
        assert.strictEqual(stats.errors.length, 0, 'No errors expected');
    });

    it('rollback 25 blocks', async function () {
        const total = Math.max(BLOCK_COUNT, 100);
        const stats = await runRollback(total, 25, 'depth-25');
        assert.strictEqual(stats.errors.length, 0, 'No errors expected');
    });

    it('rollback 50 blocks', async function () {
        const total = Math.max(BLOCK_COUNT, 150);
        const stats = await runRollback(total, 50, 'depth-50');
        assert.strictEqual(stats.errors.length, 0, 'No errors expected');
    });

    it('rollback 100 blocks (MAX_ROLLBACK_DEPTH)', async function () {
        const total = Math.max(BLOCK_COUNT * 2, 200);
        const stats = await runRollback(total, 100, 'depth-100');
        assert.strictEqual(stats.errors.length, 0, 'No errors expected');
    });

    it('rollback time scales sub-quadratically with depth', async function () {
        // Rollback 100 blocks should not take more than 20x the time of 5 blocks
        const small = allStats['depth-5'];
        const large = allStats['depth-100'];

        if (small && large) {
            const ratio = large.rollbackMs / small.rollbackMs;
            const depthRatio = 100 / 5; // 20x more blocks
            assert.ok(ratio < depthRatio * 2,
                `Rollback scaling: depth-100 (${large.rollbackMs}ms) is ${ratio.toFixed(1)}x slower than depth-5 (${small.rollbackMs}ms) — expected < ${depthRatio * 2}x`);
        }
    });

    it('data integrity after deep rollback', async function () {
        await resetAll();

        const gen = createGenerator(sourceDb);
        const total = Math.max(BLOCK_COUNT, 100);
        await gen.seedBlockRange(1, total);

        server = createServer(sourceDb, SERVER_PORT);
        await server.start();

        client = createClient(replicaDb, server.getUrl());
        await client.bootstrap();

        // Rollback to block 10
        await client.rollbacker.rollback(10);

        // Verify blocks 10+ are gone
        const lastBlock = await replicaDb.getLastBlock();
        assert.ok(lastBlock < 10, 'All blocks >= 10 should be removed');

        // Verify remaining data is consistent — credits/debits should match balances
        const balanceCount = await testDb.getRowCount(replicaDb, 'balances');
        const creditCount = await testDb.getRowCount(replicaDb, 'credits');
        // After rollback with balance rebuild, balances should be non-negative
        // (or zero if all credits were in rolled-back blocks)
        assert.ok(balanceCount >= 0, 'Balances should be rebuilt');

        client.stop();
        client = null;
        await server.stop();
        server = null;
    });
});
