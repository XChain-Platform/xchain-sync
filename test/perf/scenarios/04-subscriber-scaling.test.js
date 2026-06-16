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

const assert    = require('assert');
const sinon     = require('sinon');
const WebSocket = require('ws');
const { bootEnvironment, teardownEnvironment, resetAll,
        createServer, createGenerator, SERVER_PORT } = require('../setup/perf-setup');
const MetricsCollector = require('../setup/metrics-collector');
const ReportGenerator  = require('../setup/report-generator');

const BLOCK_COUNT = parseInt(process.env.PERF_BLOCK_COUNT || '50');

describe('04 Subscriber Scaling', function () {
    this.timeout(300000);

    const reporter = new ReportGenerator();
    const allStats = {};
    let sourceDb, server;

    before(async function () {
        const env = await bootEnvironment();
        sourceDb = env.sourceDb;
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });

    after(async function () {
        sinon.restore();
        if (Object.keys(allStats).length > 0) {
            reporter.writeJson({ allStats }, '04-scaling-combined');
        }
        await teardownEnvironment();
    });

    afterEach(async function () {
        if (server) { await server.stop(); server = null; }
    });

    function connectSubscriber(wsUrl) {
        return new Promise((resolve, reject) => {
            const ws = new WebSocket(wsUrl + '/subscribe/indexer/bitcoin/mainnet');
            ws.on('open', () => resolve(ws));
            ws.on('error', reject);
            // Timeout after 5s
            setTimeout(() => reject(new Error('WS connect timeout')), 5000);
        });
    }

    async function runScalingPoint(subscriberCount, label) {
        await resetAll();

        const gen = createGenerator(sourceDb);
        // Seed initial blocks
        await gen.seedBlockRange(1, 10);

        server = createServer(sourceDb, SERVER_PORT);
        // Allow many subscribers per IP for testing
        server.config.WS_MAX_PER_IP = subscriberCount + 10;
        await server.start();

        // Connect all subscribers
        const wsUrl = server.getWsUrl();
        const subscribers = [];
        const receivedBlocks = new Map(); // subscriberIndex → Set of blockIndices

        for (let i = 0; i < subscriberCount; i++) {
            const ws = await connectSubscriber(wsUrl);
            receivedBlocks.set(i, new Set());
            ws.on('message', (data) => {
                try {
                    const event = JSON.parse(data);
                    if (event.type === 'block' && event.block_index) {
                        receivedBlocks.get(i).add(event.block_index);
                    }
                } catch (e) {}
            });
            subscribers.push(ws);
        }

        // Seed additional blocks and measure broadcast performance
        const newBlockStart = 11;
        const newBlockEnd = 10 + BLOCK_COUNT;
        await gen.seedBlockRange(newBlockStart, newBlockEnd);

        const collector = new MetricsCollector({ name: label });
        collector.start();

        // Poll all new blocks through the server
        collector.beginOperation('broadcastAll');
        for (let i = 0; i < BLOCK_COUNT; i++) {
            const t = process.hrtime.bigint();
            await server.poll();
            const pollMs = Number(process.hrtime.bigint() - t) / 1e6;
            collector.beginBlock(newBlockStart + i);
            collector.endBlock(newBlockStart + i, { pollAndBroadcast: pollMs });
        }
        const broadcastMs = collector.endOperation('broadcastAll');

        // Wait for subscribers to receive messages (allow up to 5s)
        await new Promise(r => setTimeout(r, 2000));

        collector.stop();
        const stats = collector.getStats();

        // Count how many subscribers received all blocks
        let fullReceiveCount = 0;
        let totalReceived = 0;
        let backpressureDrops = 0;

        for (let i = 0; i < subscriberCount; i++) {
            const received = receivedBlocks.get(i).size;
            totalReceived += received;
            if (received >= BLOCK_COUNT) fullReceiveCount++;
        }

        // Count disconnected subscribers (backpressure)
        for (const ws of subscribers) {
            if (ws.readyState !== WebSocket.OPEN) backpressureDrops++;
        }

        stats.subscriberMetrics = {
            subscriberCount,
            blocksSeeded: BLOCK_COUNT,
            fullReceiveCount,
            totalBlocksReceived: totalReceived,
            avgBlocksPerSubscriber: subscriberCount > 0 ? +(totalReceived / subscriberCount).toFixed(1) : 0,
            backpressureDrops,
            totalBroadcastMs: +broadcastMs.toFixed(2),
            avgBroadcastPerBlockMs: BLOCK_COUNT > 0 ? +(broadcastMs / BLOCK_COUNT).toFixed(2) : 0
        };

        reporter.generateAll(stats, `04-${label}`);

        allStats[label] = {
            subscriberCount,
            avgBroadcastMs: stats.subscriberMetrics.avgBroadcastPerBlockMs,
            fullReceiveRate: subscriberCount > 0 ? +(fullReceiveCount / subscriberCount).toFixed(2) : 0,
            backpressureDrops,
            blocksPerSecond: stats.throughput.blocksPerSecond
        };

        // Close all subscribers
        for (const ws of subscribers) {
            try { ws.close(); } catch (e) {}
        }

        await server.stop();
        server = null;
        return stats;
    }

    it('1 subscriber', async function () {
        const stats = await runScalingPoint(1, '1-sub');
        assert.strictEqual(stats.errors.length, 0, 'No errors expected');
        assert.ok(stats.subscriberMetrics.fullReceiveCount >= 1,
            'Subscriber should receive all blocks');
    });

    it('5 subscribers', async function () {
        const stats = await runScalingPoint(5, '5-sub');
        assert.strictEqual(stats.errors.length, 0, 'No errors expected');
    });

    it('10 subscribers', async function () {
        const stats = await runScalingPoint(10, '10-sub');
        assert.strictEqual(stats.errors.length, 0, 'No errors expected');
    });

    it('25 subscribers', async function () {
        const stats = await runScalingPoint(25, '25-sub');
        assert.strictEqual(stats.errors.length, 0, 'No errors expected');
    });

    it('50 subscribers', async function () {
        const stats = await runScalingPoint(50, '50-sub');
        assert.strictEqual(stats.errors.length, 0, 'No errors expected');
        // At 50 subscribers, broadcast overhead should still be manageable
        assert.ok(stats.subscriberMetrics.avgBroadcastPerBlockMs < 5000,
            'Broadcast should not take > 5s per block at 50 subscribers');
    });
});
