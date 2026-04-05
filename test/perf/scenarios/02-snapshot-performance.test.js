'use strict';

const assert = require('assert');
const sinon  = require('sinon');
const axios  = require('axios');
const zlib   = require('zlib');
const { bootEnvironment, teardownEnvironment, resetAll,
        createServer, createGenerator, SERVER_PORT } = require('../setup/perf-setup');
const MetricsCollector = require('../setup/metrics-collector');
const ReportGenerator  = require('../setup/report-generator');

const BLOCK_COUNT = parseInt(process.env.PERF_BLOCK_COUNT || '100');

describe('02 Snapshot Performance', function () {
    this.timeout(600000);

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

    describe('Full snapshot export', function () {

        async function runFullSnapshot(label, blockCount, actionsPerBlock) {
            await resetAll();

            const gen = createGenerator(sourceDb);
            if (actionsPerBlock > 1) {
                await gen.seedBulk(blockCount, actionsPerBlock);
            } else {
                await gen.seedBlockRange(1, blockCount);
            }

            server = createServer(sourceDb, SERVER_PORT);
            await server.start();

            const collector = new MetricsCollector({ name: label });
            collector.start();

            // Measure snapshot download
            collector.beginOperation('fullSnapshot');
            const res = await axios.get(
                server.getUrl() + '/snapshot/bitcoin/mainnet',
                { responseType: 'arraybuffer', timeout: 300000, decompress: false }
            );
            const downloadMs = collector.endOperation('fullSnapshot');

            // Measure decompression
            collector.beginOperation('decompress');
            const decompressed = zlib.gunzipSync(res.data);
            const decompressMs = collector.endOperation('decompress');

            // Measure JSON parse
            collector.beginOperation('parse');
            const snapshot = JSON.parse(decompressed.toString());
            const parseMs = collector.endOperation('parse');

            collector.stop();
            const stats = collector.getStats();

            // Add snapshot-specific metrics
            stats.snapshotMetrics = {
                compressedBytes: res.data.length,
                uncompressedBytes: decompressed.length,
                compressionRatio: +(decompressed.length / res.data.length).toFixed(2),
                blockHeight: snapshot.block_height,
                tableCount: Object.keys(snapshot.tables || {}).length,
                downloadMs: +downloadMs.toFixed(2),
                decompressMs: +decompressMs.toFixed(2),
                parseMs: +parseMs.toFixed(2),
                throughputMBps: +(decompressed.length / (downloadMs / 1000) / 1048576).toFixed(2)
            };

            reporter.generateAll(stats, `02-full-${label}`);
            await server.stop();
            server = null;
            return { stats, snapshot };
        }

        it('100 blocks (baseline)', async function () {
            const { stats, snapshot } = await runFullSnapshot('100blk', Math.min(BLOCK_COUNT, 100), 1);
            assert.ok(snapshot.block_height >= 100, 'Snapshot should contain blocks');
            assert.ok(snapshot.tables.blocks.length >= 100, 'Should have all blocks');
            assert.strictEqual(stats.errors.length, 0, 'No errors expected');
        });

        it('500 blocks', async function () {
            const count = Math.min(BLOCK_COUNT * 5, 500);
            const { stats, snapshot } = await runFullSnapshot('500blk', count, 1);
            assert.ok(snapshot.block_height >= count, 'Snapshot should contain all blocks');
            assert.strictEqual(stats.errors.length, 0, 'No errors expected');
        });

        it('100 blocks with 10 actions each', async function () {
            const count = Math.min(BLOCK_COUNT, 100);
            const { stats, snapshot } = await runFullSnapshot('100blk-10act', count, 10);
            assert.ok(snapshot.block_height >= count, 'Snapshot should contain blocks');
            assert.strictEqual(stats.errors.length, 0, 'No errors expected');
        });
    });

    describe('Incremental snapshot export', function () {

        async function runIncrementalSnapshot(label, totalBlocks, sinceBlock) {
            await resetAll();

            const gen = createGenerator(sourceDb);
            await gen.seedBlockRange(1, totalBlocks);

            server = createServer(sourceDb, SERVER_PORT);
            await server.start();

            const collector = new MetricsCollector({ name: label });
            collector.start();

            collector.beginOperation('incrementalSnapshot');
            const res = await axios.get(
                server.getUrl() + '/snapshot/bitcoin/mainnet/since/' + sinceBlock,
                { responseType: 'arraybuffer', timeout: 300000, decompress: false }
            );
            const downloadMs = collector.endOperation('incrementalSnapshot');

            collector.beginOperation('decompress');
            const decompressed = zlib.gunzipSync(res.data);
            const decompressMs = collector.endOperation('decompress');

            collector.beginOperation('parse');
            const snapshot = JSON.parse(decompressed.toString());
            const parseMs = collector.endOperation('parse');

            collector.stop();
            const stats = collector.getStats();

            stats.snapshotMetrics = {
                compressedBytes: res.data.length,
                uncompressedBytes: decompressed.length,
                compressionRatio: +(decompressed.length / res.data.length).toFixed(2),
                sinceBlock,
                totalBlocks,
                deltaBlocks: totalBlocks - sinceBlock,
                downloadMs: +downloadMs.toFixed(2),
                decompressMs: +decompressMs.toFixed(2),
                parseMs: +parseMs.toFixed(2)
            };

            reporter.generateAll(stats, `02-incr-${label}`);
            await server.stop();
            server = null;
            return { stats, snapshot };
        }

        it('incremental from 90% (small delta)', async function () {
            const total = Math.min(BLOCK_COUNT * 5, 500);
            const since = Math.floor(total * 0.9);
            const { stats, snapshot } = await runIncrementalSnapshot(
                `${total}blk-from-${since}`, total, since
            );
            assert.ok(snapshot.block_height >= total, 'Should report full height');
            assert.strictEqual(stats.errors.length, 0, 'No errors expected');
        });

        it('incremental from 50% (medium delta)', async function () {
            const total = Math.min(BLOCK_COUNT * 5, 500);
            const since = Math.floor(total * 0.5);
            const { stats, snapshot } = await runIncrementalSnapshot(
                `${total}blk-from-${since}`, total, since
            );
            assert.ok(snapshot.block_height >= total, 'Should report full height');
            assert.strictEqual(stats.errors.length, 0, 'No errors expected');
        });

        it('incremental from 10% (large delta)', async function () {
            const total = Math.min(BLOCK_COUNT * 5, 500);
            const since = Math.floor(total * 0.1);
            const { stats, snapshot } = await runIncrementalSnapshot(
                `${total}blk-from-${since}`, total, since
            );
            assert.ok(snapshot.block_height >= total, 'Should report full height');
            assert.strictEqual(stats.errors.length, 0, 'No errors expected');
        });
    });
});
