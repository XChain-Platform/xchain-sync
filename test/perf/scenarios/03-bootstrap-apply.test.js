'use strict';

const assert = require('assert');
const sinon  = require('sinon');
const { bootEnvironment, teardownEnvironment, resetAll,
        createServer, createClient, createGenerator,
        SERVER_PORT, testDb } = require('../setup/perf-setup');
const MetricsCollector = require('../setup/metrics-collector');
const ReportGenerator  = require('../setup/report-generator');

const BLOCK_COUNT = parseInt(process.env.PERF_BLOCK_COUNT || '100');

describe('03 Bootstrap Apply', function () {
    this.timeout(600000);

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

    async function runBootstrap(label, blockCount, actionsPerBlock) {
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

        // Measure full bootstrap
        collector.beginOperation('bootstrap');
        client = createClient(replicaDb, server.getUrl());
        await client.bootstrap();
        const bootstrapMs = collector.endOperation('bootstrap');

        collector.stop();
        const stats = collector.getStats();

        // Compute row counts for throughput calculation
        let totalRows = 0;
        const tables = await testDb.getTables(replicaDb);
        for (const table of tables) {
            totalRows += await testDb.getRowCount(replicaDb, table);
        }

        stats.bootstrapMetrics = {
            blockCount,
            actionsPerBlock,
            totalRowsApplied: totalRows,
            bootstrapMs: +bootstrapMs.toFixed(2),
            rowsPerSecond: bootstrapMs > 0 ? +(totalRows / (bootstrapMs / 1000)).toFixed(0) : 0,
            tablesReplicated: tables.length
        };

        reporter.generateAll(stats, `03-${label}`);

        client.stop();
        client = null;
        await server.stop();
        server = null;
        return stats;
    }

    it('bootstrap 100 blocks (baseline)', async function () {
        const count = Math.min(BLOCK_COUNT, 100);
        const stats = await runBootstrap('100blk-baseline', count, 1);

        assert.strictEqual(await replicaDb.getLastBlock(), count);
        assert.strictEqual(stats.errors.length, 0, 'No errors expected');
        assert.ok(stats.bootstrapMetrics.rowsPerSecond > 0, 'Should report positive throughput');
    });

    it('bootstrap 500 blocks', async function () {
        const count = Math.min(BLOCK_COUNT * 5, 500);
        const stats = await runBootstrap('500blk', count, 1);

        assert.strictEqual(await replicaDb.getLastBlock(), count);
        assert.strictEqual(stats.errors.length, 0, 'No errors expected');
    });

    it('bootstrap 100 blocks with 10 actions each', async function () {
        const count = Math.min(BLOCK_COUNT, 100);
        const stats = await runBootstrap('100blk-10act', count, 10);

        assert.strictEqual(await replicaDb.getLastBlock(), count);
        assert.strictEqual(stats.errors.length, 0, 'No errors expected');
    });

    it('bootstrap 50 blocks with 50 actions each (heavy)', async function () {
        const count = Math.min(BLOCK_COUNT, 50);
        const stats = await runBootstrap('50blk-50act', count, 50);

        assert.strictEqual(await replicaDb.getLastBlock(), count);
        assert.strictEqual(stats.errors.length, 0, 'No errors expected');
    });

    it('data integrity after bootstrap', async function () {
        await resetAll();

        const gen = createGenerator(sourceDb);
        const count = Math.min(BLOCK_COUNT, 100);
        await gen.seedBlockRange(1, count);

        server = createServer(sourceDb, SERVER_PORT);
        await server.start();

        client = createClient(replicaDb, server.getUrl());
        await client.bootstrap();

        // Verify row counts match across all tables
        const tables = await testDb.getTables(sourceDb);
        let mismatches = [];
        for (const table of tables) {
            const sourceCount = await testDb.getRowCount(sourceDb, table);
            const replicaCount = await testDb.getRowCount(replicaDb, table);
            if (sourceCount !== replicaCount) {
                mismatches.push({ table, sourceCount, replicaCount });
            }
        }
        assert.strictEqual(mismatches.length, 0,
            'Row count mismatches: ' + mismatches.map(m => `${m.table}: src=${m.sourceCount} rep=${m.replicaCount}`).join(', '));

        client.stop();
        client = null;
        await server.stop();
        server = null;
    });
});
