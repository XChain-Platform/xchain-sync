// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available —
// contact legal@dankest.llc.

const assert        = require('assert');
const sinon         = require('sinon');
const axios         = require('axios');
const zlib          = require('zlib');
const setup         = require('./helpers/setup');
const testDb        = require('./helpers/testDb');
const fixtures      = require('./helpers/fixtures');
const ServerProcess = require('./helpers/serverProcess');
const ClientProcess = require('./helpers/clientProcess');
const { waitForReplicaBlock } = require('./helpers/waitFor');

const SERVER_PORT = 29600;

describe('E2E: Large Data Volume', function() {

    let sourceDb, replicaDb, server, client;

    before(async function() {
        await setup.globalSetup();
        sourceDb  = setup.getSourceDb();
        replicaDb = setup.getReplicaDb();

        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });

    after(async function() {
        sinon.restore();
        if (client) client.stop();
        if (server) await server.stop();
        await setup.globalTeardown();
    });

    beforeEach(async function() {
        if (client) { client.stop(); client = null; }
        if (server) { await server.stop(); server = null; }
        await setup.resetDatabases();
    });

    describe('6.1 Bootstrap with 500 blocks', function() {
        it('bootstraps large dataset within timeout', async function() {
            this.timeout(120000);

            // Seed 500 blocks
            await fixtures.seedBlocks(sourceDb, 1, 500);

            server = new ServerProcess(sourceDb, SERVER_PORT);
            await server.start();

            let startTime = Date.now();

            client = new ClientProcess(replicaDb, server.getUrl());
            await client.bootstrap();

            let elapsed = Date.now() - startTime;

            assert.strictEqual(await replicaDb.getLastBlock(), 500);
            assert.strictEqual(await testDb.getRowCount(replicaDb, 'blocks'), 500);

            let sourceCredits  = await testDb.getRowCount(sourceDb, 'credits');
            let replicaCredits = await testDb.getRowCount(replicaDb, 'credits');
            assert.strictEqual(replicaCredits, sourceCredits);

            // Should complete in reasonable time (under 90 seconds)
            assert.ok(elapsed < 90000, 'Bootstrap took too long: ' + elapsed + 'ms');
        });
    });

    describe('6.2 Incremental catch-up of 200 blocks', function() {
        it('catches up large delta within timeout', async function() {
            this.timeout(120000);

            // Bootstrap to block 100
            await fixtures.seedBlocks(sourceDb, 1, 100);

            server = new ServerProcess(sourceDb, SERVER_PORT);
            await server.start();

            client = new ClientProcess(replicaDb, server.getUrl());
            await client.bootstrap();
            assert.strictEqual(await replicaDb.getLastBlock(), 100);

            // Add 200 blocks
            await fixtures.seedBlocks(sourceDb, 101, 300);

            let startTime = Date.now();
            await client.incrementalCatchUp(101);
            let elapsed = Date.now() - startTime;

            assert.strictEqual(await replicaDb.getLastBlock(), 300);
            assert.ok(elapsed < 60000, 'Catch-up took too long: ' + elapsed + 'ms');
        });
    });

    describe('6.3 Single block with many actions', function() {
        it('syncs a block with 1000 actions', async function() {
            this.timeout(60000);

            // Seed baseline blocks
            await fixtures.seedBlocks(sourceDb, 1, 10);

            // Seed a large block
            await fixtures.seedLargeBlock(sourceDb, 11, 1000);

            server = new ServerProcess(sourceDb, SERVER_PORT);
            await server.start();

            client = new ClientProcess(replicaDb, server.getUrl());
            await client.bootstrap();

            assert.strictEqual(await replicaDb.getLastBlock(), 11);

            // Verify all actions were synced
            let sourceActions  = await testDb.getRowCount(sourceDb, 'actions');
            let replicaActions = await testDb.getRowCount(replicaDb, 'actions');
            assert.strictEqual(replicaActions, sourceActions);

            let sourceCredits  = await testDb.getRowCount(sourceDb, 'credits');
            let replicaCredits = await testDb.getRowCount(replicaDb, 'credits');
            assert.strictEqual(replicaCredits, sourceCredits);
        });
    });

    describe('6.4 Sustained throughput', function() {
        it('keeps up with 1 block per 200ms for 100 blocks', async function() {
            this.timeout(60000);

            await fixtures.seedBlocks(sourceDb, 1, 10);

            server = new ServerProcess(sourceDb, SERVER_PORT);
            await server.start();

            client = new ClientProcess(replicaDb, server.getUrl());
            await client.start();
            await waitForReplicaBlock(replicaDb, 10);

            // Insert blocks at ~1 per 200ms
            for (let i = 11; i <= 110; i++) {
                await fixtures.seedBlocks(sourceDb, i, i);
                await new Promise(r => setTimeout(r, 200));
            }

            // Wait for all blocks to sync
            await waitForReplicaBlock(replicaDb, 110, 30000);

            assert.strictEqual(await replicaDb.getLastBlock(), 110);
            assert.strictEqual(await testDb.getRowCount(replicaDb, 'blocks'), 110);
        });
    });

    describe('6.5 Snapshot download for large dataset', function() {
        it('serves gzip-compressed snapshot for 200 blocks', async function() {
            this.timeout(60000);

            await fixtures.seedBlocks(sourceDb, 1, 200);

            server = new ServerProcess(sourceDb, SERVER_PORT);
            await server.start();

            let startTime = Date.now();
            let res = await axios.get(
                server.getUrl() + '/snapshot/indexer/bitcoin/mainnet',
                { responseType: 'arraybuffer', timeout: 30000, decompress: false }
            );
            let elapsed = Date.now() - startTime;

            // Decompress
            let decompressed = zlib.gunzipSync(res.data);
            let snapshot = JSON.parse(decompressed.toString());

            // Verify structure
            assert.strictEqual(snapshot.block_height, 200);
            assert.ok(snapshot.tables, 'Snapshot should have tables');
            assert.ok(snapshot.tables.blocks, 'Snapshot should have blocks table');
            assert.strictEqual(snapshot.tables.blocks.length, 200);

            // Verify row counts match
            let sourceCredits = await testDb.getRowCount(sourceDb, 'credits');
            assert.strictEqual(snapshot.tables.credits.length, sourceCredits);

            assert.ok(elapsed < 30000, 'Snapshot download took too long: ' + elapsed + 'ms');
        });
    });
});
