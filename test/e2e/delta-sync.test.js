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
const setup         = require('./helpers/setup');
const testDb        = require('./helpers/testDb');
const fixtures      = require('./helpers/fixtures');
const ServerProcess = require('./helpers/serverProcess');
const ClientProcess = require('./helpers/clientProcess');
const { waitForReplicaBlock } = require('./helpers/waitFor');
const { assertBlockExists, assertBlockNotExists, assertReplicaByteIdentical } = require('./helpers/assertions');

const SERVER_PORT = 29200;

describe('E2E: Delta Synchronization', function() {

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

    describe('2.1 Client catches up after downtime', function() {
        it('syncs only missing blocks after client restart', async function() {
            this.timeout(30000);

            // Seed and bootstrap to block 20
            await fixtures.seedBlocks(sourceDb, 1, 20);

            server = new ServerProcess(sourceDb, SERVER_PORT);
            await server.start();

            client = new ClientProcess(replicaDb, server.getUrl());
            await client.bootstrap();
            assert.strictEqual(await replicaDb.getLastBlock(), 20);

            // Record pre-existing row counts
            let preCredits = await testDb.getRowCount(replicaDb, 'credits');

            // Add blocks 21-40 to source while client is "down"
            await fixtures.seedBlocks(sourceDb, 21, 40);

            // Client catches up via incremental snapshot
            await client.incrementalCatchUp(21);

            assert.strictEqual(await replicaDb.getLastBlock(), 40);

            // Verify new blocks exist
            await assertBlockExists(replicaDb, 21);
            await assertBlockExists(replicaDb, 40);

            // Verify old blocks untouched (row count increased, not replaced)
            let postCredits = await testDb.getRowCount(replicaDb, 'credits');
            assert.ok(postCredits > preCredits, 'Credits should have increased');
            assert.strictEqual(postCredits, 40); // 1 credit per block
            // Resume parity: the catch-up replica must be byte-identical
            await assertReplicaByteIdentical(sourceDb, replicaDb);
        });
    });

    describe('2.2 Large delta catch-up', function() {
        it('catches up 200 blocks via incremental snapshot', async function() {
            this.timeout(60000);

            // Bootstrap to block 10
            await fixtures.seedBlocks(sourceDb, 1, 10);

            server = new ServerProcess(sourceDb, SERVER_PORT);
            await server.start();

            client = new ClientProcess(replicaDb, server.getUrl());
            await client.bootstrap();

            // Add 200 blocks while client is down
            await fixtures.seedBlocks(sourceDb, 11, 210);

            // Incremental catch-up
            await client.incrementalCatchUp(11);

            assert.strictEqual(await replicaDb.getLastBlock(), 210);
            await assertReplicaByteIdentical(sourceDb, replicaDb);
        });
    });

    describe('2.3 Catch-up with diverse action types', function() {
        it('syncs blocks with different credit amounts', async function() {
            this.timeout(30000);

            // Bootstrap to block 5
            await fixtures.seedBlocks(sourceDb, 1, 5, { creditAmount: '500' });

            server = new ServerProcess(sourceDb, SERVER_PORT);
            await server.start();

            client = new ClientProcess(replicaDb, server.getUrl());
            await client.bootstrap();

            // Add blocks 6-15 with different amounts
            await fixtures.seedBlocks(sourceDb, 6, 15, { creditAmount: '9999' });

            await client.incrementalCatchUp(6);

            assert.strictEqual(await replicaDb.getLastBlock(), 15);

            // Verify new credits have the correct amount
            let newCredits = await replicaDb.doQuery(
                "SELECT c.amount FROM credits c INNER JOIN actions a ON a.action_index = c.action_index WHERE a.block_index >= 6"
            );
            for (let row of newCredits) {
                assert.strictEqual(row.amount, '9999');
            }

            // Verify old credits unchanged
            let oldCredits = await replicaDb.doQuery(
                "SELECT c.amount FROM credits c INNER JOIN actions a ON a.action_index = c.action_index WHERE a.block_index <= 5"
            );
            for (let row of oldCredits) {
                assert.strictEqual(row.amount, '500');
            }
        });
    });

    describe('2.4 No-op catch-up (already synced)', function() {
        it('handles catch-up when already at latest block', async function() {
            this.timeout(15000);

            await fixtures.seedBlocks(sourceDb, 1, 20);

            server = new ServerProcess(sourceDb, SERVER_PORT);
            await server.start();

            client = new ClientProcess(replicaDb, server.getUrl());
            await client.bootstrap();
            assert.strictEqual(await replicaDb.getLastBlock(), 20);

            // Connect live — should not trigger catch-up
            await client.connectLive();
            await new Promise(r => setTimeout(r, 2000));

            // Still at block 20
            assert.strictEqual(await replicaDb.getLastBlock(), 20);
            assert.strictEqual(await testDb.getRowCount(replicaDb, 'blocks'), 20);
        });
    });
});
