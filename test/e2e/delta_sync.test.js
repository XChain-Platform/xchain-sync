// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

const assert        = require('assert');
const sinon         = require('sinon');
const setup         = require('./helpers/setup');
const testDb        = require('./helpers/testDb');
const fixtures      = require('./helpers/fixtures');
const ServerProcess = require('./helpers/serverProcess');
const ClientProcess = require('./helpers/clientProcess');
const { waitForReplicaBlock, waitForClientEvents } = require('./helpers/waitFor');
const { assertBlockExists, assertBlockNotExists, assertReplicaByteIdentical } = require('./helpers/assertions');

const SERVER_PORT = 29200;

describe('E2E: Delta Synchronization', function() {

    let sourceDb, replicaDb, server, client;

    before(async function() {
        await setup.globalSetup();
        sourceDb  = setup.getSourceDb();
        replicaDb = setup.getReplicaDb();

        if (!process.env.E2E_VERBOSE) { sinon.stub(console, 'log'); sinon.stub(console, 'error'); }
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

            await fixtures.seedBlocks(sourceDb, 1, 20);

            server = new ServerProcess(sourceDb, SERVER_PORT);
            await server.start();

            client = new ClientProcess(replicaDb, server.getUrl());
            await client.bootstrap();
            assert.strictEqual(await replicaDb.getLastBlock(), 20);

            let preCredits = await testDb.getRowCount(replicaDb, 'credits');

            await fixtures.seedBlocks(sourceDb, 21, 40);
            // Seeded rows are servable only once the poller records them (100/cycle cap).
            await server.pollUntil(40);

            await client.incrementalCatchUp(21);

            assert.strictEqual(await replicaDb.getLastBlock(), 40);

            await assertBlockExists(replicaDb, 21);
            await assertBlockExists(replicaDb, 40);

            let postCredits = await testDb.getRowCount(replicaDb, 'credits');
            assert.ok(postCredits > preCredits, 'Credits should have increased');
            assert.strictEqual(postCredits, 40); // 1 credit per block
            await assertReplicaByteIdentical(sourceDb, replicaDb);
        });
    });

    describe('2.2 Large delta catch-up', function() {
        it('catches up 200 blocks via incremental snapshot', async function() {
            this.timeout(60000);

            await fixtures.seedBlocks(sourceDb, 1, 10);

            server = new ServerProcess(sourceDb, SERVER_PORT);
            await server.start();

            client = new ClientProcess(replicaDb, server.getUrl());
            await client.bootstrap();

            await fixtures.seedBlocks(sourceDb, 11, 210);
            await server.pollUntil(210);

            await client.incrementalCatchUp(11);

            assert.strictEqual(await replicaDb.getLastBlock(), 210);
            await assertReplicaByteIdentical(sourceDb, replicaDb);
        });
    });

    describe('2.3 Catch-up with diverse action types', function() {
        it('syncs blocks with different credit amounts', async function() {
            this.timeout(30000);

            await fixtures.seedBlocks(sourceDb, 1, 5, { creditAmount: '500' });

            server = new ServerProcess(sourceDb, SERVER_PORT);
            await server.start();

            client = new ClientProcess(replicaDb, server.getUrl());
            await client.bootstrap();

            await fixtures.seedBlocks(sourceDb, 6, 15, { creditAmount: '9999' });
            await server.pollUntil(15);

            await client.incrementalCatchUp(6);

            assert.strictEqual(await replicaDb.getLastBlock(), 15);

            let newCredits = await replicaDb.doQuery(
                "SELECT c.amount FROM credits c INNER JOIN actions a ON a.action_index = c.action_index WHERE a.block_index >= 6"
            );
            for (let row of newCredits) {
                assert.strictEqual(row.amount, '9999');
            }

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

            // The redundant catch-up this test guards against would fire from
            // the status handler, so wait for three server heartbeats to be
            // FULLY handled rather than for a duration in which they probably
            // arrived. The counter advances only after each handler settles, so
            // reaching three means the client had three real chances to
            // re-apply and took none. The old sleep also passed when the live
            // socket never connected at all.
            const seenStatus = client.getEventsHandled('status');
            await client.connectLive();
            await waitForClientEvents(client, 'status', seenStatus + 3, 10000);

            assert.strictEqual(await replicaDb.getLastBlock(), 20);
            assert.strictEqual(await testDb.getRowCount(replicaDb, 'blocks'), 20);
        });
    });
});
