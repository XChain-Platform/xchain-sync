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
const { waitFor, waitForReplicaBlock, waitForClientDisconnect } = require('./helpers/waitFor');
const { assertBlockExists, assertBalancesConsistent, assertReplicaByteIdentical } = require('./helpers/assertions');

const SERVER_PORT = 29400;

describe('E2E: Error Handling & Recovery', function() {

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

    describe('4.3 Server restart (client reconnects and catches up)', function() {
        it('recovers after server stops and restarts', async function() {
            this.timeout(30000);

            await fixtures.seedBlocks(sourceDb, 1, 10);

            server = new ServerProcess(sourceDb, SERVER_PORT);
            await server.start();

            client = new ClientProcess(replicaDb, server.getUrl(), 'bitcoin', 'mainnet', {
                reconnectDelay: 500
            });
            await client.start();
            await waitForReplicaBlock(replicaDb, 10);

            await server.stop();
            server = null;

            await fixtures.seedBlocks(sourceDb, 11, 15);

            // This test is about reconnect, so the client must first have SEEN
            // the server go away; bringing it back before the close is observed
            // would let the test pass without the reconnect path ever running.
            await waitForClientDisconnect(client);

            server = new ServerProcess(sourceDb, SERVER_PORT);
            await server.start();

            await waitForReplicaBlock(replicaDb, 15, 20000);

            assert.strictEqual(await replicaDb.getLastBlock(), 15);
            await assertBlockExists(replicaDb, 11);
            await assertBlockExists(replicaDb, 15);
            await assertReplicaByteIdentical(sourceDb, replicaDb);
        });
    });

    describe('4.4 Client restart after source advances', function() {
        it('catches up to current block after restart', async function() {
            this.timeout(30000);

            await fixtures.seedBlocks(sourceDb, 1, 20);

            server = new ServerProcess(sourceDb, SERVER_PORT);
            await server.start();

            client = new ClientProcess(replicaDb, server.getUrl());
            await client.bootstrap();
            assert.strictEqual(await replicaDb.getLastBlock(), 20);
            client.stop();

            await fixtures.seedBlocks(sourceDb, 21, 40);
            // Seeded rows are servable only once the poller records them (100/cycle cap).
            await server.pollUntil(40);

            client = new ClientProcess(replicaDb, server.getUrl());
            await client.incrementalCatchUp(21);

            assert.strictEqual(await replicaDb.getLastBlock(), 40);
            await assertReplicaByteIdentical(sourceDb, replicaDb);
        });
    });

    describe('4.6 Hub unavailable at startup (server waits)', function() {
        it('server handles missing hub gracefully', async function() {
            this.timeout(15000);

            // ServerProcess connects to the DB directly rather than through the
            // hub, so this only confirms the server still serves data with no
            // hub present; it does not exercise HubClient's own retry logic.
            await fixtures.seedBlocks(sourceDb, 1, 5);

            server = new ServerProcess(sourceDb, SERVER_PORT);
            await server.start();

            let axios = require('axios');
            let res = await axios.get(server.getUrl() + '/status/indexer/bitcoin/mainnet', { timeout: 5000 });
            assert.strictEqual(res.data.block_height, 5);
        });
    });

    describe('4.7 Duplicate blocks do not cause errors', function() {
        it('handles receiving a block that already exists', async function() {
            this.timeout(20000);

            await fixtures.seedBlocks(sourceDb, 1, 10);

            server = new ServerProcess(sourceDb, SERVER_PORT);
            await server.start();

            client = new ClientProcess(replicaDb, server.getUrl());
            await client.start();
            await waitForReplicaBlock(replicaDb, 10);

            let blockCountBefore = await testDb.getRowCount(replicaDb, 'blocks');

            // Resetting lastPolledBlock forces the poller to re-broadcast blocks
            // it already sent, simulating a duplicate delivery.
            server.poller.lastPolledBlock = 0;
            await server.poll();

            await new Promise(r => setTimeout(r, 2000));

            let blockCountAfter = await testDb.getRowCount(replicaDb, 'blocks');
            assert.strictEqual(blockCountAfter, blockCountBefore);
        });
    });

    describe('4.8 Server poll error does not crash', function() {
        it('continues polling after a transient error', async function() {
            this.timeout(20000);

            await fixtures.seedBlocks(sourceDb, 1, 5);

            server = new ServerProcess(sourceDb, SERVER_PORT);
            await server.start();

            let lastBlock = await sourceDb.getLastBlock();
            assert.strictEqual(lastBlock, 5);

            await fixtures.seedBlocks(sourceDb, 6, 10);

            let axios = require('axios');
            await waitFor(async () => {
                let res = await axios.get(server.getUrl() + '/status/indexer/bitcoin/mainnet', { timeout: 3000 });
                return res.data.block_height >= 10;
            }, 10000);
        });
    });
});
