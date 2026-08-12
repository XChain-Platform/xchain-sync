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
const { waitFor, waitForReplicaBlock } = require('./helpers/waitFor');
const { assertBlockExists, assertBlockNotExists, assertBalancesConsistent, assertHashesMatch, assertReplicaByteIdentical } = require('./helpers/assertions');

const SERVER_PORT = 29300;

describe('E2E: Reorg Propagation', function() {

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

    describe('3.1 Simple reorg (3 blocks)', function() {
        it('propagates reorg and new blocks correctly', async function() {
            this.timeout(30000);

            await fixtures.seedBlocks(sourceDb, 1, 20);

            server = new ServerProcess(sourceDb, SERVER_PORT);
            await server.start();

            client = new ClientProcess(replicaDb, server.getUrl());
            await client.start();
            await waitForReplicaBlock(replicaDb, 20);

            await fixtures.deleteBlocksFrom(sourceDb, 18);

            await server.poll();

            await waitFor(async () => {
                let block = await replicaDb.getLastBlock();
                return block !== null && block <= 17;
            }, 10000);

            await assertBlockNotExists(replicaDb, 18);
            await assertBlockNotExists(replicaDb, 19);
            await assertBlockNotExists(replicaDb, 20);

            await fixtures.seedBlocks(sourceDb, 18, 21, { creditAmount: '5555' });
            await server.poll();

            await waitForReplicaBlock(replicaDb, 21);

            await assertBlockExists(replicaDb, 18);
            await assertBlockExists(replicaDb, 21);

            let newCredits = await replicaDb.doQuery(
                "SELECT c.amount FROM credits c INNER JOIN actions a ON a.action_index = c.action_index WHERE a.block_index >= 18"
            );
            for (let row of newCredits) {
                assert.strictEqual(row.amount, '5555');
            }
            // Post-reorg parity: rolled-back replica must re-converge byte-identically.
            await assertReplicaByteIdentical(sourceDb, replicaDb);
        });
    });

    describe('3.2 Reorg with balance impact', function() {
        it('recalculates balances correctly after reorg', async function() {
            this.timeout(30000);

            await fixtures.seedBlocks(sourceDb, 1, 15, { creditAmount: '1000' });
            await fixtures.seedBlocks(sourceDb, 16, 20, { creditAmount: '5000' });

            server = new ServerProcess(sourceDb, SERVER_PORT);
            await server.start();

            client = new ClientProcess(replicaDb, server.getUrl());
            await client.start();
            await waitForReplicaBlock(replicaDb, 20);

            // Removes the blocks that carried the 5000-credit range.
            await fixtures.deleteBlocksFrom(sourceDb, 16);
            await server.poll();

            await waitFor(async () => {
                let block = await replicaDb.getLastBlock();
                return block !== null && block <= 15;
            }, 10000);

            await fixtures.seedBlocks(sourceDb, 16, 18, { creditAmount: '3000' });
            await server.poll();

            await waitForReplicaBlock(replicaDb, 18);

            await assertBalancesConsistent(replicaDb);
            await assertReplicaByteIdentical(sourceDb, replicaDb);
        });
    });

    describe('3.3 Deep reorg (10 blocks)', function() {
        it('handles deep rollback and re-sync', async function() {
            this.timeout(30000);

            await fixtures.seedBlocks(sourceDb, 1, 30);

            server = new ServerProcess(sourceDb, SERVER_PORT);
            await server.start();

            client = new ClientProcess(replicaDb, server.getUrl());
            await client.start();
            await waitForReplicaBlock(replicaDb, 30);

            await fixtures.deleteBlocksFrom(sourceDb, 21);
            await server.poll();

            await waitFor(async () => {
                let block = await replicaDb.getLastBlock();
                return block !== null && block <= 20;
            }, 10000);

            await fixtures.seedBlocks(sourceDb, 21, 35, { creditAmount: '2222' });
            await server.poll();

            await waitForReplicaBlock(replicaDb, 35);

            assert.strictEqual(await replicaDb.getLastBlock(), 35);
            await assertReplicaByteIdentical(sourceDb, replicaDb);
        });
    });

    describe('3.4 Reorg to shorter chain', function() {
        it('rolls back without immediate replacement blocks', async function() {
            this.timeout(20000);

            await fixtures.seedBlocks(sourceDb, 1, 25);

            server = new ServerProcess(sourceDb, SERVER_PORT);
            await server.start();

            client = new ClientProcess(replicaDb, server.getUrl());
            await client.start();
            await waitForReplicaBlock(replicaDb, 25);

            // No replacement blocks follow this reorg, unlike the other cases here.
            await fixtures.deleteBlocksFrom(sourceDb, 23);
            await server.poll();

            await waitFor(async () => {
                let block = await replicaDb.getLastBlock();
                return block !== null && block <= 22;
            }, 10000);

            assert.strictEqual(await replicaDb.getLastBlock(), 22);
            await assertBlockNotExists(replicaDb, 23);
            await assertReplicaByteIdentical(sourceDb, replicaDb);
        });
    });

    describe('3.5 Consecutive reorgs', function() {
        it('handles two rapid reorgs without corruption', async function() {
            this.timeout(30000);

            await fixtures.seedBlocks(sourceDb, 1, 20);

            server = new ServerProcess(sourceDb, SERVER_PORT);
            await server.start();

            client = new ClientProcess(replicaDb, server.getUrl());
            await client.start();
            await waitForReplicaBlock(replicaDb, 20);

            // First reorg: remove 18-20, add 18-22
            await fixtures.deleteBlocksFrom(sourceDb, 18);
            await server.poll();

            await waitFor(async () => {
                let block = await replicaDb.getLastBlock();
                return block !== null && block <= 17;
            }, 10000);

            await fixtures.seedBlocks(sourceDb, 18, 22, { creditAmount: '1111' });
            await server.poll();
            await waitForReplicaBlock(replicaDb, 22);

            // Second reorg: remove 20-22, add 20-23
            await fixtures.deleteBlocksFrom(sourceDb, 20);
            await server.poll();

            await waitFor(async () => {
                let block = await replicaDb.getLastBlock();
                return block !== null && block <= 19;
            }, 10000);

            await fixtures.seedBlocks(sourceDb, 20, 23, { creditAmount: '3333' });
            await server.poll();
            await waitForReplicaBlock(replicaDb, 23);

            assert.strictEqual(await replicaDb.getLastBlock(), 23);

            let credits = await replicaDb.doQuery(
                "SELECT c.amount FROM credits c INNER JOIN actions a ON a.action_index = c.action_index WHERE a.block_index >= 20"
            );
            for (let row of credits) {
                assert.strictEqual(row.amount, '3333');
            }

            await assertBalancesConsistent(replicaDb);
            await assertReplicaByteIdentical(sourceDb, replicaDb);
        });
    });
});
