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
const { waitFor, waitForReplicaBlock, waitForClientEvents } = require('./helpers/waitFor');
const { assertReplicaByteIdentical, assertBlockExists, assertBalancesConsistent, assertHashesMatch } = require('./helpers/assertions');

const SERVER_PORT = 29100;

describe('E2E: Full Lifecycle', function() {

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

    describe('1.1 Cold bootstrap from snapshot', function() {
        it('bootstraps replica from server snapshot with correct data', async function() {
            this.timeout(30000);

            await fixtures.seedBlocks(sourceDb, 1, 50);

            server = new ServerProcess(sourceDb, SERVER_PORT);
            await server.start();

            client = new ClientProcess(replicaDb, server.getUrl());
            await client.bootstrap();

            let replicaBlock = await replicaDb.getLastBlock();
            assert.strictEqual(replicaBlock, 50);

            let sourceBlockCount = await testDb.getRowCount(sourceDb, 'blocks');
            let replicaBlockCount = await testDb.getRowCount(replicaDb, 'blocks');
            assert.strictEqual(replicaBlockCount, sourceBlockCount);

            await assertHashesMatch(sourceDb, replicaDb, 50);

            // Full parity oracle: byte-identical tables + recompute conformance.
            await assertReplicaByteIdentical(sourceDb, replicaDb);
        });
    });

    describe('1.2 Live sync after bootstrap', function() {
        it('receives new blocks via WebSocket after bootstrap', async function() {
            this.timeout(30000);

            await fixtures.seedBlocks(sourceDb, 1, 20);

            server = new ServerProcess(sourceDb, SERVER_PORT);
            await server.start();

            client = new ClientProcess(replicaDb, server.getUrl());
            await client.start();

            await fixtures.seedBlocks(sourceDb, 21, 25);

            await waitForReplicaBlock(replicaDb, 25, 15000);

            let replicaBlock = await replicaDb.getLastBlock();
            assert.strictEqual(replicaBlock, 25);

            await assertBlockExists(replicaDb, 21);
            await assertBlockExists(replicaDb, 25);
        });
    });

    describe('1.3 Sustained live sync', function() {
        it('syncs 50 blocks inserted incrementally', async function() {
            this.timeout(60000);

            await fixtures.seedBlocks(sourceDb, 1, 10);

            server = new ServerProcess(sourceDb, SERVER_PORT);
            await server.start();

            client = new ClientProcess(replicaDb, server.getUrl());
            await client.start();

            for (let i = 11; i <= 60; i++) {
                await fixtures.seedBlocks(sourceDb, i, i);
                if (i % 10 === 0) {
                    await new Promise(r => setTimeout(r, 100));
                }
            }

            await waitForReplicaBlock(replicaDb, 60, 30000);

            let replicaBlock = await replicaDb.getLastBlock();
            assert.strictEqual(replicaBlock, 60);
            await assertReplicaByteIdentical(sourceDb, replicaDb);
        });
    });

    describe('1.4 Idle period (no new data)', function() {
        it('remains stable with no new blocks', async function() {
            this.timeout(15000);

            await fixtures.seedBlocks(sourceDb, 1, 10);

            server = new ServerProcess(sourceDb, SERVER_PORT);
            await server.start();

            client = new ClientProcess(replicaDb, server.getUrl());
            await client.start();

            await waitForReplicaBlock(replicaDb, 10);

            // "Idle" has to mean the client kept hearing from an idle server, not
            // that three seconds passed. Wait for four status heartbeats to be
            // fully handled: each one is a chance for the client to re-apply or
            // spuriously catch up, and the counter only advances once that
            // decision has settled. A sleep passed identically when the live
            // socket had silently dropped, which is the opposite of stable.
            const seenStatus = client.getEventsHandled('status');
            await waitForClientEvents(client, 'status', seenStatus + 4, 10000);

            let replicaBlock = await replicaDb.getLastBlock();
            assert.strictEqual(replicaBlock, 10);
            assert.strictEqual(await testDb.getRowCount(replicaDb, 'blocks'), 10);
        });
    });

    describe('1.5 Resume after idle', function() {
        it('syncs new blocks after an idle period', async function() {
            this.timeout(20000);

            await fixtures.seedBlocks(sourceDb, 1, 10);

            server = new ServerProcess(sourceDb, SERVER_PORT);
            await server.start();

            client = new ClientProcess(replicaDb, server.getUrl());
            await client.start();

            await waitForReplicaBlock(replicaDb, 10);

            await new Promise(r => setTimeout(r, 2000));

            await fixtures.seedBlocks(sourceDb, 11, 15);

            await waitForReplicaBlock(replicaDb, 15);
            assert.strictEqual(await replicaDb.getLastBlock(), 15);
            await assertReplicaByteIdentical(sourceDb, replicaDb);
        });
    });
});
