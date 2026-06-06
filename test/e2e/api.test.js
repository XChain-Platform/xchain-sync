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
const WebSocket     = require('ws');
const setup         = require('./helpers/setup');
const testDb        = require('./helpers/testDb');
const fixtures      = require('./helpers/fixtures');
const ServerProcess = require('./helpers/serverProcess');
const { waitFor }   = require('./helpers/waitFor');

const SERVER_PORT = 29900;

describe('E2E: API Correctness', function() {

    let sourceDb, replicaDb, server;

    before(async function() {
        await setup.globalSetup();
        sourceDb  = setup.getSourceDb();
        replicaDb = setup.getReplicaDb();

        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });

    after(async function() {
        sinon.restore();
        if (server) await server.stop();
        await setup.globalTeardown();
    });

    beforeEach(async function() {
        if (server) { await server.stop(); server = null; }
        await setup.resetDatabases();
    });

    describe('9.1 Status endpoint reflects live state', function() {
        it('returns current block height after new blocks', async function() {
            this.timeout(15000);

            await fixtures.seedBlocks(sourceDb, 1, 50);

            server = new ServerProcess(sourceDb, SERVER_PORT);
            await server.start();

            // Wait for poller to catch up
            await waitFor(async () => {
                let res = await axios.get(server.getUrl() + '/status/indexer/bitcoin/mainnet', { timeout: 3000 });
                return res.data.block_height >= 50;
            }, 10000);

            // Add block 51
            await fixtures.seedBlocks(sourceDb, 51, 51);
            await server.poll();

            let res = await axios.get(server.getUrl() + '/status/indexer/bitcoin/mainnet', { timeout: 5000 });
            assert.strictEqual(res.data.block_height, 51);
            assert.ok(res.data.ledger_hash, 'Should have ledger_hash');
            assert.ok(res.data.actions_hash, 'Should have actions_hash');
            assert.ok(res.data.contract_hash, 'Should have contract_hash');
            assert.ok(res.data.block_time, 'Should have block_time');

            // Verify hashes match source
            let sourceHash = await sourceDb.getBlockHashRow(51);
            assert.strictEqual(res.data.ledger_hash, sourceHash.ledger_hash);
        });
    });

    describe('9.2 Schema endpoint returns complete DDL', function() {
        it('returns CREATE TABLE statements for all tables', async function() {
            this.timeout(15000);

            await fixtures.seedBlocks(sourceDb, 1, 1);

            server = new ServerProcess(sourceDb, SERVER_PORT);
            await server.start();

            let res = await axios.get(
                server.getUrl() + '/schema/indexer/bitcoin/mainnet',
                { timeout: 10000 }
            );

            assert.ok(res.data.tables, 'Should have tables object');

            let tableNames = Object.keys(res.data.tables);
            assert.ok(tableNames.length >= 10, 'Should have at least 10 tables, got ' + tableNames.length);

            // Verify key tables exist
            assert.ok(res.data.tables.blocks, 'Should have blocks DDL');
            assert.ok(res.data.tables.transactions, 'Should have transactions DDL');
            assert.ok(res.data.tables.actions, 'Should have actions DDL');
            assert.ok(res.data.tables.credits, 'Should have credits DDL');
            assert.ok(res.data.tables.index_transactions, 'Should have index_transactions DDL');
            assert.ok(res.data.tables.sync_meta, 'Should have sync_meta DDL');

            // Each DDL should be a valid CREATE TABLE statement
            for (let table of tableNames) {
                let ddl = res.data.tables[table];
                assert.ok(ddl.includes('CREATE TABLE'), 'DDL for ' + table + ' should contain CREATE TABLE');
            }
        });
    });

    describe('9.3 WebSocket observer receives blocks', function() {
        it('receives block events in order', async function() {
            this.timeout(15000);

            await fixtures.seedBlocks(sourceDb, 1, 5);

            server = new ServerProcess(sourceDb, SERVER_PORT);
            await server.start();

            // Wait for initial polling
            await waitFor(async () => {
                let res = await axios.get(server.getUrl() + '/status/indexer/bitcoin/mainnet', { timeout: 3000 });
                return res.data.block_height >= 5;
            }, 10000);

            // Subscribe to WebSocket
            let messages = [];
            let ws = new WebSocket(server.getWsUrl() + '/subscribe/indexer/bitcoin/mainnet');
            ws.on('message', (data) => {
                messages.push(JSON.parse(data.toString()));
            });

            // Wait for connection and initial status
            await new Promise(r => setTimeout(r, 1000));

            // Add 5 blocks
            await fixtures.seedBlocks(sourceDb, 6, 10);

            // Wait for blocks to be polled and broadcast
            for (let i = 0; i < 20; i++) {
                await server.poll();
                await new Promise(r => setTimeout(r, 200));
            }

            await waitFor(() => {
                let blockMessages = messages.filter(m => m.type === 'block');
                return blockMessages.length >= 5;
            }, 10000);

            // Verify block events
            let blockMessages = messages.filter(m => m.type === 'block');
            assert.ok(blockMessages.length >= 5, 'Should have received 5 block events, got ' + blockMessages.length);

            // Verify ordering
            for (let i = 0; i < blockMessages.length - 1; i++) {
                assert.ok(blockMessages[i].block_index < blockMessages[i + 1].block_index,
                    'Block events should be in order');
            }

            // Verify block structure
            let firstBlock = blockMessages[0];
            assert.strictEqual(firstBlock.type, 'block');
            assert.ok(firstBlock.block_index, 'Block should have block_index');
            assert.ok(firstBlock.ledger_hash, 'Block should have ledger_hash');
            assert.ok(firstBlock.data, 'Block should have data');

            ws.close();
        });
    });

    describe('9.4 WebSocket observer receives reorg', function() {
        it('receives reorg event when blocks are removed', async function() {
            this.timeout(15000);

            await fixtures.seedBlocks(sourceDb, 1, 10);

            server = new ServerProcess(sourceDb, SERVER_PORT);
            await server.start();

            // Wait for initial polling
            await waitFor(async () => {
                let res = await axios.get(server.getUrl() + '/status/indexer/bitcoin/mainnet', { timeout: 3000 });
                return res.data.block_height >= 10;
            }, 10000);

            // Subscribe
            let messages = [];
            let ws = new WebSocket(server.getWsUrl() + '/subscribe/indexer/bitcoin/mainnet');
            ws.on('message', (data) => {
                messages.push(JSON.parse(data.toString()));
            });
            await new Promise(r => setTimeout(r, 1000));

            // Trigger reorg
            await fixtures.deleteBlocksFrom(sourceDb, 8);
            await server.poll();

            await waitFor(() => {
                return messages.some(m => m.type === 'reorg');
            }, 10000);

            let reorgEvents = messages.filter(m => m.type === 'reorg');
            assert.ok(reorgEvents.length >= 1, 'Should have received reorg event');
            assert.strictEqual(reorgEvents[0].block_index, 8);
            assert.strictEqual(reorgEvents[0].chain, 'bitcoin');
            assert.strictEqual(reorgEvents[0].network, 'mainnet');

            ws.close();
        });
    });

    describe('9.5 Status endpoint with no data', function() {
        it('returns null block_height when DB is empty', async function() {
            this.timeout(10000);

            server = new ServerProcess(sourceDb, SERVER_PORT);
            await server.start();

            let res = await axios.get(server.getUrl() + '/status/indexer/bitcoin/mainnet', { timeout: 5000 });
            assert.strictEqual(res.data.block_height, null);
        });
    });

    describe('9.6 Incremental snapshot endpoint', function() {
        it('returns only data since the given block', async function() {
            this.timeout(15000);

            await fixtures.seedBlocks(sourceDb, 1, 20);

            server = new ServerProcess(sourceDb, SERVER_PORT);
            await server.start();

            let res = await axios.get(
                server.getUrl() + '/snapshot/indexer/bitcoin/mainnet/since/15',
                { responseType: 'arraybuffer', timeout: 10000, decompress: false }
            );

            let decompressed = zlib.gunzipSync(res.data);
            let snapshot = JSON.parse(decompressed.toString());

            assert.strictEqual(snapshot.block_height, 20);
            assert.strictEqual(snapshot.since_block, 15);
            assert.ok(snapshot.tables, 'Should have tables');

            // Should contain only blocks 15-20 (6 blocks)
            if (snapshot.tables.blocks) {
                assert.strictEqual(snapshot.tables.blocks.length, 6);
            }
        });
    });

    describe('9.7 Unknown chain returns 404', function() {
        it('returns 404 for unknown chain/network', async function() {
            this.timeout(10000);

            server = new ServerProcess(sourceDb, SERVER_PORT);
            await server.start();

            try {
                await axios.get(server.getUrl() + '/status/indexer/dogecoin/mainnet', { timeout: 5000 });
                assert.fail('Should have returned 404');
            } catch (e) {
                assert.strictEqual(e.response.status, 404);
            }
        });
    });
});
