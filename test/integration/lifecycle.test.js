// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available —
// contact legal@dankest.llc.

const assert    = require('assert');
const sinon     = require('sinon');
const http      = require('http');
const express   = require('express');
const cors      = require('cors');
const WebSocket = require('ws');
const setup     = require('./helpers/setup');
const testDb    = require('./helpers/testDb');
const fixtures  = require('./helpers/fixtures');
const ServerPoller     = require('../../src/ServerPoller');
const BlockBroadcaster = require('../../src/BlockBroadcaster');
const TransparencyLog  = require('../../src/TransparencyLog');
const SnapshotBuilder  = require('../../src/SnapshotBuilder');
const ClientApplier    = require('../../src/ClientApplier');
const ClientRollback   = require('../../src/ClientRollback');
const HashVerifier     = require('../../src/HashVerifier');
const ClientSync       = require('../../src/ClientSync');

const LIFECYCLE_PORT = 19500;

describe('Integration: Full Lifecycle', function() {

    let sourceDb, replicaDb, server, wss, broadcaster, poller, log;

    before(async function() {
        await setup.globalSetup();
        sourceDb  = setup.getSourceDb();
        replicaDb = setup.getReplicaDb();

        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });

    after(async function() {
        sinon.restore();
        if (server) await new Promise(resolve => server.close(resolve));
        await setup.globalTeardown();
    });

    beforeEach(async function() {
        await testDb.truncateAll(sourceDb);
        await testDb.truncateAll(replicaDb);
    });

    // Helper: wait for condition
    async function waitFor(fn, timeout = 10000) {
        let start = Date.now();
        while (Date.now() - start < timeout) {
            if (await fn()) return;
            await new Promise(r => setTimeout(r, 100));
        }
        throw new Error('waitFor timeout');
    }

    function startServer() {
        let config = {
            WS_MAX_PER_IP: 10,
            WS_BACKPRESSURE_LIMIT: 50,
            BLOCK_POLL_INTERVAL: 100
        };

        broadcaster = new BlockBroadcaster(config);
        log = new TransparencyLog(sourceDb);
        poller = new ServerPoller('bitcoin', 'mainnet', sourceDb, broadcaster, log, config, testDb.util);
        let snapshotBuilder = new SnapshotBuilder(testDb.util);

        let app = express();
        app.use(cors({ origin: '*', methods: ['GET'] }));

        app.get('/snapshot/:dbType/:chain/:network', async (req, res) => {
            await snapshotBuilder.streamFullSnapshot(sourceDb, res);
        });
        app.get('/snapshot/:dbType/:chain/:network/since/:blockHeight', async (req, res) => {
            await snapshotBuilder.streamIncrementalSnapshot(sourceDb, parseInt(req.params.blockHeight), res);
        });
        app.get('/schema/:dbType/:chain/:network', async (req, res) => {
            let tables = await sourceDb.doQuery(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_type = 'BASE TABLE' ORDER BY table_name",
                [sourceDb.dbName]
            );
            let schema = {};
            for (let row of tables) {
                let tn = row.table_name || row.TABLE_NAME;
                let ddl = await sourceDb.doQuery("SHOW CREATE TABLE `" + tn + "`");
                if (ddl.length > 0) schema[tn] = ddl[0]['Create Table'];
            }
            res.json({ tables: schema });
        });
        app.get('/status/:dbType/:chain/:network', async (req, res) => {
            let lastBlock = await sourceDb.getLastBlock();
            let hashRow = lastBlock !== null ? await sourceDb.getBlockHashRow(lastBlock) : null;
            res.json({
                block_height: hashRow ? Number(hashRow.block_index) : null,
                ledger_hash: hashRow ? hashRow.ledger_hash : null,
                actions_hash: hashRow ? hashRow.actions_hash : null,
                contract_hash: hashRow ? hashRow.contract_hash : null
            });
        });

        server = http.createServer(app);
        wss = new WebSocket.Server({ noServer: true });
        server.on('upgrade', (request, socket, head) => {
            let match = request.url.match(/^\/subscribe\/([^\/]+)\/([^\/]+)\/([^\/\?]+)/);
            if (!match) { socket.destroy(); return; }
            wss.handleUpgrade(request, socket, head, (ws) => {
                broadcaster.addSubscription(ws, request, match[2], match[3], 'full', match[1]);
            });
        });

        return new Promise(resolve => server.listen(LIFECYCLE_PORT, resolve));
    }

    describe('bootstrap → live sync → reorg → recovery', function() {
        it('completes full lifecycle correctly', async function() {
            this.timeout(30000);

            // 1. Seed source with 10 blocks
            await fixtures.seedBlocks(sourceDb, 1, 10);
            await startServer();

            // 2. Bootstrap client from snapshot
            let applier    = new ClientApplier(replicaDb, testDb.util);
            let rollbacker = new ClientRollback(replicaDb, testDb.util);
            let verifier   = new HashVerifier();
            let config = {
                SYNC_SOURCES: 'http://127.0.0.1:' + LIFECYCLE_PORT,
                VERIFY_HASHES: false,
                CLIENT_RECONNECT_DELAY: 200,
                HASH_CONFIRM_TIMEOUT: 1000
            };
            let cs = new ClientSync('bitcoin', 'mainnet', replicaDb, applier, rollbacker, verifier, config, testDb.util);
            await cs._bootstrapFromSnapshot();
            cs.lastAppliedBlock = 10;
            cs.lastHashes = await replicaDb.getBlockHashRow(10);

            // Verify bootstrap
            let replicaBlocks = await testDb.getRowCount(replicaDb, 'blocks');
            assert.strictEqual(replicaBlocks, 10);

            // 3. Connect WS for live sync
            cs._connectWebSockets();
            await new Promise(r => setTimeout(r, 500));

            // 4. Add blocks 11-15 to source and poll
            await fixtures.seedBlocks(sourceDb, 11, 15);
            poller.lastPolledBlock = 10;
            await poller._poll();

            // Wait for replica to catch up
            await waitFor(async () => {
                let count = await testDb.getRowCount(replicaDb, 'blocks');
                return count === 15;
            });
            assert.strictEqual(await testDb.getRowCount(replicaDb, 'blocks'), 15);

            // 5. Simulate reorg: delete blocks 13-15, insert new blocks 13-16
            await fixtures.deleteBlocksFrom(sourceDb, 13);
            poller.lastPolledBlock = 15;
            await poller._poll(); // This broadcasts the reorg event

            // Wait for client to process reorg
            await waitFor(async () => {
                return cs.lastAppliedBlock <= 12;
            }, 5000);

            // Now source has blocks 1-12, add new 13-16
            await fixtures.seedBlocks(sourceDb, 13, 16, { creditAmount: '7777' });
            poller.lastPolledBlock = 12;
            await poller._poll();

            // Wait for replica to have 16 blocks
            await waitFor(async () => {
                let count = await testDb.getRowCount(replicaDb, 'blocks');
                return count === 16;
            });

            // 6. Final verification
            let finalBlockCount = await testDb.getRowCount(replicaDb, 'blocks');
            assert.strictEqual(finalBlockCount, 16);

            // Verify source and replica block counts match
            let sourceBlockCount = await testDb.getRowCount(sourceDb, 'blocks');
            assert.strictEqual(finalBlockCount, sourceBlockCount);

            // Verify new credit amounts for blocks 13-16
            let newCredits = await replicaDb.doQuery(
                "SELECT c.amount FROM credits c INNER JOIN actions a ON a.action_index = c.action_index INNER JOIN transactions t ON t.tx_index = a.tx_index WHERE t.block_index >= 13"
            );
            for (let row of newCredits) {
                assert.strictEqual(row.amount, '7777');
            }

            // Verify transparency log has entries
            let syncMetaCount = await testDb.getRowCount(sourceDb, 'sync_meta');
            assert.ok(syncMetaCount > 0);

            cs.stop();
        });
    });
});
