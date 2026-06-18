// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
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

const LIVE_PORT = 19400;

describe('Integration: Client Live Sync', function() {

    let sourceDb, replicaDb, server, wss, broadcaster, poller, log;

    before(async function() {
        await setup.globalSetup();
        sourceDb  = setup.getSourceDb();
        replicaDb = setup.getReplicaDb();

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
            let sinceBlock = parseInt(req.params.blockHeight);
            await snapshotBuilder.streamIncrementalSnapshot(sourceDb, sinceBlock, res);
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
            res.json({ chain: req.params.chain, network: req.params.network, tables: schema });
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

        await new Promise(resolve => server.listen(LIVE_PORT, resolve));

        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });

    after(async function() {
        sinon.restore();
        await new Promise(resolve => server.close(resolve));
        await setup.globalTeardown();
    });

    beforeEach(async function() {
        await testDb.truncateAll(sourceDb);
        await testDb.truncateAll(replicaDb);
    });

    // Helper: wait for a condition with polling
    async function waitFor(fn, timeout = 10000) {
        let start = Date.now();
        while (Date.now() - start < timeout) {
            if (await fn()) return;
            await new Promise(r => setTimeout(r, 100));
        }
        throw new Error('waitFor timeout');
    }

    describe('live block applied via WebSocket', function() {
        it('applies a single block received via WS to replica', async function() {
            this.timeout(15000);

            // Bootstrap replica with 5 blocks
            await fixtures.seedBlocks(sourceDb, 1, 5);

            let applier    = new ClientApplier(replicaDb, testDb.util);
            let rollbacker = new ClientRollback(replicaDb, testDb.util);
            let verifier   = new HashVerifier();
            let config = {
                SYNC_SOURCES: 'http://127.0.0.1:' + LIVE_PORT,
                VERIFY_HASHES: false,
                CLIENT_RECONNECT_DELAY: 200,
                HASH_CONFIRM_TIMEOUT: 1000
            };

            // Bootstrap first
            let clientSync = require('../../src/ClientSync');
            let cs = new clientSync('bitcoin', 'mainnet', replicaDb, applier, rollbacker, verifier, config, testDb.util);
            await cs._bootstrapFromSnapshot();
            cs.lastAppliedBlock = 5;
            cs.lastHashes = await replicaDb.getBlockHashRow(5);

            // Connect WS
            cs._connectWebSockets();
            await new Promise(r => setTimeout(r, 500));

            // Add block 6 to source and poll
            await fixtures.seedBlocks(sourceDb, 6, 6);
            poller.lastPolledBlock = 5;
            await poller._poll();

            // Wait for replica to have block 6
            await waitFor(async () => {
                let count = await testDb.getRowCount(replicaDb, 'blocks');
                return count === 6;
            });

            let replicaBlocks = await testDb.getRowCount(replicaDb, 'blocks');
            assert.strictEqual(replicaBlocks, 6);

            cs.stop();
        });

        it('applies multiple sequential blocks', async function() {
            this.timeout(20000);

            await fixtures.seedBlocks(sourceDb, 1, 5);

            let applier    = new ClientApplier(replicaDb, testDb.util);
            let rollbacker = new ClientRollback(replicaDb, testDb.util);
            let verifier   = new HashVerifier();
            let config = {
                SYNC_SOURCES: 'http://127.0.0.1:' + LIVE_PORT,
                VERIFY_HASHES: false,
                CLIENT_RECONNECT_DELAY: 200,
                HASH_CONFIRM_TIMEOUT: 1000
            };

            let clientSync = require('../../src/ClientSync');
            let cs = new clientSync('bitcoin', 'mainnet', replicaDb, applier, rollbacker, verifier, config, testDb.util);
            await cs._bootstrapFromSnapshot();
            cs.lastAppliedBlock = 5;
            cs.lastHashes = await replicaDb.getBlockHashRow(5);
            cs._connectWebSockets();
            await new Promise(r => setTimeout(r, 500));

            // Add blocks 6-10 and poll
            await fixtures.seedBlocks(sourceDb, 6, 10);
            poller.lastPolledBlock = 5;
            await poller._poll();

            await waitFor(async () => {
                let count = await testDb.getRowCount(replicaDb, 'blocks');
                return count === 10;
            });

            let replicaBlocks = await testDb.getRowCount(replicaDb, 'blocks');
            assert.strictEqual(replicaBlocks, 10);

            cs.stop();
        });
    });

    describe('duplicate block handling', function() {
        it('skips already-applied block without error', async function() {
            this.timeout(15000);

            await fixtures.seedBlocks(sourceDb, 1, 5);
            await fixtures.seedBlocks(replicaDb, 1, 5);

            let applier = new ClientApplier(replicaDb, testDb.util);

            // Build a block payload for block 5 (already exists)
            let payload = await poller._buildBlockPayload(5);
            if (!payload) {
                poller.lastPolledBlock = 4;
                await poller._poll();
                payload = { block_index: 5, data: {} };
            }

            // Apply: should not throw
            await applier.applyBlock(payload);

            // Count unchanged
            let count = await testDb.getRowCount(replicaDb, 'blocks');
            assert.strictEqual(count, 5);
        });
    });
});
