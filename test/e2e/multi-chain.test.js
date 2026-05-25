const assert        = require('assert');
const sinon         = require('sinon');
const http          = require('http');
const express       = require('express');
const cors          = require('cors');
const WebSocket     = require('ws');
const setup         = require('./helpers/setup');
const testDb        = require('./helpers/testDb');
const fixtures      = require('./helpers/fixtures');
const ServerPoller     = require('../../src/ServerPoller');
const BlockBroadcaster = require('../../src/BlockBroadcaster');
const TransparencyLog  = require('../../src/TransparencyLog');
const SnapshotBuilder  = require('../../src/SnapshotBuilder');
const ClientProcess = require('./helpers/clientProcess');
const { waitFor, waitForReplicaBlock } = require('./helpers/waitFor');
const { assertBlockExists } = require('./helpers/assertions');

const SERVER_PORT = 29700;

describe('E2E: Multi-Chain Synchronization', function() {

    let sourceDb, replicaDb, httpServer, wss, broadcaster;
    let btcPoller, ltcPoller, btcLog, ltcLog;
    let client;

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
        if (httpServer) await new Promise(r => httpServer.close(r));
        await setup.globalTeardown();
    });

    beforeEach(async function() {
        if (client) { client.stop(); client = null; }
        if (httpServer) { await new Promise(r => httpServer.close(r)); httpServer = null; }
        await setup.resetDatabases();
    });

    // Multi-chain server: serves two chains from the same source DB
    // (using different block ranges to simulate independent chains)
    function startMultiChainServer() {
        let config = {
            WS_MAX_PER_IP: 20,
            WS_BACKPRESSURE_LIMIT: 50,
            BLOCK_POLL_INTERVAL: 200,
            WS_STATUS_INTERVAL: 500,
            WS_PING_INTERVAL: 30000
        };

        broadcaster = new BlockBroadcaster(config);
        let snapshotBuilder = new SnapshotBuilder(testDb.util);
        btcLog = new TransparencyLog(sourceDb);
        ltcLog = new TransparencyLog(sourceDb);
        btcPoller = new ServerPoller('bitcoin', 'mainnet', sourceDb, broadcaster, btcLog, config, testDb.util);
        ltcPoller = new ServerPoller('litecoin', 'mainnet', sourceDb, broadcaster, ltcLog, config, testDb.util);

        let app = express();
        app.use(cors({ origin: '*', methods: ['GET'] }));

        // Routes mirror src/api.js — namespaced by :dbType.
        app.get('/status/:dbType/:chain/:network', async (req, res) => {
            let lastBlock = await sourceDb.getLastBlock();
            let hashRow = lastBlock !== null ? await sourceDb.getBlockHashRow(lastBlock) : null;
            res.json({
                chain:   req.params.chain,
                network: req.params.network,
                dbType:  req.params.dbType,
                block_height: hashRow ? Number(hashRow.block_index) : null,
                block_time:   hashRow ? Number(hashRow.block_time)  : null,
                ledger_hash:  hashRow ? hashRow.ledger_hash         : null,
                actions_hash: hashRow ? hashRow.actions_hash        : null,
                contract_hash:hashRow ? hashRow.contract_hash       : null
            });
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

        app.get('/snapshot/:dbType/:chain/:network', async (req, res) => {
            await snapshotBuilder.streamFullSnapshot(sourceDb, res);
        });

        app.get('/snapshot/:dbType/:chain/:network/since/:blockHeight', async (req, res) => {
            let sinceBlock = parseInt(req.params.blockHeight);
            await snapshotBuilder.streamIncrementalSnapshot(sourceDb, sinceBlock, res);
        });

        httpServer = http.createServer(app);
        wss = new WebSocket.Server({ noServer: true });

        httpServer.on('upgrade', (request, socket, head) => {
            let match = request.url.match(/^\/subscribe\/([^\/]+)\/([^\/]+)\/([^\/\?]+)/);
            if (!match) { socket.destroy(); return; }
            let [, dbType, chain, network] = match;
            wss.handleUpgrade(request, socket, head, (ws) => {
                broadcaster.addSubscription(ws, request, chain, network, 'full', dbType);
            });
        });

        return new Promise(resolve => httpServer.listen(SERVER_PORT, async () => {
            btcPoller.lastPolledBlock = await sourceDb.getLastBlock();
            ltcPoller.lastPolledBlock = await sourceDb.getLastBlock();
            await btcPoller._updateStatus();
            await ltcPoller._updateStatus();
            resolve();
        }));
    }

    describe('7.1 Two chains bootstrap independently', function() {
        it('bootstraps from shared source for both chains', async function() {
            this.timeout(30000);

            // Seed source with 20 blocks
            await fixtures.seedBlocks(sourceDb, 1, 20);

            await startMultiChainServer();

            let serverUrl = 'http://127.0.0.1:' + SERVER_PORT;

            // Bootstrap bitcoin client
            let btcClient = new ClientProcess(replicaDb, serverUrl, 'bitcoin', 'mainnet');
            await btcClient.bootstrap();

            assert.strictEqual(await replicaDb.getLastBlock(), 20);
            assert.strictEqual(await testDb.getRowCount(replicaDb, 'blocks'), 20);

            btcClient.stop();
        });
    });

    describe('7.2 WebSocket subscriptions are chain-isolated', function() {
        it('receives events only for subscribed chain', async function() {
            this.timeout(15000);

            await fixtures.seedBlocks(sourceDb, 1, 5);
            await startMultiChainServer();

            let wsUrl = 'ws://127.0.0.1:' + SERVER_PORT;

            // Subscribe to bitcoin/mainnet
            let btcMessages = [];
            let btcWs = new WebSocket(wsUrl + '/subscribe/indexer/bitcoin/mainnet');
            btcWs.on('message', (data) => {
                btcMessages.push(JSON.parse(data.toString()));
            });

            // Subscribe to litecoin/mainnet
            let ltcMessages = [];
            let ltcWs = new WebSocket(wsUrl + '/subscribe/indexer/litecoin/mainnet');
            ltcWs.on('message', (data) => {
                ltcMessages.push(JSON.parse(data.toString()));
            });

            await new Promise(r => setTimeout(r, 1000));

            // Add blocks and poll only bitcoin
            await fixtures.seedBlocks(sourceDb, 6, 8);
            btcPoller.lastPolledBlock = 5;
            await btcPoller._poll();

            await new Promise(r => setTimeout(r, 1000));

            // Bitcoin should have received block events
            let btcBlockEvents = btcMessages.filter(m => m.type === 'block');
            assert.ok(btcBlockEvents.length >= 3, 'Bitcoin should have received 3 block events, got ' + btcBlockEvents.length);

            // Litecoin should NOT have received block events (only status at most)
            let ltcBlockEvents = ltcMessages.filter(m => m.type === 'block');
            assert.strictEqual(ltcBlockEvents.length, 0, 'Litecoin should not receive bitcoin block events');

            btcWs.close();
            ltcWs.close();
        });
    });

    describe('7.3 Reorg on one chain does not affect the other', function() {
        it('bitcoin reorg does not impact litecoin subscribers', async function() {
            this.timeout(15000);

            await fixtures.seedBlocks(sourceDb, 1, 10);
            await startMultiChainServer();

            let wsUrl = 'ws://127.0.0.1:' + SERVER_PORT;

            let ltcMessages = [];
            let ltcWs = new WebSocket(wsUrl + '/subscribe/indexer/litecoin/mainnet');
            ltcWs.on('message', (data) => {
                ltcMessages.push(JSON.parse(data.toString()));
            });

            await new Promise(r => setTimeout(r, 500));

            // Simulate bitcoin reorg
            await fixtures.deleteBlocksFrom(sourceDb, 8);
            btcPoller.lastPolledBlock = 10;
            await btcPoller._poll();

            await new Promise(r => setTimeout(r, 1000));

            // Litecoin should NOT receive reorg events
            let ltcReorgEvents = ltcMessages.filter(m => m.type === 'reorg');
            assert.strictEqual(ltcReorgEvents.length, 0, 'Litecoin should not receive bitcoin reorg');

            ltcWs.close();
        });
    });
});
