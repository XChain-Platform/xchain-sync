const assert   = require('assert');
const sinon    = require('sinon');
const http     = require('http');
const express  = require('express');
const cors     = require('cors');
const WebSocket = require('ws');
const setup    = require('./helpers/setup');
const testDb   = require('./helpers/testDb');
const fixtures = require('./helpers/fixtures');
const SnapshotBuilder = require('../../src/SnapshotBuilder');
const ClientSync      = require('../../src/ClientSync');
const ClientApplier   = require('../../src/ClientApplier');
const ClientRollback  = require('../../src/ClientRollback');
const HashVerifier    = require('../../src/HashVerifier');

const SERVER_PORT = 19300;

describe('Integration: Client Bootstrap', function() {

    let sourceDb, replicaDb, server, snapshotBuilder;

    before(async function() {
        await setup.globalSetup();
        sourceDb  = setup.getSourceDb();
        replicaDb = setup.getReplicaDb();

        snapshotBuilder = new SnapshotBuilder(testDb.util);

        let app = express();
        app.use(cors({ origin: '*', methods: ['GET'] }));

        app.get('/status/:dbType/:chain/:network', async (req, res) => {
            let lastBlock = await sourceDb.getLastBlock();
            let hashRow = lastBlock !== null ? await sourceDb.getBlockHashRow(lastBlock) : null;
            res.json({
                chain: req.params.chain, network: req.params.network,
                block_height: hashRow ? Number(hashRow.block_index) : null,
                ledger_hash: hashRow ? hashRow.ledger_hash : null,
                actions_hash: hashRow ? hashRow.actions_hash : null,
                contract_hash: hashRow ? hashRow.contract_hash : null
            });
        });

        app.get('/schema/:dbType/:chain/:network', async (req, res) => {
            let tables = await sourceDb.doQuery(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_type = 'BASE TABLE' ORDER BY table_name",
                [sourceDb.dbName]
            );
            let schema = {};
            for (let row of tables) {
                let tableName = row.table_name || row.TABLE_NAME;
                let ddlRows = await sourceDb.doQuery("SHOW CREATE TABLE `" + tableName + "`");
                if (ddlRows.length > 0) schema[tableName] = ddlRows[0]['Create Table'];
            }
            res.json({ chain: req.params.chain, network: req.params.network, tables: schema });
        });

        app.get('/snapshot/:dbType/:chain/:network', async (req, res) => {
            await snapshotBuilder.streamFullSnapshot(sourceDb, res);
        });

        app.get('/snapshot/:dbType/:chain/:network/since/:blockHeight', async (req, res) => {
            let sinceBlock = parseInt(req.params.blockHeight);
            await snapshotBuilder.streamIncrementalSnapshot(sourceDb, sinceBlock, res);
        });

        server = http.createServer(app);

        let wss = new WebSocket.Server({ noServer: true });
        server.on('upgrade', (request, socket, head) => {
            let match = request.url.match(/^\/subscribe\/([^\/]+)\/([^\/]+)\/([^\/\?]+)/);
            if (!match) { socket.destroy(); return; }
            wss.handleUpgrade(request, socket, head, () => {});
        });

        await new Promise(resolve => server.listen(SERVER_PORT, resolve));

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

    function createClientSync() {
        let config = {
            SYNC_SOURCES: 'http://127.0.0.1:' + SERVER_PORT,
            VERIFY_HASHES: false,
            CLIENT_RECONNECT_DELAY: 5000,
            HASH_CONFIRM_TIMEOUT: 5000
        };
        let applier  = new ClientApplier(replicaDb, testDb.util);
        let rollback = new ClientRollback(replicaDb, testDb.util);
        let verifier = new HashVerifier();
        return new ClientSync('bitcoin', 'mainnet', replicaDb, applier, rollback, verifier, config, testDb.util);
    }

    describe('full bootstrap from snapshot', function() {
        it('replicates all block data to replica', async function() {
            this.timeout(15000);
            await fixtures.seedBlocks(sourceDb, 1, 10);

            let cs = createClientSync();
            await cs._bootstrapFromSnapshot();

            let sourceBlockCount  = await testDb.getRowCount(sourceDb, 'blocks');
            let replicaBlockCount = await testDb.getRowCount(replicaDb, 'blocks');
            assert.strictEqual(replicaBlockCount, sourceBlockCount);
            assert.strictEqual(replicaBlockCount, 10);

            let sourceTxCount  = await testDb.getRowCount(sourceDb, 'transactions');
            let replicaTxCount = await testDb.getRowCount(replicaDb, 'transactions');
            assert.strictEqual(replicaTxCount, sourceTxCount);

            let sourceCreditCount  = await testDb.getRowCount(sourceDb, 'credits');
            let replicaCreditCount = await testDb.getRowCount(replicaDb, 'credits');
            assert.strictEqual(replicaCreditCount, sourceCreditCount);
        });

        it('replicates index tables correctly', async function() {
            this.timeout(15000);
            await fixtures.seedBlocks(sourceDb, 1, 5);

            let cs = createClientSync();
            await cs._bootstrapFromSnapshot();

            let sourceAddrCount  = await testDb.getRowCount(sourceDb, 'index_addresses');
            let replicaAddrCount = await testDb.getRowCount(replicaDb, 'index_addresses');
            assert.strictEqual(replicaAddrCount, sourceAddrCount);

            let sourceTxIdxCount  = await testDb.getRowCount(sourceDb, 'index_transactions');
            let replicaTxIdxCount = await testDb.getRowCount(replicaDb, 'index_transactions');
            assert.strictEqual(replicaTxIdxCount, sourceTxIdxCount);
        });

        it('preserves exact data values (no type coercion)', async function() {
            this.timeout(15000);
            await fixtures.seedBlocks(sourceDb, 1, 1, { creditAmount: '99999999999999999' });

            let cs = createClientSync();
            await cs._bootstrapFromSnapshot();

            let credits = await replicaDb.doQuery("SELECT amount FROM credits");
            assert.strictEqual(credits.length, 1);
            assert.strictEqual(credits[0].amount, '99999999999999999');
        });
    });

    describe('incremental catch-up', function() {
        it('applies only new blocks', async function() {
            this.timeout(15000);
            await fixtures.seedBlocks(sourceDb, 1, 5);
            let cs = createClientSync();
            await cs._bootstrapFromSnapshot();

            let countAfterBootstrap = await testDb.getRowCount(replicaDb, 'blocks');
            assert.strictEqual(countAfterBootstrap, 5);

            await fixtures.seedBlocks(sourceDb, 6, 10);
            await cs._incrementalCatchUp(6);

            let countAfterCatchUp = await testDb.getRowCount(replicaDb, 'blocks');
            assert.strictEqual(countAfterCatchUp, 10);

            let block1 = await replicaDb.doQuery("SELECT * FROM blocks WHERE block_index = 1");
            assert.strictEqual(block1.length, 1);
        });
    });

    describe('startup catch-up (start)', function() {
        // Regression test for the off-by-one at ClientSync.start(): the startup
        // resume path must request lastAppliedBlock + 1. Passing lastAppliedBlock
        // re-delivers the last applied block's rows (server uses inclusive >=
        // bounds), and the non-ignore INSERT throws on the UNIQUE action_index,
        // rolling back the catch-up and freezing the replica at its old height.
        it('resumes a pre-populated replica without re-inserting applied rows', async function() {
            this.timeout(15000);

            // Replica already has blocks 1-5 (e.g. from a prior run before restart).
            await fixtures.seedBlocks(sourceDb, 1, 5);
            let cs = createClientSync();
            await cs._bootstrapFromSnapshot();
            assert.strictEqual(await testDb.getRowCount(replicaDb, 'blocks'), 5);

            // New blocks land on the source while the client is down.
            await fixtures.seedBlocks(sourceDb, 6, 10);

            // Restart: a fresh ClientSync whose start() reads lastAppliedBlock from
            // the populated replica and runs the startup incremental catch-up.
            let cs2 = createClientSync();
            sinon.stub(cs2, '_connectWebSockets').callsFake(() => { cs2.running = false; });
            await cs2.start();

            // Catch-up must complete to the source tip — with the off-by-one the
            // duplicate-key error would roll back and leave the replica stuck at 5.
            assert.strictEqual(cs2.lastAppliedBlock, 10);
            assert.strictEqual(await testDb.getRowCount(replicaDb, 'blocks'), 10);

            // No duplicate action rows — replica matches source exactly.
            assert.strictEqual(
                await testDb.getRowCount(replicaDb, 'actions'),
                await testDb.getRowCount(sourceDb, 'actions')
            );
        });
    });
});
