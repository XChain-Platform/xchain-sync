const assert   = require('assert');
const sinon    = require('sinon');
const http     = require('http');
const zlib     = require('zlib');
const express  = require('express');
const cors     = require('cors');
const axios    = require('axios');
const setup    = require('./helpers/setup');
const testDb   = require('./helpers/testDb');
const fixtures = require('./helpers/fixtures');
const MockHub  = require('./helpers/mockHub');
const SnapshotBuilder  = require('../../src/SnapshotBuilder');
const TransparencyLog  = require('../../src/TransparencyLog');
const BlockBroadcaster = require('../../src/BlockBroadcaster');
const ServerPoller     = require('../../src/ServerPoller');

const API_PORT = 19100;
const HUB_PORT = 19000;

describe('Integration: REST API', function() {

    let sourceDb, mockHub, server, baseUrl, snapshotBuilder, log;

    before(async function() {
        await setup.globalSetup();
        sourceDb = setup.getSourceDb();

        // Start mock hub
        mockHub = new MockHub();
        mockHub.setConfigs([{
            coin: 'bitcoin', network: 'mainnet',
            db_host: testDb.TEST_DB_HOST, db_port: testDb.TEST_DB_PORT,
            db_name: testDb.SOURCE_DB_NAME, db_user: testDb.TEST_DB_USER, db_pass: testDb.TEST_DB_PASS
        }]);
        await mockHub.start(HUB_PORT);

        snapshotBuilder = new SnapshotBuilder(testDb.util);
        log = new TransparencyLog(sourceDb);

        // Build Express app manually (avoids importing api.js which has side effects)
        let app = express();
        app.use(cors({ origin: '*', methods: ['GET'] }));

        app.get('/status', async (req, res) => {
            try {
                let lastBlock = await sourceDb.getLastBlock();
                let hashRow = lastBlock !== null ? await sourceDb.getBlockHashRow(lastBlock) : null;
                let result = {
                    bitcoin: {
                        mainnet: {
                            block_height: hashRow ? Number(hashRow.block_index) : null,
                            block_time: hashRow ? Number(hashRow.block_time) : null,
                            ledger_hash: hashRow ? hashRow.ledger_hash : null,
                            actions_hash: hashRow ? hashRow.actions_hash : null,
                            contract_hash: hashRow ? hashRow.contract_hash : null
                        }
                    },
                    last_updated: new Date().toISOString()
                };
                res.json(result);
            } catch (e) { res.status(500).json({ error: e.message }); }
        });

        app.get('/status/:dbType/:chain/:network', async (req, res) => {
            let { chain, network } = req.params;
            if (chain !== 'bitcoin' || network !== 'mainnet')
                return res.status(404).json({ error: 'Chain/network not found' });
            try {
                let lastBlock = await sourceDb.getLastBlock();
                let hashRow = lastBlock !== null ? await sourceDb.getBlockHashRow(lastBlock) : null;
                res.json({
                    chain, network,
                    block_height: hashRow ? Number(hashRow.block_index) : null,
                    block_time: hashRow ? Number(hashRow.block_time) : null,
                    ledger_hash: hashRow ? hashRow.ledger_hash : null,
                    actions_hash: hashRow ? hashRow.actions_hash : null,
                    contract_hash: hashRow ? hashRow.contract_hash : null
                });
            } catch (e) { res.status(500).json({ error: e.message }); }
        });

        app.get('/schema/:dbType/:chain/:network', async (req, res) => {
            try {
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
            } catch (e) { res.status(500).json({ error: e.message }); }
        });

        app.get('/snapshot/:dbType/:chain/:network', async (req, res) => {
            try {
                await snapshotBuilder.streamFullSnapshot(sourceDb, res);
            } catch (e) {
                if (!res.headersSent) res.status(500).json({ error: e.message });
            }
        });

        app.get('/snapshot/:dbType/:chain/:network/since/:blockHeight', async (req, res) => {
            let sinceBlock = parseInt(req.params.blockHeight);
            if (isNaN(sinceBlock) || sinceBlock < 0)
                return res.status(400).json({ error: 'Invalid blockHeight' });
            try {
                await snapshotBuilder.streamIncrementalSnapshot(sourceDb, sinceBlock, res);
            } catch (e) {
                if (!res.headersSent) res.status(500).json({ error: e.message });
            }
        });

        app.get('/transparency/:dbType/:chain/:network/roots', async (req, res) => {
            try {
                let page  = parseInt(req.query.page) || 0;
                let limit = parseInt(req.query.limit) || 100;
                let result = await log.getPage(page, limit);
                res.json(result);
            } catch (e) { res.status(500).json({ error: e.message }); }
        });

        server = http.createServer(app);
        await new Promise(resolve => server.listen(API_PORT, resolve));
        baseUrl = 'http://127.0.0.1:' + API_PORT;

        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });

    after(async function() {
        sinon.restore();
        await new Promise(resolve => server.close(resolve));
        await mockHub.stop();
        await setup.globalTeardown();
    });

    beforeEach(async function() {
        await testDb.truncateAll(sourceDb);
    });

    describe('GET /status', function() {
        it('returns status for all chains', async function() {
            await fixtures.seedBlocks(sourceDb, 1, 3);
            let res = await axios.get(baseUrl + '/status');
            assert.strictEqual(res.status, 200);
            assert.ok(res.data.bitcoin);
            assert.ok(res.data.bitcoin.mainnet);
            assert.strictEqual(res.data.bitcoin.mainnet.block_height, 3);
            assert.ok(res.data.bitcoin.mainnet.ledger_hash);
            assert.ok(res.data.last_updated);
        });

        it('returns null values when no blocks', async function() {
            let res = await axios.get(baseUrl + '/status');
            assert.strictEqual(res.data.bitcoin.mainnet.block_height, null);
        });
    });

    describe('GET /status/:dbType/:chain/:network', function() {
        it('returns status for specific chain', async function() {
            await fixtures.seedBlocks(sourceDb, 1, 5);
            let res = await axios.get(baseUrl + '/status/indexer/bitcoin/mainnet');
            assert.strictEqual(res.status, 200);
            assert.strictEqual(res.data.block_height, 5);
            assert.strictEqual(res.data.chain, 'bitcoin');
            assert.strictEqual(res.data.network, 'mainnet');
        });

        it('returns 404 for unknown chain', async function() {
            try {
                await axios.get(baseUrl + '/status/indexer/unknown/chain');
                assert.fail('Should have thrown');
            } catch (e) {
                assert.strictEqual(e.response.status, 404);
            }
        });
    });

    describe('GET /schema/:dbType/:chain/:network', function() {
        it('returns table DDLs', async function() {
            let res = await axios.get(baseUrl + '/schema/indexer/bitcoin/mainnet');
            assert.strictEqual(res.status, 200);
            assert.ok(res.data.tables);
            assert.ok(res.data.tables.blocks);
            assert.ok(res.data.tables.blocks.includes('CREATE TABLE'));
            assert.ok(res.data.tables.transactions);
            assert.ok(Object.keys(res.data.tables).length >= 10);
        });
    });

    describe('GET /snapshot/:dbType/:chain/:network', function() {
        it('returns gzip-compressed full snapshot', async function() {
            await fixtures.seedBlocks(sourceDb, 1, 5);
            let res = await axios.get(baseUrl + '/snapshot/indexer/bitcoin/mainnet', {
                responseType: 'arraybuffer',
                decompress: false
            });
            assert.strictEqual(res.status, 200);

            let json = zlib.gunzipSync(Buffer.from(res.data)).toString();
            let snapshot = JSON.parse(json);

            assert.strictEqual(snapshot.block_height, 5);
            assert.ok(snapshot.tables);
            assert.ok(snapshot.tables.blocks);
            assert.strictEqual(snapshot.tables.blocks.length, 5);
            assert.ok(snapshot.tables.transactions);
            assert.ok(snapshot.tables.credits);
        });

        it('returns 404 when no blocks', async function() {
            let res = await axios.get(baseUrl + '/snapshot/indexer/bitcoin/mainnet', {
                validateStatus: () => true
            });
            assert.strictEqual(res.status, 404);
        });
    });

    describe('GET /snapshot/:dbType/:chain/:network/since/:blockHeight', function() {
        it('returns incremental snapshot', async function() {
            await fixtures.seedBlocks(sourceDb, 1, 10);
            let res = await axios.get(baseUrl + '/snapshot/indexer/bitcoin/mainnet/since/6', {
                responseType: 'arraybuffer',
                decompress: false
            });

            let json = zlib.gunzipSync(Buffer.from(res.data)).toString();
            let snapshot = JSON.parse(json);

            assert.strictEqual(snapshot.block_height, 10);
            assert.strictEqual(snapshot.since_block, 6);
            assert.ok(snapshot.tables);
            if (snapshot.tables.blocks) {
                assert.strictEqual(snapshot.tables.blocks.length, 5);
            }
        });

        it('returns 400 for invalid blockHeight', async function() {
            try {
                await axios.get(baseUrl + '/snapshot/indexer/bitcoin/mainnet/since/abc');
                assert.fail('Should have thrown');
            } catch (e) {
                assert.strictEqual(e.response.status, 400);
            }
        });
    });

    describe('GET /transparency/:dbType/:chain/:network/roots', function() {
        it('returns paginated transparency log', async function() {
            for (let i = 1; i <= 20; i++) {
                await log.recordBlock(i, 1700000000 + i, 'lh' + i, 'ah' + i, 'ch' + i);
            }

            let res = await axios.get(baseUrl + '/transparency/indexer/bitcoin/mainnet/roots?page=0&limit=10');
            assert.strictEqual(res.status, 200);
            assert.strictEqual(res.data.total, 20);
            assert.strictEqual(res.data.results.length, 10);
            assert.strictEqual(res.data.page, 0);
            assert.strictEqual(res.data.limit, 10);
        });
    });
});
