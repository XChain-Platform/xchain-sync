const assert    = require('assert');
const sinon     = require('sinon');
const http      = require('http');
const express   = require('express');
const cors      = require('cors');
const axios     = require('axios');
const zlib      = require('zlib');
const WebSocket = require('ws');
const config    = require('../../src/config');
const Utility   = require('../../src/utility');
const ServerPoller     = require('../../src/ServerPoller');
const BlockBroadcaster = require('../../src/BlockBroadcaster');
const TransparencyLog  = require('../../src/TransparencyLog');
const SnapshotBuilder  = require('../../src/SnapshotBuilder');
const HubClient        = require('../../src/HubClient');
const { getMariadb }   = require('../integration/helpers/mariadbLoader');
const { TestDatabase }  = require('../integration/helpers/testDb');
const fixtures = require('../integration/helpers/fixtures');
const MockHub  = require('../integration/helpers/mockHub');

const SMOKE_DB_NAME = 'xchain_smoke_test';
const SERVER_PORT   = 19600;
const HUB_PORT      = 19601;

const TEST_DB_HOST = process.env.TEST_DB_HOST || '192.168.2.5';
const TEST_DB_PORT = parseInt(process.env.TEST_DB_PORT) || 3306;
const TEST_DB_USER = process.env.TEST_DB_USER || 'xchain-node';
const TEST_DB_PASS = process.env.TEST_DB_PASS || 'XChain4life';

describe('Smoke: Server Mode', function() {

    let db, util, mockHub, broadcaster, poller, log, snapshotBuilder;
    let server, wss, baseUrl;

    before(async function() {
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
        util = new Utility();

        // Create test database with schema
        let mariadb = await getMariadb();
        let conn = await mariadb.createConnection({
            host: TEST_DB_HOST, port: TEST_DB_PORT,
            user: TEST_DB_USER, password: TEST_DB_PASS
        });
        await conn.query("CREATE DATABASE IF NOT EXISTS `" + SMOKE_DB_NAME + "`");
        await conn.end();

        let pool = mariadb.createPool({
            host: TEST_DB_HOST, port: TEST_DB_PORT,
            user: TEST_DB_USER, password: TEST_DB_PASS,
            database: SMOKE_DB_NAME,
            connectionLimit: 5, insertIdAsNumber: true, bigIntAsNumber: true
        });
        db = new TestDatabase(pool, SMOKE_DB_NAME);

        // Seed schema from indexer SQL files
        let path = require('path');
        let fs   = require('fs');
        let sqlDir = path.join(__dirname, '..', '..', '..', 'xchain-indexer', 'src', 'sql');
        let files = fs.readdirSync(sqlDir).filter(f => f.endsWith('.sql')).sort();
        let indexFiles = files.filter(f => f.startsWith('index_'));
        let otherFiles = files.filter(f => !f.startsWith('index_'));
        for (let file of [...indexFiles, ...otherFiles]) {
            let data = fs.readFileSync(path.join(sqlDir, file), 'utf8');
            for (let q of data.split(';').map(s => s.trim()).filter(s => s)) {
                try { await db.doQuery(q); } catch (e) {}
            }
        }
        // sync_meta
        let syncSql = fs.readFileSync(path.join(__dirname, '..', '..', 'src', 'sql', 'sync_meta.sql'), 'utf8');
        for (let q of syncSql.split(';').map(s => s.trim()).filter(s => s)) {
            try { await db.doQuery(q); } catch (e) {}
        }

        // Seed 3 blocks
        await fixtures.seedBlocks(db, 1, 3);

        // Start mock hub
        mockHub = new MockHub();
        mockHub.setConfigs([{
            coin: 'bitcoin', network: 'mainnet',
            db_host: TEST_DB_HOST, db_port: TEST_DB_PORT,
            db_name: SMOKE_DB_NAME, db_user: TEST_DB_USER, db_pass: TEST_DB_PASS
        }]);
        await mockHub.start(HUB_PORT);

        // Create server components
        let cfg = {
            WS_MAX_PER_IP: 3, WS_BACKPRESSURE_LIMIT: 50,
            BLOCK_POLL_INTERVAL: 100, SNAPSHOT_RATE_FULL: 100, SNAPSHOT_RATE_INCR: 100
        };
        broadcaster = new BlockBroadcaster(cfg);
        log = new TransparencyLog(db);
        poller = new ServerPoller('bitcoin', 'mainnet', db, broadcaster, log, cfg, util);
        snapshotBuilder = new SnapshotBuilder(util);

        // Build Express app + WS
        let app = express();
        app.use(cors({ origin: '*', methods: ['GET'] }));

        app.get('/status', async (req, res) => {
            let lastBlock = await db.getLastBlock();
            let hashRow = lastBlock !== null ? await db.getBlockHashRow(lastBlock) : null;
            res.json({
                bitcoin: { mainnet: {
                    block_height: hashRow ? Number(hashRow.block_index) : null,
                    ledger_hash: hashRow ? hashRow.ledger_hash : null,
                    actions_hash: hashRow ? hashRow.actions_hash : null,
                    contract_hash: hashRow ? hashRow.contract_hash : null
                }},
                last_updated: new Date().toISOString()
            });
        });

        app.get('/status/:dbType/:chain/:network', async (req, res) => {
            let lastBlock = await db.getLastBlock();
            let hashRow = lastBlock !== null ? await db.getBlockHashRow(lastBlock) : null;
            res.json({
                chain: req.params.chain, network: req.params.network,
                block_height: hashRow ? Number(hashRow.block_index) : null,
                ledger_hash: hashRow ? hashRow.ledger_hash : null
            });
        });

        app.get('/schema/:dbType/:chain/:network', async (req, res) => {
            let tables = await db.doQuery(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_type = 'BASE TABLE' ORDER BY table_name",
                [db.dbName]
            );
            let schema = {};
            for (let row of tables) {
                let tn = row.table_name || row.TABLE_NAME;
                let ddl = await db.doQuery("SHOW CREATE TABLE `" + tn + "`");
                if (ddl.length > 0) schema[tn] = ddl[0]['Create Table'];
            }
            res.json({ tables: schema });
        });

        app.get('/snapshot/:dbType/:chain/:network', async (req, res) => {
            await snapshotBuilder.streamFullSnapshot(db, res);
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

        await new Promise(resolve => server.listen(SERVER_PORT, resolve));
        baseUrl = 'http://127.0.0.1:' + SERVER_PORT;
    });

    after(async function() {
        sinon.restore();
        if (server) await new Promise(resolve => server.close(resolve));
        await mockHub.stop();
        let mariadb = await getMariadb();
        let conn = await mariadb.createConnection({
            host: TEST_DB_HOST, port: TEST_DB_PORT,
            user: TEST_DB_USER, password: TEST_DB_PASS
        });
        await conn.query("DROP DATABASE IF EXISTS `" + SMOKE_DB_NAME + "`");
        await conn.end();
        await db.close();
    });

    // Scenario 1: Config
    it('config loads with valid defaults', function() {
        let cfg = config.getConfig();
        assert.strictEqual(cfg.SYNC_MODE, 'server');
        assert.strictEqual(typeof cfg.SYNC_API_PORT, 'number');
        assert.strictEqual(typeof cfg.BLOCK_POLL_INTERVAL, 'number');
    });

    // Scenario 2: Hub connectivity
    it('hub is reachable', async function() {
        let hub = new HubClient('127.0.0.1', HUB_PORT);
        let alive = await hub.ping();
        assert.strictEqual(alive, true);
    });

    it('hub returns indexer configs', async function() {
        let hub = new HubClient('127.0.0.1', HUB_PORT);
        let configs = await hub.getIndexerConfigs();
        assert.ok(configs.length >= 1);
        assert.strictEqual(configs[0].coin, 'bitcoin');
    });

    // Scenario 3: Source DB connectivity
    it('source DB connection succeeds', async function() {
        let conn = await db.getConnection();
        assert.ok(conn);
        conn.release();
    });

    it('getLastBlock returns a value', async function() {
        let lastBlock = await db.getLastBlock();
        assert.strictEqual(typeof lastBlock, 'number');
        assert.ok(lastBlock >= 1);
    });

    it('getBlockHashRow returns hash fields', async function() {
        let lastBlock = await db.getLastBlock();
        let row = await db.getBlockHashRow(lastBlock);
        assert.ok(row);
        assert.ok(row.ledger_hash);
        assert.ok(row.actions_hash);
        assert.ok(row.contract_hash);
    });

    // Scenario 4: Basic poll cycle
    it('poll cycle completes without error', async function() {
        poller.lastPolledBlock = 0;
        await poller._poll();
        assert.ok(poller.lastPolledBlock >= 1);
    });

    // Scenario 5: REST API
    it('GET /status returns 200 with block data', async function() {
        let res = await axios.get(baseUrl + '/status');
        assert.strictEqual(res.status, 200);
        assert.ok(res.data.bitcoin);
        assert.ok(res.data.bitcoin.mainnet.block_height >= 1);
    });

    it('GET /schema returns tables', async function() {
        let res = await axios.get(baseUrl + '/schema/indexer/bitcoin/mainnet');
        assert.strictEqual(res.status, 200);
        assert.ok(res.data.tables);
        assert.ok(Object.keys(res.data.tables).length >= 10);
    });

    it('GET /snapshot returns decompressible data', async function() {
        let res = await axios.get(baseUrl + '/snapshot/indexer/bitcoin/mainnet', {
            responseType: 'arraybuffer', decompress: false
        });
        assert.strictEqual(res.status, 200);
        let json = zlib.gunzipSync(Buffer.from(res.data)).toString();
        let snapshot = JSON.parse(json);
        assert.ok(snapshot.block_height >= 1);
        assert.ok(snapshot.tables);
    });

    // Scenario 6: WebSocket
    it('WebSocket connection accepted and receives status', async function() {
        broadcaster.updateStatus('bitcoin', 'mainnet', { block_height: 3 });
        let ws = new WebSocket('ws://127.0.0.1:' + SERVER_PORT + '/subscribe/indexer/bitcoin/mainnet');
        let msg = await new Promise((resolve, reject) => {
            let timer = setTimeout(() => reject(new Error('WS timeout')), 5000);
            ws.on('message', (data) => { clearTimeout(timer); resolve(JSON.parse(data.toString())); });
            ws.on('error', reject);
        });
        ws.close();
        assert.strictEqual(msg.type, 'status');
        assert.strictEqual(msg.block_height, 3);
    });
});
