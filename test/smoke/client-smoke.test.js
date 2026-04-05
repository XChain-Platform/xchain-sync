const assert   = require('assert');
const sinon    = require('sinon');
const http     = require('http');
const express  = require('express');
const cors     = require('cors');
const WebSocket = require('ws');
const Utility  = require('../../src/utility');
const ClientApplier   = require('../../src/ClientApplier');
const ClientRollback  = require('../../src/ClientRollback');
const ClientSync      = require('../../src/ClientSync');
const HashVerifier    = require('../../src/HashVerifier');
const SnapshotBuilder = require('../../src/SnapshotBuilder');
const { getMariadb }  = require('../integration/helpers/mariadbLoader');
const { TestDatabase } = require('../integration/helpers/testDb');
const fixtures = require('../integration/helpers/fixtures');

const SMOKE_SOURCE_DB  = 'xchain_smoke_source';
const SMOKE_REPLICA_DB = 'xchain_smoke_replica';
const SERVER_PORT = 19700;

const TEST_DB_HOST = process.env.TEST_DB_HOST || '192.168.2.5';
const TEST_DB_PORT = parseInt(process.env.TEST_DB_PORT) || 3306;
const TEST_DB_USER = process.env.TEST_DB_USER || 'xchain-node';
const TEST_DB_PASS = process.env.TEST_DB_PASS || 'XChain4life';

describe('Smoke: Client Mode', function() {

    let sourceDb, replicaDb, util, server, snapshotBuilder;

    before(async function() {
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
        util = new Utility();

        let mariadb = await getMariadb();
        let path = require('path');
        let fs   = require('fs');

        // Create source and replica databases
        let conn = await mariadb.createConnection({
            host: TEST_DB_HOST, port: TEST_DB_PORT,
            user: TEST_DB_USER, password: TEST_DB_PASS
        });
        await conn.query("CREATE DATABASE IF NOT EXISTS `" + SMOKE_SOURCE_DB + "`");
        await conn.query("CREATE DATABASE IF NOT EXISTS `" + SMOKE_REPLICA_DB + "`");
        await conn.end();

        let sourcePool = mariadb.createPool({
            host: TEST_DB_HOST, port: TEST_DB_PORT,
            user: TEST_DB_USER, password: TEST_DB_PASS,
            database: SMOKE_SOURCE_DB,
            connectionLimit: 5, insertIdAsNumber: true, bigIntAsNumber: true
        });
        sourceDb = new TestDatabase(sourcePool, SMOKE_SOURCE_DB);

        let replicaPool = mariadb.createPool({
            host: TEST_DB_HOST, port: TEST_DB_PORT,
            user: TEST_DB_USER, password: TEST_DB_PASS,
            database: SMOKE_REPLICA_DB,
            connectionLimit: 5, insertIdAsNumber: true, bigIntAsNumber: true
        });
        replicaDb = new TestDatabase(replicaPool, SMOKE_REPLICA_DB);

        // Seed schema on both databases
        let sqlDir = path.join(__dirname, '..', '..', '..', 'xchain-indexer', 'src', 'sql');
        let files = fs.readdirSync(sqlDir).filter(f => f.endsWith('.sql')).sort();
        let indexFiles = files.filter(f => f.startsWith('index_'));
        let otherFiles = files.filter(f => !f.startsWith('index_'));
        for (let file of [...indexFiles, ...otherFiles]) {
            let data = fs.readFileSync(path.join(sqlDir, file), 'utf8');
            for (let q of data.split(';').map(s => s.trim()).filter(s => s)) {
                try { await sourceDb.doQuery(q); } catch (e) {}
                try { await replicaDb.doQuery(q); } catch (e) {}
            }
        }
        let syncSql = fs.readFileSync(path.join(__dirname, '..', '..', 'src', 'sql', 'sync_meta.sql'), 'utf8');
        for (let q of syncSql.split(';').map(s => s.trim()).filter(s => s)) {
            try { await sourceDb.doQuery(q); } catch (e) {}
            try { await replicaDb.doQuery(q); } catch (e) {}
        }

        // Seed 3 blocks in source
        await fixtures.seedBlocks(sourceDb, 1, 3);

        // Start a minimal server for snapshot/schema endpoints
        snapshotBuilder = new SnapshotBuilder(util);
        let app = express();
        app.use(cors({ origin: '*', methods: ['GET'] }));

        app.get('/schema/:chain/:network', async (req, res) => {
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

        app.get('/snapshot/:chain/:network', async (req, res) => {
            await snapshotBuilder.streamFullSnapshot(sourceDb, res);
        });

        app.get('/status/:chain/:network', async (req, res) => {
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
        let wss = new WebSocket.Server({ noServer: true });
        server.on('upgrade', (request, socket, head) => {
            let match = request.url.match(/^\/subscribe\/([^\/]+)\/([^\/\?]+)/);
            if (!match) { socket.destroy(); return; }
            wss.handleUpgrade(request, socket, head, () => {});
        });

        await new Promise(resolve => server.listen(SERVER_PORT, resolve));
    });

    after(async function() {
        sinon.restore();
        if (server) await new Promise(resolve => server.close(resolve));
        let mariadb = await getMariadb();
        let conn = await mariadb.createConnection({
            host: TEST_DB_HOST, port: TEST_DB_PORT,
            user: TEST_DB_USER, password: TEST_DB_PASS
        });
        await conn.query("DROP DATABASE IF EXISTS `" + SMOKE_SOURCE_DB + "`");
        await conn.query("DROP DATABASE IF EXISTS `" + SMOKE_REPLICA_DB + "`");
        await conn.end();
        await sourceDb.close();
        await replicaDb.close();
    });

    // Scenario 7: Replica DB connectivity
    it('replica DB connection succeeds', async function() {
        let conn = await replicaDb.getConnection();
        assert.ok(conn);
        conn.release();
    });

    it('sync_meta table exists in replica', async function() {
        let rows = await replicaDb.doQuery(
            "SELECT * FROM information_schema.tables WHERE table_schema = ? AND table_name = 'sync_meta'",
            [SMOKE_REPLICA_DB]
        );
        assert.strictEqual(rows.length, 1);
    });

    // Scenario 8: Schema check
    it('replica has indexer tables', async function() {
        let rows = await replicaDb.doQuery(
            "SELECT COUNT(*) as cnt FROM information_schema.tables WHERE table_schema = ? AND table_type = 'BASE TABLE'",
            [SMOKE_REPLICA_DB]
        );
        assert.ok(Number(rows[0].cnt) >= 10);
    });

    // Scenario 9: Snapshot download
    it('snapshot is downloadable and parseable', async function() {
        let axios = require('axios');
        let zlib  = require('zlib');
        let res = await axios.get('http://127.0.0.1:' + SERVER_PORT + '/snapshot/bitcoin/mainnet', {
            responseType: 'arraybuffer', decompress: false
        });
        assert.strictEqual(res.status, 200);
        let json = zlib.gunzipSync(Buffer.from(res.data)).toString();
        let snapshot = JSON.parse(json);
        assert.ok(snapshot.block_height >= 1);
        assert.ok(snapshot.tables);
        assert.ok(snapshot.tables.blocks);
    });

    // Scenario 10: Block apply
    it('ClientApplier applies a block without error', async function() {
        let applier = new ClientApplier(replicaDb, util);

        // Build a minimal block payload
        let payload = {
            block_index: 99,
            data: {
                blocks: [{ block_index: 99, block_time: 1700000099 }],
                transactions: [{ tx_index: 990, block_index: 99, tx_hash_id: 1, source_id: 1 }]
            }
        };

        await applier.applyBlock(payload);

        let lastBlock = await replicaDb.getLastBlock();
        assert.strictEqual(lastBlock, 99);
    });

    it('full bootstrap populates replica', async function() {
        this.timeout(15000);

        // Truncate replica first
        let tables = await replicaDb.doQuery(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_type = 'BASE TABLE'",
            [SMOKE_REPLICA_DB]
        );
        await replicaDb.doQuery("SET FOREIGN_KEY_CHECKS = 0");
        for (let row of tables) {
            let tn = row.table_name || row.TABLE_NAME;
            await replicaDb.doQuery("TRUNCATE TABLE `" + tn + "`");
        }
        await replicaDb.doQuery("SET FOREIGN_KEY_CHECKS = 1");

        // Bootstrap
        let applier    = new ClientApplier(replicaDb, util);
        let rollback   = new ClientRollback(replicaDb, util);
        let verifier   = new HashVerifier();
        let cfg = {
            SYNC_SOURCES: 'http://127.0.0.1:' + SERVER_PORT,
            VERIFY_HASHES: false,
            CLIENT_RECONNECT_DELAY: 5000,
            HASH_CONFIRM_TIMEOUT: 5000
        };
        let cs = new ClientSync('bitcoin', 'mainnet', replicaDb, applier, rollback, verifier, cfg, util);
        await cs._bootstrapFromSnapshot();

        let replicaBlockCount = await replicaDb.doQuery("SELECT COUNT(*) as cnt FROM blocks");
        assert.strictEqual(Number(replicaBlockCount[0].cnt), 3);

        let replicaLastBlock = await replicaDb.getLastBlock();
        assert.strictEqual(replicaLastBlock, 3);
    });
});
