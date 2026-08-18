/*********************************************************************
 *
 * Copyright © 2025–2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC – https://dankest.llc
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This file is part of XChain Platform. Licensed under the GNU Affero
 * General Public License v3.0 or later; see LICENSE.md. A commercial
 * license (without AGPL source-disclosure terms) is available -
 * contact legal@dankest.llc.
 *
 **********************************************************************
 *
 * E2E: Decoder DB lifecycle (bootstrap + live sync)
 *
 * Exercises the Phase 1-3 decoder sync path end-to-end against real
 * MariaDB containers (test/e2e/docker-compose.e2e.yml). The server side
 * uses the real ServerPoller + BlockBroadcaster + SnapshotBuilder; the
 * client side uses the real ClientSync + ClientApplier. The HTTP+WS
 * surface in between mirrors the /:dbType/ route shape from src/api.js.
 *
 * Covered:
 *   - GET /status/decoder/:chain/:network returns block_hash (not
 *     ledger_hash/actions_hash/contract_hash).
 *   - GET /transparency/decoder/:chain/:network/roots => 400 (decoder
 *     has no transparency log).
 *   - Cold bootstrap: replica receives full snapshot, blocks +
 *     transactions + transaction_outputs + events + pubkeys match source.
 *   - Live sync: new source blocks reach replica via WebSocket; payload
 *     carries block_hash and no ledger/actions/contract_hash.
 *   - Incremental snapshot: replica catches up from a since-block,
 *     including tx-scoped tables (transaction_outputs) and events/pubkeys.
 *   - Reorg / rollback: blocks + tx-scoped rows roll back while
 *     append-only index tables stay intact.
 *
 ********************************************************************/

const assert    = require('assert');
const http      = require('http');
const express   = require('express');
const cors      = require('cors');
const { parseCorsOrigin } = require('../../src/corsOrigin');
const WebSocket = require('ws');
const axios     = require('axios');
const sinon     = require('sinon');

const Database         = require('../../src/db');
const ServerPoller     = require('../../src/ServerPoller');
const BlockBroadcaster = require('../../src/BlockBroadcaster');
const SnapshotBuilder  = require('../../src/SnapshotBuilder');
const ClientSync       = require('../../src/ClientSync');
const ClientApplier    = require('../../src/ClientApplier');
const ClientRollback   = require('../../src/ClientRollback');
const HashVerifier     = require('../../src/HashVerifier');
const Utility          = require('../../src/utility');

const decoderFixtures = require('./helpers/decoderFixtures');
const { waitFor }     = require('./helpers/waitFor');
const { getMariadb }  = require('./helpers/mariadbLoader');

const SOURCE_HOST  = process.env.E2E_DB_HOST         || '127.0.0.1';
const SOURCE_PORT  = parseInt(process.env.E2E_DB_PORT) || 23306;
const REPLICA_HOST = process.env.E2E_REPLICA_DB_HOST  || '127.0.0.1';
const REPLICA_PORT = parseInt(process.env.E2E_REPLICA_DB_PORT) || 23307;
const DB_USER      = process.env.E2E_DB_USER          || 'xchain-node';
const DB_PASS      = process.env.E2E_DB_PASS          || 'xchain-fixture-throwaway';

const SOURCE_DB_NAME  = 'xchain_e2e_decoder_source';
const REPLICA_DB_NAME = 'xchain_e2e_decoder_replica';

const SERVER_PORT = 29250;
const CHAIN       = 'bitcoin';
const NETWORK     = 'regtest';
const DB_TYPE     = 'decoder';

const util = new Utility();

// Build the mini HTTP+WS server that mirrors src/api.js for decoder
// surface. Reuses real BlockBroadcaster + SnapshotBuilder + ServerPoller
// so the test exercises actual Phase 3 code paths.
function buildServer(sourceDb, broadcaster, snapshotBuilder){
    let app = express();
    app.use(cors({ origin: parseCorsOrigin(process.env.CORS_ORIGIN), methods: ['GET'] }));

    let validateDbType = (dt) => (dt === 'indexer' || dt === 'decoder') ? dt : null;

    app.get('/status/:dbType/:chain/:network', async (req, res) => {
        let dbType = validateDbType(req.params.dbType);
        if(!dbType) return res.status(400).json({ error: 'Invalid dbType' });
        if(req.params.chain !== CHAIN || req.params.network !== NETWORK)
            return res.status(404).json({ error: 'Chain/network not found' });
        try {
            let last = await sourceDb.getLastBlock();
            let row  = last !== null ? await sourceDb.getBlockHashRow(last) : null;
            let body = {
                chain: req.params.chain,
                network: req.params.network,
                dbType: dbType,
                block_height: row ? Number(row.block_index) : null,
                block_time:   row ? Number(row.block_time)  : null
            };
            if(dbType === 'decoder'){
                body.block_hash = row ? row.block_hash : null;
            } else {
                body.ledger_hash   = row ? row.ledger_hash   : null;
                body.actions_hash  = row ? row.actions_hash  : null;
                body.contract_hash = row ? row.contract_hash : null;
            }
            res.json(body);
        } catch(e){
            res.status(500).json({ error: e.message });
        }
    });

    app.get('/schema/:dbType/:chain/:network', async (req, res) => {
        try {
            let tables = await sourceDb.doQuery(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_type = 'BASE TABLE' ORDER BY table_name",
                [sourceDb.dbName]
            );
            let schema = {};
            for(let row of tables){
                let tn = row.table_name || row.TABLE_NAME;
                let ddl = await sourceDb.doQuery("SHOW CREATE TABLE `" + tn + "`");
                if(ddl.length > 0) schema[tn] = ddl[0]['Create Table'];
            }
            res.json({ chain: req.params.chain, network: req.params.network, dbType: req.params.dbType, tables: schema });
        } catch(e){
            res.status(500).json({ error: e.message });
        }
    });

    app.get('/snapshot/:dbType/:chain/:network', async (req, res) => {
        try {
            await snapshotBuilder.streamFullSnapshot(sourceDb, res);
        } catch(e){
            if(!res.headersSent) res.status(500).json({ error: e.message });
        }
    });

    app.get('/snapshot/:dbType/:chain/:network/since/:blockHeight', async (req, res) => {
        let since = parseInt(req.params.blockHeight);
        if(isNaN(since) || since < 0) return res.status(400).json({ error: 'Invalid blockHeight' });
        try {
            await snapshotBuilder.streamIncrementalSnapshot(sourceDb, since, res);
        } catch(e){
            if(!res.headersSent) res.status(500).json({ error: e.message });
        }
    });

    // Transparency is indexer-only; decoder requests must return 400.
    app.get('/transparency/:dbType/:chain/:network/roots', (req, res) => {
        if(req.params.dbType !== 'indexer')
            return res.status(400).json({ error: 'Transparency log is indexer-only' });
        res.json({ entries: [] });
    });

    let server = http.createServer(app);
    let wss    = new WebSocket.Server({ noServer: true });

    server.on('upgrade', (request, socket, head) => {
        let m = request.url.match(/^\/subscribe\/([^\/]+)\/([^\/]+)\/([^\/\?]+)/);
        if(!m){ socket.destroy(); return; }
        let [, dbType, chain, network] = m;
        if(dbType !== 'indexer' && dbType !== 'decoder'){ socket.destroy(); return; }
        wss.handleUpgrade(request, socket, head, (ws) => {
            broadcaster.addSubscription(ws, request, chain, network, 'full', dbType);
        });
    });

    return server;
}

describe('E2E: Decoder DB Lifecycle', function() {

    let sourceDb, replicaDb;
    let broadcaster, snapshotBuilder, poller, server, pollInterval;
    let client;

    before(async function() {
        this.timeout(30000);

        // Create both databases via an admin account BEFORE constructing any
        // src/db.js Database. That class' constructor eagerly opens a pool
        // bound to the database name, and if the DB doesn't exist yet the pool
        // drives a retry storm that exhausts MariaDB's connection slots. The
        // MARIADB_USER user has no global CREATE/GRANT privilege; defaults
        // match the compose containers (root / MARIADB_ROOT_PASSWORD=test),
        // overridable with E2E_DB_ADMIN_USER / E2E_DB_ADMIN_PASS like
        // helpers/testDb.js createDatabase.
        let mariadb = await getMariadb();
        let adminUser = process.env.E2E_DB_ADMIN_USER || 'root';
        let adminPass = process.env.E2E_DB_ADMIN_PASS !== undefined ? process.env.E2E_DB_ADMIN_PASS : 'test';
        let provisionFor = async (host, port, dbName) => {
            let conn = await mariadb.createConnection({ host, port, user: adminUser, password: adminPass });
            await conn.query("CREATE DATABASE IF NOT EXISTS `" + dbName + "`");
            if (DB_USER !== adminUser) {
                await conn.query("GRANT ALL PRIVILEGES ON `" + dbName + "`.* TO `" + DB_USER + "`@'%'");
                await conn.query("FLUSH PRIVILEGES");
            }
            await conn.end();
        };
        await provisionFor(SOURCE_HOST,  SOURCE_PORT,  SOURCE_DB_NAME);
        await provisionFor(REPLICA_HOST, REPLICA_PORT, REPLICA_DB_NAME);

        sourceDb  = new Database(SOURCE_HOST,  SOURCE_PORT,  SOURCE_DB_NAME,  DB_USER, DB_PASS, util, 'decoder');
        replicaDb = new Database(REPLICA_HOST, REPLICA_PORT, REPLICA_DB_NAME, DB_USER, DB_PASS, util, 'decoder');

        await decoderFixtures.seedDecoderSchema(sourceDb);
        await decoderFixtures.seedDecoderSchema(replicaDb);

        // Mute chatty src/ console output; unstub when debugging a hook/setup failure.
        if(!process.env.E2E_DEBUG){
            sinon.stub(console, 'log');
            sinon.stub(console, 'error');
        }
    });

    after(async function() {
        sinon.restore();
        if(client) client.stop();
        if(pollInterval) clearInterval(pollInterval);
        if(poller) poller.stop();
        if(server) await new Promise(r => server.close(r));
        if(sourceDb)  await sourceDb.close();
        if(replicaDb) await replicaDb.close();
    });

    beforeEach(async function() {
        this.timeout(15000);
        if(client) { client.stop(); client = null; }
        if(pollInterval) { clearInterval(pollInterval); pollInterval = null; }
        if(poller) { poller.stop(); poller = null; }
        if(server) { await new Promise(r => server.close(r)); server = null; }
        await decoderFixtures.truncateAll(sourceDb);
        await decoderFixtures.truncateAll(replicaDb);
    });

    async function startServer(){
        broadcaster     = new BlockBroadcaster({ WS_MAX_PER_IP: 20, WS_BACKPRESSURE_LIMIT: 50, TRUST_PROXY: false });
        snapshotBuilder = new SnapshotBuilder(util);
        poller          = new ServerPoller(CHAIN, NETWORK, sourceDb, broadcaster, null,
            { BLOCK_POLL_INTERVAL: 200 }, util);

        server = buildServer(sourceDb, broadcaster, snapshotBuilder);
        await new Promise(r => server.listen(SERVER_PORT, r));

        // Drive the poller manually; match the indexer e2e harness pattern.
        poller.lastPolledBlock = await sourceDb.getLastBlock();
        await poller._updateStatus();
        pollInterval = setInterval(async () => {
            try { await poller._poll(); } catch(e){}
        }, 200);
    }

    function makeClient(){
        let applier  = new ClientApplier(replicaDb, util);
        let rollback = new ClientRollback(replicaDb, util);
        let verifier = new HashVerifier();
        let sync = new ClientSync(CHAIN, NETWORK, replicaDb, applier, rollback, verifier, {
            SYNC_SOURCES: 'http://127.0.0.1:' + SERVER_PORT,
            VERIFY_HASHES: false,
            CLIENT_RECONNECT_DELAY: 500,
            HASH_CONFIRM_TIMEOUT: 2000,
            WS_MAX_PAYLOAD: 50 * 1024 * 1024,
            SNAPSHOT_MAX_CONTENT: 500 * 1024 * 1024
        }, util);
        return {
            sync,
            bootstrap: async () => {
                await sync._bootstrapFromSnapshot();
                sync.lastAppliedBlock = await replicaDb.getLastBlock();
            },
            connectLive: () => sync._connectWebSockets(),
            incrementalCatchUp: async (sinceBlock) => {
                await sync._incrementalCatchUp(sinceBlock);
                sync.lastAppliedBlock = await replicaDb.getLastBlock();
            },
            rollback: async (toBlock) => {
                await rollback.rollback(toBlock);
                sync.lastAppliedBlock = await replicaDb.getLastBlock();
            },
            stop: () => sync.stop()
        };
    }

    describe('REST surface', function() {

        it('GET /status/decoder/... returns block_hash (no indexer hashes)', async function() {
            this.timeout(15000);
            await decoderFixtures.seedDecoderBlocks(sourceDb, 1, 3);
            await startServer();

            await waitFor(async () => {
                let r = await axios.get('http://127.0.0.1:' + SERVER_PORT + '/status/decoder/' + CHAIN + '/' + NETWORK);
                return r.data.block_height === 3;
            }, 10000);

            let res = await axios.get('http://127.0.0.1:' + SERVER_PORT + '/status/decoder/' + CHAIN + '/' + NETWORK);
            assert.strictEqual(res.data.dbType, 'decoder');
            assert.strictEqual(res.data.block_height, 3);
            assert.ok(res.data.block_hash, 'block_hash should be present');
            assert.strictEqual(res.data.ledger_hash, undefined,   'decoder status must not expose ledger_hash');
            assert.strictEqual(res.data.actions_hash, undefined,  'decoder status must not expose actions_hash');
            assert.strictEqual(res.data.contract_hash, undefined, 'decoder status must not expose contract_hash');
        });

        it('GET /transparency/decoder/... returns 400 (indexer-only)', async function() {
            this.timeout(10000);
            await startServer();

            try {
                await axios.get('http://127.0.0.1:' + SERVER_PORT + '/transparency/decoder/' + CHAIN + '/' + NETWORK + '/roots');
                assert.fail('Expected 400');
            } catch(e){
                assert.strictEqual(e.response.status, 400);
                assert.match(e.response.data.error, /indexer-only/i);
            }
        });
    });

    describe('Cold bootstrap', function() {

        it('replica bootstraps from a decoder full snapshot', async function() {
            this.timeout(30000);

            await decoderFixtures.seedDecoderBlocks(sourceDb, 1, 10);
            await startServer();

            client = makeClient();
            await client.bootstrap();

            let replicaLast = await replicaDb.getLastBlock();
            assert.strictEqual(replicaLast, 10);

            // Row-count parity across replicated decoder tables: events and pubkeys
            // are idempotently re-dumped/upserted on each snapshot so they converge
            // to source; dispensers are excluded from per-block replication (their
            // soft-expire/hard-purge mutations don't ride the block stream) but the
            // full snapshot dumps current rows so parity still holds here.
            let tables = ['blocks', 'transactions', 'transaction_outputs',
                          'index_addresses', 'index_transactions', 'events', 'pubkeys'];
            for(let t of tables){
                let s = await sourceDb.getTableCount(t);
                let r = await replicaDb.getTableCount(t);
                assert.strictEqual(r, s, t + ' row count mismatch: source=' + s + ' replica=' + r);
            }

            let srcHead = await sourceDb.getBlockHashRow(10);
            let rplHead = await replicaDb.getBlockHashRow(10);
            assert.ok(srcHead && rplHead, 'both heads should resolve');
            assert.strictEqual(rplHead.block_hash, srcHead.block_hash);
        });
    });

    describe('Incremental snapshot', function() {

        it('catches replica up from a since-block, including tx-scoped tables', async function() {
            this.timeout(30000);

            // No WS connection in this test: the /since/N incremental-snapshot
            // endpoint is the only path exercised, and it must also carry
            // transaction_outputs, which is tx-scoped rather than block-scoped
            // (previously the failure mode this test guards against).
            await decoderFixtures.seedDecoderBlocks(sourceDb, 1, 5);
            await startServer();

            client = makeClient();
            await client.bootstrap();
            assert.strictEqual(await replicaDb.getLastBlock(), 5);

            await decoderFixtures.seedDecoderBlocks(sourceDb, 6, 12);

            await client.incrementalCatchUp(6);

            assert.strictEqual(await replicaDb.getLastBlock(), 12);

            // Same table set and parity rationale as the bootstrap test above.
            for(let t of ['blocks', 'transactions', 'transaction_outputs',
                          'index_addresses', 'index_transactions', 'events', 'pubkeys']){
                let s = await sourceDb.getTableCount(t);
                let r = await replicaDb.getTableCount(t);
                assert.strictEqual(r, s, t + ' row count mismatch: source=' + s + ' replica=' + r);
            }
        });
    });

    describe('Reorg / rollback', function() {

        it('rolls back blocks + tx-scoped rows and keeps index tables intact', async function() {
            this.timeout(30000);

            await decoderFixtures.seedDecoderBlocks(sourceDb, 1, 10);
            await startServer();
            client = makeClient();
            await client.bootstrap();
            assert.strictEqual(await replicaDb.getLastBlock(), 10);

            let initialAddrCount = await replicaDb.getTableCount('index_addresses');

            // rollback(8) removes blocks 8, 9, 10, leaving the tip at 7.
            await client.rollback(8);

            assert.strictEqual(await replicaDb.getLastBlock(), 7);
            assert.strictEqual(await replicaDb.getTableCount('blocks'),       7);
            assert.strictEqual(await replicaDb.getTableCount('transactions'), 7);

            // Tx-scoped: seedDecoderBlocks creates one transaction_output per
            // block, so post-rollback we should see 7 left (was 10).
            assert.strictEqual(await replicaDb.getTableCount('transaction_outputs'), 7);

            // Index tables are append-only and should NOT be touched by rollback.
            assert.strictEqual(
                await replicaDb.getTableCount('index_addresses'),
                initialAddrCount,
                'index_addresses must not change on rollback'
            );

            let srcHead = await sourceDb.getBlockHashRow(7);
            let rplHead = await replicaDb.getBlockHashRow(7);
            assert.strictEqual(rplHead.block_hash, srcHead.block_hash);
        });
    });

    describe('Live WebSocket sync', function() {

        it('replica receives decoder block events for blocks added post-bootstrap', async function() {
            this.timeout(30000);

            await decoderFixtures.seedDecoderBlocks(sourceDb, 1, 5);
            await startServer();

            client = makeClient();
            await client.bootstrap();
            await client.connectLive();
            // Poll the client's socket to OPEN before seeding: blocks broadcast
            // while the handshake is still in flight are never re-sent.
            await waitFor(() => client.sync.wsConns[0] && client.sync.wsConns[0].readyState === WebSocket.OPEN, 10000);

            await decoderFixtures.seedDecoderBlocks(sourceDb, 6, 8);

            await waitFor(async () => {
                let b = await replicaDb.getLastBlock();
                return b !== null && b >= 8;
            }, 20000);

            let last = await replicaDb.getLastBlock();
            assert.strictEqual(last, 8);

            let head = await replicaDb.getBlockHashRow(8);
            assert.ok(head.block_hash, 'replica head should have block_hash');
            let src = await sourceDb.getBlockHashRow(8);
            assert.strictEqual(head.block_hash, src.block_hash);
        });

        it('decoder block payload carries block_hash and omits indexer hashes', async function() {
            this.timeout(15000);

            await decoderFixtures.seedDecoderBlocks(sourceDb, 1, 2);
            await startServer();

            // Subscribe with a raw WS client so we can inspect the wire format.
            let messages = [];
            let ws = new WebSocket('ws://127.0.0.1:' + SERVER_PORT + '/subscribe/decoder/' + CHAIN + '/' + NETWORK);
            ws.on('message', (data) => {
                try { messages.push(JSON.parse(data.toString())); } catch(e){}
            });
            // Poll the socket to OPEN: block 3 is broadcast exactly once.
            await waitFor(() => ws.readyState === WebSocket.OPEN, 10000);

            await decoderFixtures.seedDecoderBlocks(sourceDb, 3, 3);

            await waitFor(() => messages.some(m => m.type === 'block' && m.block_index === 3), 10000);

            let block = messages.find(m => m.type === 'block' && m.block_index === 3);
            assert.ok(block, 'should have received block 3');
            assert.strictEqual(block.dbType, 'decoder');
            assert.ok(block.block_hash, 'decoder payload should include block_hash');
            assert.strictEqual(block.ledger_hash, undefined,
                'decoder payload must not include ledger_hash');
            assert.strictEqual(block.actions_hash, undefined,
                'decoder payload must not include actions_hash');
            assert.strictEqual(block.contract_hash, undefined,
                'decoder payload must not include contract_hash');

            // Decoder payloads carry transactions + transaction_outputs (not actions).
            assert.ok(block.data, 'payload should have data');
            assert.ok(block.data.transactions, 'payload should include transactions');
            assert.strictEqual(block.data.actions, undefined,
                'decoder payload must not include actions');

            ws.close();
        });
    });
});
