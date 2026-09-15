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

'use strict';

const http      = require('http');
const express   = require('express');
const cors      = require('cors');
const { parseCorsOrigin } = require('../../../../src/http/cors_origin');
const WebSocket = require('ws');
const sinon     = require('sinon');

const Database         = require('../../../../src/db');
const ServerPoller     = require('../../../../src/server/poller');
const BlockBroadcaster = require('../../../../src/server/block_broadcaster');
const SnapshotBuilder  = require('../../../../src/server/snapshot_builder');
const ClientSync       = require('../../../../src/client/sync');
const ClientApplier    = require('../../../../src/client/applier');
const ClientRollback   = require('../../../../src/client/rollback');
const HashVerifier     = require('../../../../src/client/hash_verifier');
const Utility          = require('../../../../src/util');
// Proxy-trust and rate-limiter wiring comes from the real api.js rather than a
// parallel copy: how req.ip resolves and which limiter guards which route are
// production decisions, and a harness that re-declares them cannot notice when
// production drifts. api.js guards its env check and listen() behind
// require.main === module, so requiring it here opens no port.
const { trustProxyHops, createRateLimiters } = require('../../../../src/api');

const decoderFixtures = require('../../helpers/decoderFixtures');
const { getMariadb }   = require('../../helpers/mariadbLoader');

const SOURCE_HOST  = process.env.E2E_DB_HOST         || '127.0.0.1';
const SOURCE_PORT  = parseInt(process.env.E2E_DB_PORT) || 23306;
const REPLICA_HOST = process.env.E2E_REPLICA_DB_HOST  || '127.0.0.1';
const REPLICA_PORT = parseInt(process.env.E2E_REPLICA_DB_PORT) || 23307;
const DB_USER      = process.env.E2E_DB_USER          || 'xchain-node';
const DB_PASS      = process.env.E2E_DB_PASS          || 'xchain-fixture-throwaway';

const SOURCE_DB_NAME  = 'xchain_e2e_decoder_source';
const REPLICA_DB_NAME = 'xchain_e2e_decoder_replica';

const SERVER_PORT = 29250;

// Two genuine client addresses and one a caller can only have typed itself.
const CLIENT_A = '198.51.100.7';
const CLIENT_B = '198.51.100.8';
const SPOOFED  = '203.0.113.66';

const CHAIN       = 'bitcoin';
const NETWORK     = 'regtest';
const DB_TYPE     = 'decoder';

const util = new Utility();

// Default server config for this suite. TRANSPARENCY_RATE_LIMIT is widened well
// past the production default of 10/min because waitFor() polls on a 100ms
// cadence and would 429 its own wait loop; the limiter code path is still the
// real one, only the threshold is loosened.
const SERVER_CONFIG = {
    WS_MAX_PER_IP:           20,
    WS_BACKPRESSURE_LIMIT:   50,
    TRUST_PROXY:             false,
    BLOCK_POLL_INTERVAL:     200,
    SNAPSHOT_RATE_FULL:      100,
    SNAPSHOT_RATE_INCR:      100,
    TRANSPARENCY_RATE_LIMIT: 100000
};

function mountStatusRoute(app, sourceDb){
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
}

function mountSchemaRoute(app, sourceDb){
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
}

function mountSnapshotRoutes(app, sourceDb, snapshotBuilder, limiters){
    app.get('/snapshot/:dbType/:chain/:network', limiters.fullSnapshotLimiter, async (req, res) => {
        try {
            await snapshotBuilder.streamFullSnapshot(sourceDb, res);
        } catch(e){
            if(!res.headersSent) res.status(500).json({ error: e.message });
        }
    });

    app.get('/snapshot/:dbType/:chain/:network/since/:blockHeight', limiters.incrSnapshotLimiter, async (req, res) => {
        let since = parseInt(req.params.blockHeight);
        if(isNaN(since) || since < 0) return res.status(400).json({ error: 'Invalid blockHeight' });
        try {
            await snapshotBuilder.streamIncrementalSnapshot(sourceDb, since, res);
        } catch(e){
            if(!res.headersSent) res.status(500).json({ error: e.message });
        }
    });
}

function mountTransparencyRoute(app, limiters){
    // Transparency is indexer-only; decoder requests must return 400.
    app.get('/transparency/:dbType/:chain/:network/roots', limiters.transparencyLimiter, (req, res) => {
        if(req.params.dbType !== 'indexer')
            return res.status(400).json({ error: 'Transparency log is indexer-only' });
        res.json({ entries: [] });
    });
}

// Build the mini HTTP+WS server that mirrors src/api.js for decoder
// surface. Reuses real BlockBroadcaster + SnapshotBuilder + ServerPoller
// so the test exercises actual Phase 3 code paths.
function buildServer(sourceDb, broadcaster, snapshotBuilder, cfg){
    let app = express();
    // Must precede the limiters, which read req.ip: same ordering requirement
    // startApi() has.
    app.set('trust proxy', trustProxyHops(cfg['TRUST_PROXY']));
    app.use(cors({ origin: parseCorsOrigin(process.env.CORS_ORIGIN), methods: ['GET'] }));

    // The limiter instances startApi() mounts, on the routes it guards.
    let limiters = createRateLimiters(cfg);
    app.use(limiters.backstopLimiter);
    mountStatusRoute(app, sourceDb);
    mountSchemaRoute(app, sourceDb);
    mountSnapshotRoutes(app, sourceDb, snapshotBuilder, limiters);
    mountTransparencyRoute(app, limiters);

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

class DecoderLifecycle {
    constructor(assignState){
        this.assignState = assignState;
        this.sourceDb = null;
        this.replicaDb = null;
        this.broadcaster = null;
        this.snapshotBuilder = null;
        this.poller = null;
        this.server = null;
        this.pollInterval = null;
        this.client = null;
    }

    publish(){
        this.assignState({ sourceDb: this.sourceDb, replicaDb: this.replicaDb, client: this.client });
    }

    async provisionFor(host, port, dbName, adminUser, adminPass){
        let mariadb = await getMariadb();
        let conn = await mariadb.createConnection({ host, port, user: adminUser, password: adminPass });
        await conn.query("CREATE DATABASE IF NOT EXISTS `" + dbName + "`");
        if (DB_USER !== adminUser) {
            await conn.query("GRANT ALL PRIVILEGES ON `" + dbName + "`.* TO `" + DB_USER + "`@'%'");
            await conn.query("FLUSH PRIVILEGES");
        }
        await conn.end();
    }

    async setup(){
        // Create both databases via an admin account BEFORE constructing any
        // src/db.js Database. That class' constructor eagerly opens a pool
        // bound to the database name, and if the DB doesn't exist yet the pool
        // drives a retry storm that exhausts MariaDB's connection slots. The
        // MARIADB_USER user has no global CREATE/GRANT privilege; defaults
        // match the compose containers (root / MARIADB_ROOT_PASSWORD=test),
        // overridable with E2E_DB_ADMIN_USER / E2E_DB_ADMIN_PASS like
        // helpers/testDb.js createDatabase.
        let adminUser = process.env.E2E_DB_ADMIN_USER || 'root';
        let adminPass = process.env.E2E_DB_ADMIN_PASS !== undefined ? process.env.E2E_DB_ADMIN_PASS : 'test';
        await this.provisionFor(SOURCE_HOST,  SOURCE_PORT,  SOURCE_DB_NAME, adminUser, adminPass);
        await this.provisionFor(REPLICA_HOST, REPLICA_PORT, REPLICA_DB_NAME, adminUser, adminPass);

        this.sourceDb  = new Database(SOURCE_HOST,  SOURCE_PORT,  SOURCE_DB_NAME,  DB_USER, DB_PASS, util, DB_TYPE);
        this.replicaDb = new Database(REPLICA_HOST, REPLICA_PORT, REPLICA_DB_NAME, DB_USER, DB_PASS, util, DB_TYPE);
        await decoderFixtures.seedDecoderSchema(this.sourceDb);
        await decoderFixtures.seedDecoderSchema(this.replicaDb);
        this.publish();

        // Mute the chatty src/ console output during the run.
        // (Unstub temporarily when debugging a hook/setup failure.)
        if(!process.env.E2E_DEBUG){
            sinon.stub(console, 'log');
            sinon.stub(console, 'error');
        }
    }

    async teardown(){
        sinon.restore();
        if(this.client) this.client.stop();
        if(this.pollInterval) clearInterval(this.pollInterval);
        if(this.poller) this.poller.stop();
        if(this.server) await new Promise(r => this.server.close(r));
        if(this.sourceDb)  await this.sourceDb.close();
        if(this.replicaDb) await this.replicaDb.close();
    }

    async reset(){
        if(this.client) { this.client.stop(); this.client = null; }
        if(this.pollInterval) { clearInterval(this.pollInterval); this.pollInterval = null; }
        if(this.poller) { this.poller.stop(); this.poller = null; }
        if(this.server) { await new Promise(r => this.server.close(r)); this.server = null; }
        await decoderFixtures.truncateAll(this.sourceDb);
        await decoderFixtures.truncateAll(this.replicaDb);
        this.publish();
    }

    registerHooks(){
        const lifecycle = this;
        before(async function() { this.timeout(30000); await lifecycle.setup(); });
        after(async function() { await lifecycle.teardown(); });
        beforeEach(async function() { this.timeout(15000); await lifecycle.reset(); });
    }

    async startServer(overrides){
        // One config object for the broadcaster, the poller and the app, so a
        // test that flips TRUST_PROXY moves both budget surfaces at once, the
        // way a deployment does.
        let cfg = Object.assign({}, SERVER_CONFIG, overrides);

        this.broadcaster     = new BlockBroadcaster(cfg);
        this.snapshotBuilder = new SnapshotBuilder(util);
        this.poller          = new ServerPoller(CHAIN, NETWORK, this.sourceDb, this.broadcaster, null, cfg, util);
        this.server = buildServer(this.sourceDb, this.broadcaster, this.snapshotBuilder, cfg);
        await new Promise(r => this.server.listen(SERVER_PORT, r));

        // Drive the poller manually; match the indexer e2e harness pattern.
        this.poller.lastPolledBlock = await this.sourceDb.getLastBlock();
        await this.poller.updateStatus();
        this.pollInterval = setInterval(async () => {
            try { await this.poller.poll(); } catch(e){}
        }, 200);
    }

    makeClient(){
        let applier  = new ClientApplier(this.replicaDb, util);
        let rollback = new ClientRollback(this.replicaDb, util, undefined, 'regtest');
        let verifier = new HashVerifier();
        let sync = new ClientSync(CHAIN, NETWORK, this.replicaDb, applier, rollback, verifier, {
            SYNC_SOURCES: 'http://127.0.0.1:' + SERVER_PORT,
            VERIFY_HASHES: false,
            CLIENT_RECONNECT_DELAY: 500,
            HASH_CONFIRM_TIMEOUT: 2000,
            WS_MAX_PAYLOAD: 50 * 1024 * 1024,
            SNAPSHOT_MAX_CONTENT: 500 * 1024 * 1024
        }, util);
        this.client = {
            sync,
            bootstrap: async () => {
                await sync.bootstrapFromSnapshot();
                sync.lastAppliedBlock = await this.replicaDb.getLastBlock();
            },
            connectLive: () => sync.connectWebSockets(),
            incrementalCatchUp: async (sinceBlock) => {
                await sync.incrementalCatchUp(sinceBlock);
                sync.lastAppliedBlock = await this.replicaDb.getLastBlock();
            },
            rollback: async (toBlock) => {
                await rollback.rollback(toBlock);
                sync.lastAppliedBlock = await this.replicaDb.getLastBlock();
            },
            stop: () => sync.stop()
        };
        this.publish();
        return this.client;
    }
}

module.exports = {
    CHAIN,
    CLIENT_A,
    CLIENT_B,
    DecoderLifecycle,
    NETWORK,
    SERVER_PORT,
    SPOOFED
};
