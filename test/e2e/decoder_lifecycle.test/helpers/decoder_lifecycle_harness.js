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

const sinon     = require('sinon');

const Database       = require('../../../../src/db');
const ClientSync     = require('../../../../src/client/sync');
const ClientApplier  = require('../../../../src/client/applier');
const ClientRollback = require('../../../../src/client/rollback');
const HashVerifier   = require('../../../../src/client/hash_verifier');
const Utility        = require('../../../../src/util');

const decoderFixtures = require('../../helpers/decoderFixtures');
const { getMariadb }   = require('../../helpers/mariadbLoader');
const ServerProcess    = require('../../helpers/serverProcess');
const fixturePorts     = require('../../../../bin/fixture-ports.js');

const SOURCE_HOST  = process.env.E2E_DB_HOST         || '127.0.0.1';
const SOURCE_PORT  = fixturePorts.port('E2E_DB_PORT');
const REPLICA_HOST = process.env.E2E_REPLICA_DB_HOST  || '127.0.0.1';
const REPLICA_PORT = fixturePorts.port('E2E_REPLICA_DB_PORT');
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

class DecoderLifecycle {
    constructor(assignState){
        this.assignState = assignState;
        this.sourceDb = null;
        this.replicaDb = null;
        this.serverProcess = null;
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
        if(this.serverProcess) await this.serverProcess.stop();
        if(this.sourceDb)  await this.sourceDb.close();
        if(this.replicaDb) await this.replicaDb.close();
    }

    async reset(){
        if(this.client) { this.client.stop(); this.client = null; }
        if(this.serverProcess) { await this.serverProcess.stop(); this.serverProcess = null; }
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
        this.serverProcess = new ServerProcess(this.sourceDb, SERVER_PORT, CHAIN, NETWORK);
        Object.assign(this.serverProcess.config, SERVER_CONFIG, overrides);
        await this.serverProcess.start();
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
