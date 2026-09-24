// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// Covers shared HTTP parity setup. One part of index_map_parity_http.test.js.
const sinon        = require('sinon');
const http         = require('http');
const express      = require('express');
const cors         = require('cors');
const { parseCorsOrigin } = require('../../../../src/http/cors_origin');
const setup        = require('../../helpers/setup');
const testDb       = require('../../helpers/testDb');
const fixtures     = require('../../helpers/fixtures');
const Database     = require('../../../../src/db');
const BlockHasher  = require('../../../../src/client/block_hasher');
const HashVerifier = require('../../../../src/client/hash_verifier');
const ClientApplier= require('../../../../src/client/applier');
const ClientRollback = require('../../../../src/client/rollback');
const ClientSync   = require('../../../../src/client/sync');
const { getReplicatedTables } = require('../../../../src/schema/replicated_tables');

const PORT = 19733;
const H = 3;                       // tip block both sides are seeded to
const CHAIN = 'bitcoin', NETWORK = 'mainnet';
const SERVER_CFG = { INDEX_MAP_PARITY_CHECK: true };

// Deterministic in-block id->address subset (block_index set) shared by both DBs.
// High explicit ids avoid colliding with the AUTO_INCREMENT ids seedBlocks uses.
const STAMPED = [
    { id: 1001, address: 'bc1qstamped0alice000000000000000000000aaa', block_index: 1 },
    { id: 1002, address: 'bc1qstamped0bob00000000000000000000000bbb', block_index: 2 },
    { id: 1003, address: 'bc1qstamped0carol000000000000000000000ccc', block_index: 3 },
];

async function stamp(db, rows){
    await db.doQuery("DELETE FROM index_addresses WHERE block_index IS NOT NULL");
    for(let r of rows)
        await db.doQuery("INSERT INTO index_addresses (`id`,`address`,`block_index`) VALUES (?,?,?)",
            [r.id, r.address, r.block_index]);
}

// Faithful mirror of api.js buildStatusRow (server mode) for the fields the client
// reads. Uses the SHIPPED BlockHasher.computeIndexMapChecksum for the checksum.
function makeStatusHandler(sourceDb, util, cfg){
    return async function(req, res){
        try {
            let hashRow = await sourceDb.getBlockHashRow(H);
            let row = {
                block_height: H,
                source_height: H,
                lag_blocks: 0,
                ledger_hash:   hashRow ? hashRow.ledger_hash   : null,
                actions_hash:  hashRow ? hashRow.actions_hash  : null,
                contract_hash: hashRow ? hashRow.contract_hash : null,
            };
            row.table_counts = {};
            for(let t of getReplicatedTables('indexer')){
                try { row.table_counts[t] = await sourceDb.getTableCount(t); } catch(e){}
            }
            row.index_map_checksum = null;
            if(cfg['INDEX_MAP_PARITY_CHECK'])
                row.index_map_checksum = await new BlockHasher(sourceDb, util).computeIndexMapChecksum(H);
            res.json(row);
        } catch(e){ res.status(500).json({ error: e.message }); }
    };
}

function createClientSync(realReplica, util){
    let applier  = new ClientApplier(realReplica, util, CHAIN, NETWORK);
    let rollback = new ClientRollback(realReplica, util, CHAIN, 'regtest');
    let cfg = {
        SYNC_SOURCES: 'http://127.0.0.1:' + PORT,
        INDEX_MAP_PARITY_CHECK: true,
        VERIFY_HASHES: true,
        VERIFY_RECOMPUTE: false,           // skip the consensus-recompute halt path
        VERIFY_STATE_HASH: false,
        VERIFY_STATE_COMMITMENT: false,
        SYNC_BOOTSTRAP_DEPTH: {}
    };
    return new ClientSync(CHAIN, NETWORK, realReplica, applier, rollback, new HashVerifier(), cfg, util);
}

function registerIndexMapParityHttpHooks(){
    const suite = {};
    before(async function() {
        await setup.globalSetup();
        suite.sourceDb  = setup.getSourceDb();
        suite.replicaDb = setup.getReplicaDb();
        const util = testDb.util;

        // Identical, deterministic block chain on both DBs so getBlockHashRow(H)
        // and the three committed hashes match (no noisy cross-source error).
        await fixtures.seedBlocks(suite.sourceDb, 1, H);
        await fixtures.seedBlocks(suite.replicaDb, 1, H);
        await stamp(suite.sourceDb, STAMPED);
        await stamp(suite.replicaDb, STAMPED);

        // Real src/db.js Database for the replica: ClientSync needs getBlockHashRow,
        // getTableCount, doQuery AND getSyncState/setSyncState (the TestDatabase
        // wrapper lacks the sync_state methods the counter uses).
        suite.realReplica = new Database(testDb.TEST_DB_HOST, testDb.TEST_DB_PORT,
            testDb.REPLICA_DB_NAME, testDb.TEST_DB_USER, testDb.TEST_DB_PASS, util, 'indexer');

        // Real HTTP server publishing the shipped checksum on /status.
        let app = express();
        app.use(cors({ origin: parseCorsOrigin(process.env.CORS_ORIGIN), methods: ['GET'] }));
        app.get('/status/:dbType/:chain/:network', makeStatusHandler(suite.sourceDb, util, SERVER_CFG));
        await new Promise(r => { suite.server = http.createServer(app).listen(PORT, r); });
        suite.clientSync = createClientSync(suite.realReplica, util);
    });

    after(async function() {
        if(suite.server) await new Promise(r => suite.server.close(r));
        if(suite.realReplica && suite.realReplica.close){ try { await suite.realReplica.close(); } catch(e){} }
        await setup.globalTeardown();
    });

    beforeEach(function(){ suite.warnSpy = sinon.spy(console, 'warn'); });
    afterEach(function(){ suite.warnSpy.restore(); });
    suite.sawParityWarn = () => suite.warnSpy.getCalls().some(c =>
        String(c.args[0] || '').includes('INDEX_MAP_PARITY mismatch'));
    return suite;
}

module.exports = { PORT, H, STAMPED, stamp, registerIndexMapParityHttpHooks };
