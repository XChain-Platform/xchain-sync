// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.
//
// END-TO-END advisory index-map parity over real HTTP. A real Express server
// publishes index_map_checksum on /status (computed with the SHIPPED
// BlockHasher.computeIndexMapChecksum over a real source DB), and the SHIPPED
// ClientSync._verifyAgainstSource fetches /status over HTTP, recomputes over a
// real replica DB, and compares. Exercises the full transport + client consume
// path the pure-DB drill (index-map-parity.test.js) could not.
//
// The /status handler mirrors api.js buildStatusRow's server-mode shape (the
// fields the client reads: block_height, the three hashes, table_counts, and the
// index_map_checksum), using the exact shipped checksum call. buildStatusRow is
// not exported, so it is mirrored rather than imported; the CLIENT side here is
// 100% shipped code.

const assert       = require('assert');
const sinon        = require('sinon');
const http         = require('http');
const express      = require('express');
const cors         = require('cors');
const { parseCorsOrigin } = require('../../src/corsOrigin');
const setup        = require('./helpers/setup');
const testDb       = require('./helpers/testDb');
const fixtures     = require('./helpers/fixtures');
const Database     = require('../../src/db');
const BlockHasher  = require('../../src/BlockHasher');
const HashVerifier = require('../../src/HashVerifier');
const ClientApplier= require('../../src/ClientApplier');
const ClientRollback = require('../../src/ClientRollback');
const ClientSync   = require('../../src/ClientSync');
const { getReplicatedTables } = require('../../src/replicatedTables');

const PORT = 19733;
const H = 3;                       // tip block both sides are seeded to
const CHAIN = 'bitcoin', NETWORK = 'mainnet';

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

describe('Integration: index-map parity over HTTP (e2e)', function() {
    this.timeout(180000);

    let sourceDb, replicaDb, realReplica, server, util, clientSync, warnSpy;
    const SERVER_CFG = { INDEX_MAP_PARITY_CHECK: true };
    const countKey = 'index_map_mismatch_count:indexer';
    const lastKey  = 'index_map_mismatch_last_block:indexer';

    before(async function() {
        await setup.globalSetup();
        sourceDb  = setup.getSourceDb();
        replicaDb = setup.getReplicaDb();
        util      = testDb.util;

        // Identical, deterministic block chain on both DBs so getBlockHashRow(H)
        // and the three committed hashes match (no noisy cross-source error).
        await fixtures.seedBlocks(sourceDb, 1, H);
        await fixtures.seedBlocks(replicaDb, 1, H);
        await stamp(sourceDb, STAMPED);
        await stamp(replicaDb, STAMPED);

        // Real src/db.js Database for the replica: ClientSync needs getBlockHashRow,
        // getTableCount, doQuery AND getSyncState/setSyncState (the TestDatabase
        // wrapper lacks the sync_state methods the counter uses).
        realReplica = new Database(testDb.TEST_DB_HOST, testDb.TEST_DB_PORT,
            testDb.REPLICA_DB_NAME, testDb.TEST_DB_USER, testDb.TEST_DB_PASS, util, 'indexer');

        // Real HTTP server publishing the shipped checksum on /status.
        let app = express();
        app.use(cors({ origin: parseCorsOrigin(process.env.CORS_ORIGIN), methods: ['GET'] }));
        app.get('/status/:dbType/:chain/:network', makeStatusHandler(sourceDb, util, SERVER_CFG));
        await new Promise(r => { server = http.createServer(app).listen(PORT, r); });

        let applier  = new ClientApplier(realReplica, util, CHAIN, NETWORK);
        let rollback = new ClientRollback(realReplica, util, CHAIN);
        let cfg = {
            SYNC_SOURCES: 'http://127.0.0.1:' + PORT,
            INDEX_MAP_PARITY_CHECK: true,
            VERIFY_HASHES: true,
            VERIFY_RECOMPUTE: false,           // skip the consensus-recompute halt path
            VERIFY_STATE_HASH: false,
            VERIFY_STATE_COMMITMENT: false,
            SYNC_BOOTSTRAP_DEPTH: {}
        };
        clientSync = new ClientSync(CHAIN, NETWORK, realReplica, applier, rollback, new HashVerifier(), cfg, util);
    });

    after(async function() {
        if(server) await new Promise(r => server.close(r));
        if(realReplica && realReplica.close){ try { await realReplica.close(); } catch(e){} }
        await setup.globalTeardown();
    });

    beforeEach(function(){ warnSpy = sinon.spy(console, 'warn'); });
    afterEach(function(){ warnSpy.restore(); });

    const sawParityWarn = () => warnSpy.getCalls().some(c =>
        String(c.args[0] || '').includes('INDEX_MAP_PARITY mismatch'));

    it('faithful replica: /status checksum matches, no mismatch, no counter', async function() {
        await stamp(replicaDb, STAMPED);                       // ensure faithful
        await clientSync._verifyAgainstSource('http://127.0.0.1:' + PORT, H);
        assert.strictEqual(sawParityWarn(), false, 'no parity warning on a faithful replica');
        let c = await realReplica.getSyncState(countKey);
        assert.strictEqual(c, null, 'counter unset when everything agrees');
    });

    it('divergence: equal-count swapped identity fires advisory mismatch, no halt', async function() {
        // Replica id=1002 locally points elsewhere; SAME row count, different content.
        await realReplica.doQuery("UPDATE index_addresses SET address=? WHERE id=1002",
            ['bc1qstamped0MALLORY00000000000000000mmmmmmm']);

        await clientSync._verifyAgainstSource('http://127.0.0.1:' + PORT, H);

        assert.strictEqual(sawParityWarn(), true, 'parity mismatch must be logged');
        assert.strictEqual(clientSync.isHalted(), false, 'advisory: client must NOT halt');
        let c  = await realReplica.getSyncState(countKey);
        let lb = await realReplica.getSyncState(lastKey);
        assert.strictEqual(c, '1', 'mismatch counter incremented');
        assert.strictEqual(lb, String(H), 'last divergent block recorded');
    });

    it('recovery: faithful again raises no new mismatch (counter steady)', async function() {
        await realReplica.doQuery("UPDATE index_addresses SET address=? WHERE id=1002",
            [STAMPED[1].address]);                              // restore
        // Add a benign NULL-block row the source lacks (API read-path seed): excluded.
        await realReplica.doQuery("INSERT INTO index_addresses (`id`,`address`,`block_index`) VALUES (?,?,NULL)",
            [2001, 'bc1qapiseed00000000000000000000000000seed1']);

        await clientSync._verifyAgainstSource('http://127.0.0.1:' + PORT, H);

        assert.strictEqual(sawParityWarn(), false, 'no false alarm once faithful (NULL-block excluded)');
        let c = await realReplica.getSyncState(countKey);
        assert.strictEqual(c, '1', 'counter unchanged from the single real divergence');
    });
});
