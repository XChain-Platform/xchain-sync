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
 * E2E: Decoder `dispensers` replace-table reconcile.
 *
 * dispensers rides neither the block stream nor the id-cursor lookup
 * paging (no monotonic id; the decoder soft-expires via UPDATE then
 * hard-purges via DELETE), and it is in SnapshotBuilder's decoderSkip set,
 * so neither the full snapshot nor an incremental catch-up ever carries it.
 * Convergence is entirely the periodic replace-table reconcile:
 *   SnapshotBuilder.streamDispensers (keyset-paged full dump)
 *   -> ClientSync._reconcileDispensers (paged re-fetch under the apply lock)
 *   -> ClientApplier.applyDispensersReplace (atomic DELETE + INSERT).
 *
 * Unit tests cover the individual pieces, but nothing previously proved
 * end-to-end convergence over a real HTTP surface against a drifted source.
 * Covered here:
 *   - Seed-from-empty: a full-snapshot bootstrap leaves dispensers empty
 *     (snapshot-skipped); a reconcile seeds them to parity.
 *   - Drift convergence: a source that has hard-purged, soft-expired AND
 *     added dispenser rows out-of-band converges exactly on reconcile,
 *     including multi-page keyset paging (LOOKUP_PAGE_SIZE forced small).
 *   - Nth-catch-up cadence: DISPENSERS_RECONCILE_EVERY gates the reconcile
 *     (every=1 converges on the next catch-up; a high `every` leaves the
 *     interim drift in place without erroring the catch-up).
 *
 ********************************************************************/

const assert    = require('assert');
const http      = require('http');
const express   = require('express');
const cors      = require('cors');
const { parseCorsOrigin } = require('../../src/corsOrigin');
const WebSocket = require('ws');
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
const { getMariadb }  = require('./helpers/mariadbLoader');

const SOURCE_HOST  = process.env.E2E_DB_HOST         || '127.0.0.1';
const SOURCE_PORT  = parseInt(process.env.E2E_DB_PORT) || 23306;
const REPLICA_HOST = process.env.E2E_REPLICA_DB_HOST  || '127.0.0.1';
const REPLICA_PORT = parseInt(process.env.E2E_REPLICA_DB_PORT) || 23307;
const DB_USER      = process.env.E2E_DB_USER          || 'xchain-node';
const DB_PASS      = process.env.E2E_DB_PASS          || 'xchain-fixture-throwaway';

const SOURCE_DB_NAME  = 'xchain_e2e_disp_source';
const REPLICA_DB_NAME = 'xchain_e2e_disp_replica';

const SERVER_PORT = 29251;
const CHAIN       = 'bitcoin';
const NETWORK     = 'regtest';

const util = new Utility();

// Mini HTTP+WS server mirroring the decoder surface of src/api.js, plus the
// /snapshot-dispensers reconcile route (param parsing copied from api.js so the
// test exercises the real ClientSync._reconcileDispensers fetch path).
function buildServer(sourceDb, broadcaster, snapshotBuilder){
    let app = express();
    app.use(cors({ origin: parseCorsOrigin(process.env.CORS_ORIGIN), methods: ['GET'] }));

    let validateDbType = (dt) => (dt === 'indexer' || dt === 'decoder') ? dt : null;

    app.get('/status/:dbType/:chain/:network', async (req, res) => {
        let dbType = validateDbType(req.params.dbType);
        if(!dbType) return res.status(400).json({ error: 'Invalid dbType' });
        try {
            let last = await sourceDb.getLastBlock();
            let row  = last !== null ? await sourceDb.getBlockHashRow(last) : null;
            res.json({
                chain: req.params.chain, network: req.params.network, dbType: dbType,
                block_height: row ? Number(row.block_index) : null,
                block_time:   row ? Number(row.block_time)  : null,
                block_hash:   row ? row.block_hash : null
            });
        } catch(e){ res.status(500).json({ error: e.message }); }
    });

    app.get('/snapshot/:dbType/:chain/:network', async (req, res) => {
        try { await snapshotBuilder.streamFullSnapshot(sourceDb, res); }
        catch(e){ if(!res.headersSent) res.status(500).json({ error: e.message }); }
    });

    app.get('/snapshot/:dbType/:chain/:network/since/:blockHeight', async (req, res) => {
        let since = parseInt(req.params.blockHeight);
        if(isNaN(since) || since < 0) return res.status(400).json({ error: 'Invalid blockHeight' });
        try { await snapshotBuilder.streamIncrementalSnapshot(sourceDb, since, res); }
        catch(e){ if(!res.headersSent) res.status(500).json({ error: e.message }); }
    });

    // dispensers reconcile page (decoder-only). Mirrors api.js cursor parsing.
    app.get('/snapshot-dispensers/:dbType/:chain/:network', async (req, res) => {
        let dbType = validateDbType(req.params.dbType);
        if(!dbType) return res.status(400).json({ error: 'Invalid dbType' });
        if(dbType !== 'decoder') return res.status(400).json({ error: 'dispensers reconcile is decoder-only' });
        let afterTx   = (req.query.after_tx   !== undefined) ? parseInt(req.query.after_tx)   : NaN;
        let afterAddr = (req.query.after_addr !== undefined) ? parseInt(req.query.after_addr) : NaN;
        if((req.query.after_tx   !== undefined && (isNaN(afterTx)   || afterTx   < 0)) ||
           (req.query.after_addr !== undefined && (isNaN(afterAddr) || afterAddr < 0)))
            return res.status(400).json({ error: 'Invalid cursor' });
        let limit = parseInt(req.query.limit);
        if(isNaN(limit)) limit = undefined;
        try { await snapshotBuilder.streamDispensers(sourceDb, afterTx, afterAddr, limit, res); }
        catch(e){ if(!res.headersSent) res.status(500).json({ error: e.message }); }
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

// Insert a dispenser row directly (decoder writes these out-of-band of the
// block stream; the fixtures' seedDecoderBlocks intentionally does not touch
// the table). expiration is raw seconds (BIGINT, Y2038-safe); expired_block_index
// NULL = open, a height = soft-expired by that block.
async function insertDispenser(db, txIndex, addressId, expiration, expiredBlock){
    await db.doQuery(
        'INSERT INTO dispensers (tx_index, address_id, expiration, expired_block_index) VALUES (?, ?, ?, ?)',
        [txIndex, addressId, expiration, (expiredBlock === undefined ? null : expiredBlock)]
    );
}

// Full ordered dump of dispensers for source-vs-replica comparison.
async function dumpDispensers(db){
    let rows = await db.doQuery('SELECT tx_index, address_id, expiration, expired_block_index FROM dispensers ORDER BY tx_index, address_id');
    return rows.map(r => ({
        tx_index: Number(r.tx_index),
        address_id: Number(r.address_id),
        expiration: r.expiration === null ? null : Number(r.expiration),
        expired_block_index: r.expired_block_index === null ? null : Number(r.expired_block_index)
    }));
}

describe('E2E: Decoder dispensers reconcile', function() {

    let sourceDb, replicaDb;
    let broadcaster, snapshotBuilder, poller, server, pollInterval;
    let client;

    before(async function() {
        this.timeout(30000);
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
        poller.lastPolledBlock = await sourceDb.getLastBlock();
        await poller._updateStatus();
        pollInterval = setInterval(async () => { try { await poller._poll(); } catch(e){} }, 200);
    }

    // `every` => DISPENSERS_RECONCILE_EVERY; `pageSize` => LOOKUP_PAGE_SIZE
    // (forced small to exercise the keyset cursor across multiple pages).
    function makeClient(opts){
        opts = opts || {};
        let applier  = new ClientApplier(replicaDb, util);
        let rollback = new ClientRollback(replicaDb, util);
        let verifier = new HashVerifier();
        let cfg = {
            SYNC_SOURCES: 'http://127.0.0.1:' + SERVER_PORT,
            VERIFY_HASHES: false,
            CLIENT_RECONNECT_DELAY: 500,
            HASH_CONFIRM_TIMEOUT: 2000,
            WS_MAX_PAYLOAD: 50 * 1024 * 1024,
            SNAPSHOT_MAX_CONTENT: 500 * 1024 * 1024
        };
        if(opts.every !== undefined)    cfg['DISPENSERS_RECONCILE_EVERY'] = String(opts.every);
        if(opts.pageSize !== undefined) cfg['LOOKUP_PAGE_SIZE']           = String(opts.pageSize);
        let sync = new ClientSync(CHAIN, NETWORK, replicaDb, applier, rollback, verifier, cfg, util);
        return {
            sync,
            bootstrap: async () => {
                await sync._bootstrapFromSnapshot();
                sync.lastAppliedBlock = await replicaDb.getLastBlock();
            },
            reconcile: () => sync._reconcileDispensers('http://127.0.0.1:' + SERVER_PORT),
            incrementalCatchUp: async (sinceBlock) => {
                await sync._incrementalCatchUp(sinceBlock);
                sync.lastAppliedBlock = await replicaDb.getLastBlock();
            },
            stop: () => sync.stop()
        };
    }

    it('a full-snapshot bootstrap seeds dispensers to parity; reconcile is idempotent', async function() {
        this.timeout(30000);

        await decoderFixtures.seedDecoderBlocks(sourceDb, 1, 5);
        // Two open dispensers + one already soft-expired on source.
        await insertDispenser(sourceDb, 10, 2, 4000000000, null);
        await insertDispenser(sourceDb, 20, 3, 4000000001, null);
        await insertDispenser(sourceDb, 30, 4, 4000000002, 4);
        await startServer();

        client = makeClient({ pageSize: 2 });
        await client.bootstrap();

        // dispensers IS carried by the full snapshot (only the INCREMENTAL path
        // skips it; see SnapshotBuilder decoderSkip), so the replica converges at
        // bootstrap. The reconcile exists for the UPDATE/DELETE drift the snapshot
        // dump cannot replay, and for the truncated (from-height) bootstrap path.
        assert.strictEqual(await replicaDb.getLastBlock(), 5);
        assert.deepStrictEqual(await dumpDispensers(replicaDb), await dumpDispensers(sourceDb),
            'full-snapshot bootstrap must seed dispensers to parity');
        assert.strictEqual((await dumpDispensers(replicaDb)).length, 3);

        // A reconcile (multi-page: 3 rows, page size 2) is idempotent on an
        // already-converged replica: re-dump + atomic replace yields the same set.
        await client.reconcile();
        assert.deepStrictEqual(await dumpDispensers(replicaDb), await dumpDispensers(sourceDb));
        assert.strictEqual((await dumpDispensers(replicaDb)).length, 3);
    });

    it('converges a drifted replica: hard-purge + soft-expire + insert all mirror on reconcile', async function() {
        this.timeout(30000);

        await decoderFixtures.seedDecoderBlocks(sourceDb, 1, 5);
        await insertDispenser(sourceDb, 10, 2, 4000000000, null);
        await insertDispenser(sourceDb, 20, 3, 4000000001, null);
        await insertDispenser(sourceDb, 30, 4, 4000000002, null);
        await startServer();

        client = makeClient({ pageSize: 2 });
        await client.bootstrap();
        await client.reconcile();
        assert.deepStrictEqual(await dumpDispensers(replicaDb), await dumpDispensers(sourceDb),
            'precondition: replica seeded to parity');

        // Mutate source dispensers OUT OF BAND of the block stream (the real
        // decoder paths: hard-purge a reorg-safe-deep row, soft-expire another,
        // and a new dispenser appears). None of this rides any replicated table.
        await sourceDb.doQuery('DELETE FROM dispensers WHERE tx_index = 20'); // hard-purge
        await sourceDb.doQuery('UPDATE dispensers SET expired_block_index = 5 WHERE tx_index = 30'); // soft-expire
        await insertDispenser(sourceDb, 40, 5, 4000000009, null);            // new

        assert.notDeepStrictEqual(await dumpDispensers(replicaDb), await dumpDispensers(sourceDb));

        await client.reconcile();
        let src = await dumpDispensers(sourceDb);
        let rpl = await dumpDispensers(replicaDb);
        assert.deepStrictEqual(rpl, src, 'replica must converge exactly to source after reconcile');
        // Spot-check the three mutation kinds landed.
        assert.ok(!rpl.find(d => d.tx_index === 20), 'hard-purged row must be gone');
        assert.strictEqual(rpl.find(d => d.tx_index === 30).expired_block_index, 5, 'soft-expire mark must mirror');
        assert.ok(rpl.find(d => d.tx_index === 40), 'newly-added dispenser must appear');
    });

    it('Nth-catch-up cadence: every=1 reconciles drift away; a high `every` leaves it (no error)', async function() {
        this.timeout(30000);

        await decoderFixtures.seedDecoderBlocks(sourceDb, 1, 5);
        await insertDispenser(sourceDb, 10, 2, 4000000000, null);
        await insertDispenser(sourceDb, 20, 3, 4000000001, null);
        await startServer();

        // every=99: a single catch-up must NOT reconcile.
        client = makeClient({ every: 99 });
        await client.bootstrap();
        await client.reconcile(); // seed to parity first
        assert.deepStrictEqual(await dumpDispensers(replicaDb), await dumpDispensers(sourceDb));

        // Drift the source, then advance blocks so the catch-up has real work.
        await sourceDb.doQuery('DELETE FROM dispensers WHERE tx_index = 20');
        await decoderFixtures.seedDecoderBlocks(sourceDb, 6, 8);
        await client.incrementalCatchUp(6);
        assert.strictEqual(await replicaDb.getLastBlock(), 8, 'blocks must still catch up');
        // count=1, 1 % 99 != 0 => no reconcile => drift persists, and the catch-up
        // did not throw (interim completeness check excludes dispensers).
        assert.notDeepStrictEqual(await dumpDispensers(replicaDb), await dumpDispensers(sourceDb),
            'with every=99 a single catch-up must leave the dispensers drift in place');
        client.stop();

        // every=1: the next catch-up reconciles and converges.
        await decoderFixtures.truncateAll(sourceDb);
        await decoderFixtures.truncateAll(replicaDb);
        await decoderFixtures.seedDecoderBlocks(sourceDb, 1, 5);
        await insertDispenser(sourceDb, 10, 2, 4000000000, null);
        await insertDispenser(sourceDb, 20, 3, 4000000001, null);
        poller.lastPolledBlock = await sourceDb.getLastBlock();

        client = makeClient({ every: 1 });
        await client.bootstrap();
        await client.reconcile();
        await sourceDb.doQuery('DELETE FROM dispensers WHERE tx_index = 20');
        await insertDispenser(sourceDb, 50, 6, 4000000050, null);
        await decoderFixtures.seedDecoderBlocks(sourceDb, 6, 8);

        await client.incrementalCatchUp(6);
        assert.strictEqual(await replicaDb.getLastBlock(), 8);
        assert.deepStrictEqual(await dumpDispensers(replicaDb), await dumpDispensers(sourceDb),
            'with every=1 the catch-up must reconcile dispensers to parity');
    });
});
