// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

const sinon    = require('sinon');
const http     = require('http');
const express  = require('express');
const cors     = require('cors');
const { parseCorsOrigin } = require('../../../src/http/cors_origin');
const setup    = require('../helpers/setup');
const testDb   = require('../helpers/testDb');
const MockHub  = require('../helpers/mockHub');
const SnapshotBuilder  = require('../../../src/server/snapshot_builder');
const TransparencyLog  = require('../../../src/server/transparency_log');
// Trust-proxy and rate-limiter wiring is imported from the real api.js rather
// than re-declared here. api.js now guards its startup env check and listen()
// behind require.main === module (see module.exports at the bottom), so
// requiring it for these two seams no longer opens a port or starts polling.
const { trustProxyHops, createRateLimiters } = require('../../../src/api');

const API_PORT = 19100;
const HUB_PORT = 19000;

function mountStatusRoutes(app, state) {
    app.get('/status', async (req, res) => {
        try {
            let lastBlock = await state.sourceDb.getLastBlock();
            let hashRow = lastBlock !== null ? await state.sourceDb.getBlockHashRow(lastBlock) : null;
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
            let lastBlock = await state.sourceDb.getLastBlock();
            let hashRow = lastBlock !== null ? await state.sourceDb.getBlockHashRow(lastBlock) : null;
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
}

function mountSnapshotRoutes(app, state, limiters) {
    app.get('/schema/:dbType/:chain/:network', async (req, res) => {
        try {
            let tables = await state.sourceDb.doQuery(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_type = 'BASE TABLE' ORDER BY table_name",
                [state.sourceDb.dbName]
            );
            let schema = {};
            for (let row of tables) {
                let tableName = row.table_name || row.TABLE_NAME;
                let ddlRows = await state.sourceDb.doQuery("SHOW CREATE TABLE `" + tableName + "`");
                if (ddlRows.length > 0) schema[tableName] = ddlRows[0]['Create Table'];
            }
            res.json({ chain: req.params.chain, network: req.params.network, tables: schema });
        } catch (e) { res.status(500).json({ error: e.message }); }
    });

    app.get('/snapshot/:dbType/:chain/:network', limiters.fullSnapshotLimiter, async (req, res) => {
        try {
            await state.snapshotBuilder.streamFullSnapshot(state.sourceDb, res);
        } catch (e) {
            if (!res.headersSent) res.status(500).json({ error: e.message });
        }
    });

    app.get('/snapshot/:dbType/:chain/:network/since/:blockHeight', limiters.incrSnapshotLimiter, async (req, res) => {
        let sinceBlock = parseInt(req.params.blockHeight);
        if (isNaN(sinceBlock) || sinceBlock < 0)
            return res.status(400).json({ error: 'Invalid blockHeight' });
        try {
            await state.snapshotBuilder.streamIncrementalSnapshot(state.sourceDb, sinceBlock, res);
        } catch (e) {
            if (!res.headersSent) res.status(500).json({ error: e.message });
        }
    });
}

function mountTransparencyRoutes(app, state, limiters) {
    app.get('/transparency/:dbType/:chain/:network/roots', limiters.transparencyLimiter, async (req, res) => {
        try {
            let page  = parseInt(req.query.page) || 0;
            let limit = parseInt(req.query.limit) || 100;
            let result = await state.log.getPage(page, limit);
            res.json(result);
        } catch (e) { res.status(500).json({ error: e.message }); }
    });

    app.get('/transparency/:dbType/:chain/:network/proof/:block_index', limiters.transparencyLimiter, async (req, res) => {
        if (state.testSyncMode !== 'server')
            return res.status(403).json({ error: 'Transparency log only available in server mode' });
        if (req.params.dbType !== 'indexer')
            return res.status(400).json({ error: 'Transparency log is indexer-only' });
        let { chain, network, block_index } = req.params;
        if (chain !== 'bitcoin' || network !== 'mainnet')
            return res.status(404).json({ error: 'Chain/network not found' });
        try {
            let result = await state.log.getProof(block_index);
            if (!result) return res.status(404).json({ error: 'Block not found' });
            res.json(result);
        } catch (e) { res.status(500).json({ error: e.message }); }
    });

    app.get('/transparency/:dbType/:chain/:network/root/latest', limiters.transparencyLimiter, async (req, res) => {
        if (state.testSyncMode !== 'server')
            return res.status(403).json({ error: 'Transparency log only available in server mode' });
        if (req.params.dbType !== 'indexer')
            return res.status(400).json({ error: 'Transparency log is indexer-only' });
        let { chain, network } = req.params;
        if (chain !== 'bitcoin' || network !== 'mainnet')
            return res.status(404).json({ error: 'Chain/network not found' });
        try {
            let result = await state.log.getLatestRoot();
            res.json(result || { epoch: null, merkle_root: null });
        } catch (e) { res.status(500).json({ error: e.message }); }
    });
}

async function startHarness(state) {
    await setup.globalSetup();
    state.sourceDb = setup.getSourceDb();

    // Start mock hub
    state.mockHub = new MockHub();
    state.mockHub.setConfigs([{
        coin: 'bitcoin', network: 'mainnet',
        db_host: testDb.TEST_DB_HOST, db_port: testDb.TEST_DB_PORT,
        db_name: testDb.SOURCE_DB_NAME, db_user: testDb.TEST_DB_USER, db_pass: testDb.TEST_DB_PASS
    }]);
    await state.mockHub.start(HUB_PORT);

    state.snapshotBuilder = new SnapshotBuilder(testDb.util);
    state.log = new TransparencyLog(state.sourceDb);

    // Trust-proxy and rate-limiter wiring come from src/api.js itself (not a
    // re-declaration), so a change to either seam is exercised here the same
    // way it is in production.  was exactly this class of bug: a
    // hand-rolled app that never set 'trust proxy' or mounted a limiter would
    // pass this suite while the real service resolved every caller to one IP.
    let app = express();
    app.set('trust proxy', trustProxyHops(false)); // matches production default: no reverse proxy trusted
    app.use(cors({ origin: parseCorsOrigin(process.env.CORS_ORIGIN), methods: ['GET'] }));

    // Same limiter instances startApi() builds and mounts, not a
    // re-declaration of their windows/limits/keying. TRANSPARENCY_RATE_LIMIT
    // is widened from production's 10/min default: this suite's `before()`
    // builds the app once for the whole file, and its transparency-route
    // tests collectively issue more requests than that within one run.
    let limiters = createRateLimiters({
        SNAPSHOT_RATE_FULL: 100,
        SNAPSHOT_RATE_INCR: 100,
        TRANSPARENCY_RATE_LIMIT: 100000
    });
    app.use(limiters.backstopLimiter);
    mountStatusRoutes(app, state);
    mountSnapshotRoutes(app, state, limiters);
    mountTransparencyRoutes(app, state, limiters);

    state.server = http.createServer(app);
    await new Promise(resolve => state.server.listen(API_PORT, resolve));
    state.baseUrl = 'http://127.0.0.1:' + API_PORT;
}

async function stopHarness(state) {
    sinon.restore();
    await new Promise(resolve => state.server.close(resolve));
    await state.mockHub.stop();
    await setup.globalTeardown();
}

function defineRestApiSuite(registerTests) {
    describe('Integration: REST API', function() {
        const state = { testSyncMode: 'server' };

        before(async function() {
            await startHarness(state);
            sinon.stub(console, 'log');
            sinon.stub(console, 'error');
        });

        after(async function() {
            await stopHarness(state);
        });

        beforeEach(async function() {
            await testDb.truncateAll(state.sourceDb);
        });

        registerTests(state);
    });
}

module.exports = { defineRestApiSuite };
