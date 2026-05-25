/*********************************************************************
 *
 * Copyright © 2025 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC – https://dankest.llc
 *
 * Licensed under the Dankest Community License (Apache License 2.0 + Additional Terms).
 * You may not use this file except in compliance with that License.
 *
 * A copy of the License is available at:
 *     https://dankest.llc/license
 *
 * This software is provided "AS IS", without warranties or conditions of any kind.
 *
 **********************************************************************
 *
 * XChain Indexer Sync - API
 *
 * Entry point. Creates Express app with REST routes, attaches WebSocket
 * server for real-time subscriptions, and starts the SyncService.
 *
 ********************************************************************/

const dotenv      = require('dotenv');
const express     = require('express');
const helmet      = require('helmet');
const cors        = require('cors');
const http        = require('http');
const WebSocket   = require('ws');
const rateLimit   = require('express-rate-limit');
const config      = require('./config');
const SyncService = require('./SyncService');
const { createApiKeyMiddleware } = require('./middleware');

// Parse .env
dotenv.config();

// Validate required environment variables
const REQUIRED_ENV = ['HUB_API_HOST'];
for(const key of REQUIRED_ENV){
    if(!process.env[key]){
        console.error('Missing required environment variable: ' + key);
        process.exit(1);
    }
}

// Get configuration
const cfg = config.getConfig();

async function startApi(){

    // Create Express app
    const app = express();
    app.use(helmet());
    app.use(cors({ origin: cfg['CORS_ORIGIN'], methods: ['GET'] }));
    app.use(createApiKeyMiddleware(cfg['SYNC_API_KEY']));

    // Rate limiters for snapshot endpoints
    const fullSnapshotLimiter = rateLimit({
        windowMs: 60 * 60 * 1000,
        max: cfg['SNAPSHOT_RATE_FULL'],
        standardHeaders: true,
        legacyHeaders: false,
        message: { error: 'Full snapshot rate limit exceeded. Try again later.' }
    });

    const incrSnapshotLimiter = rateLimit({
        windowMs: 60 * 60 * 1000,
        max: cfg['SNAPSHOT_RATE_INCR'],
        standardHeaders: true,
        legacyHeaders: false,
        message: { error: 'Incremental snapshot rate limit exceeded. Try again later.' }
    });

    const transparencyLimiter = rateLimit({
        windowMs: 60 * 1000,
        max: cfg['TRANSPARENCY_RATE_LIMIT'] || 10,
        standardHeaders: true,
        legacyHeaders: false,
        message: { error: 'Transparency endpoint rate limit exceeded. Try again later.' }
    });

    // Initialize the SyncService
    const syncService = new SyncService(cfg);

    // ── REST Routes (server mode only, but status works in client mode too) ──

    // GET /status — all chains
    app.get('/status', (req, res) => {
        let chains = syncService.getChains();
        let result = {};
        let promises = chains.map(async ({ coin, network }) => {
            let db = syncService.getDatabase(coin, network);
            if(!db) return;
            let hashRow = await db.getBlockHashRow(await db.getLastBlock());
            if(!result[coin]) result[coin] = {};
            result[coin][network] = {
                block_height: hashRow ? Number(hashRow.block_index) : null,
                block_time: hashRow ? Number(hashRow.block_time) : null,
                ledger_hash: hashRow ? hashRow.ledger_hash : null,
                actions_hash: hashRow ? hashRow.actions_hash : null,
                contract_hash: hashRow ? hashRow.contract_hash : null
            };
        });
        Promise.all(promises).then(() => {
            result.last_updated = new Date().toISOString();
            res.json(result);
        }).catch(e => {
            console.error('[API error] /status:', e.message);
            res.status(500).json({ error: 'Internal server error' });
        });
    });

    // GET /status/:chain/:network
    app.get('/status/:chain/:network', async (req, res) => {
        let { chain, network } = req.params;
        let db = syncService.getDatabase(chain, network);
        if(!db) return res.status(404).json({ error: 'Chain/network not found' });

        try {
            let lastBlock = await db.getLastBlock();
            let hashRow = lastBlock !== null ? await db.getBlockHashRow(lastBlock) : null;
            res.json({
                chain, network,
                block_height: hashRow ? Number(hashRow.block_index) : null,
                block_time: hashRow ? Number(hashRow.block_time) : null,
                ledger_hash: hashRow ? hashRow.ledger_hash : null,
                actions_hash: hashRow ? hashRow.actions_hash : null,
                contract_hash: hashRow ? hashRow.contract_hash : null,
                last_updated: new Date().toISOString()
            });
        } catch(e){
            console.error('[API error] /status/:chain/:network:', e.message);
            res.status(500).json({ error: 'Internal server error' });
        }
    });

    // GET /schema/:chain/:network — table DDLs for schema replication (server mode)
    app.get('/schema/:chain/:network', async (req, res) => {
        if(cfg['SYNC_MODE'] !== 'server')
            return res.status(403).json({ error: 'Schema only available in server mode' });

        let { chain, network } = req.params;
        let db = syncService.getDatabase(chain, network);
        if(!db) return res.status(404).json({ error: 'Chain/network not found' });

        try {
            let tables = await db.doQuery(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_type = 'BASE TABLE' ORDER BY table_name",
                [db.dbName]
            );
            let schema = {};
            for(let row of tables){
                let tableName = row.table_name || row.TABLE_NAME;
                let ddlRows = await db.doQuery("SHOW CREATE TABLE `" + tableName + "`");
                if(ddlRows.length > 0)
                    schema[tableName] = ddlRows[0]['Create Table'];
            }
            res.json({ chain, network, tables: schema });
        } catch(e){
            console.error('[API error] /schema/:chain/:network:', e.message);
            res.status(500).json({ error: 'Internal server error' });
        }
    });

    // GET /snapshot/:chain/:network — full snapshot (server mode)
    app.get('/snapshot/:chain/:network', fullSnapshotLimiter, async (req, res) => {
        if(cfg['SYNC_MODE'] !== 'server')
            return res.status(403).json({ error: 'Snapshots only available in server mode' });

        let { chain, network } = req.params;
        let db = syncService.getDatabase(chain, network);
        if(!db) return res.status(404).json({ error: 'Chain/network not found' });

        let builder = syncService.getSnapshotBuilder();
        if(!builder) return res.status(500).json({ error: 'Snapshot builder not initialized' });

        try {
            await builder.streamFullSnapshot(db, res);
        } catch(e){
            console.error('[API error] /snapshot/:chain/:network:', e.message);
            if(!res.headersSent)
                res.status(500).json({ error: 'Internal server error' });
        }
    });

    // GET /snapshot/:chain/:network/since/:blockHeight — incremental snapshot (server mode)
    app.get('/snapshot/:chain/:network/since/:blockHeight', incrSnapshotLimiter, async (req, res) => {
        if(cfg['SYNC_MODE'] !== 'server')
            return res.status(403).json({ error: 'Snapshots only available in server mode' });

        let { chain, network, blockHeight } = req.params;
        let sinceBlock = parseInt(blockHeight);
        if(isNaN(sinceBlock) || sinceBlock < 0)
            return res.status(400).json({ error: 'Invalid blockHeight' });

        let db = syncService.getDatabase(chain, network);
        if(!db) return res.status(404).json({ error: 'Chain/network not found' });

        let builder = syncService.getSnapshotBuilder();
        if(!builder) return res.status(500).json({ error: 'Snapshot builder not initialized' });

        try {
            await builder.streamIncrementalSnapshot(db, sinceBlock, res);
        } catch(e){
            console.error('[API error] /snapshot/:chain/:network/since/:blockHeight:', e.message);
            if(!res.headersSent)
                res.status(500).json({ error: 'Internal server error' });
        }
    });

    // GET /transparency/:chain/:network/roots — transparency log
    app.get('/transparency/:chain/:network/roots', transparencyLimiter, async (req, res) => {
        if(cfg['SYNC_MODE'] !== 'server')
            return res.status(403).json({ error: 'Transparency log only available in server mode' });

        let { chain, network } = req.params;
        let log = syncService.getTransparencyLog(chain, network);
        if(!log) return res.status(404).json({ error: 'Chain/network not found' });

        let page  = parseInt(req.query.page) || 0;
        let limit = parseInt(req.query.limit) || 100;

        try {
            let result = await log.getPage(page, limit);
            res.json(result);
        } catch(e){
            console.error('[API error] /transparency/:chain/:network/roots:', e.message);
            res.status(500).json({ error: 'Internal server error' });
        }
    });

    // GET /transparency/:chain/:network/proof/:block_index — Merkle inclusion proof
    app.get('/transparency/:chain/:network/proof/:block_index', transparencyLimiter, async (req, res) => {
        if(cfg['SYNC_MODE'] !== 'server')
            return res.status(403).json({ error: 'Transparency log only available in server mode' });

        let { chain, network, block_index } = req.params;
        let log = syncService.getTransparencyLog(chain, network);
        if(!log) return res.status(404).json({ error: 'Chain/network not found' });

        try {
            let result = await log.getProof(block_index);
            if(!result) return res.status(404).json({ error: 'Block not found' });
            res.json(result);
        } catch(e){
            console.error('[API error] /transparency/.../proof/' + block_index + ':', e.message);
            res.status(500).json({ error: 'Internal server error' });
        }
    });

    // GET /transparency/:chain/:network/root/latest — latest committed Merkle root
    app.get('/transparency/:chain/:network/root/latest', transparencyLimiter, async (req, res) => {
        if(cfg['SYNC_MODE'] !== 'server')
            return res.status(403).json({ error: 'Transparency log only available in server mode' });

        let { chain, network } = req.params;
        let log = syncService.getTransparencyLog(chain, network);
        if(!log) return res.status(404).json({ error: 'Chain/network not found' });

        try {
            let result = await log.getLatestRoot();
            res.json(result || { epoch: null, merkle_root: null });
        } catch(e){
            console.error('[API error] /transparency/.../root/latest:', e.message);
            res.status(500).json({ error: 'Internal server error' });
        }
    });

    // ── HTTP + WebSocket Server ──

    const server = http.createServer(app);

    // WebSocket server attached to the same HTTP server
    const wss = new WebSocket.Server({ noServer: true });

    // Handle WebSocket upgrade requests
    server.on('upgrade', (request, socket, head) => {
        // API key authentication for WebSocket connections
        let apiKey = cfg['SYNC_API_KEY'];
        if(apiKey){
            let authHeader = request.headers['authorization'];
            if(!authHeader || authHeader !== 'Bearer ' + apiKey){
                socket.write('HTTP/1.1 401 Unauthorized\r\n\r\n');
                socket.destroy();
                return;
            }
        }

        // Parse the path: /subscribe/:chain/:network[?sync_mode=full|infra-only]
        let match = request.url.match(/^\/subscribe\/([^\/]+)\/([^\/\?]+)(?:\?(.*))?/);
        if(!match){
            socket.destroy();
            return;
        }

        let chain   = match[1];
        let network = match[2];

        // Parse query string for sync_mode preference
        // Subscribers can request 'full' (default — all tables) or 'infra-only' (only cross-chain
        // infrastructure tables: stakes, delegations, validator_rewards, prices, etc.)
        let syncMode = 'full';
        if(match[3]){
            let qs = new URLSearchParams(match[3]);
            let mode = qs.get('sync_mode');
            if(mode === 'infra-only') syncMode = 'infra-only';
        }

        // Verify this chain/network is supported
        let db = syncService.getDatabase(chain, network);
        if(!db){
            socket.destroy();
            return;
        }

        // Only allow WebSocket subscriptions in server mode
        let broadcaster = syncService.getBroadcaster();
        if(!broadcaster){
            socket.destroy();
            return;
        }

        wss.handleUpgrade(request, socket, head, (ws) => {
            broadcaster.addSubscription(ws, request, chain, network, syncMode);
        });
    });

    // WebSocket ping interval to detect dead connections
    const pingInterval = setInterval(() => {
        wss.clients.forEach((ws) => {
            if(ws.isAlive === false){
                ws.terminate();
                return;
            }
            ws.isAlive = false;
            ws.ping();
        });
    }, cfg['WS_PING_INTERVAL']);

    wss.on('connection', (ws) => {
        ws.isAlive = true;
        ws.on('pong', () => { ws.isAlive = true; });
    });

    wss.on('close', () => {
        clearInterval(pingInterval);
    });

    // Periodic status broadcasts (server mode)
    if(cfg['SYNC_MODE'] === 'server'){
        setInterval(() => {
            let broadcaster = syncService.getBroadcaster();
            if(!broadcaster) return;
            let chains = syncService.getChains();
            for(let { coin, network } of chains){
                broadcaster.broadcastStatus(coin, network);
            }
        }, cfg['WS_STATUS_INTERVAL']);
    }

    // Start the HTTP server
    server.listen(cfg['SYNC_API_PORT'], () => {
        console.log('xchain-sync API listening on port ' + cfg['SYNC_API_PORT']);
    });

    // Start the SyncService (discovers chains and begins polling/syncing)
    syncService.start().catch((error) => {
        console.error('Fatal SyncService error:', error);
        process.exit(1);
    });
}

startApi();
