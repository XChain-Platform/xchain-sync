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
 * XChain Sync - API
 *
 * Entry point. Creates Express app with REST routes, attaches WebSocket
 * server for real-time subscriptions, and starts the SyncService.
 *
 * All routes are namespaced by :dbType (indexer or decoder), e.g.
 * /snapshot/indexer/BTC/mainnet, /subscribe/decoder/LTC/testnet.
 * Transparency endpoints are indexer-only (decoder has no synthetic
 * chain-of-state hashes — see xchain-sync-decoder-db-decisions).
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
const { getReplicatedTables }    = require('./replicatedTables');

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
    //
    // All routes use the /:dbType/:chain/:network namespace.
    // :dbType is one of 'indexer' or 'decoder'.

    // Validate the :dbType path segment (used by every route).
    // Returns the canonical type string, or null if invalid.
    function validateDbType(dbType){
        if(dbType === 'indexer' || dbType === 'decoder') return dbType;
        return null;
    }

    // Build the status row for one (db, dbType, chain, network) tuple.
    async function buildStatusRow(db, dbType, chain, network){
        let lastBlock = await db.getLastBlock();
        let hashRow = lastBlock !== null ? await db.getBlockHashRow(lastBlock) : null;
        let row = {
            block_height: hashRow ? Number(hashRow.block_index) : null,
            block_time:   hashRow ? Number(hashRow.block_time) : null
        };
        if(dbType === 'decoder'){
            row.block_hash = hashRow ? hashRow.block_hash : null;
        } else {
            row.ledger_hash   = hashRow ? hashRow.ledger_hash : null;
            row.actions_hash  = hashRow ? hashRow.actions_hash : null;
            row.contract_hash = hashRow ? hashRow.contract_hash : null;
        }
        if(cfg['SYNC_MODE'] === 'server'){
            // Server is the source — these fields are not applicable
            row.server_block  = null;
            row.blocks_behind = null;
            // Expose per-subscriber applied-block lag so operators can see a
            // validator falling behind before the backpressure limit force-closes
            // it. appliedBlock/lag are null for subscribers that haven't sent a
            // heartbeat yet (e.g. older client builds).
            let broadcaster = syncService.getBroadcaster();
            row.subscribers = broadcaster ? broadcaster.getSubscribers(chain, network, dbType) : [];
        } else {
            let clientState  = syncService.getClientSyncState(chain, network, dbType);
            let serverBlock  = clientState.lastKnownServerBlock;
            row.server_block  = serverBlock;
            row.blocks_behind = (serverBlock !== null && row.block_height !== null)
                ? serverBlock - row.block_height
                : null;
        }
        // Per-table row counts for replica-completeness verification.
        //
        // The committed ledger/actions/contract hashes are computed on the source
        // during block processing and replicated verbatim, so a follower missing
        // entire tables still agrees on every hash — the hashes describe the
        // source's blockchain computation, not what actually landed downstream.
        // Publishing row counts gives followers an independent completeness
        // signal: ClientSync._verifyAgainstSource compares these against its own
        // counts and flags any table the source has rows in but the follower does
        // not. Scoped to the per-block replicated set (see replicatedTables.js) so
        // legitimately-divergent snapshot-only / operator-local tables don't raise
        // false alarms. COUNT(*) per table is acceptable here — /status is an
        // operator-polled endpoint, not a hot path.
        row.table_counts = {};
        for(let table of getReplicatedTables(dbType)){
            try {
                row.table_counts[table] = await db.getTableCount(table);
            } catch(e){
                // Table absent in this schema (older replica, or decoder vs
                // indexer split) — omit it rather than fail the whole status.
            }
        }
        return row;
    }

    // GET /status — all chains, nested by coin → network → dbType
    app.get('/status', (req, res) => {
        let chains = syncService.getChains();
        let result = {};
        let promises = chains.map(async ({ coin, network, dbType }) => {
            let db = syncService.getDatabase(coin, network, dbType);
            if(!db) return;
            let row = await buildStatusRow(db, dbType, coin, network);
            if(!result[coin]) result[coin] = {};
            if(!result[coin][network]) result[coin][network] = {};
            result[coin][network][dbType] = row;
        });
        Promise.all(promises).then(() => {
            result.last_updated = new Date().toISOString();
            res.json(result);
        }).catch(e => {
            console.error('[API error] /status:', e.message);
            res.status(500).json({ error: 'Internal server error' });
        });
    });

    // GET /status/:dbType/:chain/:network
    app.get('/status/:dbType/:chain/:network', async (req, res) => {
        let dbType = validateDbType(req.params.dbType);
        if(!dbType) return res.status(400).json({ error: "Invalid dbType — must be 'indexer' or 'decoder'" });

        let { chain, network } = req.params;
        let db = syncService.getDatabase(chain, network, dbType);
        if(!db) return res.status(404).json({ error: 'Chain/network/dbType not found' });

        try {
            let row = await buildStatusRow(db, dbType, chain, network);
            row.chain = chain;
            row.network = network;
            row.dbType = dbType;
            row.last_updated = new Date().toISOString();
            res.json(row);
        } catch(e){
            console.error('[API error] /status/:dbType/:chain/:network:', e);
            res.status(500).json({ error: 'Internal server error' });
        }
    });

    // GET /schema/:dbType/:chain/:network — table DDLs for schema replication (server mode)
    app.get('/schema/:dbType/:chain/:network', async (req, res) => {
        if(cfg['SYNC_MODE'] !== 'server')
            return res.status(403).json({ error: 'Schema only available in server mode' });

        let dbType = validateDbType(req.params.dbType);
        if(!dbType) return res.status(400).json({ error: "Invalid dbType — must be 'indexer' or 'decoder'" });

        let { chain, network } = req.params;
        let db = syncService.getDatabase(chain, network, dbType);
        if(!db) return res.status(404).json({ error: 'Chain/network/dbType not found' });

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
            res.json({ chain, network, dbType, tables: schema });
        } catch(e){
            console.error('[API error] /schema/:dbType/:chain/:network:', e);
            res.status(500).json({ error: 'Internal server error' });
        }
    });

    // GET /snapshot/:dbType/:chain/:network — full snapshot (server mode)
    app.get('/snapshot/:dbType/:chain/:network', fullSnapshotLimiter, async (req, res) => {
        if(cfg['SYNC_MODE'] !== 'server')
            return res.status(403).json({ error: 'Snapshots only available in server mode' });

        let dbType = validateDbType(req.params.dbType);
        if(!dbType) return res.status(400).json({ error: "Invalid dbType — must be 'indexer' or 'decoder'" });

        let { chain, network } = req.params;
        let db = syncService.getDatabase(chain, network, dbType);
        if(!db) return res.status(404).json({ error: 'Chain/network/dbType not found' });

        let builder = syncService.getSnapshotBuilder();
        if(!builder) return res.status(500).json({ error: 'Snapshot builder not initialized' });

        try {
            await builder.streamFullSnapshot(db, res);
        } catch(e){
            console.error('[API error] /snapshot/:dbType/:chain/:network:', e);
            if(!res.headersSent)
                res.status(500).json({ error: 'Internal server error' });
        }
    });

    // GET /snapshot/:dbType/:chain/:network/since/:blockHeight — incremental snapshot (server mode)
    app.get('/snapshot/:dbType/:chain/:network/since/:blockHeight', incrSnapshotLimiter, async (req, res) => {
        if(cfg['SYNC_MODE'] !== 'server')
            return res.status(403).json({ error: 'Snapshots only available in server mode' });

        let dbType = validateDbType(req.params.dbType);
        if(!dbType) return res.status(400).json({ error: "Invalid dbType — must be 'indexer' or 'decoder'" });

        let { chain, network, blockHeight } = req.params;
        let sinceBlock = parseInt(blockHeight);
        if(isNaN(sinceBlock) || sinceBlock < 0)
            return res.status(400).json({ error: 'Invalid blockHeight' });

        let db = syncService.getDatabase(chain, network, dbType);
        if(!db) return res.status(404).json({ error: 'Chain/network/dbType not found' });

        let builder = syncService.getSnapshotBuilder();
        if(!builder) return res.status(500).json({ error: 'Snapshot builder not initialized' });

        try {
            await builder.streamIncrementalSnapshot(db, sinceBlock, res);
        } catch(e){
            console.error('[API error] /snapshot/:dbType/:chain/:network/since/:blockHeight:', e);
            if(!res.headersSent)
                res.status(500).json({ error: 'Internal server error' });
        }
    });

    // ── Transparency endpoints (indexer only) ──
    // Decoder DB doesn't have synthetic chain-of-state hashes, so the
    // transparency log doesn't apply. Decoder requests return 400.

    // GET /transparency/:dbType/:chain/:network/roots — transparency log
    app.get('/transparency/:dbType/:chain/:network/roots', transparencyLimiter, async (req, res) => {
        if(cfg['SYNC_MODE'] !== 'server')
            return res.status(403).json({ error: 'Transparency log only available in server mode' });
        if(req.params.dbType !== 'indexer')
            return res.status(400).json({ error: 'Transparency log is indexer-only — decoder DB has no synthetic chain-of-state hashes' });

        let { chain, network } = req.params;
        let log = syncService.getTransparencyLog(chain, network);
        if(!log) return res.status(404).json({ error: 'Chain/network not found' });

        let page  = parseInt(req.query.page) || 0;
        let limit = parseInt(req.query.limit) || 100;

        try {
            let result = await log.getPage(page, limit);
            res.json(result);
        } catch(e){
            console.error('[API error] /transparency/:dbType/:chain/:network/roots:', e);
            res.status(500).json({ error: 'Internal server error' });
        }
    });

    // GET /transparency/:dbType/:chain/:network/proof/:block_index — Merkle inclusion proof
    app.get('/transparency/:dbType/:chain/:network/proof/:block_index', transparencyLimiter, async (req, res) => {
        if(cfg['SYNC_MODE'] !== 'server')
            return res.status(403).json({ error: 'Transparency log only available in server mode' });
        if(req.params.dbType !== 'indexer')
            return res.status(400).json({ error: 'Transparency log is indexer-only' });

        let { chain, network, block_index } = req.params;
        let log = syncService.getTransparencyLog(chain, network);
        if(!log) return res.status(404).json({ error: 'Chain/network not found' });

        try {
            let result = await log.getProof(block_index);
            if(!result) return res.status(404).json({ error: 'Block not found' });
            res.json(result);
        } catch(e){
            console.error('[API error] /transparency/.../proof/' + block_index + ':', e);
            res.status(500).json({ error: 'Internal server error' });
        }
    });

    // GET /transparency/:dbType/:chain/:network/root/latest — latest committed Merkle root
    app.get('/transparency/:dbType/:chain/:network/root/latest', transparencyLimiter, async (req, res) => {
        if(cfg['SYNC_MODE'] !== 'server')
            return res.status(403).json({ error: 'Transparency log only available in server mode' });
        if(req.params.dbType !== 'indexer')
            return res.status(400).json({ error: 'Transparency log is indexer-only' });

        let { chain, network } = req.params;
        let log = syncService.getTransparencyLog(chain, network);
        if(!log) return res.status(404).json({ error: 'Chain/network not found' });

        try {
            let result = await log.getLatestRoot();
            res.json(result || { epoch: null, merkle_root: null });
        } catch(e){
            console.error('[API error] /transparency/.../root/latest:', e);
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

        // Parse the path: /subscribe/:dbType/:chain/:network[?sync_mode=full|infra-only]
        let match = request.url.match(/^\/subscribe\/([^\/]+)\/([^\/]+)\/([^\/\?]+)(?:\?(.*))?/);
        if(!match){
            socket.destroy();
            return;
        }

        let dbType  = match[1];
        let chain   = match[2];
        let network = match[3];

        // Reject unknown dbTypes
        if(dbType !== 'indexer' && dbType !== 'decoder'){
            socket.destroy();
            return;
        }

        // Parse query string for sync_mode preference
        // Subscribers can request 'full' (default — all tables) or 'infra-only' (only cross-chain
        // infrastructure tables: stakes, delegations, validator_rewards, prices, etc.)
        // 'infra-only' is indexer-only; decoder always serves the full table set.
        let syncMode = 'full';
        if(match[4]){
            let qs = new URLSearchParams(match[4]);
            let mode = qs.get('sync_mode');
            if(mode === 'infra-only' && dbType === 'indexer') syncMode = 'infra-only';
        }

        // Verify this chain/network/dbType is supported
        let db = syncService.getDatabase(chain, network, dbType);
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
            broadcaster.addSubscription(ws, request, chain, network, syncMode, dbType);
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

    // Periodic status broadcasts (server mode) — once per (chain, network, dbType)
    if(cfg['SYNC_MODE'] === 'server'){
        setInterval(() => {
            let broadcaster = syncService.getBroadcaster();
            if(!broadcaster) return;
            let chains = syncService.getChains();
            for(let { coin, network, dbType } of chains){
                broadcaster.broadcastStatus(coin, network, dbType);
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
