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
 * XChain Sync - API
 *
 * Entry point. Creates Express app with REST routes, attaches WebSocket
 * server for real-time subscriptions, and starts the SyncService.
 *
 * All routes are namespaced by :dbType (indexer or decoder), e.g.
 * /snapshot/indexer/BTC/mainnet, /subscribe/decoder/LTC/testnet.
 * Transparency endpoints are indexer-only (decoder has no synthetic
 * chain-of-state hashes; see xchain-sync-decoder-db-decisions).
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
const Utility     = require('./utility');
const BlockHasher = require('./BlockHasher');
const { createApiKeyMiddleware, safeEqual } = require('./middleware');
const { getReplicatedTables }    = require('./replicatedTables');

// Stateless helper for the advisory index-map parity checksum published on
// /status (server mode). getDataHash holds no per-call state, so one shared
// instance is safe. See BlockHasher.computeIndexMapChecksum (NON-consensus).
const statusUtil = new Utility();

dotenv.config();

const REQUIRED_ENV = ['HUB_API_HOST'];
for(const key of REQUIRED_ENV){
    if(!process.env[key]){
        console.error('Missing required environment variable: ' + key);
        process.exit(1);
    }
}

const cfg = config.getConfig();

// SYNC_API_KEY is optional, matching the other services: unset leaves the
// REST/WS replication endpoints open (single-host / regtest / managed
// deployments; xchain-node injects no key) and is warned about loudly below;
// when configured, every endpoint fails closed (401) without it. The
// destructive /halt/clear admin route additionally refuses to run at all
// while no key is configured.
if(!cfg['SYNC_API_KEY'])
    console.warn('WARNING: SYNC_API_KEY is not set. The REST/WS API is UNAUTHENTICATED and /halt/clear is disabled. Set a key for any shared or public-facing deployment.');

async function startApi(){

    const app = express();
    app.use(helmet());
    app.use(cors({ origin: cfg['CORS_ORIGIN'], methods: ['GET', 'POST'] }));
    app.use(express.json({ limit: '16kb' }));
    app.use(createApiKeyMiddleware(cfg['SYNC_API_KEY']));

    // Rate limiters for snapshot endpoints
    // Key snapshot limits per (client IP + chain/network/dbType), NOT per IP alone.
    // A single replica bootstraps every chain it follows from one IP, so a global
    // per-IP bucket would let one chain's snapshot exhaust the budget and 429 all the
    // others. block-height (incremental /since/:blockHeight) is intentionally excluded
    // so the bucket is stable per resource across catch-ups.
    const snapshotKey = (req) => req.ip + '|' + req.params.dbType + '/' + req.params.chain + '/' + req.params.network;

    const fullSnapshotLimiter = rateLimit({
        windowMs: 60 * 60 * 1000,
        limit: cfg['SNAPSHOT_RATE_FULL'],
        keyGenerator: snapshotKey,
        standardHeaders: true,
        legacyHeaders: false,
        message: { error: 'Full snapshot rate limit exceeded. Try again later.' }
    });

    const incrSnapshotLimiter = rateLimit({
        windowMs: 60 * 60 * 1000,
        limit: cfg['SNAPSHOT_RATE_INCR'],
        keyGenerator: snapshotKey,
        standardHeaders: true,
        legacyHeaders: false,
        message: { error: 'Incremental snapshot rate limit exceeded. Try again later.' }
    });

    const transparencyLimiter = rateLimit({
        windowMs: 60 * 1000,
        limit: cfg['TRANSPARENCY_RATE_LIMIT'] || 10,
        standardHeaders: true,
        legacyHeaders: false,
        message: { error: 'Transparency endpoint rate limit exceeded. Try again later.' }
    });

    const heartbeatLimiter = rateLimit({
        windowMs: 60 * 1000,
        limit: 120,
        standardHeaders: true,
        legacyHeaders: false,
        message: { error: 'Heartbeat rate limit exceeded.' }
    });

    const syncService = new SyncService(cfg);

    // REST Routes (server mode only, but status works in client mode too)
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
        if(cfg['SYNC_MODE'] === 'server'){
            // In server mode, block_height is the broadcaster's last-polled position
            // (how far the poller has actually broadcast), not the source DB tip.
            // Using the source DB tip here hides poller lag: if the poller is wedged
            // or catching up, block_height would show a climbing source tip with no
            // lag signal. The WS _updateStatus path (ServerPoller) correctly separates
            // lastPolledBlock from the source tip; REST now matches those semantics.
            let broadcaster = syncService.getBroadcaster();
            let statusData = broadcaster ? broadcaster.statusData : null;
            // statusData is keyed by "chain:network:dbType" inside BlockBroadcaster.
            // The status object stored by ServerPoller._updateStatus has block_height
            // (polled position) and source_block_height (DB tip) already separated.
            let key = chain + ':' + network + ':' + (dbType || 'indexer');
            let pollerStatus = (statusData && statusData.get) ? statusData.get(key) : null;

            let polledBlock = (pollerStatus && pollerStatus.block_height != null)
                ? pollerStatus.block_height : null;
            let sourceBlock = (pollerStatus && pollerStatus.source_block_height != null)
                ? pollerStatus.source_block_height : (await db.getLastBlock());

            let hashRow = polledBlock !== null ? await db.getBlockHashRow(polledBlock) : null;
            let row = {
                block_height:  polledBlock,
                source_height: sourceBlock,
                lag_blocks:    (sourceBlock !== null && polledBlock !== null)
                                   ? Math.max(0, sourceBlock - polledBlock) : null,
                block_time:    hashRow ? Number(hashRow.block_time) : null,
                poll_error_count: (pollerStatus && pollerStatus.poll_error_count != null)
                                   ? pollerStatus.poll_error_count : 0
            };
            if(dbType === 'decoder'){
                row.block_hash = hashRow ? hashRow.block_hash : null;
            } else {
                row.ledger_hash   = hashRow ? hashRow.ledger_hash : null;
                row.actions_hash  = hashRow ? hashRow.actions_hash : null;
                row.contract_hash = hashRow ? hashRow.contract_hash : null;
                // Advisory id->address map parity (NON-consensus, default off). Computed
                // over the deterministic subset of index_addresses on the SOURCE, bounded
                // to the SAME polledBlock height this status publishes, so a follower at
                // that exact height can recompute over its replica and compare. A divergent
                // id map is invisible to the three resolved-string hashes above and to a
                // plain row count (equal count, different content), so this is the only
                // signal that catches it. Off by default (it scans the subset; see
                // BlockHasher.computeIndexMapChecksum cost note); null => follower skips.
                row.index_map_checksum = null;
                if(cfg['INDEX_MAP_PARITY_CHECK'] && polledBlock !== null){
                    try {
                        row.index_map_checksum = await new BlockHasher(db, statusUtil).computeIndexMapChecksum(polledBlock);
                    } catch(e){
                        console.error('[API] index_map_checksum compute failed for ' + chain + '/' + network +
                            ' at block ' + polledBlock + ' (advisory, returning null):', e.message);
                    }
                }
            }
            // Expose per-subscriber applied-block lag so operators can see a
            // validator falling behind before the backpressure limit force-closes it.
            row.subscribers = broadcaster ? broadcaster.getSubscribers(chain, network, dbType) : [];
            // Per-table row counts (same logic as client-mode path below)
            row.table_counts = {};
            for(let table of getReplicatedTables(dbType)){
                try {
                    row.table_counts[table] = await db.getTableCount(table);
                } catch(e){
                    // Table absent in this schema; omit rather than fail the whole status.
                }
            }
            // Lifetime full-snapshot serve count (incremented by SnapshotBuilder
            // on each successful streamFullSnapshot completion; 0 until first serve).
            let builder = syncService.getSnapshotBuilder();
            row.snapshots_served = builder ? (builder.snapshotsServed || 0) : 0;
            return row;
        }

        // Client mode: block_height is whatever the replica DB has applied.
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
        {
            let clientState  = syncService.getClientSyncState(chain, network, dbType);
            let sourceHeight = clientState.lastKnownServerBlock;
            row.source_height = sourceHeight;
            row.lag_blocks    = (sourceHeight !== null && row.block_height !== null)
                ? Math.max(0, sourceHeight - row.block_height)
                : null;
            // Freshness of source_height/lag_blocks. lastKnownServerBlock only advances
            // on live WS events, so after a silent disconnect it freezes and lag_blocks
            // settles to 0 once the replica catches up to the stale tip. This flag tells
            // an operator the lag figure is computed against a source height we have not
            // heard confirmed recently (null = no live event seen yet, staleness unknown).
            row.source_height_stale = clientState.sourceHeightStale;
            // Consensus-divergence halt: a halted client has STOPPED applying and
            // requires operator clearance. Surfaced so the dashboard monitor and
            // peers see a forked/Byzantine validator immediately.
            row.halted = clientState.halted || false;
            if(clientState.halted) row.halt = clientState.haltInfo;
            // Truncated-replica visibility: lets an explorer or operator know
            // this replica cannot answer pre-base history queries.
            row.truncated      = clientState.truncated || false;
            row.bootstrap_base = clientState.bootstrapBase != null ? clientState.bootstrapBase : null;
        }
        // Per-table row counts for replica-completeness verification.
        //
        // The committed ledger/actions/contract hashes are computed on the source
        // during block processing and replicated verbatim, so a follower missing
        // entire tables still agrees on every hash. The hashes describe the
        // source's blockchain computation, not what actually landed downstream.
        // Publishing row counts gives followers an independent completeness
        // signal: ClientSync._verifyAgainstSource compares these against its own
        // counts and flags any table the source has rows in but the follower does
        // not. Scoped to the per-block replicated set (see replicatedTables.js) so
        // legitimately-divergent snapshot-only / operator-local tables don't raise
        // false alarms. COUNT(*) per table is acceptable here; /status is an
        // operator-polled endpoint, not a hot path.
        row.table_counts = {};
        for(let table of getReplicatedTables(dbType)){
            try {
                row.table_counts[table] = await db.getTableCount(table);
            } catch(e){
                // Table absent in this schema (older replica, or decoder vs
                // indexer split); omit rather than fail the whole status.
            }
        }
        return row;
    }

    // GET /health : lightweight liveness + DB circuit-breaker visibility.
    // /status reports per-chain block heights and lag, but not whether a
    // database connection has tripped its circuit breaker open after repeated
    // failures. When that happens the replicator stops applying blocks while the
    // process stays up, so a bare liveness probe still looks fine. This endpoint
    // surfaces the per-database circuit state so monitoring can tell a healthy
    // replicator apart from one stalled on a database outage.
    app.get('/health', (req, res) => {
        let chains = syncService.getChains();
        let databases = [];
        let degraded = false;
        for(let { coin, network, dbType } of chains){
            let db = syncService.getDatabase(coin, network, dbType);
            if(!db) continue;
            let circuit = db.circuitState || null;
            // The circuit breaker only opens after circuitThreshold (10)
            // consecutive acquisition failures (db.js), so a dead origin DB would
            // otherwise read 'healthy' for up to 10 BLOCK_POLL_INTERVAL cycles
            // while every snapshot request is already 500ing. ServerPoller's
            // pollErrorCount resets to 0 on the next successful poll, so a
            // non-zero count means the poller is currently in a failing streak:
            // the earliest reliable outage signal. Flip to 503 degraded on it.
            let poller = syncService.getPoller(coin, network, dbType);
            let pollErrorCount = poller ? poller.pollErrorCount : 0;
            if(circuit === 'open' || pollErrorCount > 0) degraded = true;
            databases.push({ chain: coin, network: network, dbType: dbType, circuit: circuit, poll_error_count: pollErrorCount });
        }
        if(degraded) res.status(503);
        res.json({
            status:       degraded ? 'degraded' : 'healthy',
            mode:         cfg['SYNC_MODE'],
            databases:    databases,
            // Age of the last successful hub-config fetch (null until the first success).
            // Sync rediscovers chains from hub config on a timer; a climbing age here while
            // status stays healthy means the hub is unreachable and the chain set is stale.
            hub_config_age_seconds: syncService.getHubConfigAgeSeconds(),
            last_updated: new Date().toISOString()
        });
    });

    // GET /status : all chains, nested by coin/network/dbType
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
            res.status(500).json({ error: 'Internal server error', code: 'INTERNAL_ERROR' });
        });
    });

    // GET /status/:dbType/:chain/:network
    app.get('/status/:dbType/:chain/:network', async (req, res) => {
        let dbType = validateDbType(req.params.dbType);
        if(!dbType) return res.status(400).json({ error: "Invalid dbType. Must be 'indexer' or 'decoder'", code: 'BAD_REQUEST' });

        let { chain, network } = req.params;
        let db = syncService.getDatabase(chain, network, dbType);
        if(!db) return res.status(404).json({ error: 'Chain/network/dbType not found', code: 'NOT_FOUND' });

        try {
            let row = await buildStatusRow(db, dbType, chain, network);
            row.chain = chain;
            row.network = network;
            row.dbType = dbType;
            row.last_updated = new Date().toISOString();
            res.json(row);
        } catch(e){
            console.error('[API error] /status/:dbType/:chain/:network:', e);
            res.status(500).json({ error: 'Internal server error', code: 'INTERNAL_ERROR' });
        }
    });

    // GET /checkpoint/:dbType/:chain/:network/latest  : newest quorum-signed checkpoint
    // GET /checkpoint/:dbType/:chain/:network/:height  : the checkpoint at a height
    //
    // Serves the hub-mirrored, federation-signed checkpoint rows (indexer DB only)
    // so a CLIENT replica can anchor its independently-recomputed state_root to the
    // federation quorum (SPV) instead of trusting this server's claimed values. The
    // signatures are self-authenticating; the client verifies them against its OWN
    // out-of-band pinned validator set, never a set this endpoint supplies. The table
    // is append-only (a reorged height is superseded by a higher checkpoint_seq), so
    // both routes take the MAX checkpoint_seq. The /latest literal is registered
    // before /:height so it is not parsed as a height.
    function serializeCheckpoint(r){
        return {
            chain: r.chain, network: r.network,
            block_index: Number(r.block_index),
            block_hash: r.block_hash, ledger_hash: r.ledger_hash,
            actions_hash: r.actions_hash, contract_hash: r.contract_hash,
            checkpoint_seq: Number(r.checkpoint_seq),
            snapshot_block: Number(r.snapshot_block),
            state_root: r.state_root,
            state_root_version: r.state_root_version == null ? null : Number(r.state_root_version),
            block_merkle_root: r.block_merkle_root,
            block_merkle_version: r.block_merkle_version == null ? null : Number(r.block_merkle_version),
            validator_signatures: r.validator_signatures
        };
    }
    const CHECKPOINT_COLS = 'chain, network, block_index, block_hash, ledger_hash, actions_hash, ' +
        'contract_hash, checkpoint_seq, snapshot_block, state_root, state_root_version, ' +
        'block_merkle_root, block_merkle_version, validator_signatures';

    app.get('/checkpoint/:dbType/:chain/:network/latest', incrSnapshotLimiter, async (req, res) => {
        let dbType = validateDbType(req.params.dbType);
        if(!dbType) return res.status(400).json({ error: "Invalid dbType. Must be 'indexer' or 'decoder'", code: 'BAD_REQUEST' });
        if(dbType !== 'indexer') return res.status(400).json({ error: 'Checkpoints exist only for indexer DBs', code: 'BAD_REQUEST' });
        let { chain, network } = req.params;
        let db = syncService.getDatabase(chain, network, dbType);
        if(!db) return res.status(404).json({ error: 'Chain/network/dbType not found', code: 'NOT_FOUND' });
        try {
            let rows = await db.doQuery(
                'SELECT ' + CHECKPOINT_COLS + ' FROM state_checkpoints ORDER BY block_index DESC, checkpoint_seq DESC LIMIT 1');
            if(!rows || !rows.length) return res.status(404).json({ error: 'No checkpoints', code: 'NOT_FOUND' });
            res.json(serializeCheckpoint(rows[0]));
        } catch(e){
            console.error('[API error] /checkpoint/.../latest:', e);
            res.status(500).json({ error: 'Internal server error', code: 'INTERNAL_ERROR' });
        }
    });

    // GET /checkpoint/:dbType/:chain/:network/range?from=&to=
    //
    // The signed-checkpoint chain over a block range, oldest first, one row per
    // block_index (MAX checkpoint_seq, so a reorged height is represented by its
    // surviving checkpoint). A CLIENT replica walks this to roll its pinned trust
    // root FORWARD across validator rotation: the launch (pinned) set eventually
    // stops signing, so the client proves each successor oracle_publish set against
    // the committed BTC stakes_root and adopts the next checkpoint (spec §7.3), the
    // sync analogue of the SDK light client's followForward. Indexer-only; result
    // is capped at CHECKPOINT_RANGE_LIMIT rows so the client pages by advancing
    // `from`. Singular path, consistent with /checkpoint/.../:height and
    // /checkpoint/.../latest. MUST be registered before /:height, since :height
    // matches any single segment and would otherwise swallow 'range' as a height.
    const CHECKPOINT_RANGE_LIMIT = 2000;
    app.get('/checkpoint/:dbType/:chain/:network/range', incrSnapshotLimiter, async (req, res) => {
        let dbType = validateDbType(req.params.dbType);
        if(!dbType) return res.status(400).json({ error: "Invalid dbType. Must be 'indexer' or 'decoder'", code: 'BAD_REQUEST' });
        if(dbType !== 'indexer') return res.status(400).json({ error: 'Checkpoints exist only for indexer DBs', code: 'BAD_REQUEST' });
        let from = parseInt(req.query.from, 10);
        let to = parseInt(req.query.to, 10);
        if(!Number.isFinite(from) || from < 0) return res.status(400).json({ error: 'Invalid from', code: 'BAD_REQUEST' });
        if(!Number.isFinite(to) || to < from) return res.status(400).json({ error: 'Invalid to (must be >= from)', code: 'BAD_REQUEST' });
        let { chain, network } = req.params;
        let db = syncService.getDatabase(chain, network, dbType);
        if(!db) return res.status(404).json({ error: 'Chain/network/dbType not found', code: 'NOT_FOUND' });
        try {
            let rows = await db.doQuery(
                'SELECT ' + CHECKPOINT_COLS + ' FROM state_checkpoints sc ' +
                'WHERE block_index >= ? AND block_index <= ? ' +
                'AND checkpoint_seq = (SELECT MAX(s2.checkpoint_seq) FROM state_checkpoints s2 WHERE s2.block_index = sc.block_index) ' +
                'ORDER BY block_index ASC LIMIT ?',
                [from, to, CHECKPOINT_RANGE_LIMIT]);
            res.json({ checkpoints: (rows || []).map(serializeCheckpoint) });
        } catch(e){
            console.error('[API error] /checkpoint/.../range:', e);
            res.status(500).json({ error: 'Internal server error', code: 'INTERNAL_ERROR' });
        }
    });

    app.get('/checkpoint/:dbType/:chain/:network/:height', incrSnapshotLimiter, async (req, res) => {
        let dbType = validateDbType(req.params.dbType);
        if(!dbType) return res.status(400).json({ error: "Invalid dbType. Must be 'indexer' or 'decoder'", code: 'BAD_REQUEST' });
        if(dbType !== 'indexer') return res.status(400).json({ error: 'Checkpoints exist only for indexer DBs', code: 'BAD_REQUEST' });
        let h = parseInt(req.params.height, 10);
        if(!Number.isFinite(h) || h < 0) return res.status(400).json({ error: 'Invalid height', code: 'BAD_REQUEST' });
        let { chain, network } = req.params;
        let db = syncService.getDatabase(chain, network, dbType);
        if(!db) return res.status(404).json({ error: 'Chain/network/dbType not found', code: 'NOT_FOUND' });
        try {
            let rows = await db.doQuery(
                'SELECT ' + CHECKPOINT_COLS + ' FROM state_checkpoints WHERE block_index=? ORDER BY checkpoint_seq DESC LIMIT 1',
                [h]);
            if(!rows || !rows.length) return res.status(404).json({ error: 'No checkpoint at that height', code: 'NOT_FOUND' });
            res.json(serializeCheckpoint(rows[0]));
        } catch(e){
            console.error('[API error] /checkpoint/:dbType/:chain/:network/:height:', e);
            res.status(500).json({ error: 'Internal server error', code: 'INTERNAL_ERROR' });
        }
    });

    // GET /catalog : the databases this server offers to sync, with sizes + tips.
    // Open/read-only. One server-wide information_schema query (cached ~30s) so
    // page loads don't hammer the DB. Powers the sync.xchain.io "Browse databases" UI.
    let _catalogCache = { at: 0, payload: null };
    app.get('/catalog', async (req, res) => {
        try {
            let now = Date.now();
            if(_catalogCache.payload && (now - _catalogCache.at) < 30000){
                return res.json(_catalogCache.payload);
            }
            let chains = syncService.getChains();
            let statsByName = {};
            if(chains.length){
                let anyDb = syncService.getDatabase(chains[0].coin, chains[0].network, chains[0].dbType);
                if(anyDb){
                    for(let s of await anyDb.getDatabaseStats()) statsByName[s.db_name] = s;
                }
            }
            let databases = await Promise.all(chains.map(async ({ coin, network, dbType }) => {
                let db = syncService.getDatabase(coin, network, dbType);
                let dbName = db ? db.dbName : null;
                let blockHeight = null;
                try { if(db) blockHeight = await db.getLastBlock(); } catch(e){}
                let st = (dbName && statsByName[dbName]) || {};
                let dataBytes  = Number(st.data_bytes || 0);
                let indexBytes = Number(st.index_bytes || 0);
                return {
                    coin, network, dbType,
                    db_name:      dbName,
                    block_height: blockHeight,
                    table_count:  Number(st.tables || 0),
                    data_bytes:   dataBytes,
                    index_bytes:  indexBytes,
                    total_bytes:  dataBytes + indexBytes
                };
            }));
            let payload = { generated_at: new Date().toISOString(), databases };
            _catalogCache = { at: now, payload };
            res.json(payload);
        } catch(e){
            console.error('[API error] /catalog:', e);
            res.status(500).json({ error: 'Internal server error', code: 'INTERNAL_ERROR' });
        }
    });

    // GET /schema/:dbType/:chain/:network : table DDLs for schema replication (server mode)
    app.get('/schema/:dbType/:chain/:network', async (req, res) => {
        if(cfg['SYNC_MODE'] !== 'server')
            return res.status(403).json({ error: 'Schema only available in server mode', code: 'FORBIDDEN' });

        let dbType = validateDbType(req.params.dbType);
        if(!dbType) return res.status(400).json({ error: "Invalid dbType. Must be 'indexer' or 'decoder'", code: 'BAD_REQUEST' });

        let { chain, network } = req.params;
        let db = syncService.getDatabase(chain, network, dbType);
        if(!db) return res.status(404).json({ error: 'Chain/network/dbType not found', code: 'NOT_FOUND' });

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
            res.status(500).json({ error: 'Internal server error', code: 'INTERNAL_ERROR' });
        }
    });

    // GET /snapshot/:dbType/:chain/:network : full snapshot (server mode)
    app.get('/snapshot/:dbType/:chain/:network', fullSnapshotLimiter, async (req, res) => {
        if(cfg['SYNC_MODE'] !== 'server')
            return res.status(403).json({ error: 'Snapshots only available in server mode', code: 'FORBIDDEN' });

        let dbType = validateDbType(req.params.dbType);
        if(!dbType) return res.status(400).json({ error: "Invalid dbType. Must be 'indexer' or 'decoder'", code: 'BAD_REQUEST' });

        let { chain, network } = req.params;
        let db = syncService.getDatabase(chain, network, dbType);
        if(!db) return res.status(404).json({ error: 'Chain/network/dbType not found', code: 'NOT_FOUND' });

        let builder = syncService.getSnapshotBuilder();
        if(!builder) return res.status(500).json({ error: 'Snapshot builder not initialized', code: 'INTERNAL_ERROR' });

        try {
            await builder.streamFullSnapshot(db, res);
        } catch(e){
            console.error('[API error] /snapshot/:dbType/:chain/:network:', e);
            if(!res.headersSent)
                res.status(500).json({ error: 'Internal server error', code: 'INTERNAL_ERROR' });
        }
    });

    // GET /snapshot/:dbType/:chain/:network/since/:blockHeight : incremental snapshot (server mode)
    app.get('/snapshot/:dbType/:chain/:network/since/:blockHeight', incrSnapshotLimiter, async (req, res) => {
        if(cfg['SYNC_MODE'] !== 'server')
            return res.status(403).json({ error: 'Snapshots only available in server mode', code: 'FORBIDDEN' });

        let dbType = validateDbType(req.params.dbType);
        if(!dbType) return res.status(400).json({ error: "Invalid dbType. Must be 'indexer' or 'decoder'", code: 'BAD_REQUEST' });

        let { chain, network, blockHeight } = req.params;
        let sinceBlock = parseInt(blockHeight);
        if(isNaN(sinceBlock) || sinceBlock < 0)
            return res.status(400).json({ error: 'Invalid blockHeight', code: 'BAD_REQUEST' });

        let db = syncService.getDatabase(chain, network, dbType);
        if(!db) return res.status(404).json({ error: 'Chain/network/dbType not found', code: 'NOT_FOUND' });

        let builder = syncService.getSnapshotBuilder();
        if(!builder) return res.status(500).json({ error: 'Snapshot builder not initialized', code: 'INTERNAL_ERROR' });

        // skip_lookups=1: omit the append-only `.index` lookup tables (index_*,
        // decoder pubkeys/events). A truncated/fast-chain replica syncs those via the
        // paged /snapshot-rows route so a single multi-million-row full-dump can't
        // blow past the client's content limit. Default off (full bundled snapshot).
        let skipLookups = (req.query.skip_lookups === '1' || req.query.skip_lookups === 'true');

        try {
            // `chain` is the coin key (e.g. 'BTC'); the builder needs it to resolve the
            // frozen ACTIVATION_DELAY_BLOCKS for the in-place updated-rows channel.
            await builder.streamIncrementalSnapshot(db, sinceBlock, res, chain, { skipLookups });
        } catch(e){
            console.error('[API error] /snapshot/:dbType/:chain/:network/since/:blockHeight:', e);
            if(!res.headersSent)
                res.status(500).json({ error: 'Internal server error', code: 'INTERNAL_ERROR' });
        }
    });

    // GET /snapshot-rows/:dbType/:chain/:network/:table?after_id=&limit= : one
    // id-ordered page of an append-only lookup table (server mode). Lets a truncated
    // replica sync a multi-million-row lookup table (e.g. index_transactions) in
    // bounded pages instead of one full-dump that would exceed the content limit.
    // The builder allowlists :table to the pageable `.index` set (also the
    // SQL-identifier guard). Rate-limited as an incremental fetch.
    app.get('/snapshot-rows/:dbType/:chain/:network/:table', incrSnapshotLimiter, async (req, res) => {
        if(cfg['SYNC_MODE'] !== 'server')
            return res.status(403).json({ error: 'Snapshots only available in server mode', code: 'FORBIDDEN' });

        let dbType = validateDbType(req.params.dbType);
        if(!dbType) return res.status(400).json({ error: "Invalid dbType. Must be 'indexer' or 'decoder'", code: 'BAD_REQUEST' });

        let { chain, network, table } = req.params;
        let afterId = parseInt(req.query.after_id);
        if(isNaN(afterId)) afterId = 0;
        if(afterId < 0) return res.status(400).json({ error: 'Invalid after_id', code: 'BAD_REQUEST' });
        let limit = parseInt(req.query.limit);
        if(isNaN(limit)) limit = undefined; // builder applies its default

        let db = syncService.getDatabase(chain, network, dbType);
        if(!db) return res.status(404).json({ error: 'Chain/network/dbType not found', code: 'NOT_FOUND' });

        let builder = syncService.getSnapshotBuilder();
        if(!builder) return res.status(500).json({ error: 'Snapshot builder not initialized', code: 'INTERNAL_ERROR' });

        try {
            await builder.streamTableRowsById(db, table, afterId, limit, res);
        } catch(e){
            console.error('[API error] /snapshot-rows/:dbType/:chain/:network/:table:', e);
            if(!res.headersSent)
                res.status(500).json({ error: 'Internal server error', code: 'INTERNAL_ERROR' });
        }
    });

    // GET /snapshot-dispensers/:dbType/:chain/:network?after_tx=&after_addr=&limit=
    // One keyset page of the decoder `dispensers` table for the client's replace-table
    // reconcile. dispensers rides neither the block stream nor the id-cursor lookup
    // paging (no monotonic id; the decoder soft-expires/hard-purges rows), so the
    // client periodically re-dumps the full table and swaps it in atomically; see
    // SnapshotBuilder.streamDispensers + ClientSync._reconcileDispensers. Decoder-only;
    // rate-limited as an incremental fetch.
    app.get('/snapshot-dispensers/:dbType/:chain/:network', incrSnapshotLimiter, async (req, res) => {
        if(cfg['SYNC_MODE'] !== 'server')
            return res.status(403).json({ error: 'Snapshots only available in server mode', code: 'FORBIDDEN' });

        let dbType = validateDbType(req.params.dbType);
        if(!dbType) return res.status(400).json({ error: "Invalid dbType. Must be 'indexer' or 'decoder'", code: 'BAD_REQUEST' });
        if(dbType !== 'decoder') return res.status(400).json({ error: 'dispensers reconcile is decoder-only', code: 'BAD_REQUEST' });

        let { chain, network } = req.params;
        let afterTx   = (req.query.after_tx   !== undefined) ? parseInt(req.query.after_tx)   : NaN;
        let afterAddr = (req.query.after_addr !== undefined) ? parseInt(req.query.after_addr) : NaN;
        if((req.query.after_tx   !== undefined && (isNaN(afterTx)   || afterTx   < 0)) ||
           (req.query.after_addr !== undefined && (isNaN(afterAddr) || afterAddr < 0)))
            return res.status(400).json({ error: 'Invalid cursor', code: 'BAD_REQUEST' });
        let limit = parseInt(req.query.limit);
        if(isNaN(limit)) limit = undefined; // builder applies its default

        let db = syncService.getDatabase(chain, network, dbType);
        if(!db) return res.status(404).json({ error: 'Chain/network/dbType not found', code: 'NOT_FOUND' });

        let builder = syncService.getSnapshotBuilder();
        if(!builder) return res.status(500).json({ error: 'Snapshot builder not initialized', code: 'INTERNAL_ERROR' });

        try {
            await builder.streamDispensers(db, afterTx, afterAddr, limit, res);
        } catch(e){
            console.error('[API error] /snapshot-dispensers/:dbType/:chain/:network:', e);
            if(!res.headersSent)
                res.status(500).json({ error: 'Internal server error', code: 'INTERNAL_ERROR' });
        }
    });

    // Validator heartbeat endpoints (server mode only)

    // POST /validator-heartbeat/:dbType/:chain/:network
    // Accepts { validator_id, applied_height, applied_block_time? } from a named validator.
    // Stores the entry in BlockBroadcaster keyed by validator_id so operators can
    // observe per-validator lag without requiring an active WebSocket connection.
    app.post('/validator-heartbeat/:dbType/:chain/:network', heartbeatLimiter, (req, res) => {
        if(cfg['SYNC_MODE'] !== 'server')
            return res.status(403).json({ error: 'Validator heartbeat only available in server mode', code: 'FORBIDDEN' });

        let dbType = validateDbType(req.params.dbType);
        if(!dbType) return res.status(400).json({ error: "Invalid dbType. Must be 'indexer' or 'decoder'", code: 'BAD_REQUEST' });

        let { chain, network } = req.params;
        let db = syncService.getDatabase(chain, network, dbType);
        if(!db) return res.status(404).json({ error: 'Chain/network/dbType not found', code: 'NOT_FOUND' });

        let { validator_id, applied_height, applied_block_time } = req.body || {};

        if(typeof validator_id !== 'string' || !validator_id.trim() || validator_id.length > 256)
            return res.status(400).json({ error: 'validator_id must be a non-empty string (max 256 chars)', code: 'BAD_REQUEST' });
        if(typeof applied_height !== 'number' || !Number.isInteger(applied_height) || applied_height < 0)
            return res.status(400).json({ error: 'applied_height must be a non-negative integer', code: 'BAD_REQUEST' });
        if(applied_block_time !== undefined && applied_block_time !== null && typeof applied_block_time !== 'number')
            return res.status(400).json({ error: 'applied_block_time must be a number', code: 'BAD_REQUEST' });

        let broadcaster = syncService.getBroadcaster();
        if(!broadcaster) return res.status(503).json({ error: 'Broadcaster not initialized', code: 'SERVICE_UNAVAILABLE' });

        broadcaster.recordValidatorHeartbeat(chain, network, dbType, validator_id.trim(), applied_height, applied_block_time || null);
        res.json({ ok: true });
    });

    // GET /validator-status : all chains, nested by coin/network/dbType/validators
    app.get('/validator-status', (req, res) => {
        if(cfg['SYNC_MODE'] !== 'server')
            return res.status(403).json({ error: 'Validator status only available in server mode', code: 'FORBIDDEN' });

        let broadcaster = syncService.getBroadcaster();
        if(!broadcaster) return res.status(503).json({ error: 'Broadcaster not initialized', code: 'SERVICE_UNAVAILABLE' });

        let chains = syncService.getChains();
        let result = {};
        for(let { coin, network, dbType } of chains){
            // getValidatorHeartbeats returns { validators, total, expected_total,
            // unknown_count }; surface it directly so the expected-roster denominator,
            // unknown_count, and any 'stale'/'absent' entries ride each leaf of the tree.
            if(!result[coin]) result[coin] = {};
            if(!result[coin][network]) result[coin][network] = {};
            result[coin][network][dbType] = broadcaster.getValidatorHeartbeats(coin, network, dbType);
        }
        result.last_updated = new Date().toISOString();
        res.json(result);
    });

    // GET /validator-status/:dbType/:chain/:network
    app.get('/validator-status/:dbType/:chain/:network', (req, res) => {
        if(cfg['SYNC_MODE'] !== 'server')
            return res.status(403).json({ error: 'Validator status only available in server mode', code: 'FORBIDDEN' });

        let dbType = validateDbType(req.params.dbType);
        if(!dbType) return res.status(400).json({ error: "Invalid dbType. Must be 'indexer' or 'decoder'", code: 'BAD_REQUEST' });

        let { chain, network } = req.params;
        let db = syncService.getDatabase(chain, network, dbType);
        if(!db) return res.status(404).json({ error: 'Chain/network/dbType not found', code: 'NOT_FOUND' });

        let broadcaster = syncService.getBroadcaster();
        if(!broadcaster) return res.status(503).json({ error: 'Broadcaster not initialized', code: 'SERVICE_UNAVAILABLE' });

        // getValidatorHeartbeats returns { validators, total, expected_total,
        // unknown_count }; spread it so the roster denominator and counts sit
        // alongside the validators map (with 'stale'/'absent' entries) in the response.
        let vstatus = broadcaster.getValidatorHeartbeats(chain, network, dbType);
        res.json({ chain, network, dbType, ...vstatus, last_updated: new Date().toISOString() });
    });

    // Transparency endpoints (indexer only)
    // Decoder DB doesn't have synthetic chain-of-state hashes, so the
    // transparency log doesn't apply. Decoder requests return 400.

    // GET /transparency/:dbType/:chain/:network/roots : transparency log entries
    app.get('/transparency/:dbType/:chain/:network/roots', transparencyLimiter, async (req, res) => {
        if(cfg['SYNC_MODE'] !== 'server')
            return res.status(403).json({ error: 'Transparency log only available in server mode', code: 'FORBIDDEN' });
        if(req.params.dbType !== 'indexer')
            return res.status(400).json({ error: 'Transparency log is indexer-only. Decoder DB has no synthetic chain-of-state hashes.', code: 'BAD_REQUEST' });

        let { chain, network } = req.params;
        let log = syncService.getTransparencyLog(chain, network);
        if(!log) return res.status(404).json({ error: 'Chain/network not found', code: 'NOT_FOUND' });

        let page  = parseInt(req.query.page) || 0;
        let limit = parseInt(req.query.limit) || 100;

        try {
            let result = await log.getPage(page, limit);
            res.json(result);
        } catch(e){
            console.error('[API error] /transparency/:dbType/:chain/:network/roots:', e);
            res.status(500).json({ error: 'Internal server error', code: 'INTERNAL_ERROR' });
        }
    });

    // GET /transparency/:dbType/:chain/:network/proof/:block_index : Merkle inclusion proof
    app.get('/transparency/:dbType/:chain/:network/proof/:block_index', transparencyLimiter, async (req, res) => {
        if(cfg['SYNC_MODE'] !== 'server')
            return res.status(403).json({ error: 'Transparency log only available in server mode', code: 'FORBIDDEN' });
        if(req.params.dbType !== 'indexer')
            return res.status(400).json({ error: 'Transparency log is indexer-only', code: 'BAD_REQUEST' });

        let { chain, network, block_index } = req.params;
        let log = syncService.getTransparencyLog(chain, network);
        if(!log) return res.status(404).json({ error: 'Chain/network not found', code: 'NOT_FOUND' });

        try {
            let result = await log.getProof(block_index);
            if(!result) return res.status(404).json({ error: 'Block not found', code: 'NOT_FOUND' });
            res.json(result);
        } catch(e){
            console.error('[API error] /transparency/.../proof/' + block_index + ':', e);
            res.status(500).json({ error: 'Internal server error', code: 'INTERNAL_ERROR' });
        }
    });

    // GET /transparency/:dbType/:chain/:network/root/latest : latest committed Merkle root
    app.get('/transparency/:dbType/:chain/:network/root/latest', transparencyLimiter, async (req, res) => {
        if(cfg['SYNC_MODE'] !== 'server')
            return res.status(403).json({ error: 'Transparency log only available in server mode', code: 'FORBIDDEN' });
        if(req.params.dbType !== 'indexer')
            return res.status(400).json({ error: 'Transparency log is indexer-only', code: 'BAD_REQUEST' });

        let { chain, network } = req.params;
        let log = syncService.getTransparencyLog(chain, network);
        if(!log) return res.status(404).json({ error: 'Chain/network not found', code: 'NOT_FOUND' });

        try {
            let result = await log.getLatestRoot();
            res.json(result || { epoch: null, merkle_root: null });
        } catch(e){
            console.error('[API error] /transparency/.../root/latest:', e);
            res.status(500).json({ error: 'Internal server error', code: 'INTERNAL_ERROR' });
        }
    });

    // POST /halt/clear/:dbType/:chain/:network : operator acknowledges an
    // investigated consensus-divergence halt and lets the client resume. Never
    // automatic: a halted validator must not self-resume onto a contested chain.
    // Bearer-authenticated (SYNC_API_KEY) and ALWAYS fails closed: unlike the
    // replication endpoints, this route is rejected outright when no key is
    // configured (resuming a halted validator must never be reachable
    // unauthenticated). Restart the service afterwards for a clean catch-up of
    // any blocks missed during the halt.
    app.post('/halt/clear/:dbType/:chain/:network', async (req, res) => {
        let apiKey = cfg['SYNC_API_KEY'];
        if(!apiKey || !safeEqual(req.headers['authorization'], 'Bearer ' + apiKey))
            return res.status(401).json({ error: 'Unauthorized', code: 'UNAUTHORIZED' });
        if(cfg['SYNC_MODE'] === 'server')
            return res.status(403).json({ error: 'Halt clearing only applies to client mode', code: 'FORBIDDEN' });

        let dbType = validateDbType(req.params.dbType);
        if(!dbType) return res.status(400).json({ error: "Invalid dbType. Must be 'indexer' or 'decoder'", code: 'BAD_REQUEST' });
        let { chain, network } = req.params;

        let client = syncService.getClientSync(chain, network, dbType);
        if(!client) return res.status(404).json({ error: 'Chain/network/dbType client not found', code: 'NOT_FOUND' });
        if(!client.isHalted()) return res.json({ ok: true, halted: false, message: 'Client was not halted' });

        try {
            let was = await client.clearHalt();
            res.json({ ok: true, cleared: true, was, note: 'Restart the sync service for a clean catch-up.' });
        } catch(e){
            console.error('[API error] /halt/clear:', e);
            res.status(500).json({ error: 'Internal server error', code: 'INTERNAL_ERROR' });
        }
    });

    const server = http.createServer(app);

    const wss = new WebSocket.Server({ noServer: true });

    server.on('upgrade', (request, socket, head) => {
        // API key authentication for WebSocket connections (enforced only when
        // a key is configured; see the SYNC_API_KEY note at the top). Managed
        // validators replicate keyless, so an unconditional reject here would
        // sever their streaming sync.
        let apiKey = cfg['SYNC_API_KEY'];
        if(apiKey){
            let authHeader = request.headers['authorization'];
            if(!authHeader || !safeEqual(authHeader, 'Bearer ' + apiKey)){
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

        if(dbType !== 'indexer' && dbType !== 'decoder'){
            socket.destroy();
            return;
        }

        // Parse query string for sync_mode preference.
        // Subscribers can request 'full' (default, all tables) or 'infra-only' (only cross-chain
        // infrastructure tables: stakes, delegations, validator_rewards, prices, etc.).
        // 'infra-only' is indexer-only; decoder always serves the full table set.
        let syncMode = 'full';
        if(match[4]){
            let qs = new URLSearchParams(match[4]);
            let mode = qs.get('sync_mode');
            if(mode === 'infra-only' && dbType === 'indexer') syncMode = 'infra-only';
        }

        let db = syncService.getDatabase(chain, network, dbType);
        if(!db){
            socket.destroy();
            return;
        }

        // WebSocket subscriptions are server-mode only.
        let broadcaster = syncService.getBroadcaster();
        if(!broadcaster){
            socket.destroy();
            return;
        }

        wss.handleUpgrade(request, socket, head, (ws) => {
            // Fire the wss 'connection' handler so the keepalive wiring (ws.isAlive +
            // the pong listener set in wss.on('connection')) actually runs. A manual
            // handleUpgrade does NOT emit 'connection' on its own, so without this the
            // pong tracking is never attached and the ping interval terminates every
            // subscriber on its second tick (~2x WS_PING_INTERVAL) regardless of pongs.
            wss.emit('connection', ws, request);
            broadcaster.addSubscription(ws, request, chain, network, syncMode, dbType);
        });
    });

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

    // Periodic status broadcasts (server mode), once per (chain, network, dbType)
    if(cfg['SYNC_MODE'] === 'server'){
        setInterval(() => {
            let broadcaster = syncService.getBroadcaster();
            if(!broadcaster) return;
            let chains = syncService.getChains();
            for(let { coin, network, dbType } of chains){
                broadcaster.broadcastStatus(coin, network, dbType);
            }
        }, cfg['WS_STATUS_INTERVAL']);

        // Stale validator-heartbeat eviction, run every 30 seconds.
        // Removes entries whose last_seen exceeds VALIDATOR_HEARTBEAT_TTL so the map
        // does not accumulate dead entries after validators disconnect.
        setInterval(() => {
            let broadcaster = syncService.getBroadcaster();
            if(broadcaster) broadcaster.evictStaleValidators(cfg['VALIDATOR_HEARTBEAT_TTL']);
        }, 30000);
    }

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
