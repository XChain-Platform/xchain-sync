/*********************************************************************
 *
 * Copyright © 2025–2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC – https://dankest.llc
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This file is part of XChain Platform. Licensed under the GNU Affero
 * General Public License v3.0 or later; see LICENSE.md. A commercial
 * license (without AGPL source-disclosure terms) is available —
 * contact legal@dankest.llc.
 *
 **********************************************************************
 *
 * XChain Sync - Sync Service
 *
 * Top-level orchestrator. Discovers chains via the hub, creates
 * database pools for both indexer and decoder DBs, and branches
 * into server or client mode.
 *
 ********************************************************************/

const Database        = require('./db');
const HubClient       = require('./HubClient');
const ServerPoller    = require('./ServerPoller');
const BlockBroadcaster = require('./BlockBroadcaster');
const SnapshotBuilder = require('./SnapshotBuilder');
const TransparencyLog = require('./TransparencyLog');
const ClientSync      = require('./ClientSync');
const ClientApplier   = require('./ClientApplier');
const ClientRollback  = require('./ClientRollback');
const HashVerifier    = require('./HashVerifier');
const Utility         = require('./utility');

class SyncService {

    constructor(config) {
        this.config = config;
        this.util   = new Utility();

        // Hub client for chain discovery
        let hubEndpoints = HubClient.parseEndpoints(config);
        this.hubClient = new HubClient(hubEndpoints);

        // Database pools per chain/network/dbType:
        //   Map<"chain:network:dbType", { db, config, dbType }>
        // dbType is one of: 'indexer', 'decoder'
        this.databases = new Map();

        // Active pollers/syncs per chain/network/dbType
        this.pollers = new Map();
        this.clientSyncs = new Map();

        // Shared components
        this.broadcaster    = null;
        this.snapshotBuilder = null;
        this.hashVerifier   = new HashVerifier();
    }

    // Start the sync service
    async start(){
        console.log('Starting SyncService in ' + this.config['SYNC_MODE'] + ' mode...');

        // Wait for hub to be available
        await this._waitForHub();

        // Server-mode shared components must exist BEFORE discovery: _discoverChains()
        // starts a ServerPoller per chain, and each poller captures this.broadcaster at
        // construction. Creating them later (in _startServerMode, after discovery) left
        // every poller with a null broadcaster and crashed on the first _updateStatus
        // (TypeError: Cannot read properties of null (reading 'getSubscriberCount')).
        if(this.config['SYNC_MODE'] === 'server'){
            this.broadcaster     = new BlockBroadcaster(this.config);
            this.snapshotBuilder = new SnapshotBuilder(this.util);
        }

        // Discover chains and create DB pools
        await this._discoverChains();

        if(this.databases.size === 0){
            console.log('No indexer/decoder databases found. Waiting for hub config...');
        }

        // Start mode-specific components
        if(this.config['SYNC_MODE'] === 'server'){
            await this._startServerMode();
        } else {
            await this._startClientMode();
        }

        // Schedule periodic hub re-poll to detect new chains
        this._scheduleHubRepoll();
    }

    // Wait for the hub to respond
    async _waitForHub(){
        let maxWaitMs = this.config['MAX_HUB_WAIT_MS'];
        if(maxWaitMs === undefined || maxWaitMs === null)
            maxWaitMs = parseInt(process.env.MAX_HUB_WAIT_MS) || 300000;
        let startedAt = Date.now();
        let attempts = 0;
        while(true){
            if(Date.now() - startedAt >= maxWaitMs){
                console.error('Hub at ' + this.config['HUB_API_HOST'] + ':' + this.config['HUB_PORT']
                    + ' was unreachable after ' + Math.round(maxWaitMs / 1000) + 's (MAX_HUB_WAIT_MS); exiting.');
                process.exit(1);
            }
            let alive = await this.hubClient.ping();
            if(alive){
                console.log('Hub is reachable');
                return;
            }
            attempts++;
            console.log('Waiting for hub at ' + this.config['HUB_API_HOST'] + ':' + this.config['HUB_PORT']
                + '... (attempt ' + attempts + ')');
            await this.util.sleep(3000);
        }
    }

    // Discover chains from the hub and create DB pools for both indexer
    // and decoder DBs. Decoder DBs use the same connection/sync machinery
    // as indexer DBs but skip the transparency log (decoder content is
    // deterministic from the coin node — no synthetic chain-of-state hash
    // needed).
    async _discoverChains(){
        let indexerConfigs = await this.hubClient.getIndexerConfigs();
        let decoderConfigs = await this.hubClient.getDecoderConfigs();
        let allConfigs = indexerConfigs.concat(decoderConfigs);
        let newChains = [];

        for(let cfg of allConfigs){
            let key = cfg.coin + ':' + cfg.network + ':' + cfg.dbType;
            if(this.databases.has(key)) continue;

            console.log('Discovered ' + cfg.dbType + ': ' + cfg.coin + '/' + cfg.network + ' -> ' + cfg.db_name);

            let db;
            if(this.config['SYNC_MODE'] === 'client'){
                // Client mode: create replica database with same name using client DB credentials
                db = new Database(
                    this.config['REPLICA_DB_HOST'],
                    this.config['REPLICA_DB_PORT'],
                    cfg.db_name,
                    this.config['REPLICA_DB_USER'],
                    this.config['REPLICA_DB_PASS'],
                    this.util,
                    cfg.dbType
                );
                // Ensure the replica database exists
                await db.createDatabase();
                // Schema replication: try direct DB connection first (faster), fall back to
                // server's /schema endpoint during ClientSync bootstrap if DB is unreachable
                try {
                    let sourceDb = new Database(cfg.db_host, cfg.db_port, cfg.db_name, cfg.db_user, cfg.db_pass, this.util, cfg.dbType);
                    // Single-attempt probe: a node-internal source DB host is often
                    // unreachable from the replica box, and verifyDatabase() would
                    // retry forever — hanging discovery instead of falling through to
                    // the server /schema fetch below.
                    let sourceExists = await sourceDb.verifyDatabaseOnce();
                    if(sourceExists){
                        await db.replicateSchema(sourceDb);
                    }
                    await sourceDb.close();
                } catch(e){
                    // Source DB not reachable — schema will be fetched from server via /schema endpoint
                    console.log('Source DB not reachable for ' + cfg.coin + '/' + cfg.network + '/' + cfg.dbType + ' — schema will be fetched from sync server');
                }
                // sync_meta table (for transparency log) is indexer-only — skip for decoder
                if(cfg.dbType === 'indexer'){
                    await db.verifySyncTables();
                }
                // Self-heal column drift on a pre-existing replica before any row
                // data is accepted. Runs regardless of which schema path applied
                // above (direct replicateSchema or, when the source DB is
                // unreachable, the server /schema fetch during ClientSync
                // bootstrap) since it derives columns from authoritative
                // definitions rather than the source DB. No-op when the table is
                // absent or already current.
                await db.ensureReplicatedColumns();
            } else {
                // Server mode: connect to the authoritative DB using hub-provided credentials
                db = new Database(cfg.db_host, cfg.db_port, cfg.db_name, cfg.db_user, cfg.db_pass, this.util, cfg.dbType);
                // sync_meta table (for transparency log) is indexer-only — skip for decoder
                if(cfg.dbType === 'indexer'){
                    await db.verifySyncTables();
                }
            }

            this.databases.set(key, { db, config: cfg, dbType: cfg.dbType });
            newChains.push({ key, db, config: cfg });
        }

        // Start components for newly discovered chains.
        // Both server and client modes now support indexer + decoder DBs.
        if(newChains.length > 0){
            for(let { key, db, config: cfg } of newChains){
                if(this.config['SYNC_MODE'] === 'server'){
                    this._startPollerForChain(key, db, cfg);
                } else {
                    this._startClientSyncForChain(key, db, cfg);
                }
            }
        }

        return newChains;
    }

    // Start server mode components
    async _startServerMode(){
        // Idempotent: these are normally created in start() before _discoverChains()
        // so pollers can capture a live broadcaster. Guard so a direct call (or future
        // refactor) still works without clobbering the instance the pollers already hold.
        if(!this.broadcaster)     this.broadcaster     = new BlockBroadcaster(this.config);
        if(!this.snapshotBuilder) this.snapshotBuilder = new SnapshotBuilder(this.util);

        // Start a poller for each discovered DB (both indexer and decoder).
        // ServerPoller reads dbType from db.dbType and switches table lists +
        // payload structure accordingly.
        for(let [key, { db, config: cfg }] of this.databases){
            this._startPollerForChain(key, db, cfg);
        }

        console.log('Server mode started with ' + this.databases.size + ' poller(s)');
    }

    // Start a poller for a single chain/network/dbType.
    // TransparencyLog is created only for indexer DBs — decoder content is
    // deterministic from the coin node and doesn't need a synthetic hash chain.
    _startPollerForChain(key, db, cfg){
        if(this.pollers.has(key)) return;

        let log    = (cfg.dbType === 'indexer') ? new TransparencyLog(db, this.config['MERKLE_EPOCH_SIZE']) : null;
        let poller = new ServerPoller(cfg.coin, cfg.network, db, this.broadcaster, log, this.config, this.util);
        this.pollers.set(key, poller);

        // Start polling in background (don't await — runs indefinitely).
        // A throw here means this chain's poller is permanently dead, which is
        // invisible at the /status endpoint (stale block_height, live timestamp).
        // Log the full error and exit so the container restart policy surfaces it.
        poller.start().catch(e => {
            console.error('Poller crashed for ' + key + ' — exiting for restart:', e);
            process.exit(1);
        });
    }

    // Start client mode components
    async _startClientMode(){
        // ClientSync reads dbType from db.dbType and threads it through URLs +
        // skips three-hash verification for decoder DBs.
        for(let [key, { db, config: cfg }] of this.databases){
            this._startClientSyncForChain(key, db, cfg);
        }
        console.log('Client mode started with ' + this.databases.size + ' sync(s)');
    }

    // Start client sync for a single chain/network
    _startClientSyncForChain(key, db, cfg){
        if(this.clientSyncs.has(key)) return;

        let applier  = new ClientApplier(db, this.util);
        let rollback = new ClientRollback(db, this.util);
        let sync     = new ClientSync(cfg.coin, cfg.network, db, applier, rollback, this.hashVerifier, this.config, this.util);
        this.clientSyncs.set(key, sync);

        // Start syncing in background. A throw here means this chain's replica
        // sync is permanently dead while the process still appears healthy.
        // Log the full error and exit so the container restart policy surfaces it.
        sync.start().catch(e => {
            console.error('ClientSync crashed for ' + key + ' — exiting for restart:', e);
            process.exit(1);
        });
    }

    // Schedule periodic hub re-poll to detect new chains
    _scheduleHubRepoll(){
        setInterval(async () => {
            try {
                let newChains = await this._discoverChains();
                if(newChains.length > 0)
                    console.log('Discovered ' + newChains.length + ' new chain(s) from hub');
            } catch(e){
                console.error('Hub re-poll error:', e);
            }
        }, this.config['HUB_REPOLL_INTERVAL']);
    }

    // Get the broadcaster (used by api.js for WebSocket setup)
    getBroadcaster(){
        return this.broadcaster;
    }

    // Get the snapshot builder (used by api.js for REST endpoints)
    getSnapshotBuilder(){
        return this.snapshotBuilder;
    }

    // Get the database for a chain/network/dbType (used by api.js for status/snapshot endpoints).
    // dbType defaults to 'indexer' for callers that haven't been updated to be dbType-aware yet.
    getDatabase(chain, network, dbType){
        let type = dbType || 'indexer';
        let key = chain + ':' + network + ':' + type;
        let entry = this.databases.get(key);
        return entry ? entry.db : null;
    }

    // Seconds since the hub last returned config to us (drives chain discovery), or null
    // if it has never succeeded. Exposed on /health so an operator can see when the hub
    // view sync replicates against has gone stale during a hub outage.
    getHubConfigAgeSeconds(){
        let at = this.hubClient ? this.hubClient.lastSuccessfulFetchAt : null;
        return (at != null) ? Math.floor((Date.now() - at) / 1000) : null;
    }

    // Get all discovered chain/network/dbType triples
    getChains(){
        let chains = [];
        for(let [key, { config: cfg }] of this.databases){
            chains.push({ coin: cfg.coin, network: cfg.network, dbType: cfg.dbType });
        }
        return chains;
    }

    // Get client sync state for a chain/network/dbType (client mode only).
    // Returns { lastKnownServerBlock } — null values when not yet observed.
    getClientSyncState(chain, network, dbType){
        let type = dbType || 'indexer';
        let key  = chain + ':' + network + ':' + type;
        let sync = this.clientSyncs.get(key);
        return { lastKnownServerBlock: sync ? sync.lastKnownServerBlock : null };
    }

    // Get the transparency log for a chain/network. Always indexer-only —
    // the decoder DB does not maintain a transparency log (see decoder-DB
    // architecture decisions: skip TransparencyLog for decoder).
    getTransparencyLog(chain, network){
        let key = chain + ':' + network + ':indexer';
        let entry = this.databases.get(key);
        if(!entry) return null;
        // Find the poller's transparency log
        let poller = this.pollers.get(key);
        if(poller) return poller.transparencyLog;
        // If no poller, create a temporary log reader
        return new TransparencyLog(entry.db, this.config['MERKLE_EPOCH_SIZE']);
    }
}

module.exports = SyncService;
