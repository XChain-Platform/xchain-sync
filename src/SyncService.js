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
 * XChain Indexer Sync - Sync Service
 *
 * Top-level orchestrator. Discovers chains via the hub, creates
 * database pools, and branches into server or client mode.
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
        this.hubClient = new HubClient(config['HUB_API_HOST'], config['HUB_PORT'], config['HUB_PROTOCOL']);

        // Database pools per chain/network: Map<"chain:network", Database>
        this.databases = new Map();

        // Active pollers/syncs per chain/network
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

        // Discover chains and create DB pools
        await this._discoverChains();

        if(this.databases.size === 0){
            console.log('No indexer databases found. Waiting for hub config...');
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
        let attempts = 0;
        while(true){
            let alive = await this.hubClient.ping();
            if(alive){
                console.log('Hub is reachable');
                return;
            }
            attempts++;
            if(attempts % 10 === 0)
                console.log('Waiting for hub at ' + this.config['HUB_API_HOST'] + ':' + this.config['HUB_PORT'] + '...');
            await this.util.sleep(3000);
        }
    }

    // Discover chains from the hub and create DB pools
    async _discoverChains(){
        let indexerConfigs = await this.hubClient.getIndexerConfigs();
        let newChains = [];

        for(let cfg of indexerConfigs){
            let key = cfg.coin + ':' + cfg.network;
            if(this.databases.has(key)) continue;

            console.log('Discovered indexer: ' + cfg.coin + '/' + cfg.network + ' -> ' + cfg.db_name);

            let db;
            if(this.config['SYNC_MODE'] === 'client'){
                // Client mode: create replica database with same name using client DB credentials
                db = new Database(
                    this.config['REPLICA_DB_HOST'],
                    this.config['REPLICA_DB_PORT'],
                    cfg.db_name,
                    this.config['REPLICA_DB_USER'],
                    this.config['REPLICA_DB_PASS'],
                    this.util
                );
                // Ensure the replica database exists
                await db.createDatabase();
                // Schema replication: try direct DB connection first (faster), fall back to
                // server's /schema endpoint during ClientSync bootstrap if DB is unreachable
                try {
                    let sourceDb = new Database(cfg.db_host, cfg.db_port, cfg.db_name, cfg.db_user, cfg.db_pass, this.util);
                    let sourceExists = await sourceDb.verifyDatabase();
                    if(sourceExists){
                        await db.replicateSchema(sourceDb);
                    }
                    await sourceDb.close();
                } catch(e){
                    // Source DB not reachable — schema will be fetched from server via /schema endpoint
                    console.log('Source DB not reachable for ' + cfg.coin + '/' + cfg.network + ' — schema will be fetched from sync server');
                }
                // Ensure sync-service-owned tables exist (sync_meta)
                await db.verifySyncTables();
            } else {
                // Server mode: connect to the authoritative indexer DB using hub-provided credentials
                db = new Database(cfg.db_host, cfg.db_port, cfg.db_name, cfg.db_user, cfg.db_pass, this.util);
                // Ensure sync-service-owned tables exist (sync_meta for transparency log)
                await db.verifySyncTables();
            }

            this.databases.set(key, { db, config: cfg });
            newChains.push({ key, db, config: cfg });
        }

        // Start components for newly discovered chains
        if(newChains.length > 0){
            if(this.config['SYNC_MODE'] === 'server'){
                for(let { key, db, config: cfg } of newChains)
                    this._startPollerForChain(key, db, cfg);
            } else {
                for(let { key, db, config: cfg } of newChains)
                    this._startClientSyncForChain(key, db, cfg);
            }
        }

        return newChains;
    }

    // Start server mode components
    async _startServerMode(){
        this.broadcaster     = new BlockBroadcaster(this.config);
        this.snapshotBuilder = new SnapshotBuilder(this.util);

        // Start a poller for each discovered chain
        for(let [key, { db, config: cfg }] of this.databases){
            this._startPollerForChain(key, db, cfg);
        }

        console.log('Server mode started with ' + this.databases.size + ' chain(s)');
    }

    // Start a poller for a single chain/network
    _startPollerForChain(key, db, cfg){
        if(this.pollers.has(key)) return;

        let log    = new TransparencyLog(db);
        let poller = new ServerPoller(cfg.coin, cfg.network, db, this.broadcaster, log, this.config, this.util);
        this.pollers.set(key, poller);

        // Start polling in background (don't await — runs indefinitely)
        poller.start().catch(e => {
            console.error('Poller error for ' + key + ':', e.message);
        });
    }

    // Start client mode components
    async _startClientMode(){
        for(let [key, { db, config: cfg }] of this.databases){
            this._startClientSyncForChain(key, db, cfg);
        }
        console.log('Client mode started with ' + this.databases.size + ' chain(s)');
    }

    // Start client sync for a single chain/network
    _startClientSyncForChain(key, db, cfg){
        if(this.clientSyncs.has(key)) return;

        let applier  = new ClientApplier(db, this.util);
        let rollback = new ClientRollback(db, this.util);
        let sync     = new ClientSync(cfg.coin, cfg.network, db, applier, rollback, this.hashVerifier, this.config, this.util);
        this.clientSyncs.set(key, sync);

        // Start syncing in background
        sync.start().catch(e => {
            console.error('ClientSync error for ' + key + ':', e.message);
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
                console.error('Hub re-poll error:', e.message);
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

    // Get the database for a chain/network (used by api.js for status/snapshot endpoints)
    getDatabase(chain, network){
        let key = chain + ':' + network;
        let entry = this.databases.get(key);
        return entry ? entry.db : null;
    }

    // Get all discovered chain/network pairs
    getChains(){
        let chains = [];
        for(let [key, { config: cfg }] of this.databases){
            chains.push({ coin: cfg.coin, network: cfg.network });
        }
        return chains;
    }

    // Get the transparency log for a chain/network
    getTransparencyLog(chain, network){
        let key = chain + ':' + network;
        let entry = this.databases.get(key);
        if(!entry) return null;
        // Find the poller's transparency log
        let poller = this.pollers.get(key);
        if(poller) return poller.transparencyLog;
        // If no poller, create a temporary log reader
        return new TransparencyLog(entry.db);
    }
}

module.exports = SyncService;
