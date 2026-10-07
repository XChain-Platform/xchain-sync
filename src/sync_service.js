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
 * XChain Sync - Sync Service
 *
 * Top-level orchestrator. Discovers chains via the hub, creates
 * database pools for both indexer and decoder DBs, and branches
 * into server or client mode.
 *
 ********************************************************************/

const Database        = require('./db');
const HubClient       = require('./hub/client');
const ServerPoller    = require('./server/poller');
const BlockBroadcaster = require('./server/block_broadcaster');
const SnapshotBuilder = require('./server/snapshot_builder');
const TransparencyLog = require('./server/transparency_log');
const ClientSync      = require('./client/sync');
const ClientApplier   = require('./client/applier');
const ClientRollback  = require('./client/rollback');
const HashVerifier    = require('./client/hash_verifier');
const { assertTransactionsDataWidth } = require('./client/decoder_link');
const stateCommitment = require('./state_commitment');
const { assertBootstrapDepthChains } = require('./config');
const { assertPinnedEnvOverrides }   = require('./client/pinned_validators');
const { coinTicker, activationDelayBlocks } = require('./consensus-constants');
const Utility         = require('./util');
// Resolved at each call site rather than bound once: the shim is installed by the
// entry file after this module is required, and getLogger() hands back a lazy
// façade that reaches the real sink once that has happened.
const { getLogger }   = require('./observability');
const util = require('node:util');
const envConfig = require('./config');

class SyncService {

    constructor(config) {
        this.config = config;
        this.util   = new Utility();

        let hubEndpoints = HubClient.parseEndpoints(config);
        this.hubClient = new HubClient(hubEndpoints);

        // Map<"chain:network:dbType", { db, config, dbType }>, dbType being
        // 'indexer' or 'decoder'. The pollers/syncs maps use the same key.
        this.databases = new Map();

        this.pollers = new Map();
        this.clientSyncs = new Map();

        // Keys of indexer chains skipped for an unrecognized coin, so a hub re-poll logs each once.
        this.unrecognizedCoinKeys = new Set();

        this.broadcaster    = null;
        this.snapshotBuilder = null;
        this.hashVerifier   = new HashVerifier();

        // Startup readiness, false until start() has discovered chains and begun
        // polling. api.js listens BEFORE start() runs and start() can sit in
        // waitForHub for MAX_HUB_WAIT_MS (default 5 minutes), during which
        // getChains() is empty and /health's per-chain loop finds nothing to
        // degrade on, so the probe read healthy while nothing was syncing.
        this.ready = false;
    }

    // True once start() has completed. Kept separate from "has chains" so a
    // discovered-but-legitimately-empty chain set (everything SYNC_EXCLUDEd)
    // still reads healthy rather than degrading forever.
    isReady(){
        return this.ready;
    }

    async start(){
        getLogger().info('Starting SyncService in ' + this.config['SYNC_MODE'] + ' mode...');

        await this.waitForHub();

        // Server-mode shared components must exist BEFORE discovery: discoverChains()
        // starts a ServerPoller per chain, and each poller captures this.broadcaster at
        // construction. Creating them later (in startServerMode, after discovery) left
        // every poller with a null broadcaster and crashed on the first updateStatus
        // (TypeError: Cannot read properties of null (reading 'getSubscriberCount')).
        if(this.config['SYNC_MODE'] === 'server'){
            this.broadcaster     = new BlockBroadcaster(this.config);
            this.snapshotBuilder = new SnapshotBuilder(this.util);
        }

        await this.discoverChains();

        if(this.databases.size === 0){
            getLogger().info('No indexer/decoder databases found. Waiting for hub config...');
        }

        if(this.config['SYNC_MODE'] === 'server'){
            await this.startServerMode();
        } else {
            await this.startClientMode();
        }

        this.scheduleHubRepoll();
        this.startStateTreeMetric();
        this.startSyncMetaRetention();

        // Last statement in start(): everything a /health caller is entitled to
        // assume is running is running by here.
        this.ready = true;
    }

    // Stop every background loop this service owns and release its DB pools.
    // Called from the process SIGTERM/SIGINT drain (src/http/shutdown.js): start()
    // fans work out into pollers, client syncs and two intervals, none of which
    // the entry point can reach, so the fan-in belongs here beside the fan-out.
    //
    // ready goes false FIRST so /health reports the service as not-ready for the
    // whole drain window; the poll loops below only check their flag between
    // iterations and a replica answering healthy while it stops replicating is
    // exactly what a rolling upgrade must not see.
    //
    // Idempotent, and every step is best-effort: one refusing pool must not
    // strand the rest, because the caller's hard-exit timer is the only other
    // thing standing between a stuck drain and a lingering container.
    async stop(){
        this.ready = false;

        for(let poller of this.pollers.values()){
            try { if(typeof poller.stop === 'function') poller.stop(); }
            catch(e){ getLogger().warn(util.format('SyncService.stop: poller stop failed:', e && e.message ? e.message : e)); }
        }

        for(let sync of this.clientSyncs.values()){
            try { if(typeof sync.stop === 'function') sync.stop(); }
            catch(e){ getLogger().warn(util.format('SyncService.stop: client sync stop failed:', e && e.message ? e.message : e)); }
        }

        // Both are unref'd, so they cannot hold the loop open on their own, but a
        // hub re-poll firing mid-drain would create fresh pools and DB handles
        // behind the close below.
        if(this._hubRepollTimer){ clearInterval(this._hubRepollTimer); this._hubRepollTimer = null; }
        if(this._stateTreeMetricTimer){ clearInterval(this._stateTreeMetricTimer); this._stateTreeMetricTimer = null; }
        if(this._syncMetaRetentionTimer){ clearInterval(this._syncMetaRetentionTimer); this._syncMetaRetentionTimer = null; }

        // Pools close LAST: a poller mid-iteration above still needs its connection
        // to finish or roll back the statement it is on.
        let closed = new Set();
        for(let { db } of this.databases.values()){
            if(!db || typeof db.close !== 'function' || closed.has(db)) continue;
            closed.add(db);
            try { await db.close(); }
            catch(e){ getLogger().warn(util.format('SyncService.stop: database close failed:', e && e.message ? e.message : e)); }
        }

        getLogger().info('SyncService stopped (' + this.pollers.size + ' poller(s), '
            + this.clientSyncs.size + ' client sync(s), ' + closed.size + ' pool(s) closed).');
    }

    async waitForHub(){
        let maxWaitMs = this.config['MAX_HUB_WAIT_MS'];
        if(maxWaitMs === undefined || maxWaitMs === null)
            maxWaitMs = envConfig.maxHubWaitMsFromEnv();
        let startedAt = Date.now();
        let attempts = 0;
        while(true){
            if(Date.now() - startedAt >= maxWaitMs){
                getLogger().error('Hub at ' + this.config['HUB_API_HOST'] + ':' + this.config['HUB_PORT']
                    + ' was unreachable after ' + Math.round(maxWaitMs / 1000) + 's (MAX_HUB_WAIT_MS); exiting.');
                process.exit(1);
            }
            let alive = await this.hubClient.ping();
            if(alive){
                getLogger().info('Hub is reachable');
                return;
            }
            attempts++;
            getLogger().info('Waiting for hub at ' + this.config['HUB_API_HOST'] + ':' + this.config['HUB_PORT']
                + '... (attempt ' + attempts + ')');
            await this.util.sleep(3000);
        }
    }

    // Discover chains from the hub and create DB pools for both indexer
    // and decoder DBs. Decoder DBs skip the transparency log: their content
    // is deterministic from the coin node.
    async discoverChains(){
        let indexerConfigs = await this.hubClient.getIndexerConfigs();
        let decoderConfigs = await this.hubClient.getDecoderConfigs();
        let allConfigs = indexerConfigs.concat(decoderConfigs);
        let newChains = [];

        for(let cfg of allConfigs){
            let key = cfg.coin + ':' + cfg.network + ':' + cfg.dbType;
            if(this.databases.has(key)) continue;
            if(this.unrecognizedCoinKeys.has(key)) continue;

            // SYNC_EXCLUDE drops a chain before any DB pool or ClientSync exists,
            // so an excluded chain cannot crash-loop the process.
            if(this.config['SYNC_EXCLUDE'] && this.config['SYNC_EXCLUDE'].includes(key)){
                getLogger().info('Skipping excluded chain (SYNC_EXCLUDE): ' + key);
                continue;
            }

            // A server cannot safely serve an indexer coin without a frozen activation delay.
            if(this.config['SYNC_MODE'] === 'server' && cfg.dbType === 'indexer' && activationDelayBlocks(cfg.coin) === undefined){
                if(!this.unrecognizedCoinKeys.has(key)){
                    this.unrecognizedCoinKeys.add(key);
                    getLogger().error('Skipping indexer chain ' + key + ': coin "' + cfg.coin +
                        '" is not in this server\'s coin bundle (no frozen ACTIVATION_DELAY_BLOCKS); upgrade to serve it');
                }
                continue;
            }

            getLogger().info('Discovered ' + cfg.dbType + ': ' + cfg.coin + '/' + cfg.network + ' -> ' + cfg.db_name);

            let db;
            if(this.config['SYNC_MODE'] === 'client'){
                db = await this.openClientReplica(cfg);
                // A decoder replica whose transactions.data cannot store a 4-byte payload
                // would halt on the first such row. Refuse this chain only: it is closed
                // and left unregistered (re-judged on the next hub re-poll) while every
                // other chain keeps syncing.
                try {
                    await assertTransactionsDataWidth(db);
                } catch(widthErr){
                    getLogger().error(util.format('Refusing to start ' + key + ':', widthErr));
                    try { await db.close(); } catch(closeErr){ /* pool already gone */ }
                    continue;
                }
            } else {
                db = await this.openServerDatabase(cfg);
            }

            this.databases.set(key, { db, config: cfg, dbType: cfg.dbType });
            newChains.push({ key, db, config: cfg });
        }

        this.validateFirstDiscoveryPass();
        this.startDiscoveredChains(newChains);

        return newChains.filter(({ key }) => this.databases.has(key));
    }

    // Client mode: the replica keeps the source's db_name but uses the client's own creds.
    async openClientReplica(cfg){
        let db = new Database(
            this.config['REPLICA_DB_HOST'],
            this.config['REPLICA_DB_PORT'],
            cfg.db_name,
            this.config['REPLICA_DB_USER'],
            this.config['REPLICA_DB_PASS'],
            this.util,
            cfg.dbType
        );
        await db.createDatabase();
        await this.replicateSchemaFromSource(db, cfg);
        await this.healReplicaSchema(db);
        return db;
    }

    // Tries the source DB directly (faster); when it is unreachable the schema
    // arrives from the server /schema endpoint during ClientSync bootstrap.
    async replicateSchemaFromSource(db, cfg){
        let sourceDb = null;
        try {
            sourceDb = new Database(cfg.db_host, cfg.db_port, cfg.db_name, cfg.db_user, cfg.db_pass, this.util, cfg.dbType);
            // Single attempt: verifyDatabase() would retry forever against a
            // node-internal host and hang discovery.
            let sourceExists = await sourceDb.verifyDatabaseOnce();
            if(sourceExists){
                await db.replicateSchema(sourceDb);
            }
        } catch(e){
            // A refused column self-heal is not an unreachable source: the /schema
            // fetch will not fix it, so report it and let ClientSync record the halt.
            if(e && e.columnFailures){
                getLogger().error('Schema replication for ' + cfg.coin + '/' + cfg.network + '/' + cfg.dbType +
                    ' left columns missing on the replica: ' + e.message);
            } else {
                getLogger().info('Source DB not reachable for ' + cfg.coin + '/' + cfg.network + '/' + cfg.dbType + '; schema will be fetched from sync server');
            }
        } finally {
            // Close in finally: a thrown replicateSchema would otherwise leak the source pool.
            if(sourceDb){
                try { await sourceDb.close(); } catch(closeErr){ /* pool already gone */ }
            }
        }
    }

    // Runs on both schema paths (direct replicateSchema or the server /schema
    // fetch), so every step is idempotent.
    async healReplicaSchema(db){
        // dbType-aware: indexer replicas get the full sync set, decoder replicas only sync_halt.
        await db.verifySyncTables();
        // Sync runs no migrations, so legacy timestamp columns are retyped here.
        await db.ensureDatetimeColumns({ includeFollowerDerived: true });
        // Derives columns from authoritative definitions, not the source DB.
        await db.ensureReplicatedColumns();
        // The /schema fetch never carries index changes, so heal secondary indexes here.
        await db.ensureReplicaSecondaryIndexes();
        // Neither heal above retypes an existing column; widen raw-wire charsets here.
        await db.ensureReplicaUtf8mb4Columns();
        // Fail closed on collation drift in the stake-weight ordering columns,
        // judged after the repairs above. Twin of xchain-indexer's
        // assertStakeWeightOrderingCollation.
        await db.assertStakeWeightOrderingCollation();
    }

    // Server mode: connect to the DB this server polls and serves. With
    // REPLICA_DB_HOST set it serves a local replica (same db_name from the hub)
    // instead of the hub-provided coordinates.
    async openServerDatabase(cfg){
        let db;
        if(this.config['REPLICA_DB_HOST']){
            db = new Database(this.config['REPLICA_DB_HOST'], this.config['REPLICA_DB_PORT'], cfg.db_name, this.config['REPLICA_DB_USER'], this.config['REPLICA_DB_PASS'], this.util, cfg.dbType);
        } else {
            db = new Database(cfg.db_host, cfg.db_port, cfg.db_name, cfg.db_user, cfg.db_pass, this.util, cfg.dbType);
        }
        await db.verifySyncTables();
        // Leave the indexer's own table to the indexer by excluding follower-derived columns.
        await db.ensureDatetimeColumns({ includeFollowerDerived: false });
        await db.assertStakeWeightOrderingCollation();
        return db;
    }

    // Client mode, first pass only: an unmatched SYNC_BOOTSTRAP_DEPTH_* key falls
    // through to depth 0 (the full-history snapshot), so refuse it before any
    // ClientSync starts.
    validateFirstDiscoveryPass(){
        if(this.config['SYNC_MODE'] !== 'server' && !this._bootstrapDepthChecked && this.databases.size > 0){
            this._bootstrapDepthChecked = true;
            assertBootstrapDepthChains(this.config, this.getChains());
            // An invalid CHECKPOINT_VALIDATORS_*/CHECKPOINT_SEED_* value resolves like an
            // absent one and would skip the quorum anchor, so refuse it on the same pass.
            assertPinnedEnvOverrides();
        }
    }

    startDiscoveredChains(newChains){
        for(const { key, db, config: cfg } of newChains){
            if(this.config['SYNC_MODE'] === 'server'){
                this.startPollerForChain(key, db, cfg);
            } else {
                try {
                    this.startClientSyncForChain(key, db, cfg);
                } catch(e){
                    if(cfg.dbType !== 'indexer' || activationDelayBlocks(cfg.coin) !== undefined ||
                        !e || !String(e.message).startsWith('ClientRollback: unrecognized coin "')) throw e;
                    this.databases.delete(key);
                    this.clientSyncs.delete(key);
                    this.unrecognizedCoinKeys.add(key);
                    getLogger().error('Skipping indexer chain ' + key + ': coin "' + cfg.coin +
                        '" is not in this client\'s coin bundle (no frozen ACTIVATION_DELAY_BLOCKS); upgrade to sync it');
                    try {
                        const closing = db.close();
                        if(closing && typeof closing.catch === 'function') closing.catch(() => {});
                    } catch(closeErr){ /* pool already gone */ }
                    continue;
                }
            }
        }
    }

    async startServerMode(){
        // Idempotent: these are normally created in start() before discoverChains()
        // so pollers can capture a live broadcaster. Guard so a direct call (or future
        // refactor) still works without clobbering the instance the pollers already hold.
        if(!this.broadcaster)     this.broadcaster     = new BlockBroadcaster(this.config);
        if(!this.snapshotBuilder) this.snapshotBuilder = new SnapshotBuilder(this.util);

        // Start a poller for each discovered DB (both indexer and decoder).
        // ServerPoller reads dbType from db.dbType and switches table lists +
        // payload structure accordingly.
        for(let [key, { db, config: cfg }] of this.databases){
            this.startPollerForChain(key, db, cfg);
        }

        getLogger().info('Server mode started with ' + this.databases.size + ' poller(s)' +
            (this.config['REPLICA_DB_READONLY'] ? ' (READ-ONLY replica: transparency log is serve-only)' : ''));
    }

    // Start a poller for a single chain/network/dbType.
    // TransparencyLog is created only for indexer DBs. Decoder content is
    // deterministic from the coin node and doesn't need a synthetic hash chain.
    startPollerForChain(key, db, cfg){
        if(this.pollers.has(key)) return;

        let log    = (cfg.dbType === 'indexer')
            ? new TransparencyLog(db, this.config['MERKLE_EPOCH_SIZE'], this.config['REPLICA_DB_READONLY'],
                                  this.config['SYNC_META_RETENTION_BLOCKS'])
            : null;
        let poller = new ServerPoller(cfg.coin, cfg.network, db, this.broadcaster, log, this.config, this.util);
        this.pollers.set(key, poller);

        // Start polling in background (fire and forget; runs indefinitely).
        // A throw here means this chain's poller is permanently dead, which is
        // invisible at the /status endpoint (stale block_height, live timestamp).
        // Log the full error and exit so the container restart policy surfaces it.
        poller.start().catch(e => {
            getLogger().error(util.format('Poller crashed for ' + key + '; exiting for restart:', e));
            process.exit(1);
        });
    }

    async startClientMode(){
        // ClientSync reads dbType from db.dbType and threads it through URLs +
        // skips three-hash verification for decoder DBs.
        for(let [key, { db, config: cfg }] of this.databases){
            this.startClientSyncForChain(key, db, cfg);
        }
        getLogger().info('Client mode started with ' + this.databases.size + ' sync(s)');
    }

    startClientSyncForChain(key, db, cfg){
        if(this.clientSyncs.has(key)) return;

        let applier  = new ClientApplier(db, this.util, cfg.coin, cfg.network);
        let rollback = new ClientRollback(db, this.util, cfg.coin, cfg.network);
        let sync     = new ClientSync(cfg.coin, cfg.network, db, applier, rollback, this.hashVerifier, this.config, this.util);
        this.clientSyncs.set(key, sync);

        // Start syncing in background. A throw here means this chain's replica
        // sync is permanently dead while the process still appears healthy.
        // Log the full error and exit so the container restart policy surfaces it.
        sync.start().catch(e => {
            getLogger().error(util.format('ClientSync crashed for ' + key + '; exiting for restart:', e));
            process.exit(1);
        });
    }

    scheduleHubRepoll(){
        if(this._hubRepollTimer) return;
        // Handle retained so stop() can clear it: a re-poll that fires during the
        // drain discovers chains and builds fresh DB pools behind the close.
        this._hubRepollTimer = setInterval(async () => {
            try {
                let newChains = await this.discoverChains();
                if(newChains.length > 0)
                    getLogger().info('Discovered ' + newChains.length + ' new chain(s) from hub');
            } catch(e){
                getLogger().error(util.format('Hub re-poll error:', e));
            }
        }, this.config['HUB_REPOLL_INTERVAL']);
        if(this._hubRepollTimer.unref) this._hubRepollTimer.unref();
    }

    // Periodically emit a read-only orphan-count metric for each replicated indexer DB's
    // state_tree_nodes store so its unbounded COW growth is observable (twin of the indexer's
    // metric; the follower strands MORE nodes via buildFull every BTC block). Unref'd interval,
    // self-overlap guarded, reads on a POOLED connection (db.pool, NOT the apply transaction).
    // No deletion: see stateCommitment.reportOrphanStats. STATE_TREE_METRIC_INTERVAL_MS (default
    // 4h; 0 disables). Decoder DBs are skipped (no state_tree_* tables).
    startStateTreeMetric(){
        if(this._stateTreeMetricTimer) return;
        const raw = envConfig.stateTreeMetricIntervalMsFromEnv();
        const intervalMs = Number.isFinite(raw) ? raw : (4 * 60 * 60 * 1000);
        if(intervalMs === 0) return;   // explicitly disabled
        this._stateTreeMetricRunning = false;
        this._stateTreeMetricTimer = setInterval(async () => {
            if(this._stateTreeMetricRunning) return;
            this._stateTreeMetricRunning = true;
            try {
                for(const [key, { db, config: cfg, dbType }] of this.databases){
                    if(dbType !== 'indexer') continue;   // state_tree_* live only in indexer DBs
                    // Pooled query that bypasses any in-flight apply transaction.
                    const query = async (sql, args) => {
                        const c = await db.pool.getConnection();
                        try { return await c.query(sql, args); }
                        finally { try { await c.release(); } catch(_){} }
                    };
                    try {
                        // Pass the ticker: state_tree_roots rows carry it, so the hub's full name matches none
                        const stats = await stateCommitment.reportOrphanStats(query, coinTicker(cfg.coin), cfg.network);
                        if(stats.totalNodes === 0) continue;
                        getLogger().info('[METRIC] ' + JSON.stringify({
                            metric: 'state_tree_orphan_nodes', component: 'sync', key: key,
                            chain: cfg.coin, network: cfg.network,
                            total_nodes: stats.totalNodes, reachable_nodes: stats.reachableNodes,
                            orphan_count: stats.orphanCount, reachability_skipped: stats.reachabilitySkipped,
                            // Publish the truncation flag or the line reads as a full-store figure:
                            // when the mark stops at the cap, orphan_count is an UPPER bound.
                            reachability_estimated: stats.reachabilityEstimated === true,
                            ts: Date.now()
                        }));
                    } catch(err) {
                        getLogger().warn(util.format('SyncService: state_tree orphan-metric failed for ' + key + ':', err.message || err));
                    }
                }
            } finally {
                this._stateTreeMetricRunning = false;
            }
        }, intervalMs);
        if(this._stateTreeMetricTimer.unref) this._stateTreeMetricTimer.unref();
        getLogger().info('SyncService: state_tree orphan-metric started (interval ' + intervalMs + 'ms)');
    }

    // Make SYNC_META_RETENTION_BLOCKS real in CLIENT mode. On the server the window is
    // driven by TransparencyLog.recordBlock at epoch boundaries; a client never builds a
    // TransparencyLog at all, only INSERT-IGNOREs sync_meta rows, and the source's own
    // pruning DELETEs are not carried over replication, so a configured window was inert
    // and the replica's sync_meta grew forever.
    //
    // A periodic timer rather than a per-block hook: bulk snapshot catch-up applies many
    // blocks at once and would skip epoch-boundary events, whereas pruneSyncMeta recomputes
    // its own cutoff from the current tip and is an idempotent range delete, so calling it
    // on a clock is both sufficient and safe. Modelled on startStateTreeMetric: one unref'd
    // interval, self-overlap guarded, try/catch per DB, cleared in stop().
    //
    // Server mode is deliberately untouched: it already prunes, and a second driver there
    // would add concurrent DELETE load to a path that works.
    startSyncMetaRetention(){
        if(this._syncMetaRetentionTimer) return;
        if(this.config['SYNC_MODE'] === 'server') return;
        const keep = parseInt(this.config['SYNC_META_RETENTION_BLOCKS'], 10);
        if(!Number.isFinite(keep) || keep <= 0) return;   // default 0: retention is off, no timer
        // Honours REPLICA_DB_READONLY: pruneSyncMeta short-circuits on it, so a
        // serve-only deployment still deletes nothing.
        const readOnly = this.config['REPLICA_DB_READONLY'];
        // config.js already resolves this key from the environment and applies the
        // 1-hour default, so the interval is read from config and nowhere else. The
        // fallback below covers a config object assembled without the key (a test
        // fixture), not an unset environment variable.
        const raw = parseInt(this.config['SYNC_META_RETENTION_INTERVAL_MS'], 10);
        const intervalMs = (Number.isFinite(raw) && raw > 0) ? raw : (60 * 60 * 1000);

        this._syncMetaRetentionRunning = false;
        this._syncMetaRetentionTimer = setInterval(async () => {
            if(this._syncMetaRetentionRunning) return;
            this._syncMetaRetentionRunning = true;
            try {
                for(const [key, { db, dbType }] of this.databases){
                    if(dbType !== 'indexer') continue;   // sync_meta lives only in indexer DBs
                    try {
                        const log = new TransparencyLog(db, this.config['MERKLE_EPOCH_SIZE'], readOnly, keep);
                        await log.pruneSyncMeta();
                    } catch(err){
                        getLogger().warn('SyncService: sync_meta retention failed for ' + key + ': ' +
                                         (err && err.message ? err.message : err));
                    }
                }
            } finally {
                this._syncMetaRetentionRunning = false;
            }
        }, intervalMs);
        if(this._syncMetaRetentionTimer.unref) this._syncMetaRetentionTimer.unref();
        getLogger().info('SyncService: client sync_meta retention started (window ' + keep +
                    ' blocks, interval ' + intervalMs + 'ms); inclusion proofs below the window ' +
                    'stop being serveable from this replica');
    }

    getBroadcaster(){
        return this.broadcaster;
    }

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

    getChains(){
        let chains = [];
        for(let [key, { config: cfg }] of this.databases){
            chains.push({ coin: cfg.coin, network: cfg.network, dbType: cfg.dbType });
        }
        return chains;
    }

    // Get client sync state for a chain/network/dbType (client mode only).
    // Returns an object with null values for fields not yet observed.
    getClientSyncState(chain, network, dbType){
        let type = dbType || 'indexer';
        let key  = chain + ':' + network + ':' + type;
        let sync = this.clientSyncs.get(key);
        return {
            lastKnownServerBlock: sync ? sync.lastKnownServerBlock : null,
            sourceHeightStale: sync ? sync.isSourceHeightStale() : null,
            // The upstream's OWN replication verdict, relayed on its status events.
            // Tri-state stale: null is unknown, never fresh.
            upstreamReplica: (sync && typeof sync.getUpstreamReplicaState === 'function')
                ? sync.getUpstreamReplicaState()
                : { stale: null, secondsBehind: null, sourceHeight: null },
            halted:        sync ? sync.isHalted() : false,
            haltInfo:      (sync && sync.isHalted()) ? sync.getHaltInfo() : null,
            trainActivation: (sync && typeof sync.getTrainActivation === 'function')
                ? sync.getTrainActivation() : null,
            truncated:     sync ? sync.isTruncated() : false,
            bootstrapBase: sync ? sync.getBootstrapBase() : null,
            // Multi-source Byzantine quorum surface.
            sourceQuorum:     sync ? sync.getSourceQuorum() : null,
            sourcesConfigured: sync ? sync.getConfiguredSourceCount() : null,
            sourcesActive:    sync ? sync.getActiveSourceCount() : null,
            sourcesAgreeing:  sync ? sync.getSourcesAgreeing() : null,
            sourcesEvicted:   sync ? sync.getEvictedSources() : []
        };
    }

    // Resolve a live ClientSync (for the operator halt-clear endpoint).
    getClientSync(chain, network, dbType){
        return this.clientSyncs.get(chain + ':' + network + ':' + (dbType || 'indexer')) || null;
    }

    // Resolve a live ServerPoller (server mode only; empty in client mode).
    // Exposed so /health can read pollErrorCount, the earliest outage signal,
    // before the DB circuit breaker arms.
    getPoller(chain, network, dbType){
        return this.pollers.get(chain + ':' + network + ':' + (dbType || 'indexer')) || null;
    }

    // Indexer-only: decoder content is deterministic from the coin node, so decoder
    // DBs deliberately maintain no transparency log.
    getTransparencyLog(chain, network){
        let key = chain + ':' + network + ':indexer';
        let entry = this.databases.get(key);
        if(!entry) return null;
        let poller = this.pollers.get(key);
        if(poller) return poller.transparencyLog;
        // readOnly and the retention window are passed, not dropped. Client mode always
        // takes this fallback (this.pollers is empty there), so omitting them silently
        // handed every caller a log that believed it was writable and unwindowed.
        return new TransparencyLog(entry.db, this.config['MERKLE_EPOCH_SIZE'],
                                   this.config['REPLICA_DB_READONLY'],
                                   this.config['SYNC_META_RETENTION_BLOCKS']);
    }
}

module.exports = SyncService;
