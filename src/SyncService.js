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
const HubClient       = require('./HubClient');
const ServerPoller    = require('./ServerPoller');
const BlockBroadcaster = require('./BlockBroadcaster');
const SnapshotBuilder = require('./SnapshotBuilder');
const TransparencyLog = require('./TransparencyLog');
const ClientSync      = require('./ClientSync');
const ClientApplier   = require('./ClientApplier');
const ClientRollback  = require('./ClientRollback');
const HashVerifier    = require('./HashVerifier');
const stateCommitment = require('./stateCommitment');
const { assertBootstrapDepthChains } = require('./config');
const Utility         = require('./utility');

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

        this.broadcaster    = null;
        this.snapshotBuilder = null;
        this.hashVerifier   = new HashVerifier();

        // Startup readiness, false until start() has discovered chains and begun
        // polling. api.js listens BEFORE start() runs and start() can sit in
        // _waitForHub for MAX_HUB_WAIT_MS (default 5 minutes), during which
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
        console.log('Starting SyncService in ' + this.config['SYNC_MODE'] + ' mode...');

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

        await this._discoverChains();

        if(this.databases.size === 0){
            console.log('No indexer/decoder databases found. Waiting for hub config...');
        }

        if(this.config['SYNC_MODE'] === 'server'){
            await this._startServerMode();
        } else {
            await this._startClientMode();
        }

        this._scheduleHubRepoll();
        this._startStateTreeMetric();

        // Last statement in start(): everything a /health caller is entitled to
        // assume is running is running by here.
        this.ready = true;
    }

    // Stop every background loop this service owns and release its DB pools.
    // Called from the process SIGTERM/SIGINT drain (src/shutdown.js): start()
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
            catch(e){ console.warn('SyncService.stop: poller stop failed:', e && e.message ? e.message : e); }
        }

        for(let sync of this.clientSyncs.values()){
            try { if(typeof sync.stop === 'function') sync.stop(); }
            catch(e){ console.warn('SyncService.stop: client sync stop failed:', e && e.message ? e.message : e); }
        }

        // Both are unref'd, so they cannot hold the loop open on their own, but a
        // hub re-poll firing mid-drain would create fresh pools and DB handles
        // behind the close below.
        if(this._hubRepollTimer){ clearInterval(this._hubRepollTimer); this._hubRepollTimer = null; }
        if(this._stateTreeMetricTimer){ clearInterval(this._stateTreeMetricTimer); this._stateTreeMetricTimer = null; }

        // Pools close LAST: a poller mid-iteration above still needs its connection
        // to finish or roll back the statement it is on.
        let closed = new Set();
        for(let { db } of this.databases.values()){
            if(!db || typeof db.close !== 'function' || closed.has(db)) continue;
            closed.add(db);
            try { await db.close(); }
            catch(e){ console.warn('SyncService.stop: database close failed:', e && e.message ? e.message : e); }
        }

        console.log('SyncService stopped (' + this.pollers.size + ' poller(s), '
            + this.clientSyncs.size + ' client sync(s), ' + closed.size + ' pool(s) closed).');
    }

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
    // deterministic from the coin node; no synthetic chain-of-state hash
    // needed).
    async _discoverChains(){
        let indexerConfigs = await this.hubClient.getIndexerConfigs();
        let decoderConfigs = await this.hubClient.getDecoderConfigs();
        let allConfigs = indexerConfigs.concat(decoderConfigs);
        let newChains = [];

        for(let cfg of allConfigs){
            let key = cfg.coin + ':' + cfg.network + ':' + cfg.dbType;
            if(this.databases.has(key)) continue;

            // Per-chain exclude (SYNC_EXCLUDE): drop a chain the client must not
            // replicate (e.g. a fast chain that cannot full-snapshot bootstrap).
            // Skipped before any DB pool / ClientSync is created, so an excluded
            // chain can never crash-loop the process.
            if(this.config['SYNC_EXCLUDE'] && this.config['SYNC_EXCLUDE'].includes(key)){
                console.log('Skipping excluded chain (SYNC_EXCLUDE): ' + key);
                continue;
            }

            console.log('Discovered ' + cfg.dbType + ': ' + cfg.coin + '/' + cfg.network + ' -> ' + cfg.db_name);

            let db;
            if(this.config['SYNC_MODE'] === 'client'){
                // The replica keeps the source's db_name but uses the client's own creds.
                db = new Database(
                    this.config['REPLICA_DB_HOST'],
                    this.config['REPLICA_DB_PORT'],
                    cfg.db_name,
                    this.config['REPLICA_DB_USER'],
                    this.config['REPLICA_DB_PASS'],
                    this.util,
                    cfg.dbType
                );
                await db.createDatabase();
                // Schema replication: try direct DB connection first (faster), fall back to
                // server's /schema endpoint during ClientSync bootstrap if DB is unreachable
                let sourceDb = null;
                try {
                    sourceDb = new Database(cfg.db_host, cfg.db_port, cfg.db_name, cfg.db_user, cfg.db_pass, this.util, cfg.dbType);
                    // Single-attempt probe: a node-internal source DB host is often
                    // unreachable from the replica box, and verifyDatabase() would
                    // retry forever, hanging discovery instead of falling through to
                    // the server /schema fetch below.
                    let sourceExists = await sourceDb.verifyDatabaseOnce();
                    if(sourceExists){
                        await db.replicateSchema(sourceDb);
                    }
                } catch(e){
                    // A column self-heal the server REFUSED is not an unreachable source:
                    // the schema really is behind and the /schema fetch will not fix it, so
                    // it must not be filed under the reachability message. Say so loudly and
                    // let ClientSync's schema apply record the durable halt.
                    if(e && e.columnFailures){
                        console.error('Schema replication for ' + cfg.coin + '/' + cfg.network + '/' + cfg.dbType +
                            ' left columns missing on the replica: ' + e.message);
                    } else {
                        // Source DB not reachable; schema will be fetched from server via /schema endpoint
                        console.log('Source DB not reachable for ' + cfg.coin + '/' + cfg.network + '/' + cfg.dbType + '; schema will be fetched from sync server');
                    }
                } finally {
                    // Close in finally: a thrown replicateSchema used to leak the source
                    // pool, and a refused ALTER now throws.
                    if(sourceDb){
                        try { await sourceDb.close(); } catch(closeErr){ /* pool already gone */ }
                    }
                }
                // Sync-owned tables: verifySyncTables is dbType-aware. Indexer
                // replicas get the full set (sync_meta/merkle_epochs/sync_halt),
                // decoder replicas get only sync_halt (the durable divergence
                // halt applies to both shapes; the transparency log does not).
                await db.verifySyncTables();
                // Self-heal column drift on a pre-existing replica before any row
                // data is accepted. Runs regardless of which schema path applied
                // above (direct replicateSchema or, when the source DB is
                // unreachable, the server /schema fetch during ClientSync
                // bootstrap) since it derives columns from authoritative
                // definitions rather than the source DB. No-op when the table is
                // absent or already current.
                await db.ensureReplicatedColumns();
                // Same rationale for secondary-index drift (missing indexes AND stale
                // UNIQUE keys, e.g. the votes append-only migration): the direct
                // replicateSchema path calls this, but the common client topology has
                // the source DB unreachable and bootstraps schema from the server
                // /schema fetch, which never carries index changes. Run it here so
                // both paths self-heal. Idempotent, so the double-call on the
                // direct-DB path is a cheap no-op.
                await db.ensureReplicaSecondaryIndexes();
                // Same again for the raw-wire-field charset widen: neither self-heal above
                // retypes an existing column, so a replica bootstrapped before the
                // 2026-09-02 indexer migration keeps utf8mb3 on contracts.code and the
                // grammar-constrained fields and halts on the first 4-byte character the
                // widened origin accepts. Idempotent, so the double-call on the direct-DB
                // path is a cheap no-op.
                await db.ensureReplicaUtf8mb4Columns();
                // Fail closed on collation drift in the columns the stake-weight
                // snapshot orders on. The follower rebuilds stakes_root from the
                // byte-mirrored _cappedStakeWeightsSql, whose window caps truncate on
                // that order, so a replica whose index_addresses.address collation
                // deviates from the source's picks different cap survivors and then
                // halts on a root it computed wrong. Runs AFTER the schema self-heal
                // above, so a replica that was going to be repaired is judged on its
                // repaired state. Twin of xchain-indexer's
                // _assertStakeWeightOrderingCollation.
                await db.assertStakeWeightOrderingCollation();
            } else {
                // Server mode: connect to the DB this server polls + serves.
                // Default: the authoritative DB at the hub-provided coordinates.
                // Override: when REPLICA_DB_HOST is set, serve from a LOCAL replica
                // (same db_name from the hub) using REPLICA_DB_* creds instead of the
                // hub's coordinates. This lets a box that pulled the DBs as a client
                // (e.g. sync.xchain.io) re-serve those local replicas to downstream
                // clients, while still enumerating chains/db_names from the hub.
                if(this.config['REPLICA_DB_HOST']){
                    db = new Database(this.config['REPLICA_DB_HOST'], this.config['REPLICA_DB_PORT'], cfg.db_name, this.config['REPLICA_DB_USER'], this.config['REPLICA_DB_PASS'], this.util, cfg.dbType);
                } else {
                    db = new Database(cfg.db_host, cfg.db_port, cfg.db_name, cfg.db_user, cfg.db_pass, this.util, cfg.dbType);
                }
                // Sync-owned tables (dbType-aware: indexer = full set, decoder =
                // sync_halt only); same rationale as the client branch above.
                await db.verifySyncTables();
            }

            this.databases.set(key, { db, config: cfg, dbType: cfg.dbType });
            newChains.push({ key, db, config: cfg });
        }

        // REFUSE a SYNC_BOOTSTRAP_DEPTH_* key that names no chain the hub published,
        // BEFORE any ClientSync starts. An unmatched key is not inert: the lookup falls
        // through to depth 0, which is the full-history snapshot branch, so a typo'd or
        // stale key silently starts exactly the unbounded bootstrap the key existed to
        // prevent. Client mode only (the env var governs nothing on a server) and only
        // on the first discovery pass, so a later hub re-poll cannot kill a process that
        // already validated and is happily replicating.
        if(this.config['SYNC_MODE'] !== 'server' && !this._bootstrapDepthChecked && this.databases.size > 0){
            this._bootstrapDepthChecked = true;
            assertBootstrapDepthChains(this.config, this.getChains());
        }

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

        console.log('Server mode started with ' + this.databases.size + ' poller(s)' +
            (this.config['REPLICA_DB_READONLY'] ? ' (READ-ONLY replica: transparency log is serve-only)' : ''));
    }

    // Start a poller for a single chain/network/dbType.
    // TransparencyLog is created only for indexer DBs. Decoder content is
    // deterministic from the coin node and doesn't need a synthetic hash chain.
    _startPollerForChain(key, db, cfg){
        if(this.pollers.has(key)) return;

        let log    = (cfg.dbType === 'indexer')
            ? new TransparencyLog(db, this.config['MERKLE_EPOCH_SIZE'], this.config['REPLICA_DB_READONLY'],
                                  this.config['SYNC_META_RETENTION_BLOCKS'])
            : null;
        let poller = new ServerPoller(cfg.coin, cfg.network, db, this.broadcaster, log, this.config, this.util);
        this.pollers.set(key, poller);

        // Fire and forget: a throw here means this chain's poller is permanently dead,
        // which /status cannot show (stale block_height under a live timestamp), so log
        // the full error and exit and let the container restart policy surface it.
        poller.start().catch(e => {
            console.error('Poller crashed for ' + key + '; exiting for restart:', e);
            process.exit(1);
        });
    }

    async _startClientMode(){
        // ClientSync reads dbType from db.dbType and threads it through URLs +
        // skips three-hash verification for decoder DBs.
        for(let [key, { db, config: cfg }] of this.databases){
            this._startClientSyncForChain(key, db, cfg);
        }
        console.log('Client mode started with ' + this.databases.size + ' sync(s)');
    }

    _startClientSyncForChain(key, db, cfg){
        if(this.clientSyncs.has(key)) return;

        let applier  = new ClientApplier(db, this.util, cfg.coin, cfg.network);
        let rollback = new ClientRollback(db, this.util, cfg.coin);
        let sync     = new ClientSync(cfg.coin, cfg.network, db, applier, rollback, this.hashVerifier, this.config, this.util);
        this.clientSyncs.set(key, sync);

        // Start syncing in background. A throw here means this chain's replica
        // sync is permanently dead while the process still appears healthy.
        // Log the full error and exit so the container restart policy surfaces it.
        sync.start().catch(e => {
            console.error('ClientSync crashed for ' + key + '; exiting for restart:', e);
            process.exit(1);
        });
    }

    _scheduleHubRepoll(){
        if(this._hubRepollTimer) return;
        // Handle retained so stop() can clear it: a re-poll that fires during the
        // drain discovers chains and builds fresh DB pools behind the close.
        this._hubRepollTimer = setInterval(async () => {
            try {
                let newChains = await this._discoverChains();
                if(newChains.length > 0)
                    console.log('Discovered ' + newChains.length + ' new chain(s) from hub');
            } catch(e){
                console.error('Hub re-poll error:', e);
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
    _startStateTreeMetric(){
        if(this._stateTreeMetricTimer) return;
        const raw = parseInt(process.env.STATE_TREE_METRIC_INTERVAL_MS, 10);
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
                        const stats = await stateCommitment.reportOrphanStats(query, cfg.coin, cfg.network);
                        if(stats.totalNodes === 0) continue;
                        console.log('[METRIC] ' + JSON.stringify({
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
                        console.warn('SyncService: state_tree orphan-metric failed for ' + key + ':', err.message || err);
                    }
                }
            } finally {
                this._stateTreeMetricRunning = false;
            }
        }, intervalMs);
        if(this._stateTreeMetricTimer.unref) this._stateTreeMetricTimer.unref();
        console.log('SyncService: state_tree orphan-metric started (interval ' + intervalMs + 'ms)');
    }

    getBroadcaster(){
        return this.broadcaster;
    }

    getSnapshotBuilder(){
        return this.snapshotBuilder;
    }

    // dbType defaults to 'indexer' for callers that are not yet dbType-aware.
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

    // Client mode only; fields not yet observed come back null rather than absent.
    getClientSyncState(chain, network, dbType){
        let type = dbType || 'indexer';
        let key  = chain + ':' + network + ':' + type;
        let sync = this.clientSyncs.get(key);
        return {
            lastKnownServerBlock: sync ? sync.lastKnownServerBlock : null,
            sourceHeightStale: sync ? sync.isSourceHeightStale() : null,
            halted:        sync ? sync.isHalted() : false,
            haltInfo:      (sync && sync.isHalted()) ? sync.getHaltInfo() : null,
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
        return new TransparencyLog(entry.db, this.config['MERKLE_EPOCH_SIZE']);
    }
}

module.exports = SyncService;
