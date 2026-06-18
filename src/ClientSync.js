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
 * XChain Sync - Client Sync
 *
 * Client-mode orchestrator for one chain/network/dbType triple.
 * Handles bootstrap (full snapshot), catch-up (incremental snapshot),
 * and live sync (WebSocket subscription). Manages reconnection and
 * gap detection.
 *
 * dbType is read from db.dbType. Decoder DB instances skip the
 * three-hash cross-source verification (decoder has no synthetic
 * ledger/actions/contract hashes; content is deterministic from
 * the coin node).
 *
 ********************************************************************/

const WebSocket   = require('ws');
const axios       = require('axios');
const zlib        = require('zlib');
const validation  = require('./validation');
const BlockHasher = require('./BlockHasher');
const replicatedTables = require('./replicatedTables');
const { SCHEMA_VERSION } = require('./schema-version');
const { activationDelayBlocks, gasTickSymbol } = require('./consensus-constants');

class ClientSync {

    constructor(chain, network, db, applier, rollback, hashVerifier, config, util) {
        this.chain        = chain;
        this.network      = network;
        this.db           = db;
        this.dbType       = (db && db.dbType) ? db.dbType : 'indexer';
        this.applier      = applier;
        this.rollback     = rollback;
        this.hashVerifier = hashVerifier;
        this.config       = config;
        this.util         = util;
        // Independent block-hash recomputation (true byzantine / replication-
        // integrity detection). Verifies the replicated raw rows actually hash to
        // the committed hash, rather than trusting verbatim-replicated hashes.
        this.blockHasher  = new BlockHasher(db, util);

        // VERIFY_RECOMPUTE=false is DECLARED UNSAFE for consensus-relevant
        // replicas (operator decision 2026-06-12): the recompute is the only
        // verification of the catch-up JOIN block, so without it a reorg that
        // crosses a disconnect/restart silently forks this replica onto the new
        // chain while it keeps the orphaned blocks. Warn loudly at construction
        // so the operator sees it once per client session, on every entry path.
        if(this.dbType === 'indexer' && this.config['VERIFY_RECOMPUTE'] === false){
            console.error('================================================================');
            console.error('WARNING: VERIFY_RECOMPUTE is DISABLED for ' + this.chain + '/' +
                this.network + '/indexer. This mode is UNSAFE for consensus-relevant');
            console.error('replicas: a reorg occurring while this client is disconnected or');
            console.error('restarting will be stitched onto the orphaned tip UNVERIFIED and');
            console.error('the replica will silently follow the forked chain. Use only for');
            console.error('throwaway read-only mirrors whose state nothing downstream trusts.');
            console.error('================================================================');
        }

        this.sources    = this.config['SYNC_SOURCES'].split(',').map(s => s.trim()).filter(s => s);
        this.running    = false;
        this.wsConns    = [];
        this.lastAppliedBlock     = null;
        this.lastHashes           = null;
        this.lastKnownServerBlock = null;

        // Truncated-replica join block. Set by _bootstrapFromHeight when this chain
        // is seeded from a recent height (SYNC_BOOTSTRAP_DEPTH_*) rather than full
        // history. The join block has no in-replica predecessor, so its chained
        // previous_hash cannot be recomputed; _verifyRecompute skips ONLY this block.
        // null = full-history replica (every block is independently recomputed).
        this._bootstrapBase = null;

        // Truncated-mode depth (blocks) for THIS chain, from
        // SYNC_BOOTSTRAP_DEPTH_<CHAIN>_<NETWORK>. >= 1 means this is a fast/large
        // chain seeded from a recent height: both bootstrap AND catch-up sync the
        // append-only lookup tables out of band via the id-cursor paged route
        // (a single full-dump of e.g. index_transactions exceeds the content limit).
        // 0 = full-history replica (unchanged bundled-snapshot path). Read from
        // config (always available), NOT from _bootstrapBase, so it governs catch-up
        // correctly after a restart (when the replica is non-empty and _bootstrapBase
        // is null). Applies to BOTH dbTypes of the chain: the indexer recompute/join
        // handling is indexer-specific, but the decoder (no synthetic chain hash, just
        // block_hash continuity) seeds the same way and its 2.4M-row index_transactions
        // is the same content-limit wall, so a depth-configured chain truncates both.
        let _depthMap = this.config['SYNC_BOOTSTRAP_DEPTH'] || {};
        this._truncatedDepth = _depthMap[(this.chain + ':' + this.network).toUpperCase()] || 0;

        // Guard: a truncation depth must exceed MAX_ROLLBACK_DEPTH. The join floor
        // `base = tip - depth` is the deepest block the replica holds; a reorg that
        // rewinds further than `depth` blocks would need to roll back BELOW the floor,
        // which the replica cannot do (no pre-base history). MAX_ROLLBACK_DEPTH (default
        // 100) is the deepest reorg the client will roll back, so a depth <=
        // MAX_ROLLBACK_DEPTH lets an in-window reorg request a rollback past the floor.
        // Clamp the effective depth up to MAX_ROLLBACK_DEPTH + 1 and warn loudly so a
        // misconfigured small depth can't quietly strand the replica. (depth 0 = full-
        // history replica, not truncated, so it is exempt.)
        if(this._truncatedDepth >= 1){
            let maxRollback = Number(this.config['MAX_ROLLBACK_DEPTH']);
            if(!Number.isFinite(maxRollback) || maxRollback < 1) maxRollback = 100;
            if(this._truncatedDepth <= maxRollback){
                let clamped = maxRollback + 1;
                console.warn('SYNC_BOOTSTRAP_DEPTH for ' + this.chain + '/' + this.network +
                    ' is ' + this._truncatedDepth + ', which is <= MAX_ROLLBACK_DEPTH (' + maxRollback +
                    '). A reorg within the rollback window could request a rollback below the truncation ' +
                    'floor, which a truncated replica cannot perform. Clamping bootstrap depth up to ' +
                    clamped + ' so the held window always exceeds the max rollback depth.');
                this._truncatedDepth = clamped;
            }
        }

        // Pending blocks from secondary sources for cross-verification
        this.pendingHashes = new Map(); // blockHeight -> { sourceIndex: hashes }

        // Applied-block heartbeat state. After committing each live block we report
        // our applied height back to the source servers so operators can observe
        // this validator's lag via the server's /status endpoint. Debounced to avoid
        // a round-trip per block under fast sync: flush every 10 blocks, or after 5s,
        // whichever comes first.
        this._hbLastSentBlock   = null;
        this._hbTimer           = null;
        this.lastAppliedBlockTime = null;

        // Stable identifier for this validator, used in POST /validator-heartbeat.
        // Operators set VALIDATOR_ID explicitly; we fall back to the system hostname.
        this.validatorId = process.env.VALIDATOR_ID || require('os').hostname() || 'unknown';

        // Divergence halt. Set (in-memory + durably in sync_halt) when a confirmed
        // cross-source consensus-hash divergence is detected. Once halted the client
        // applies NO further blocks and stays halted across restarts until an
        // operator clears it. null = healthy.
        this._halted = null; // { blockIndex, reason, mismatches, sources, at }

        // Throttled gap logging. On an inherently fast chain (e.g. Dogecoin
        // testnet, which mints blocks at ~10/sec and is tens of millions of
        // blocks high) the replica perpetually trails the live tip, so every
        // incoming block while behind would otherwise emit a "block gap" /
        // "continuity" line (thousands per minute, burying real faults in the
        // journal). Trailing-while-catching-up is a NORMAL condition, not an
        // error: we log the first occurrence, then at most one summary line per
        // _gapLogIntervalMs, folding the suppressed count into it.
        this._gapLogIntervalMs = Number(this.config['GAP_LOG_INTERVAL_MS'] || 30000);
        this._gapLogLastAt     = 0;
        this._gapLogSuppressed = 0;
    }

    // Throttled logger for normal catch-up lag (see constructor). Collapses the
    // per-block gap/continuity flood into one summary line per window. `now` is
    // injected by tests; production omits it and uses the wall clock.
    _logGap(message, now){
        now = (typeof now === 'number') ? now : Date.now();
        if(this._gapLogLastAt && (now - this._gapLogLastAt) < this._gapLogIntervalMs){
            this._gapLogSuppressed++;
            return;
        }
        let suffix = this._gapLogSuppressed > 0
            ? ' (+' + this._gapLogSuppressed + ' similar in last ' +
              Math.round((now - this._gapLogLastAt) / 1000) + 's)'
            : '';
        console.log(message + suffix);
        this._gapLogLastAt = now;
        this._gapLogSuppressed = 0;
    }

    // Start the client sync loop.
    // Surface the replica's data-integrity posture at startup. The only defense
    // that actually REJECTS fabricated content is cross-source hash divergence
    // (2+ sources, VERIFY_HASHES, HALT_ON_DIVERGENCE). With a single source the
    // independent recompute only re-derives the local rows and compares them to
    // the hashes published by that same server (a server serving internally
    // consistent fake rows + matching fake hashes passes). The decoder path has
    // no hash rejection at all (completeness is row-count advisory only). None
    // of this is silently unsafe, but it is a trust assumption operators must
    // make deliberately, so say it out loud rather than burying it in docs.
    _warnTrustPosture(){
        if(this.sources.length < 2){
            console.warn(
                'SECURITY: ' + this.dbType + ' replica is running SINGLE-SOURCE (' +
                (this.sources[0] || '<none>') + '). Content integrity rests entirely on TLS trust ' +
                'of that one server. Cross-source divergence detection is INACTIVE, and the local ' +
                'recompute only checks rows against hashes published by the same server. Configure ' +
                '2+ independent SYNC_SOURCES for Byzantine integrity.'
            );
        }
        if(this.dbType === 'decoder'){
            console.warn(
                'SECURITY: decoder replication has no hash-based rejection. Completeness is ' +
                'row-count advisory only (a shortfall is logged, never rejected). A decoder replica ' +
                'trusts its source(s) for row content. Treat decoder sources as trusted infrastructure.'
            );
        }
    }

    async start(){
        this.running = true;
        console.log('ClientSync starting for ' + this.chain + '/' + this.network + '/' + this.dbType);

        this._warnTrustPosture();

        // A divergence halt is durable: if a prior run recorded an uncleared halt,
        // stay halted (do NOT catch up / apply) until an operator clears it. A
        // halted validator must never silently resume onto a contested chain.
        try {
            let prior = await this.db.getActiveHalt(this.dbType);
            if(prior){
                this._halted = {
                    blockIndex: Number(prior.block_index), reason: prior.reason,
                    mismatches: this._safeParse(prior.mismatches), sources: this._safeParse(prior.sources),
                    at: prior.detected_at
                };
                console.error('ClientSync is HALTED on a prior consensus divergence at block ' +
                    prior.block_index + ' (' + this.chain + '/' + this.network + '/' + this.dbType +
                    '). Not resuming until cleared. Detected at ' + prior.detected_at + '.');
                // Stay alive but idle so /status can report the halt.
                while(this.running && this._halted){ await this.util.sleep(5000); }
                if(!this._halted) return this.start(); // cleared at runtime -> restart cleanly
                return;
            }
        } catch(e){ console.error('halt-state check failed (continuing):', e); }

        // Reload the durable truncation join floor (set by a prior _bootstrapFromHeight).
        // _bootstrapBase is otherwise in-memory only, so after a restart the incremental
        // resume path below would leave it null: the join-block recompute skip and the
        // truncation floor would be silently lost. Must run before the resume branch so
        // _verifyRecompute and the depth guard see the persisted floor.
        await this._loadBootstrapBase();

        // Check local replica state
        this.lastAppliedBlock = await this.db.getLastBlock();

        if(this.lastAppliedBlock === null){
            // Empty database. Choose the bootstrap strategy: a chain with a
            // configured SYNC_BOOTSTRAP_DEPTH seeds from a recent height (fast
            // chains whose full snapshot can't be applied in one pass); everything
            // else takes the full-history snapshot.
            if(this._truncatedDepth >= 1){
                console.log('No local data found; bootstrapping ' + this.chain + '/' + this.network +
                    ' from recent height (SYNC_BOOTSTRAP_DEPTH=' + this._truncatedDepth + ', truncated replica)...');
                await this._bootstrapFromHeightRetry(this._truncatedDepth);
            } else {
                console.log('No local data found, bootstrapping from full snapshot...');
                await this._bootstrapFromSnapshot();
            }
        } else {
            // Partial data: incremental catch-up.
            // Pass the next needed block (lastAppliedBlock + 1): the server uses
            // inclusive >= bounds, so passing lastAppliedBlock re-delivers already
            // applied rows and the non-ignore INSERT throws a duplicate-key error.
            console.log('Resuming from block ' + this.lastAppliedBlock);
            await this._incrementalCatchUp(this.lastAppliedBlock + 1);
        }

        // Never enter live-follow on an empty replica. _bootstrapFromSnapshot now
        // throws on permanent exhaustion (unwinding before we get here, propagated to
        // the supervisor for a clean restart), but guard the tip directly too: if for
        // any reason we reach this point with no committed block, the live WS path
        // would apply the first block onto an empty DB with every continuity/fork/
        // duplicate guard disabled (all gated on lastAppliedBlock !== null), silently
        // orphaning all blocks below it. Refuse rather than corrupt the replica.
        if(this.lastAppliedBlock === null){
            throw new Error('Refusing to enter live-follow: replica still empty after bootstrap for ' +
                this.chain + '/' + this.network + '/' + this.dbType);
        }

        // Load last block hashes for continuity checking
        this.lastHashes = await this.db.getBlockHashRow(this.lastAppliedBlock);

        // Open WebSocket connections to all sources
        this._connectWebSockets();

        // Keep alive
        while(this.running){
            await this.util.sleep(5000);
        }
    }

    // Stop the client sync
    stop(){
        this.running = false;
        if(this._hbTimer){
            clearTimeout(this._hbTimer);
            this._hbTimer = null;
        }
        for(let ws of this.wsConns){
            try { ws.close(); } catch(e){}
        }
        this.wsConns = [];
    }

    // Schedule an applied-block heartbeat (debounced). Flushes immediately once at
    // least 10 blocks have been applied since the last report; otherwise arms a 5s
    // timer so a trickle of blocks still gets reported without a per-block send.
    _scheduleHeartbeat(){
        if(this._hbLastSentBlock === null ||
           (this.lastAppliedBlock - this._hbLastSentBlock) >= 10){
            this._flushHeartbeat();
        } else if(!this._hbTimer){
            this._hbTimer = setTimeout(() => {
                this._hbTimer = null;
                this._flushHeartbeat();
            }, 5000);
        }
    }

    // Send the current applied height to every open source connection. Best-effort:
    // a server running an older build simply ignores the message, and a failed send
    // is swallowed (the next heartbeat will carry the latest height anyway).
    // Also fires a REST POST /validator-heartbeat for named-validator tracking.
    _flushHeartbeat(){
        if(this.lastAppliedBlock === null) return;
        if(this._hbTimer){
            clearTimeout(this._hbTimer);
            this._hbTimer = null;
        }
        let msg = JSON.stringify({ type: 'heartbeat', appliedBlock: this.lastAppliedBlock });
        for(let ws of this.wsConns){
            try {
                if(ws && ws.readyState === WebSocket.OPEN)
                    ws.send(msg);
            } catch(e){ /* best-effort */ }
        }
        this._hbLastSentBlock = this.lastAppliedBlock;

        // REST heartbeat: fire-and-forget to each configured source.
        for(let source of this.sources){
            this._sendRestHeartbeat(source).catch(() => {});
        }
    }

    // POST the current applied height to a source server's /validator-heartbeat endpoint.
    // Best-effort: errors are suppressed at the call site.
    async _sendRestHeartbeat(source){
        let url = source + '/validator-heartbeat/' + this.dbType + '/' + this.chain + '/' + this.network;
        let body = {
            validator_id:       this.validatorId,
            applied_height:     this.lastAppliedBlock,
            applied_block_time: this.lastAppliedBlockTime
        };
        let headers = {};
        let apiKey  = this.config['SYNC_API_KEY'];
        if(apiKey) headers['Authorization'] = 'Bearer ' + apiKey;
        await axios.post(url, body, { timeout: 5000, headers });
    }

    // Fetch and apply schema from a remote sync server
    async _fetchAndApplySchema(source){
        console.log('Fetching schema from ' + source + '...');
        try {
            let url = source + '/schema/' + this.dbType + '/' + this.chain + '/' + this.network;
            let response = await axios.get(url, { timeout: 30000 });
            let schema = response.data;
            if(schema && schema.tables){
                for(let tableName in schema.tables){
                    let createSql = schema.tables[tableName];
                    if(!createSql) continue;

                    // Validate table name and DDL before executing
                    let idCheck = validation.validateIdentifier(tableName);
                    if(!idCheck.valid){
                        console.error('Rejected table name from schema: ' + tableName + ' (' + idCheck.reason + ')');
                        continue;
                    }
                    let ddlCheck = validation.validateDdl(createSql);
                    if(!ddlCheck.valid){
                        console.error('Rejected DDL for table ' + tableName + ': ' + ddlCheck.reason);
                        continue;
                    }

                    try {
                        // Check if table already exists
                        let exists = await this.db.doQuery(
                            "SELECT * FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
                            [this.db.dbName, tableName]
                        );
                        if(exists.length === 0){
                            await this.db.doQuery(createSql);
                            console.log('  Created table: ' + tableName);
                        } else {
                            // Table already exists: propagate any columns the
                            // master has added since this replica was bootstrapped.
                            // Without this the path is CREATE-only and a replica
                            // that pre-dates a column addition stalls on the first
                            // snapshot carrying it ("Unknown column ... in field
                            // list"). Runs before the snapshot apply, so the ALTERs
                            // are outside any snapshot transaction.
                            await this.db.addMissingColumns(tableName, createSql);
                        }
                    } catch(e){
                        // May fail on ordering; retry will catch it
                    }
                }
                console.log('Schema applied from ' + source);
            }
        } catch(e){
            console.error('Failed to fetch schema from ' + source + ':', e);
        }
    }

    // A missing table (errno 1146) or missing column (1054) during an apply
    // means the source's schema moved ahead of this replica AFTER bootstrap.
    // _fetchAndApplySchema only runs at bootstrap, so a server-side table
    // addition wedges every already-bootstrapped replica on the first snapshot
    // carrying rows for it (live case: anchor_actions, added server-side while
    // the replicas pre-dated it). Re-apply the source schema (it CREATEs
    // missing tables and ALTERs in missing columns) so the next apply attempt
    // can proceed. Debounced to one heal per minute so a failure the schema
    // can't fix (e.g. rejected DDL) can't hammer the /schema endpoint.
    async _healSchemaIfStale(e){
        let errno = e ? e.errno : null;
        if(errno !== 1146 && errno !== 1054) return false;
        let now = Date.now();
        if(this._lastSchemaHeal && (now - this._lastSchemaHeal) < 60000) return false;
        this._lastSchemaHeal = now;
        console.log('Apply failed on a schema gap (errno ' + errno + ') for ' +
            this.chain + '/' + this.network + '; re-applying source schema');
        await this._fetchAndApplySchema(this.sources[0]);
        return true;
    }

    // Bootstrap from a full snapshot.
    //
    // Drives a bounded retry-with-backoff loop around _bootstrapRotateSources (one
    // full pass over every configured source). A bootstrap that exhausts every
    // source must NEVER fall through and let start() enter live-follow on an empty
    // replica: that would apply the first live block onto an empty DB with all
    // continuity/fork/duplicate guards disabled (they are gated on
    // lastAppliedBlock !== null), durably halting (VERIFY_RECOMPUTE) or silently
    // orphaning every pre-bootstrap block. So:
    //   - success on any round -> return (lastAppliedBlock is committed)
    //   - all rounds exhausted -> THROW, propagating failure to start() and on to the
    //     supervisor (SyncService exits the process for a container restart)
    // This is the only recovery for the production single-source topology, where
    // there is no second source to rotate to and the old code returned normally.
    async _bootstrapFromSnapshot(){
        if(!this.sources[0]){
            console.error('No sync sources configured');
            throw new Error('Bootstrap failed: no sync sources configured for ' +
                this.chain + '/' + this.network + '/' + this.dbType);
        }

        let maxRetries = this.config['BOOTSTRAP_MAX_RETRIES'];
        maxRetries = Number.isFinite(maxRetries) ? Math.max(0, maxRetries) : 5;
        let baseMs = this.config['BOOTSTRAP_RETRY_BASE_MS'] || 2000;
        let maxMs  = this.config['BOOTSTRAP_RETRY_MAX_MS']  || 60000;

        for(let round = 0; ; round++){
            if(await this._bootstrapRotateSources()) return; // success: tip committed
            if(round >= maxRetries){
                // Exhausted all sources across every retry round. Do not return:
                // signal failure so start() never enters live-follow empty-handed.
                throw new Error('Bootstrap failed: all sync sources exhausted after ' +
                    (round + 1) + ' round(s) for ' + this.chain + '/' + this.network + '/' + this.dbType);
            }
            let delay = Math.min(maxMs, baseMs * Math.pow(2, round));
            console.warn('Bootstrap round ' + (round + 1) + ' exhausted all sources for ' +
                this.chain + '/' + this.network + '/' + this.dbType + '; retrying in ' + delay + 'ms');
            await this.util.sleep(delay);
        }
    }

    // One full pass over the configured sources for a bootstrap. Returns true once a
    // snapshot has been applied and the tip committed, false if every source failed
    // this round. `attempt` tracks rotation depth so a multi-source pass tries each
    // source exactly once and never recurses indefinitely.
    async _bootstrapRotateSources(attempt){
        attempt = attempt || 0;
        let source = this.sources[0];
        if(!source) return false;

        // Fetch and apply schema before downloading data
        await this._fetchAndApplySchema(source);

        console.log('Downloading full snapshot from ' + source + '...');
        try {
            let url = source + '/snapshot/' + this.dbType + '/' + this.chain + '/' + this.network;
            let response = await axios.get(url, {
                responseType: 'arraybuffer',
                timeout: 600000, // 10 minute timeout for large snapshots
                decompress: true,
                maxContentLength: this.config['SNAPSHOT_MAX_CONTENT']
            });

            let jsonStr = response.data;
            if(Buffer.isBuffer(jsonStr)){
                // Try to decompress if gzipped
                try {
                    jsonStr = zlib.gunzipSync(jsonStr);
                } catch(e){
                    // May already be decompressed by axios
                }
            }

            let snapshotData = JSON.parse(jsonStr.toString());
            await this.applier.applyFullSnapshot(snapshotData);
            this.lastAppliedBlock = snapshotData.block_height;

            // Verify against second source if available.
            if(this.sources.length > 1){
                if(this.dbType === 'indexer'){
                    // Indexer cross-source hash + table-count check, gated on VERIFY_HASHES.
                    if(this.config['VERIFY_HASHES'])
                        await this._verifyAgainstSource(this.sources[1], this.lastAppliedBlock);
                } else {
                    // Decoder has no synthetic chain-of-state hashes to compare, but a full
                    // snapshot can still arrive truncated (network cut mid-stream) or stale.
                    // Cross-check the source's published per-table row counts so an incomplete
                    // bootstrap fails loudly instead of being accepted as complete. This runs
                    // independent of VERIFY_HASHES: row counts need no synthetic hashes, so the
                    // indexer-only hash gate does not apply here.
                    await this._verifyDecoderCompleteness(this.sources[1], this.lastAppliedBlock);
                }
            }

            console.log('Bootstrap complete at block ' + this.lastAppliedBlock);
            return true;
        } catch(e){
            console.error('Bootstrap failed:', e);
            // Try next source, but only if we haven't exhausted all sources
            if(this.sources.length > 1 && attempt < this.sources.length - 1){
                console.log('Trying secondary source...');
                this.sources.push(this.sources.shift());
                return this._bootstrapRotateSources(attempt + 1);
            }
            console.error('All sync sources exhausted after ' + (attempt + 1) + ' attempt(s)');
            return false;
        }
    }

    // Bounded retry-with-backoff wrapper around _bootstrapFromHeight, mirroring
    // _bootstrapFromSnapshot's contract: a truncated bootstrap that keeps failing
    // must NEVER fall through to live-follow on an empty replica. Success returns;
    // exhaustion THROWS, propagating to start() and on to the supervisor for a
    // clean container restart. Reuses BOOTSTRAP_* tuning.
    async _bootstrapFromHeightRetry(depth){
        if(!this.sources[0]){
            throw new Error('Bootstrap-from-height failed: no sync sources configured for ' +
                this.chain + '/' + this.network + '/' + this.dbType);
        }
        let maxRetries = this.config['BOOTSTRAP_MAX_RETRIES'];
        maxRetries = Number.isFinite(maxRetries) ? Math.max(0, maxRetries) : 5;
        let baseMs = this.config['BOOTSTRAP_RETRY_BASE_MS'] || 2000;
        let maxMs  = this.config['BOOTSTRAP_RETRY_MAX_MS']  || 60000;

        for(let round = 0; ; round++){
            try {
                if(await this._bootstrapFromHeight(depth)) return; // success: tip committed
            } catch(e){
                console.error('Bootstrap-from-height round ' + (round + 1) + ' failed for ' +
                    this.chain + '/' + this.network + ':', e);
            }
            if(round >= maxRetries){
                throw new Error('Bootstrap-from-height failed: exhausted after ' + (round + 1) +
                    ' round(s) for ' + this.chain + '/' + this.network + '/' + this.dbType);
            }
            let delay = Math.min(maxMs, baseMs * Math.pow(2, round));
            console.warn('Bootstrap-from-height round ' + (round + 1) + ' failed for ' +
                this.chain + '/' + this.network + '; retrying in ' + delay + 'ms');
            await this.util.sleep(delay);
        }
    }

    // Seed a TRUNCATED replica from a recent height instead of full history.
    // Opt-in per chain (SYNC_BOOTSTRAP_DEPTH_<CHAIN>_<NETWORK>) for fast chains
    // whose full-history snapshot cannot be buffered+applied in one client pass.
    // Returns true once the tip is committed; throws on a failure the caller can
    // retry. Works for BOTH dbTypes: the terminal-block recompute below is a no-op
    // for the decoder (_verifyRecompute returns null; it has no synthetic chain
    // hashes), so the decoder simply seeds [base..tip] and live-follows by block_hash
    // continuity, which only needs the immediate predecessor (present from base up).
    //
    // CONSENSUS NOTE (load-bearing, indexer): the join block `base` has no `base-1`
    // predecessor on a truncated replica, so BlockHasher.computeBlockHashes(base)
    // folds a NULL previous_hash and would false-HALT on recompute. We record
    // this._bootstrapBase so _verifyRecompute skips recompute for ONLY that one
    // block; base's committed hashes arrive verbatim in the snapshot, and every
    // block > base recomputes normally (each folds its predecessor's committed
    // hash, which is present in [base..tip] and replicated verbatim). The chained
    // verification is therefore intact from base+1 upward.
    async _bootstrapFromHeight(depth){
        let source = this.sources[0];
        if(!source) throw new Error('Bootstrap-from-height: no sync source');

        // 1. Discover the source tip.
        let statusUrl = source + '/status/' + this.dbType + '/' + this.chain + '/' + this.network;
        let statusResp = await axios.get(statusUrl, { timeout: 30000 });
        let status = statusResp.data || {};
        // A server reports block_height (last broadcast position) and source_height
        // (DB tip). The incremental snapshot is built from the DB, so prefer the DB
        // tip; fall back to block_height. Either way `base` only needs to be recent.
        let tip = (typeof status.source_height === 'number') ? status.source_height
                : (typeof status.block_height === 'number') ? status.block_height : null;
        if(typeof tip !== 'number'){
            throw new Error('Bootstrap-from-height: source tip unavailable from ' + statusUrl);
        }
        let base = Math.max(0, tip - depth);
        console.log('Bootstrap-from-height: source tip=' + tip + ', depth=' + depth +
            ', base=' + base + ' for ' + this.chain + '/' + this.network);

        // 2. Apply schema before any data (same as the full-bootstrap path).
        await this._fetchAndApplySchema(source);

        // 3. Sync the append-only lookup tables (index_*) by id-cursor paging BEFORE
        //    the block window. A single full-dump of e.g. index_transactions (~8.5M
        //    rows on DOGE testnet) exceeds SNAPSHOT_MAX_CONTENT and aborts the
        //    download; paging keeps each request bounded. Done first so the block
        //    window's FK targets (blocks.*_hash_id -> index_transactions) are present.
        await this._syncLookupTablesPaged(source);

        // 4. Fetch and apply the block window from `base` with skip_lookups=1 (the
        //    lookups were just paged in). Bounded by the truncation window, so this
        //    response stays small. applyIncrementalSnapshot also applies updated_rows,
        //    cooldown credits, and rebuilds touched balances (all still bundled here).
        let url = source + '/snapshot/' + this.dbType + '/' + this.chain + '/' + this.network + '/since/' + base + '?skip_lookups=1';
        let response = await axios.get(url, {
            responseType: 'arraybuffer',
            timeout: 600000,
            decompress: true,
            maxContentLength: this.config['SNAPSHOT_MAX_CONTENT']
        });
        let jsonStr = response.data;
        if(Buffer.isBuffer(jsonStr)){
            try { jsonStr = zlib.gunzipSync(jsonStr); } catch(e){}
        }
        let snapshotData = JSON.parse(jsonStr.toString());

        // Record the join block BEFORE apply so a recompute triggered during/after
        // apply already sees the skip. since_block is authoritative (the server
        // echoes the exact bound it served); fall back to our requested base.
        this._bootstrapBase = (typeof snapshotData.since_block === 'number') ? snapshotData.since_block : base;
        // Persist the join floor durably. _bootstrapBase is otherwise an in-memory-
        // only field: after a restart the incremental path leaves it null, dropping
        // both the join-block recompute skip (a false-HALT on the join block) and the
        // truncation floor. Reloaded by _loadBootstrapBase() at startup.
        await this._persistBootstrapBase(this._bootstrapBase);

        await this._withApplyLock(() => this.applier.applyIncrementalSnapshot(snapshotData));
        if(typeof snapshotData.block_height === 'number'){
            this.lastAppliedBlock = snapshotData.block_height;
        }

        // 4b. Re-page the append-only lookups AFTER the block window. Step 3 paged
        //     index_* up to the source's high-water T1, but the step-4 snapshot is a
        //     fresh REPEATABLE READ at the source's CURRENT tip T2, which is higher
        //     whenever the source produced a block during paging (the normal case on
        //     a fast chain like DOGE testnet, where paging spans millions of rows /
        //     many seconds). Blocks (T1..T2] then carry blocks.*_hash_id pointing at
        //     index_transactions rows that were never paged in; getBlockHashRow
        //     resolves them as NULL (LEFT JOIN miss), which (a) commits gap blocks
        //     with unresolvable consensus hashes, (b) silently skips the terminal
        //     recompute below (its NULL-ledger_hash guard goes false), and (c) halts
        //     the first live block when it folds a NULL predecessor hash. Re-paging
        //     now (cursor = current MAX(id), so only the new (T1..T2] rows) fills
        //     those FK targets before recompute + live-follow. Idempotent and cheap
        //     when no block landed (one MAX(id) probe per table, zero pages).
        //     <SYNC-LOOKUP-REPAGE> keep aligned with _runIncrementalCatchUp.
        await this._syncLookupTablesPaged(source);

        // 4. Verify the TERMINAL block (folds tip-1's committed hash, which is
        //    present in [base..tip]). The join block `base` is intentionally NOT
        //    recomputed here (no base-1 predecessor); _verifyRecompute skips it.
        if(this.config['VERIFY_RECOMPUTE'] && typeof this.lastAppliedBlock === 'number' &&
           this.lastAppliedBlock > this._bootstrapBase){
            let termCommitted = await this.db.getBlockHashRow(this.lastAppliedBlock);
            if(termCommitted && termCommitted.ledger_hash){
                let termMismatches = await this._verifyRecompute({ block_index: this.lastAppliedBlock }, {
                    ledger_hash:   termCommitted.ledger_hash,
                    actions_hash:  termCommitted.actions_hash,
                    contract_hash: termCommitted.contract_hash
                });
                if(termMismatches){
                    await this._haltOnDivergence(this.lastAppliedBlock, termMismatches,
                        this.sources.slice(0, 1), 'local-recompute-divergence');
                    // Halted: leave lastAppliedBlock as-is so start()'s empty-replica
                    // guard does not fire; the durable halt blocks all further applies.
                    return true;
                }
            }
        }

        // Decoder: seed + converge `dispensers` (never carried by the incremental
        // block window or the id-cursor lookup paging) and verify the replica is
        // row-complete against the source. Without this a truncated decoder replica
        // starts with ZERO dispenser rows and no completeness signal ever fires.
        // Best-effort; the reconcile leaves the local table intact on failure.
        if(this.dbType === 'decoder'){
            await this._reconcileDispensers(source);
            await this._verifyDecoderCompleteness(source, this.lastAppliedBlock);
        }

        console.log('Bootstrap-from-height complete: ' + this.chain + '/' + this.network +
            ' replica holds [' + this._bootstrapBase + '..' + this.lastAppliedBlock + ']' +
            ' (truncated; pre-' + this._bootstrapBase + ' history and full-history aggregates unavailable)');
        return true;
    }

    // Per-page row count for _syncLookupTablesPaged. Bounded so no single request
    // approaches SNAPSHOT_MAX_CONTENT (the server clamps to its own ceiling too).
    _lookupPageSize(){
        let n = parseInt(this.config['LOOKUP_PAGE_SIZE'], 10);
        if(isNaN(n) || n < 1) n = 50000;
        return Math.min(100000, n);
    }

    // Sync the append-only lookup tables (topology `.index` set: index_*) by id
    // cursor instead of a single full-dump. For each table, start at the replica's
    // current MAX(id) (0 when empty) and page /snapshot-rows/.../<table>?after_id=
    // until has_more=false, applying each bounded page. This is the fix for fast/
    // large chains where one full-dump of e.g. index_transactions (~8.5M rows)
    // exceeds SNAPSHOT_MAX_CONTENT and aborts the download. Used by BOTH the
    // truncated bootstrap (max id 0 -> all rows) and truncated catch-up (current
    // max -> only new rows, since the tables are INSERT-only with monotonic ids).
    // Each page is applied via the existing incremental surface; index_* are in
    // ClientApplier.ignoreTables (INSERT IGNORE), so re-sent rows are idempotent.
    async _syncLookupTablesPaged(source){
        let tables = replicatedTables.getTopology(this.dbType).index || [];
        let pageSize = this._lookupPageSize();
        let expected = SCHEMA_VERSION[this.dbType];
        for(let table of tables){
            // Replica's current high-water cursor for this table (0 if empty/absent).
            // Cursor column is per-table (id, or address_id for decoder pubkeys).
            let col = replicatedTables.lookupCursorColumn(table);
            let afterId = 0;
            try {
                let r = await this.db.doQuery('SELECT MAX(`' + col + '`) AS m FROM `' + table + '`');
                if(r && r[0] && r[0].m != null) afterId = Number(r[0].m);
            } catch(e){
                afterId = 0; // table not present yet -> treat as empty (schema applied earlier)
            }
            let pages = 0;
            while(true){
                let url = source + '/snapshot-rows/' + this.dbType + '/' + this.chain + '/' +
                    this.network + '/' + table + '?after_id=' + afterId + '&limit=' + pageSize;
                let response = await axios.get(url, {
                    responseType: 'arraybuffer',
                    timeout: 600000,
                    decompress: true,
                    maxContentLength: this.config['SNAPSHOT_MAX_CONTENT']
                });
                let jsonStr = response.data;
                if(Buffer.isBuffer(jsonStr)){
                    try { jsonStr = zlib.gunzipSync(jsonStr); } catch(e){}
                }
                let page = JSON.parse(jsonStr.toString());
                if(page.schema_version !== expected){
                    throw new Error('Lookup page schema mismatch for ' + table + ': server=' +
                        page.schema_version + ' client=' + expected);
                }
                let rows = page.rows || [];
                if(rows.length){
                    // Apply through the existing incremental surface, carrying just this
                    // table. index_* are INSERT IGNORE, so re-sent rows no-op; a lookup
                    // page has no credits/debits/updated_rows, so the balance/escrow
                    // rebuilds inside applyIncrementalSnapshot are skipped.
                    await this._withApplyLock(() => this.applier.applyIncrementalSnapshot({
                        schema_version: expected,
                        tables: { [table]: rows }
                    }));
                }
                pages++;
                if(!page.has_more) break;
                // Advance the cursor; max_id is the server's last returned id. Guard
                // against non-progress so a misbehaving server can't spin us forever.
                let nextAfter = (typeof page.max_id === 'number') ? page.max_id : afterId;
                if(nextAfter <= afterId){
                    console.warn('Lookup paging for ' + table + ' made no progress past id ' +
                        afterId + '; stopping');
                    break;
                }
                afterId = nextAfter;
            }
            if(pages > 1) console.log('Lookup-sync ' + table + ': ' + pages + ' page(s) up to id ' + afterId);
        }
    }

    // Serialize all replica-mutating operations (live block apply, incremental
    // catch-up apply, reorg rollback) so two write transactions never overlap on
    // the replica DB. The in-flight guard on catch-up only serializes
    // catch-up-vs-catch-up; this also covers catch-up-vs-live and live-vs-live
    // (multiple sources). Without it, a catch-up and a concurrent live block race
    // on the same rows (e.g. both INSERT the same block's sync_meta) and one
    // transaction blocks the other until innodb_lock_wait_timeout (~50s), stalling
    // recovery (observed as ER_LOCK_WAIT_TIMEOUT on sync_meta during a source-DB
    // outage recovery). Simple promise-chain mutex; a failing op still releases.
    async _withApplyLock(fn){
        let prev = this._applyLock || Promise.resolve();
        let release;
        this._applyLock = new Promise(r => { release = r; });
        await prev.catch(() => {});
        try {
            return await fn();
        } finally {
            release();
        }
    }

    // Incremental catch-up.
    //
    // Range-idempotent and serialized. Callers pass an advisory sinceBlock, but it
    // is intentionally ignored: catch-up always resumes from the replica's actual
    // committed tip (re-read from the DB), never from the in-memory cursor, which
    // can lag a concurrently-applied block. A single in-flight guard coalesces
    // overlapping calls: two status/gap triggers firing under fault would
    // otherwise fetch overlapping ranges and re-insert already-applied rows,
    // crashing on keyed tables (blocks.id, tx_index) or silently duplicating
    // keyless ones (credits/debits). Together these guarantee each applied range
    // begins strictly above committed data, so the non-IGNORE INSERTs never
    // collide. (applyIncrementalSnapshot is itself atomic, one transaction, so a
    // failed catch-up leaves the committed tip unchanged and the next attempt
    // re-reads the same resume point.)
    async _incrementalCatchUp(sinceBlock){
        // Refuse to advance once halted on a divergence (same contract as
        // _applyBlockEvent). The live apply path has carried this guard since the
        // halts were made durable, but gap detection (_handleBlock) and status
        // events still triggered catch-ups while halted, and the catch-up apply
        // path would happily advance the replica past the divergence (the same
        // half-enforced-halt failure mode the live-path guard closed).
        if(this._halted){
            console.error('Refusing incremental catch-up since block ' + sinceBlock +
                '; client is HALTED on a consensus divergence at block ' + this._halted.blockIndex);
            return;
        }
        // Serialize catch-ups so two never apply overlapping ranges. A request that
        // arrives while one is in flight is not dropped: it sets a pending flag, and
        // the in-flight runner loops once more after it finishes. That closes any
        // residual gap (e.g. the source advanced mid-catch-up) without ever running
        // two catch-ups concurrently, so it fixes the duplication race AND avoids
        // leaving the replica a block short when triggers coalesce.
        if(this._catchUpInFlight){
            this._catchUpPending = true;
            return this._catchUpInFlight;
        }
        this._catchUpInFlight = (async () => {
            let keepGoing = true;
            while(keepGoing){
                this._catchUpPending = false;
                let before = this.lastAppliedBlock;
                await this._runIncrementalCatchUp();
                // Always make the first pass; re-run only if another trigger arrived
                // during this pass AND the pass actually advanced the tip AND we're
                // still live. The progress gate is essential: without it, a catch-up
                // that keeps failing (e.g. a 404 while the source is transiently behind
                // `since` during a reorg) under a flood of gap-detection status events
                // would re-set the pending flag every pass and spin forever. On no
                // progress we stop and let the next status/block event re-trigger a
                // fresh catch-up once the source has actually advanced. (`this.running`
                // gates only the re-run, not the initial pass; callers invoke catch-up
                // directly before start() sets running.)
                keepGoing = this._catchUpPending && (this.lastAppliedBlock !== before) && this.running;
            }
        })().finally(() => { this._catchUpInFlight = null; });
        return this._catchUpInFlight;
    }

    async _runIncrementalCatchUp(){
        let source = this.sources[0];
        if(!source) return;

        // Resume from the committed tip + 1 (the server's since/ bound is inclusive),
        // re-read here so a lagging in-memory cursor can never re-request applied rows.
        let dbTip = await this.db.getLastBlock();
        let sinceBlock = (dbTip === null ? 0 : dbTip) + 1;

        console.log('Incremental catch-up from block ' + sinceBlock + '...');
        try {
            // Truncated/fast chains: sync the append-only lookup tables by id cursor
            // first (only NEW rows, since the replica's MAX(id) is the cursor), then
            // fetch the block window with skip_lookups=1. Without this the bundled
            // snapshot re-full-dumps multi-million-row lookups on EVERY catch-up and
            // exceeds the content limit (the same wall as bootstrap). Full-history
            // chains keep the single bundled snapshot (lookups included) unchanged.
            let skipLookups = this._truncatedDepth >= 1;
            if(skipLookups){
                await this._syncLookupTablesPaged(source);
            }
            let url = source + '/snapshot/' + this.dbType + '/' + this.chain + '/' + this.network + '/since/' + sinceBlock +
                (skipLookups ? '?skip_lookups=1' : '');
            let response = await axios.get(url, {
                responseType: 'arraybuffer',
                timeout: 300000,
                decompress: true,
                maxContentLength: this.config['SNAPSHOT_MAX_CONTENT']
            });

            let jsonStr = response.data;
            if(Buffer.isBuffer(jsonStr)){
                try { jsonStr = zlib.gunzipSync(jsonStr); } catch(e){}
            }

            let snapshotData = JSON.parse(jsonStr.toString());
            await this._withApplyLock(() => this.applier.applyIncrementalSnapshot(snapshotData));
            if(typeof snapshotData.block_height === 'number')
                this.lastAppliedBlock = snapshotData.block_height;

            // Re-page lookups AFTER the block window, same reason as bootstrap: the
            // skip_lookups snapshot is a fresh REPEATABLE READ at the source tip T2,
            // which exceeds the paging high-water T1 whenever the source advanced
            // during paging, so blocks (T1..T2] reference index_* rows not yet pulled.
            // Re-paging (cursor = current MAX(id)) fills (T1..T2] before recompute so
            // getBlockHashRow resolves non-NULL and the join/terminal recompute below
            // actually runs instead of silently skipping on a NULL ledger_hash.
            // <SYNC-LOOKUP-REPAGE> keep aligned with _bootstrapFromHeight.
            if(skipLookups){
                await this._syncLookupTablesPaged(source);
            }

            // Verify the catch-up range. The live path recomputes every applied
            // block's consensus hashes, but a catch-up jumps a range in one
            // apply with no recompute. A reorg that happened while this client
            // was DISCONNECTED (the one fork the live event stream cannot deliver)
            // was previously stitched onto the replica's orphaned tip unverified,
            // silently following the new chain while keeping the orphaned blocks
            // below the join.
            //
            // Recomputing the FIRST re-delivered block (join block) closes the
            // orphan-stitch fault: its chain hashes fold the previous block's
            // committed hashes, so a join onto an orphan cannot reproduce the
            // committed hash -> durable halt, same contract as the live path.
            //
            // Recomputing the TERMINAL block (block_height) closes the
            // truncated/corrupted interior fault: its committed hashes fold the
            // whole applied range via the chained sync_meta rows, so a payload
            // that is correct at the join but corrupted past it is detected here
            // rather than being silently accepted and chained over by the next
            // live block's recompute.
            //
            // Both recomputes are gated on VERIFY_RECOMPUTE.
            if(this.dbType === 'indexer' && this.config['VERIFY_RECOMPUTE'] &&
               typeof snapshotData.since_block === 'number'){
                let joinBlock = snapshotData.since_block;
                let committed = await this.db.getBlockHashRow(joinBlock);
                if(committed && committed.ledger_hash){
                    let mismatches = await this._verifyRecompute({ block_index: joinBlock }, {
                        ledger_hash:   committed.ledger_hash,
                        actions_hash:  committed.actions_hash,
                        contract_hash: committed.contract_hash
                    });
                    if(mismatches){
                        await this._haltOnDivergence(joinBlock, mismatches,
                            this.sources.slice(0, 1), 'local-recompute-divergence');
                        return;
                    }
                }
                // Also recompute the terminal block if the range spans more than one block.
                let terminalBlock = snapshotData.block_height;
                if(typeof terminalBlock === 'number' && terminalBlock > joinBlock){
                    let termCommitted = await this.db.getBlockHashRow(terminalBlock);
                    if(termCommitted && termCommitted.ledger_hash){
                        let termMismatches = await this._verifyRecompute({ block_index: terminalBlock }, {
                            ledger_hash:   termCommitted.ledger_hash,
                            actions_hash:  termCommitted.actions_hash,
                            contract_hash: termCommitted.contract_hash
                        });
                        if(termMismatches){
                            await this._haltOnDivergence(terminalBlock, termMismatches,
                                this.sources.slice(0, 1), 'local-recompute-divergence');
                            return;
                        }
                    }
                }
            }

            // Decoder: converge `dispensers` and verify row counts. dispensers
            // cannot ride the block stream or the id-cursor lookup paging, so it
            // drifts between reconciles; re-dump + atomic replace every Nth catch-up
            // (cheap: the table is bounded by the decoder's hard-purge depth), and
            // INCLUDE dispensers in the completeness check only on those same cycles
            // so the interim drift does not spam TABLE_COUNT_MISMATCH. Other decoder
            // tables converge via the block stream / full-dumps and are checked on
            // every catch-up. Best-effort; gated to the decoder dbType.
            if(this.dbType === 'decoder'){
                this._catchUpCount = (this._catchUpCount || 0) + 1;
                let every = parseInt(this.config['DISPENSERS_RECONCILE_EVERY'], 10);
                if(isNaN(every) || every < 1) every = 20;
                let didReconcile = (this._catchUpCount % every === 0);
                if(didReconcile) await this._reconcileDispensers(source);
                await this._verifyDecoderCompleteness(source, this.lastAppliedBlock,
                    didReconcile ? null : new Set(['dispensers']));
            }
        } catch(e){
            console.error('Incremental catch-up failed:', e);
            // Schema-gap failures are fixable right now: heal and retry once.
            // The heal's debounce bounds the recursion: a second schema-gap
            // failure inside the window returns false and falls through.
            if(await this._healSchemaIfStale(e))
                return this._runIncrementalCatchUp();
        }
    }

    // Verify local block hashes against a remote source.
    // Indexer-only: decoder DB has no synthetic chain-of-state hashes to compare.
    async _verifyAgainstSource(source, blockHeight){
        if(this.dbType !== 'indexer') return;
        try {
            let url = source + '/status/' + this.dbType + '/' + this.chain + '/' + this.network;
            let response = await axios.get(url, { timeout: 10000 });
            let remoteStatus = response.data;

            let localHashes = await this.db.getBlockHashRow(blockHeight);
            if(!localHashes) return;

            let result = this.hashVerifier.compareBlockHashes(blockHeight, {
                ledger_hash: localHashes.ledger_hash,
                actions_hash: localHashes.actions_hash,
                contract_hash: localHashes.contract_hash
            }, {
                ledger_hash: remoteStatus.ledger_hash,
                actions_hash: remoteStatus.actions_hash,
                contract_hash: remoteStatus.contract_hash
            });

            if(!result.match){
                console.error('HASH MISMATCH at block ' + blockHeight + ' against ' + source);
                console.error('Mismatches:', JSON.stringify(result.mismatches));
            } else {
                console.log('Hash verification passed against ' + source);
            }

            // Independent recomputation (validator track): the comparison above is a
            // transport check (verbatim-replicated local hash vs the source's
            // published hash). Additionally recompute the LOCAL committed hash from
            // the LOCAL replicated raw rows: this catches a catch-up snapshot whose
            // DATA does not match its committed hash, which the verbatim comparison
            // cannot. (The live per-block path does the same in _applyBlockEvent.)
            if(this.config['VERIFY_RECOMPUTE']){
                let recomputeMismatches = await this._verifyRecompute({ block_index: blockHeight }, {
                    ledger_hash:   localHashes.ledger_hash,
                    actions_hash:  localHashes.actions_hash,
                    contract_hash: localHashes.contract_hash
                });
                if(recomputeMismatches){
                    await this._haltOnDivergence(blockHeight, recomputeMismatches, [source], 'local-recompute-divergence');
                    return;
                }
            }

            // Replica-completeness check (additive; never overrides the hash result).
            //
            // The committed ledger/actions/contract hashes are computed on the
            // source during block processing and replicated verbatim, so a follower
            // missing entire tables still agrees on every hash (the hashes describe
            // the source's blockchain computation, not what actually landed
            // downstream). The source now publishes per-table row counts on /status
            // (api.buildStatusRow); compare them against our own to surface any table
            // the source has rows in that we do not. A shortfall is logged as a
            // health signal for operators: it does NOT reject the block, since a
            // passing hash check is still a valid consensus result.
            let countMismatches = await this._verifyTableCounts(remoteStatus.table_counts);
            if(countMismatches.length){
                console.error('TABLE_COUNT_MISMATCH at block ' + blockHeight + ' against ' + source +
                    '; follower may be missing replicated rows:');
                console.error(JSON.stringify(countMismatches));
            } else if(remoteStatus.table_counts){
                console.log('Table-count verification passed against ' + source);
            }
        } catch(e){
            console.error('Hash verification failed against ' + source + ':', e);
        }
    }

    // Cross-check decoder snapshot completeness against a source's published
    // per-table row counts. Decoder has no synthetic ledger/actions/contract
    // hashes to compare, but a truncated or stale full snapshot still leaves the
    // follower with fewer rows than the source. _verifyAgainstSource (indexer-only)
    // never runs for decoder, so this is the only completeness signal at bootstrap.
    // Best-effort and additive: a shortfall is logged loudly so operators see an
    // incomplete bootstrap; a transient /status fetch failure is swallowed so it
    // doesn't abort an otherwise-good snapshot.
    async _verifyDecoderCompleteness(source, blockHeight, excludeTables){
        if(this.dbType !== 'decoder') return;
        try {
            let url = source + '/status/' + this.dbType + '/' + this.chain + '/' + this.network;
            let response = await axios.get(url, { timeout: 10000 });
            let remoteStatus = response.data;

            let countMismatches = await this._verifyTableCounts(remoteStatus.table_counts, excludeTables);
            if(countMismatches.length){
                console.error('TABLE_COUNT_MISMATCH at block ' + blockHeight + ' against ' + source +
                    '; decoder snapshot may be truncated or incomplete:');
                console.error(JSON.stringify(countMismatches));
            } else if(remoteStatus.table_counts){
                console.log('Table-count verification passed against ' + source);
            }
        } catch(e){
            console.error('Decoder completeness check failed against ' + source + ':', e);
        }
    }

    // Compare the source's published per-table row counts against this replica's
    // own counts. Returns an array of { table, sourceCount, localCount, delta } for
    // every table the source has MORE rows in than the follower (a shortfall that
    // indicates missing replicated data). Followers legitimately holding extra local
    // rows are ignored: only source-ahead deltas signal incomplete replication.
    // Best-effort: a table that can't be counted locally (absent in this replica's
    // schema) is reported as a full shortfall rather than silently skipped.
    async _verifyTableCounts(remoteCounts, excludeTables){
        let mismatches = [];
        if(!remoteCounts || typeof remoteCounts !== 'object') return mismatches;
        for(let table of Object.keys(remoteCounts)){
            // Callers can exclude a table whose drift is expected between convergence
            // passes (e.g. `dispensers` between replace-table reconciles) so a known,
            // separately-tracked divergence does not spam TABLE_COUNT_MISMATCH.
            if(excludeTables && excludeTables.has(table)) continue;
            // table names here come straight from the remote source's /status
            // payload: validate before they reach getTableCount's identifier
            // interpolation, mirroring the schema-application loop above. Skip
            // (don't fault) an invalid key so one bad name can't manufacture a
            // false count mismatch and trip a needless recompute/halt.
            let idCheck = validation.validateIdentifier(table);
            if(!idCheck.valid){
                console.error('Rejected table name in remote table_counts: ' + table + ' (' + idCheck.reason + ')');
                continue;
            }
            let remote = Number(remoteCounts[table]);
            if(!Number.isFinite(remote)) continue;
            let local;
            try {
                local = Number(await this.db.getTableCount(table));
            } catch(e){
                local = 0;
            }
            if(!Number.isFinite(local)) local = 0;
            if(remote > local)
                mismatches.push({ table: table, sourceCount: remote, localCount: local, delta: remote - local });
        }
        return mismatches;
    }

    // Re-fetch the decoder `dispensers` table in full and replace the local copy.
    // dispensers cannot ride the block stream or the id-cursor lookup paging (no
    // monotonic id; the decoder soft-expires then hard-purges rows), so a truncated
    // bootstrap never seeds it and an incremental catch-up lets it drift. This keyset-
    // paged re-dump + atomic replace (ClientApplier.applyDispensersReplace) is the
    // convergence path; _verifyDecoderCompleteness then verifies row counts without
    // false alarms. Decoder-only, best-effort: any fetch/parse failure aborts WITHOUT
    // touching the local table (the replace runs only once every page is in hand).
    async _reconcileDispensers(source){
        if(this.dbType !== 'decoder') return;
        if(!source) return;
        try {
            let all = [];
            let afterTx = null, afterAddr = null;
            let pageSize = this._lookupPageSize();
            for(let guard = 0; guard < 1000000; guard++){
                let url = source + '/snapshot-dispensers/' + this.dbType + '/' + this.chain + '/' + this.network +
                    '?limit=' + pageSize +
                    (afterTx !== null ? '&after_tx=' + afterTx + '&after_addr=' + afterAddr : '');
                let response = await axios.get(url, {
                    responseType: 'arraybuffer',
                    timeout: 300000,
                    decompress: true,
                    maxContentLength: this.config['SNAPSHOT_MAX_CONTENT']
                });
                let jsonStr = response.data;
                if(Buffer.isBuffer(jsonStr)){
                    try { jsonStr = zlib.gunzipSync(jsonStr); } catch(e){}
                }
                let page = JSON.parse(jsonStr.toString());
                let rows = Array.isArray(page.rows) ? page.rows : [];
                for(let r of rows) all.push(r);
                if(!page.has_more || rows.length === 0) break;
                afterTx = page.max_tx; afterAddr = page.max_addr;
            }
            await this._withApplyLock(() => this.applier.applyDispensersReplace(all));
            console.log('Dispensers reconcile: replaced ' + all.length + ' rows from ' + source +
                ' for ' + this.chain + '/' + this.network);
        } catch(e){
            // Best-effort: leave the existing local dispensers intact on any failure.
            console.error('Dispensers reconcile failed against ' + source +
                ' (local table left intact):', (e && e.message) ? e.message : e);
        }
    }

    // Connect WebSocket to all sources for live sync
    _connectWebSockets(){
        for(let i = 0; i < this.sources.length; i++){
            this._connectWebSocket(this.sources[i], i);
        }
    }

    // Connect a single WebSocket
    _connectWebSocket(source, sourceIndex){
        // Per-chain sync mode preference: 'full' (default) or 'infra-only'
        // Set via env: SYNC_MODE_BTC, SYNC_MODE_LTC, SYNC_MODE_DOGE (e.g., SYNC_MODE_DOGE=infra-only)
        let envKey   = 'SYNC_MODE_' + String(this.chain).toUpperCase();
        let syncMode = process.env[envKey] || this.config[envKey] || 'full';
        let modeQs   = (syncMode === 'infra-only') ? '?sync_mode=infra-only' : '';
        let wsUrl    = source.replace(/^http/, 'ws') + '/subscribe/' + this.dbType + '/' + this.chain + '/' + this.network + modeQs;
        console.log('Connecting WebSocket to ' + wsUrl + ' (sync_mode=' + syncMode + ')');

        let ws;
        try {
            ws = new WebSocket(wsUrl, { maxPayload: this.config['WS_MAX_PAYLOAD'] });
        } catch(e){
            console.error('WebSocket connection error:', e);
            this._scheduleReconnect(source, sourceIndex);
            return;
        }

        ws.on('open', () => {
            console.log('WebSocket connected to ' + source + ' for ' + this.chain + '/' + this.network);
        });

        ws.on('message', async (data) => {
            try {
                let event = JSON.parse(data.toString());
                let check = validation.validateWsEvent(event);
                if(!check.valid){
                    console.error('Invalid WS event from ' + source + ': ' + check.reason);
                    return;
                }
                await this._handleEvent(event, sourceIndex);
            } catch(e){
                console.error('Error handling WebSocket message:', e);
            }
        });

        ws.on('close', () => {
            console.log('WebSocket disconnected from ' + source);
            this._scheduleReconnect(source, sourceIndex);
        });

        ws.on('error', (err) => {
            console.error('WebSocket error from ' + source + ':', err.message);
        });

        this.wsConns[sourceIndex] = ws;
    }

    // Schedule a WebSocket reconnection
    _scheduleReconnect(source, sourceIndex){
        if(!this.running) return;
        setTimeout(() => {
            if(this.running)
                this._connectWebSocket(source, sourceIndex);
        }, this.config['CLIENT_RECONNECT_DELAY']);
    }

    // Handle an incoming WebSocket event
    async _handleEvent(event, sourceIndex){
        if(event.type === 'block'){
            // Track the server's advancing block height
            if(typeof event.block_index === 'number' &&
               (this.lastKnownServerBlock === null || event.block_index > this.lastKnownServerBlock)){
                this.lastKnownServerBlock = event.block_index;
            }
            await this._handleBlock(event, sourceIndex);
        } else if(event.type === 'reorg'){
            await this._handleReorg(event);
        } else if(event.type === 'status'){
            // Track the server's current block height
            if(typeof event.block_height === 'number' &&
               (this.lastKnownServerBlock === null || event.block_height > this.lastKnownServerBlock)){
                this.lastKnownServerBlock = event.block_height;
            }
            // Check for gaps on status update. Use a strict '>' (not '>='): a
            // server exactly one block ahead is the normal steady state (that
            // next block arrives over the live WS stream), so only a shortfall of
            // two or more blocks is a real gap worth an out-of-band catch-up. This
            // mirrors the decoder gap check in _handleBlock and avoids firing a
            // redundant incremental fetch on every status tick during live sync;
            // a genuinely dropped block is still picked up on the next status tick.
            // Design corner: after a reconnect the server may be exactly one block
            // ahead (lastAppliedBlock + 1 === event.block_height). This check
            // intentionally does NOT fire an incremental catch-up for that case.
            // The missing block arrives naturally on the next live WS block event;
            // if the chain is idle and that event never comes, the one-block gap
            // persists until the next block is mined and verifyChainContinuity
            // detects the skip and triggers catch-up. This keeps the steady-state
            // path quiet and avoids spurious snapshot fetches.
            if(this.lastAppliedBlock !== null && event.block_height > this.lastAppliedBlock + 1){
                this._logGap('Block gap detected: local=' + this.lastAppliedBlock + ' remote=' + event.block_height);
                await this._incrementalCatchUp(this.lastAppliedBlock + 1);
            }
        }
    }

    // Handle a block event
    async _handleBlock(event, sourceIndex){
        let blockIndex = event.block_index;

        // Defense in depth: refuse to apply a live block onto an empty replica.
        // start() bootstraps before live-follow, so reaching here with no committed
        // tip means bootstrap was skipped or silently failed. Applying the first live
        // block now would leave every block below it permanently missing (and because
        // the duplicate/continuity/fork guards below are ALL gated on
        // lastAppliedBlock !== null, control would otherwise fall straight through to
        // _applyBlockEvent). block_index 0 (true genesis) is the one legitimate
        // from-empty apply; for anything above it, refuse and trigger a catch-up to
        // rebuild from the source rather than orphaning the blocks beneath it.
        if(this.lastAppliedBlock === null && blockIndex > 0){
            console.error('Refusing to apply block ' + blockIndex + ' onto an empty replica (' +
                this.chain + '/' + this.network + '/' + this.dbType + '). Bootstrap did not complete; ' +
                'triggering catch-up instead of orphaning blocks below it');
            await this._incrementalCatchUp(blockIndex);
            return;
        }

        // Skip if we already have this block, but first guard against a fork at the
        // current head. A block re-delivered at our committed tip with a DIFFERENT
        // block_hash than the one we stored means the source replaced that block (a
        // short reorg we never observed on the live stream). Silently skipping it
        // would pin this replica to an orphaned tip, so treat it as a continuity
        // error and catch up (symmetric with the indexer's hash-continuity check
        // below, which catches the same class of fault via its chain hashes). Decoder
        // events carry only the block's own block_hash (no replicated previous-hash
        // link), so a head re-delivery is the one fork the stored hash can detect
        // without hash-chain math.
        if(this.lastAppliedBlock !== null && blockIndex <= this.lastAppliedBlock){
            if(this.dbType === 'decoder' &&
               blockIndex === this.lastAppliedBlock &&
               this.lastHashes && this.lastHashes.block_hash &&
               event.block_hash && event.block_hash !== this.lastHashes.block_hash){
                console.error('Chain continuity error (decoder): fork at head block ' + blockIndex +
                    '; stored block_hash ' + this.lastHashes.block_hash +
                    ' != incoming ' + event.block_hash + '; triggering catch-up');
                await this._incrementalCatchUp(this.lastAppliedBlock + 1);
            }
            return;
        }

        // Both dbTypes require block-height continuity: indexer uses chain hashes to detect
        // gaps and forks; decoder has no synthetic hashes but still needs gap detection so
        // blocks silently dropped between bootstrap and the first WS event are caught up.
        if(this.lastAppliedBlock !== null){
            if(this.dbType === 'indexer'){
                let continuity = this.hashVerifier.verifyChainContinuity(
                    this.lastAppliedBlock, this.lastHashes, event
                );
                if(!continuity.valid){
                    // Normal trailing-tip lag on a fast chain (server ahead of our
                    // committed height): not a fault. Log throttled at info level;
                    // a genuine fork at our head is caught separately above as an error.
                    this._logGap('Catch-up lag (indexer): ' + continuity.reason);
                    await this._incrementalCatchUp(this.lastAppliedBlock + 1);
                    return;
                }
            } else if(blockIndex > this.lastAppliedBlock + 1){
                this._logGap('Block gap detected (decoder): local=' + this.lastAppliedBlock + ' incoming=' + blockIndex);
                await this._incrementalCatchUp(this.lastAppliedBlock + 1);
                return;
            }
        }

        // Cross-source verification: indexer only (decoder has no synthetic chain hashes)
        if(this.dbType === 'indexer' && this.config['VERIFY_HASHES'] && this.sources.length > 1){
            // Store hashes from this source
            if(!this.pendingHashes.has(blockIndex))
                this.pendingHashes.set(blockIndex, {});
            this.pendingHashes.get(blockIndex)[sourceIndex] = {
                ledger_hash: event.ledger_hash,
                actions_hash: event.actions_hash,
                contract_hash: event.contract_hash
            };

            // Check if we have hashes from at least 2 sources
            let pending = this.pendingHashes.get(blockIndex);
            let sourceIndices = Object.keys(pending);
            if(sourceIndices.length < 2){
                // Wait for second source (with timeout)
                if(sourceIndex === 0){
                    setTimeout(async () => {
                        // If still waiting after timeout, handle based on strict mode
                        if(this.pendingHashes.has(blockIndex) && this.lastAppliedBlock < blockIndex){
                            if(this.config['HASH_CONFIRM_STRICT']){
                                console.error('STRICT: Cross-source timeout for block ' + blockIndex + ', rejecting (HASH_CONFIRM_STRICT=true)');
                                this.pendingHashes.delete(blockIndex);
                            } else {
                                console.log('Cross-source timeout for block ' + blockIndex + ', applying from primary');
                                try {
                                    await this._applyBlockEvent(event);
                                } catch(e){
                                    console.error('Error applying block ' + blockIndex + ' after cross-source timeout:', e);
                                }
                                this.pendingHashes.delete(blockIndex);
                            }
                        }
                    }, this.config['HASH_CONFIRM_TIMEOUT']);
                }
                return;
            }

            // Compare hashes
            let hashA = pending[sourceIndices[0]];
            let hashB = pending[sourceIndices[1]];
            let result = this.hashVerifier.compareBlockHashes(blockIndex, hashA, hashB);
            if(!result.match){
                this.pendingHashes.delete(blockIndex);
                if(this.config['HALT_ON_DIVERGENCE']){
                    // Durable, alerting halt: one source is on a forked/Byzantine chain.
                    await this._haltOnDivergence(blockIndex, result.mismatches,
                        [this.sources[sourceIndices[0]], this.sources[sourceIndices[1]]], 'cross-source-divergence');
                    return;
                }
                console.error('DISCREPANCY ALERT: Hash mismatch at block ' + blockIndex);
                console.error('Mismatches:', JSON.stringify(result.mismatches));
                return; // Don't apply contested blocks (log-only mode)
            }

            this.pendingHashes.delete(blockIndex);
        }

        // Apply the block
        await this._applyBlockEvent(event);
    }

    // Durable HALT on a confirmed cross-source consensus divergence. Two honest
    // sources committed different ledger/actions/contract hashes for the SAME
    // block: one is on a forked/Byzantine chain. We must NOT pick one and apply
    // it (that risks replicating a forked chain), and must NOT silently stall.
    // Stop applying, record the halt durably (survives restart), and alert loudly
    // until an operator investigates and clears it.
    async _haltOnDivergence(blockIndex, mismatches, sources, reason){
        if(this._halted) return; // already halted
        this._halted = {
            blockIndex, reason: reason || 'cross-source-divergence',
            mismatches: mismatches || [], sources: sources || [],
            at: new Date().toISOString()
        };
        try { await this.db.recordHalt(this.dbType, blockIndex, this._halted.reason, mismatches, sources); }
        catch(e){ console.error('CRITICAL: failed to persist divergence halt (still halting in-memory):', e); }
        console.error('================================================================');
        console.error('CONSENSUS DIVERGENCE HALT: ' + this.chain + '/' + this.network + '/' + this.dbType);
        if(this._halted.reason === 'local-recompute-divergence'){
            console.error('block ' + blockIndex + ': local recompute diverged from committed hash. Replica');
            console.error('integrity failure. HALTING (applying no further blocks). Operator must');
            console.error('investigate replica state and clear before this validator can resume.');
        } else if(this._halted.reason === 'max-rollback-depth-exceeded'){
            console.error('block ' + blockIndex + ': reorg too deep to roll back safely (exceeds');
            console.error('MAX_ROLLBACK_DEPTH). The replica is stranded on the orphaned fork and');
            console.error('cannot rewind to the new canonical base. HALTING (applying no further');
            console.error('blocks). Operator must investigate, resnapshot/rewind, and clear before');
            console.error('this validator can resume.');
        } else {
            console.error('block ' + blockIndex + ': sources disagree on the consensus hash. One is on a');
            console.error('forked/Byzantine chain. HALTING (applying no further blocks). Operator must');
            console.error('investigate and clear before this validator can resume.');
        }
        console.error('mismatches: ' + JSON.stringify(mismatches));
        console.error('sources: ' + JSON.stringify(sources));
        console.error('================================================================');
        // Stop the live apply path; pending cross-source hashes are now moot.
        this.pendingHashes.clear();
    }

    // Recompute a block's consensus hashes from the replica's raw rows and compare
    // to the committed hashes (carried in the live block event, or read locally).
    // Returns an array of mismatches [{field, computed, committed}] or null on a
    // clean match. Indexer-only (decoder has no synthetic chain hashes).
    //
    // A recompute ERROR (transient DB hiccup, schema gap) is logged loudly and
    // treated as a non-halt: a local infrastructure fault must not fork this
    // validator off the chain (halts are reserved for genuine DATA divergence).
    async _verifyRecompute(event, committedOverride){
        if(this.dbType !== 'indexer') return null;
        // Truncated-replica join block: `base` has no in-replica `base-1`
        // predecessor, so its chained previous_hash cannot be reproduced and a
        // recompute would false-HALT. Its committed hashes arrived verbatim in the
        // bootstrap snapshot; every block > base still recomputes normally (folds a
        // present predecessor). Skip ONLY this one block. null replica (full
        // history) leaves _bootstrapBase null and never matches.
        if(this._bootstrapBase !== null && event && event.block_index === this._bootstrapBase){
            return null;
        }
        let computed;
        try {
            computed = await this.blockHasher.computeBlockHashes(event.block_index);
        } catch(e){
            console.error('Recompute verification errored at block ' +
                (event && event.block_index) + ' (NOT halting on a recompute error):', e);
            return null;
        }
        let committed = committedOverride || {
            ledger_hash:   event.ledger_hash,
            actions_hash:  event.actions_hash,
            contract_hash: event.contract_hash
        };
        let mismatches = [];
        ['ledger_hash','actions_hash','contract_hash'].forEach(f => {
            if(computed[f] !== committed[f])
                mismatches.push({ field: f, computed: computed[f], committed: committed[f] });
        });
        return mismatches.length ? mismatches : null;
    }

    isHalted(){ return this._halted !== null; }
    getHaltInfo(){ return this._halted; }

    // Returns the truncation join floor (the lowest block this replica holds),
    // or null when the replica holds full history.
    getBootstrapBase(){ return this._bootstrapBase; }

    // True when this replica was seeded from a recent height and therefore
    // cannot answer pre-base history queries.
    isTruncated(){ return typeof this._bootstrapBase === 'number' && this._bootstrapBase > 0; }

    _safeParse(s){ try { return JSON.parse(s); } catch(e){ return s; } }

    // Durable-marker key for this client's truncation join floor. Namespaced by
    // dbType so the indexer and decoder replicas of one chain don't clobber each
    // other (they share neither DB nor floor, but the key space is shared if they
    // ever did).
    _bootstrapBaseKey(){ return 'bootstrap_base:' + this.dbType; }

    // Persist the truncation join floor durably so it survives a restart. Guarded:
    // the durable store is optional (older db instances / test mocks may not expose
    // it), so a missing setSyncState degrades to in-memory-only behaviour rather
    // than throwing. Fail-soft (the db helper itself swallows persistence errors).
    async _persistBootstrapBase(base){
        if(base === null || base === undefined) return;
        if(!this.db || typeof this.db.setSyncState !== 'function') return;
        await this.db.setSyncState(this._bootstrapBaseKey(), String(base));
    }

    // Reload the persisted truncation join floor at startup. Only overwrites the
    // in-memory _bootstrapBase when a durable value exists AND the field is not
    // already set (a fresh bootstrap in this same process already set it). A
    // full-history replica never wrote one, so this is a no-op there and the floor
    // stays null (every block recomputes). Guarded for db instances without the
    // durable store.
    async _loadBootstrapBase(){
        if(this._bootstrapBase !== null && this._bootstrapBase !== undefined) return;
        if(!this.db || typeof this.db.getSyncState !== 'function') return;
        let v = await this.db.getSyncState(this._bootstrapBaseKey());
        if(v === null || v === undefined) return;
        let n = Number(v);
        if(Number.isFinite(n)){
            this._bootstrapBase = n;
            console.log('Reloaded truncation join floor _bootstrapBase=' + n + ' for ' +
                this.chain + '/' + this.network + '/' + this.dbType + ' (survives restart)');
        }
    }

    // Operator clear: acknowledge an investigated divergence and allow resume.
    // Never automatic: a halted validator must not self-resume onto a contested
    // chain. Caller is responsible for restarting the sync loop afterwards.
    async clearHalt(){
        try { await this.db.clearHalt(this.dbType); } catch(e){ console.error('clearHalt persistence failed:', e); }
        const was = this._halted;
        this._halted = null;
        console.log('Divergence halt CLEARED for ' + this.chain + '/' + this.network + '/' + this.dbType +
            (was ? ' (was halted at block ' + was.blockIndex + ')' : ''));
        return was;
    }

    // Apply a verified block event
    async _applyBlockEvent(event){
        // Refuse to apply anything once halted on a divergence: never replicate
        // onto a chain we could not agree with the fleet on.
        if(this._halted){
            console.error('Refusing to apply block ' + (event && event.block_index) +
                '; client is HALTED on a consensus divergence at block ' + this._halted.blockIndex);
            return;
        }
        try {
            await this._withApplyLock(() => this.applier.applyBlock(event));
            // Independent recomputation (validator track). The block's raw rows are
            // now in the replica; recompute its consensus hashes and confirm they
            // match the committed hashes the source published for it. A mismatch
            // means the replicated DATA does not hash to the committed hash
            // (replication corruption, a partial apply, or a source serving rows
            // inconsistent with its own committed hash), so HALT durably rather
            // than advance onto unverifiable state.
            if(this.dbType === 'indexer' && this.config['VERIFY_RECOMPUTE']){
                let mismatches = await this._verifyRecompute(event);
                if(mismatches){
                    await this._haltOnDivergence(event.block_index, mismatches,
                        this.sources.slice(0, 1), 'local-recompute-divergence');
                    return; // halted: do not advance lastAppliedBlock
                }
            }
            // Replication-integrity check (validator track): the three hashes above cover
            // only immutable block-scoped rows. The state_hash covers the in-place mutations
            // + backdated refund credits the updated_rows / cooldownCredits channels carry,
            // so a follower that silently dropped one of those applies now HALTS instead of
            // serving divergent balances/status until the next full snapshot. APPLY-TIME ONLY:
            // the mutated rows are in their block-`event.block_index` state at exactly this
            // instant (just applied, before lastAppliedBlock advances). This must NEVER move
            // into the historical _verifyRecompute paths (catch-up / cross-source), where
            // those rows have since been mutated again. NULL state_hash (pre-feature blocks)
            // is skipped.
            if(this.dbType === 'indexer' && this.config['VERIFY_STATE_HASH'] !== false && event.state_hash != null){
                let delay = activationDelayBlocks(this.chain);
                let localState = await this.blockHasher.computeStateHash(
                    event.block_index, (delay === undefined) ? null : delay, gasTickSymbol());
                if(localState !== event.state_hash){
                    await this._haltOnDivergence(event.block_index,
                        [{ field: 'state_hash', a: event.state_hash, b: localState }],
                        this.sources.slice(0, 1), 'state-hash-divergence');
                    return; // halted: do not advance lastAppliedBlock
                }
            }
            // Light-client state-commitment check (SPV spec sec.4-5). applyBlock recomputed
            // + persisted the per-block SMT roots over the replica INSIDE its txn (atomic
            // with the data apply) and exposed them on applier._lastComputedRoots. Compare
            // to the source's committed roots and HALT durably on divergence, the same as
            // the state_hash check. APPLY-TIME ONLY (the roots were computed against the
            // just-applied replica state). Verifies balances_root + block_merkle_root +
            // state_root; state_root folds the BTC-only stakes_root (now recomputed by the
            // follower, see stateCommitment.gatherStakeEntries), so a stakes divergence
            // surfaces as a state_root mismatch. A NULL event.balances_root (block before
            // the flag-day) or a null _lastComputedRoots (block skipped/duplicate this
            // apply, already verified when first applied) is skipped, never a divergence.
            if(this.dbType === 'indexer' && this.config['VERIFY_STATE_COMMITMENT'] !== false
                    && event.balances_root != null && this.applier._lastComputedRoots){
                let computed   = this.applier._lastComputedRoots;
                let mismatches = [];
                if(computed.balances_root !== event.balances_root)
                    mismatches.push({ field: 'balances_root', a: event.balances_root, b: computed.balances_root });
                if(event.block_merkle_root != null && computed.block_merkle_root !== event.block_merkle_root)
                    mismatches.push({ field: 'block_merkle_root', a: event.block_merkle_root, b: computed.block_merkle_root });
                if(event.state_root != null && computed.state_root !== event.state_root)
                    mismatches.push({ field: 'state_root', a: event.state_root, b: computed.state_root });
                if(mismatches.length){
                    await this._haltOnDivergence(event.block_index, mismatches,
                        this.sources.slice(0, 1), 'state-commitment-divergence');
                    return; // halted: do not advance lastAppliedBlock
                }
            }
            this.lastAppliedBlock     = event.block_index;
            this.lastAppliedBlockTime = (typeof event.block_time === 'number') ? event.block_time : null;
            // Report our applied height back to the source(s), debounced.
            this._scheduleHeartbeat();
            if(this.dbType === 'decoder'){
                this.lastHashes = { block_hash: event.block_hash };
            } else {
                this.lastHashes = {
                    ledger_hash: event.ledger_hash,
                    actions_hash: event.actions_hash,
                    contract_hash: event.contract_hash
                };
            }

            // Clean up old pending hashes
            for(let [key] of this.pendingHashes){
                if(key <= this.lastAppliedBlock)
                    this.pendingHashes.delete(key);
            }
        } catch(e){
            console.error('Error applying block ' + event.block_index + ':', e);
            // Heal a schema gap but don't re-apply the block inline: the
            // skipped block leaves a gap that the next status event's gap
            // detection closes via incremental catch-up, post-heal.
            await this._healSchemaIfStale(e);
        }
    }

    // Handle a reorg event
    async _handleReorg(event){
        console.log('Reorg event received for ' + this.chain + '/' + this.network + ' at block ' + event.block_index);

        // Enforce max rollback depth
        if(this.lastAppliedBlock !== null){
            let depth = this.lastAppliedBlock - event.block_index + 1;
            if(depth > this.config['MAX_ROLLBACK_DEPTH']){
                // A reorg too deep to roll back safely must FAIL CLOSED, not fail open.
                // Returning bare here would leave lastAppliedBlock pointing at the now-
                // orphaned tip: every canonical block the source re-streams from
                // event.block_index upward is <= lastAppliedBlock, so _handleBlock's
                // `blockIndex <= lastAppliedBlock` guard silently drops it and the replica
                // serves the orphaned fork indefinitely. The indexer track might eventually
                // self-halt once canonical hashes overtake the old tip and VERIFY_RECOMPUTE
                // catches the chained-hash break, but the decoder track has no recompute net
                // and would stay permanently diverged with halted:false on /status. So record
                // a durable halt via the same contract used for consensus divergence and let
                // the operator investigate/clear, rather than advancing onto the fork.
                await this._haltOnDivergence(event.block_index,
                    [{ field: 'rollback_depth', depth, max: this.config['MAX_ROLLBACK_DEPTH'] }],
                    this.sources.slice(0, 1), 'max-rollback-depth-exceeded');
                return; // halted: no rollback, lastAppliedBlock left as-is, no further applies
            }
        }

        try {
            await this._withApplyLock(() => this.rollback.rollback(event.block_index));
            this.lastAppliedBlock = event.block_index - 1;
            if(this.lastAppliedBlock > 0)
                this.lastHashes = await this.db.getBlockHashRow(this.lastAppliedBlock);
            else
                this.lastHashes = null;
        } catch(e){
            console.error('Reorg rollback failed:', e);
        }
    }
}

module.exports = ClientSync;
