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
const tableLifecycle = require('./tableLifecycle');
const { SCHEMA_VERSION } = require('./schema-version');
const { activationDelayBlocks, gasTickSymbol, coinTicker, btcStakeCapabilities, VALIDATOR_QUERY_LIMIT } = require('./consensus-constants');
const { bootstrapDepthKey } = require('./config');
const checkpointVerifier = require('./checkpoint');
const M = require('./merkle');
const { getPinnedValidators, getPinnedCheckpoint } = require('./pinnedValidators');

// Permanent bootstrap exhaustion. start()-time throws already unwind to
// SyncService's sync.start().catch(... process.exit(1)) restart contract on their
// own, but the same exhaustion is also reachable MID-STREAM (the size-cap fallback
// in _runIncrementalCatchUp re-runs _bootstrapFromSnapshot from live WS handling),
// where the serialized WS event chain's catch would otherwise swallow it and leave
// the process alive but permanently stalled (running=true, tip never advances, no
// supervisor restart). A dedicated error type lets that catch distinguish the
// unrecoverable case (_handleWsChainError) and escalate it to the same exit.
class BootstrapExhaustedError extends Error {}

class ClientSync {

    constructor(chain, network, db, applier, rollback, hashVerifier, config, util) {
        this.chain        = chain;
        // Canonical TICKER form of `chain`, for consensus / activation lookups keyed
        // '<TICKER>:<network>' (see coinTicker). `this.chain` keeps the caller's form
        // because the transport routes legitimately use the full name
        // (/snapshot/indexer/dogecoin/testnet); only the consensus lookups need this.
        this.coinTicker   = coinTicker(chain);
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

        // Per-chain subscribe mode: 'full' (default) or 'infra-only' (SYNC_MODE_<CHAIN>,
        // e.g. SYNC_MODE_DOGE=infra-only). Resolved ONCE here: the server filters an
        // infra-only subscriber's live blocks down to ServerPoller.infraTables (stakes,
        // delegations, validator_rewards, prices, reward_claims + index tables), so the
        // replica is deliberately incomplete and the apply-time VERIFY_* gates, which
        // recompute consensus hashes / the state_hash / the SMT roots over the replica's
        // rows, cannot pass: the first filtered block would trip a DURABLE
        // local-recompute-divergence halt mislabelling a configured mode as corruption.
        // Refuse to start in that combination and name the remedy, rather than silently
        // weakening gates the repo declares UNSAFE to turn off (operator decision
        // 2026-06-12): the all-gates-off posture stays an explicit operator choice.
        let modeKey    = 'SYNC_MODE_' + String(this.chain).toUpperCase();
        this._syncMode = process.env[modeKey] || this.config[modeKey] || 'full';
        if(this.dbType === 'indexer' && this._syncMode === 'infra-only'){
            let haltingGates = [];
            if(this.config['VERIFY_RECOMPUTE'])                  haltingGates.push('VERIFY_RECOMPUTE');
            if(this.config['VERIFY_STATE_HASH'] !== false)       haltingGates.push('VERIFY_STATE_HASH');
            if(this.config['VERIFY_STATE_COMMITMENT'] !== false) haltingGates.push('VERIFY_STATE_COMMITMENT');
            if(haltingGates.length){
                throw new Error(modeKey + '=infra-only on ' + this.chain + '/' + this.network + '/indexer ' +
                    'cannot run with halting verification enabled (' + haltingGates.join(', ') + '): the ' +
                    'source filters infra-only live blocks to the infrastructure tables, so the apply-time ' +
                    'recompute over the withheld rows would durably halt the replica on its first filtered ' +
                    'block. Either unset ' + modeKey + ' (full replica) or, for a throwaway infra mirror whose ' +
                    'state nothing downstream trusts, set ' + haltingGates.map(g => g + '=false').join(' ') +
                    ' explicitly (DECLARED UNSAFE for consensus-relevant replicas).');
            }
        }

        this.sources    = this.config['SYNC_SOURCES'].split(',').map(s => s.trim()).filter(s => s);
        this.running    = false;
        this.wsConns    = [];
        this.lastAppliedBlock     = null;
        this.lastHashes           = null;
        this.lastKnownServerBlock = null;

        // Wall-clock of the last WebSocket event received from ANY source. Drives the
        // source_height_stale signal on /status: lastKnownServerBlock only advances on
        // live events, so after a silent WS drop it freezes and lag_blocks reads 0 once
        // the replica catches up to it. A stale timestamp surfaces that the live signal
        // has gone quiet even though lag still computes to 0. null = no event seen yet
        // (staleness reported as unknown, not stale, during initial bootstrap).
        this._lastWsEventAt = null;

        // Highest checkpoint_seq the SPV anchor has successfully verified. The anchor
        // rejects a fetched checkpoint whose seq regresses below this: a genuine
        // federation sequence only advances, so a lower seq means the source rewound
        // (withholding the newer checkpoints that would catch a forged tail). null until
        // the first checkpoint is anchored. INERT unless VERIFY_CHECKPOINT_QUORUM is on.
        this._lastVerifiedCheckpointSeq = null;

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
        // Keyed through bootstrapDepthKey, NOT by `this.chain` directly: the hub hands
        // this constructor the full lowercase name ('dogecoin') while the env key names
        // the chain however the operator spelled it, so both sides must fold onto the
        // ticker or the lookup misses and falls through to 0, the full-snapshot branch.
        let _depthMap = this.config['SYNC_BOOTSTRAP_DEPTH'] || {};
        this._truncatedDepth = _depthMap[bootstrapDepthKey(this.chain, this.network)] || 0;

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

        // Per-block fallback timers for cross-source confirmation. Keyed by blockIndex;
        // armed once (regardless of which source arrives first) so a non-primary source
        // that delivers first still triggers the timeout if the other source never arrives.
        this._applyTimers = new Map();

        // Blocks that timed out cross-source confirmation while HASH_CONFIRM_STRICT
        // is on. Strict mode refuses to apply a block that only one source confirmed;
        // rejecting it at the live path alone is not enough, because the very next
        // status/gap trigger would re-fetch and apply the block single-source via the
        // incremental catch-up path, silently defeating the strict gate. A height
        // recorded here blocks single-source catch-up until the block is confirmed by
        // a second source over the live stream (which clears it) or an operator
        // intervenes. INERT unless HASH_CONFIRM_STRICT is on with 2+ sources.
        this._strictConfirmPending = new Set();

        // Multi-source Byzantine quorum.
        // Effective agreement threshold over the configured source set. SOURCE_QUORUM=0
        // (unset) selects the simple-majority default ceil((N+1)/2): N=1 -> 1 (single-
        // source posture), N=2 -> 2 (a 1-1 split has no majority and halts, exactly as
        // the prior pairwise path), N=3 -> 2, N=4 -> 3 (=2f+1, tolerates f=1 Byzantine of
        // 3f+1). An explicit value is clamped to [1, N]. Below-majority values let f
        // colluding sources out-vote the honest set: an operator's deliberate choice.
        let _numSources = this.sources.length;
        let _rawQuorum = Number(this.config['SOURCE_QUORUM'] || 0);
        if(_rawQuorum >= 1){
            this.sourceQuorum = Math.min(Math.max(1, _rawQuorum), Math.max(1, _numSources));
        } else {
            this.sourceQuorum = _numSources >= 1 ? Math.ceil((_numSources + 1) / 2) : 1;
        }

        // Byzantine-source strike accounting. Per source index, the recent block indices
        // at which that source dissented from the applied quorum majority. Pruned to a
        // sliding window (SOURCE_STRIKE_WINDOW blocks); a source whose live strike count
        // reaches SOURCE_EVICT_THRESHOLD is evicted from the active quorum denominator.
        this._sourceStrikes = new Map();    // sourceIndex -> [blockIndex, ...]
        this._evictedSources = new Set();   // sourceIndex
        this._sourceEvictThreshold = Math.max(1, Number(this.config['SOURCE_EVICT_THRESHOLD'] || 3));
        this._sourceStrikeWindow   = Math.max(1, Number(this.config['SOURCE_STRIKE_WINDOW'] || 200));

        // Count of sources that agreed on the most recently applied block (for /status).
        this._lastSourcesAgreeing = null;

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

        // Throttle stamp for the periodic replica-completeness sweep against the
        // primary source (see _maybeVerifyCompleteness). 0 = never swept, so the
        // first equal-height status tick of a process runs one baseline sweep.
        this._lastCompletenessSweepAt = 0;

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
        // Cross-source quorum only defends against a MINORITY of Byzantine sources; if
        // every configured source colludes on the same fabrication, agreement is
        // unanimous and wrong. Only the checkpoint-quorum anchor breaks that, because
        // its trust root is the pinned federation set, not the sources. Warn a
        // consensus-relevant indexer replica that runs with the anchor off or with an
        // empty pinned set, so the "I have N sources therefore I am safe" operator
        // learns that N colluding sources still need the anchor.
        if(this.dbType === 'indexer' && this.config['VERIFY_RECOMPUTE'] !== false){
            let pinned = getPinnedValidators(this.chain, this.network);
            let havePinned = Array.isArray(pinned) && pinned.length > 0;
            if(!this.config['VERIFY_CHECKPOINT_QUORUM'] || !havePinned){
                console.warn(
                    'SECURITY: ' + this.chain + '/' + this.network + '/indexer replica has NO active ' +
                    'checkpoint-quorum anchor (' +
                    (!this.config['VERIFY_CHECKPOINT_QUORUM'] ? 'VERIFY_CHECKPOINT_QUORUM is off'
                        : 'no pinned validator set configured') +
                    '). Cross-source quorum only outvotes a MINORITY of Byzantine sources; if ALL ' +
                    'configured sources collude they agree unanimously and wrong. The federation ' +
                    'checkpoint anchor is the only trust root that catches all-sources-collude. Enable ' +
                    'VERIFY_CHECKPOINT_QUORUM with a pinned validator set for any consensus-relevant replica.'
                );
            }
        }
    }

    // One-time startup WARN naming every per-block replicated table this replica's
    // schema lacks.
    //
    // The errno-1146 tolerance in every apply path is correct and stays: an older
    // replica schema must not wedge on a table the source has gained. What it costs
    // is that the gap is SILENT. The replica keeps reporting halted:false and
    // lag_blocks:0 while entire tables never arrive, `_verifyTableCounts` cannot see
    // it (it only compares tables the source published a count for against local
    // counts, and a source-side count for a table this replica lacks reads as a
    // count shortfall at best), and the only trace is a repeating ER_NO_SUCH_TABLE
    // stack per table per apply. That shape on mainnet is a data-completeness
    // failure no monitor would catch: observed 2026-07-29/30 on regtest replicas,
    // where the six BET tables logged ~480 error lines in 20 minutes under a green
    // status endpoint.
    //
    // So: say it ONCE, loudly, by name, at startup, and publish the same list on
    // /status (api.buildStatusRow) so a monitor alerts on the array rather than on
    // log-scraped stack traces. Advisory only: never halts, never throws, and a
    // failure to read the table listing leaves the list null (unknown), never [].
    async _warnMissingTables(){
        try {
            let present = await this.db.listExistingTables();
            let missing = replicatedTables.missingReplicatedTables(present, this.dbType);
            this._missingTables = missing;
            if(missing && missing.length){
                console.warn('MISSING_REPLICATED_TABLES: ' + this.chain + '/' + this.network + '/' +
                    this.dbType + ' replica schema is missing ' + missing.length +
                    ' table(s) that this build replicates per block: ' + missing.join(', ') +
                    '. Rows for these tables are SKIPPED (errno 1146 is tolerated so a schema gap ' +
                    'cannot wedge the replica), so replication is partial while /status still ' +
                    'reports halted:false. Migrate this replica to the source schema; the same ' +
                    'list is published as /status missing_tables.');
            }
        } catch(e){
            this._missingTables = null;
            console.error('Missing-table check failed for ' + this.chain + '/' + this.network + '/' +
                this.dbType + ' (advisory, continuing):', e.message);
        }
    }

    // Per-block replicated tables absent from this replica's schema, or null when
    // the check has not run / could not read the table listing.
    getMissingTables(){ return this._missingTables === undefined ? null : this._missingTables; }

    // Multi-source Byzantine quorum helpers.

    // Stable comparison key for a source's committed hash tuple.
    _hashTupleKey(h){
        if(!h) return 'null';
        return String(h.ledger_hash) + '|' + String(h.actions_hash) + '|' + String(h.contract_hash);
    }

    // Sources still eligible to vote (configured minus evicted).
    _activeSourceCount(){ return this.sources.length - this._evictedSources.size; }

    // Effective quorum, clamped to the number of active (non-evicted) sources so an
    // eviction lowers the denominator rather than making quorum permanently unreachable.
    _effectiveQuorum(){ return Math.min(this.sourceQuorum, Math.max(1, this._activeSourceCount())); }

    // Record a divergence strike against a source for a block, prune the sliding
    // window, and evict once the threshold is reached (subject to the keep-quorum-
    // viable guard). Idempotent per (source, block).
    _strikeSource(sourceIndex, blockIndex){
        if(this._evictedSources.has(sourceIndex)) return;
        let strikes = this._sourceStrikes.get(sourceIndex) || [];
        if(!strikes.length || strikes[strikes.length - 1] !== blockIndex) strikes.push(blockIndex);
        // Prune strikes older than the sliding window.
        let floor = blockIndex - this._sourceStrikeWindow;
        strikes = strikes.filter(b => b > floor);
        this._sourceStrikes.set(sourceIndex, strikes);
        console.warn('SOURCE STRIKE: ' + (this.sources[sourceIndex] || ('#' + sourceIndex)) +
            ' dissented from the quorum majority at block ' + blockIndex + ' (' + strikes.length + '/' +
            this._sourceEvictThreshold + ' within ' + this._sourceStrikeWindow + ' blocks) for ' +
            this.chain + '/' + this.network + '/' + this.dbType);
        if(strikes.length >= this._sourceEvictThreshold) this._evictSource(sourceIndex);
    }

    // Evict a Byzantine-suspected source: remove it from the active quorum denominator,
    // close its WebSocket (reconnect is suppressed for evicted sources), and alert.
    // Never evicts below two active sources, or cross-source verification collapses to
    // a single-source posture.
    _evictSource(sourceIndex){
        if(this._evictedSources.has(sourceIndex)) return;
        let label = this.sources[sourceIndex] || ('#' + sourceIndex);
        if(this._activeSourceCount() - 1 < 2){
            console.error('SOURCE EVICTION SUPPRESSED: ' + label + ' reached the strike threshold but ' +
                'evicting it would leave fewer than 2 active sources for ' + this.chain + '/' + this.network +
                '/' + this.dbType + '. Retaining it; per-block no-source-quorum halts still guard safety.');
            return;
        }
        this._evictedSources.add(sourceIndex);
        this._sourceStrikes.delete(sourceIndex);
        let ws = this.wsConns[sourceIndex];
        if(ws){
            try { ws._xchainEvicted = true; ws.close(); } catch(e){ /* best-effort */ }
        }
        console.error('================================================================');
        console.error('SOURCE EVICTED: ' + label + ' for ' + this.chain + '/' + this.network + '/' + this.dbType);
        console.error('It reached ' + this._sourceEvictThreshold + ' divergence strikes within ' +
            this._sourceStrikeWindow + ' blocks (dissented from the quorum majority). Its WebSocket is');
        console.error('closed and it is removed from the active quorum denominator (now ' +
            this._activeSourceCount() + ' active source(s)). A quorum still stands behind every applied');
        console.error('block. Investigate the evicted source for a fork/Byzantine fault.');
        console.error('================================================================');
    }

    // /status getters for the Byzantine quorum surface.
    getSourceQuorum(){ return this._effectiveQuorum(); }
    getConfiguredSourceCount(){ return this.sources.length; }
    getActiveSourceCount(){ return this._activeSourceCount(); }
    getEvictedSources(){ return [...this._evictedSources].map(i => this.sources[i] || ('#' + i)); }
    getSourcesAgreeing(){ return this._lastSourcesAgreeing; }

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
        } catch(e){
            // Fail CLOSED: an uncertain halt-state check (transient DB error while
            // reading sync_halt) must NOT be treated as "no halt" and resume catch-up -
            // a durably-halted replica could then silently resume onto a contested
            // chain. Hold in an idle halted state until a restart can positively read
            // the halt table (getActiveHalt now fails closed / throws rather than
            // returning [] on a query error).
            console.error('halt-state check failed; staying HALTED (fail-closed) until it can be read:', e);
            this._halted = {
                blockIndex: -1, reason: 'halt-state-check-failed',
                mismatches: [], sources: [], at: null
            };
            while(this.running && this._halted){ await this.util.sleep(5000); }
            if(!this._halted) return this.start(); // cleared at runtime -> restart cleanly
            return;
        }

        // Reload the durable truncation join floor (set by a prior _bootstrapFromHeight).
        // _bootstrapBase is otherwise in-memory only, so after a restart the incremental
        // resume path below would leave it null: the join-block recompute skip and the
        // truncation floor would be silently lost. Must run before the resume branch so
        // _verifyRecompute and the depth guard see the persisted floor.
        await this._loadBootstrapBase();

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
            // Reconcile the schema first. _fetchAndApplySchema otherwise runs only at
            // bootstrap, so a replica bootstrapped BEFORE the source added a table never
            // receives it on resume; and a zero-row source table (no VOTE / anchor-reconcile
            // activity on this chain) streams nothing, so neither the apply-path heal nor
            // the completeness-check heal ever fires to create it. The result is a permanent
            // ER_NO_SUCH_TABLE on every /status table-count sweep (observed live: polls /
            // poll_results / vote_delegations / votes / anchor_reward_reconcile_log on the BTC
            // replicas). Re-applying here is idempotent (replicateSchema only CREATEs missing
            // tables, never ALTERs or drops) and non-fatal (a fetch failure just logs and
            // returns), so a restart converges the schema even with no row flow to trigger a heal.
            await this._fetchAndApplySchema(this.sources[0]);
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

        // One loud, one-time signal for a schema gap that would otherwise replicate
        // silently. Runs after bootstrap / catch-up (both of which apply the source
        // schema, which CREATEs missing tables), so anything still absent here is a
        // gap no automatic heal closed, on a replica that is about to enter
        // live-follow reporting halted:false and lag_blocks:0 for tables that will
        // never arrive.
        await this._warnMissingTables();

        this.lastHashes = await this.db.getBlockHashRow(this.lastAppliedBlock);

        this._connectWebSockets();

        // Keep alive
        while(this.running){
            await this.util.sleep(5000);
        }
    }

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

    async _fetchAndApplySchema(source){
        console.log('Fetching schema from ' + source + '...');
        let schema;
        try {
            let url = source + '/schema/' + this.dbType + '/' + this.chain + '/' + this.network;
            let response = await axios.get(url, { timeout: 30000 });
            schema = response.data;
        } catch(e){
            // A fetch/transport failure is not a schema fault: the source may be
            // briefly unreachable. Leave it to the bootstrap retry/rotate loop.
            console.error('Failed to fetch schema from ' + source + ':', e);
            return;
        }
        if(!schema || !schema.tables) return;

        // Validate every table name + DDL up front, then collect the apply set.
        let pending = [];
        for(let tableName in schema.tables){
            let createSql = schema.tables[tableName];
            if(!createSql) continue;
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
            pending.push({ tableName, createSql });
        }

        // Multi-pass fixpoint. A CREATE can fail because a table it FK-references
        // has not been created yet; retrying the not-yet-applied tables until a
        // full pass makes no progress resolves that ordering deterministically
        // within this one bootstrap, instead of relying on lucky iteration order
        // across bootstrap re-routes. Crucially it also SEPARATES ordering misses
        // (which clear once their dependency lands) from genuine faults (disk-full,
        // permissions, malformed DDL), which persist to the fixpoint.
        //
        // Transient lock-timeout (errno 1205) handling: an ALTER TABLE that hits a
        // concurrent lock waits and times out is retriable. Instead of letting a
        // single transient failure roll straight into the fixpoint (no progress ->
        // halt), each table gets up to SCHEMA_TRANSIENT_MAX_RETRIES extra attempts
        // with an exponential backoff before it is treated as a persistent failure.
        // Real faults (disk-full, permission denied, malformed DDL) typically do not
        // produce errno 1205 and bypass the backoff entirely.
        const SCHEMA_TRANSIENT_ERRNO = 1205;  // ER_LOCK_WAIT_TIMEOUT
        const SCHEMA_TRANSIENT_MAX_RETRIES = 3;
        const SCHEMA_TRANSIENT_BASE_MS     = 2000;
        let lastErr = new Map();
        while(pending.length){
            let stillPending = [];
            let progressed = false;
            for(let { tableName, createSql } of pending){
                let attempt = 0;
                let succeeded = false;
                while(attempt <= SCHEMA_TRANSIENT_MAX_RETRIES){
                    try {
                        let exists = await this.db.doQuery(
                            "SELECT * FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
                            [this.db.dbName, tableName]
                        );
                        if(exists.length === 0){
                            await this.db.doQuery(createSql);
                            console.log('  Created table: ' + tableName);
                        } else {
                            // Table already exists: propagate any columns the master has
                            // added since this replica was bootstrapped. Without this the
                            // path is CREATE-only and a replica that pre-dates a column
                            // addition stalls on the first snapshot carrying it. Runs
                            // before the snapshot apply, so the ALTERs are outside any
                            // snapshot transaction.
                            await this.db.addMissingColumns(tableName, createSql);
                        }
                        lastErr.delete(tableName);
                        succeeded = true;
                        break;
                    } catch(e){
                        // Transient lock-timeout: retry with backoff up to the cap,
                        // then fall through to the fixpoint as a persistent failure.
                        if(e.errno === SCHEMA_TRANSIENT_ERRNO && attempt < SCHEMA_TRANSIENT_MAX_RETRIES){
                            let delay = SCHEMA_TRANSIENT_BASE_MS * Math.pow(2, attempt);
                            console.warn('Schema apply lock-timeout on ' + tableName +
                                ' (errno 1205), retrying in ' + delay + 'ms (attempt ' +
                                (attempt + 1) + '/' + SCHEMA_TRANSIENT_MAX_RETRIES + ')');
                            await this.util.sleep(delay);
                            attempt++;
                        } else {
                            lastErr.set(tableName, e);
                            break;
                        }
                    }
                }
                if(succeeded){
                    progressed = true;
                } else {
                    stillPending.push({ tableName, createSql });
                }
            }
            if(!progressed) break; // fixpoint: nothing advanced this pass
            pending = stillPending;
        }

        if(pending.length){
            // Genuine DDL faults survived the ordering fixpoint. Fail closed: the
            // old single-pass catch swallowed these, so the table was never created
            // and the next snapshot apply looped forever on errno 1146/1054 with
            // halted:false (no signal). Record a durable halt instead.
            let failed = pending.map(p => {
                let e = lastErr.get(p.tableName) || {};
                return { table: p.tableName, errno: e.errno || null, message: e.message || null };
            });
            await this._haltOnSchemaFailure(source, failed);
            return;
        }
        console.log('Schema applied from ' + source);
    }

    // Durable halt for an unrecoverable schema apply (distinct from the
    // consensus-divergence halt: same persistence + /status surface via
    // recordHalt/isHalted, but its own reason and messaging so operators are not
    // misled into chasing a forked chain). Reached only after the ordering
    // fixpoint AND after transient lock-timeouts have been retried with backoff,
    // so FK-ordering misses and brief ALTER contention never trigger it. Only
    // persistent faults (disk-full, permissions, malformed DDL, lock-timeout
    // that outlasts the retry cap) reach here.
    async _haltOnSchemaFailure(source, failedTables){
        if(this._halted) return;
        let blockIndex = (this.lastAppliedBlock != null) ? this.lastAppliedBlock : 0;
        this._halted = {
            blockIndex, reason: 'schema-apply-failed',
            mismatches: failedTables || [], sources: [source],
            at: new Date().toISOString()
        };
        try { await this.db.recordHalt(this.dbType, blockIndex, this._halted.reason, failedTables, [source]); }
        catch(e){ console.error('CRITICAL: failed to persist schema-apply halt (still halting in-memory):', e); }
        console.error('================================================================');
        console.error('SCHEMA APPLY HALT: ' + this.chain + '/' + this.network + '/' + this.dbType);
        console.error('after the multi-pass apply these tables still could not be created');
        console.error('or altered (a genuine DDL fault, not FK ordering): ' + JSON.stringify(failedTables));
        console.error('the replica cannot build a complete schema, so a snapshot apply would');
        console.error('loop forever on errno 1146/1054. HALTING (applying no further blocks).');
        console.error('Operator must fix the DDL fault (disk, permissions, lock, malformed');
        console.error('DDL) and clear the halt before this replica can resume.');
        console.error('================================================================');
        this.pendingHashes.clear();
        this._strictConfirmPending.clear();
        for(let [, timer] of this._applyTimers) clearTimeout(timer);
        this._applyTimers.clear();
    }

    // True when a snapshot download aborted because the body outgrew the axios
    // ceiling (SNAPSHOT_MAX_CONTENT). One definition for both the incremental
    // fallback and the bootstrap halt, so the two can never disagree on what the
    // size wall looks like.
    _isContentLengthOverflow(e){
        if(!e) return false;
        return e.code === 'ERR_FR_MAX_CONTENT_LENGTH_EXCEEDED' ||
               !!(e.message && e.message.includes('maxContentLength'));
    }

    // Seconds the source says to wait after a 429, or null when the error is not
    // a rate-limit. Reads Retry-After first, then express-rate-limit's
    // RateLimit-Reset; returns 0 when neither header is present so the caller can
    // still report the 429 itself.
    _rateLimitRetryAfterSeconds(e){
        let resp = e && e.response;
        if(!resp || resp.status !== 429) return null;
        let headers = resp.headers || {};
        let raw = headers['retry-after'] !== undefined ? headers['retry-after'] : headers['ratelimit-reset'];
        let secs = parseInt(raw, 10);
        return Number.isFinite(secs) && secs >= 0 ? secs : 0;
    }

    // Durable halt for a full snapshot that no longer fits under
    // SNAPSHOT_MAX_CONTENT (same recordHalt/isHalted persistence and /status
    // surface as the schema-apply halt, its own reason so an operator is not sent
    // chasing DDL). Reached only from the bootstrap path, where the payload size
    // is a property of the chain rather than of this attempt, so no retry, source
    // rotation, or process restart can change the outcome.
    async _haltOnSnapshotTooLarge(source, cause){
        if(this._halted) return;
        let blockIndex = (this.lastAppliedBlock != null) ? this.lastAppliedBlock : 0;
        let detail = [{ limit_bytes: this.config['SNAPSHOT_MAX_CONTENT'] || null,
                        message: (cause && cause.message) || null }];
        this._halted = {
            blockIndex, reason: 'snapshot-too-large',
            mismatches: detail, sources: [source],
            at: new Date().toISOString()
        };
        try { await this.db.recordHalt(this.dbType, blockIndex, this._halted.reason, detail, [source]); }
        catch(e){ console.error('CRITICAL: failed to persist snapshot-too-large halt (still halting in-memory):', e); }
        console.error('================================================================');
        console.error('SNAPSHOT TOO LARGE HALT: ' + this.chain + '/' + this.network + '/' + this.dbType);
        console.error('the full-history snapshot from ' + source + ' exceeds SNAPSHOT_MAX_CONTENT (' +
            (this.config['SNAPSHOT_MAX_CONTENT'] || 'unset') + ' bytes).');
        console.error('every source serves the same payload, so retrying, rotating sources, or');
        console.error('restarting the process cannot get past this. HALTING (applying no further');
        console.error('blocks) rather than crash-looping and exhausting the source snapshot budget.');
        console.error('Operator must either reseed this replica as a truncated one');
        console.error('(SYNC_BOOTSTRAP_DEPTH) or raise SNAPSHOT_MAX_CONTENT, then clear the halt.');
        console.error('================================================================');
        this.pendingHashes.clear();
        this._strictConfirmPending.clear();
        for(let [, timer] of this._applyTimers) clearTimeout(timer);
        this._applyTimers.clear();
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
            throw new BootstrapExhaustedError('Bootstrap failed: no sync sources configured for ' +
                this.chain + '/' + this.network + '/' + this.dbType);
        }

        // config.js always populates these three keys with clamped defaults
        // (5 / 2000 / 60000 via parseIntMin0/parseIntMin1), so no consumer-side
        // fallback is needed; trusting them keeps the default in one place.
        let maxRetries = this.config['BOOTSTRAP_MAX_RETRIES'];
        let baseMs = this.config['BOOTSTRAP_RETRY_BASE_MS'];
        let maxMs  = this.config['BOOTSTRAP_RETRY_MAX_MS'];

        for(let round = 0; ; round++){
            if(await this._bootstrapRotateSources()) return; // success: tip committed
            if(this._halted){
                // A halt was recorded mid-bootstrap (schema-apply fault, or a full
                // snapshot that cannot fit under SNAPSHOT_MAX_CONTENT). Retrying
                // cannot help in either case, so stop burning rounds: throw so
                // start()/SyncService restarts and start() lands in the durable
                // idle-halted state until an operator clears it.
                throw new Error('Bootstrap aborted by ' + this._halted.reason + ' halt for ' +
                    this.chain + '/' + this.network + '/' + this.dbType + '; operator must clear the halt');
            }
            if(round >= maxRetries){
                // Exhausted all sources across every retry round. Do not return:
                // signal failure so start() never enters live-follow empty-handed.
                // Typed so a mid-stream caller (the WS event chain) can recognize
                // permanent exhaustion and escalate instead of swallowing it.
                throw new BootstrapExhaustedError('Bootstrap failed: all sync sources exhausted after ' +
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
        // A schema-apply halt means the replica can't build a complete schema;
        // abort this round (the snapshot apply would only fail 1146/1054).
        if(this._halted) return false;

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
                try {
                    jsonStr = zlib.gunzipSync(jsonStr);
                } catch(e){
                    // May already be decompressed by axios
                }
            }

            let snapshotData;
            try {
                snapshotData = JSON.parse(jsonStr.toString());
            } catch(parseErr){
                throw new Error('Snapshot download truncated or corrupt from ' + source +
                    ' (JSON.parse failed; likely a network interruption mid-transfer): ' + parseErr.message);
            }
            // Serialize the full-snapshot apply under the shared write mutex. Bootstrap
            // at start() is single-threaded, but this same path is the runtime recovery
            // fallback for an oversized incremental catch-up (_runIncrementalCatchUp),
            // where live-follow is already active: a concurrent live block apply or a
            // cross-source fallback timer would otherwise open a second write transaction
            // on the replica and clobber the snapshot's DELETE+reload mid-flight.
            await this._withApplyLock(() => this.applier.applyFullSnapshot(snapshotData));
            this.lastAppliedBlock = snapshotData.block_height;
            // Pair lastHashes with the height just set (see _refreshTipHashes). A full
            // snapshot carries its own lookups, so no re-page ordering applies here;
            // this path is the oversized-catch-up fallback as well as start()'s.
            await this._refreshTipHashes();

            // A full-history snapshot reseeds complete state and correct SMT roots,
            // so any prior truncation join floor (set by an earlier _bootstrapFromHeight,
            // e.g. when the oversized-incremental fallback lands here on a chain whose
            // full snapshot does fit) no longer applies. Clear the in-memory floor and
            // its durable marker explicitly so isTruncated() reports false and the
            // apply-time VERIFY_STATE_COMMITMENT net re-arms on live blocks in THIS
            // session, without waiting for a restart to self-heal.
            await this._clearBootstrapBase();

            // Verify against secondary sources if available.
            if(this.sources.length > 1){
                if(this.dbType === 'indexer'){
                    // Indexer cross-source hash + table-count check, gated on VERIFY_HASHES.
                    // Bootstrap Byzantine cross-check: sources[0] supplied the
                    // applied snapshot (1 vote toward quorum). Seek agreement from enough
                    // ADDITIONAL sources to reach SOURCE_QUORUM. _verifyAgainstSource halts
                    // durably on a same-height divergence; a transport failure or tip-skew
                    // to a secondary is tolerated (not counted) so an unreachable spare
                    // cannot DoS bootstrap, but a shortfall below quorum is warned loudly.
                    if(this.config['VERIFY_HASHES']){
                        let need = Math.max(0, this._effectiveQuorum() - 1);
                        let agreed = 0;
                        for(let i = 1; i < this.sources.length && agreed < need; i++){
                            let verdict = await this._verifyAgainstSource(this.sources[i], this.lastAppliedBlock);
                            if(this._halted) return false; // a divergence halted us
                            if(verdict === 'agree') agreed++;
                        }
                        if(agreed < need){
                            console.warn('SECURITY: bootstrap cross-check reached only ' + (agreed + 1) +
                                ' agreeing source(s) of the ' + this._effectiveQuorum() + ' required for quorum for ' +
                                this.chain + '/' + this.network + '/indexer; proceeding on reachable sources, but the ' +
                                'bootstrap tip is under-verified until live quorum forms.');
                        }
                    }
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
            // The full-history snapshot no longer fits under SNAPSHOT_MAX_CONTENT.
            // Halt durably instead of rotating/retrying: every source serves the
            // same oversized payload, so the rounds exhaust, BootstrapExhaustedError
            // exits the process, and systemd restarts straight back into the same
            // wall. That crash loop is what silently froze the DOGE:testnet replica
            // and then 429'd its own snapshot budget. The operator remedy
            // (reseed truncated via SYNC_BOOTSTRAP_DEPTH, or raise the ceiling) is
            // a decision no retry can make.
            if(this._isContentLengthOverflow(e)){
                await this._haltOnSnapshotTooLarge(source, e);
                return false;
            }
            // Name a 429 rather than burying it in the axios dump: the snapshot
            // limiter is hourly (SNAPSHOT_RATE_FULL) while this ladder retries in
            // seconds, so an operator reading the log must see the wait it implies.
            let retryAfter = this._rateLimitRetryAfterSeconds(e);
            if(retryAfter !== null){
                console.error('Bootstrap rate-limited (HTTP 429) by ' + source + ' for ' +
                    this.chain + '/' + this.network + '/' + this.dbType +
                    '; the source will not serve another full snapshot for ' + retryAfter + 's.');
            }
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
        // config.js always populates these three keys with clamped defaults
        // (5 / 2000 / 60000 via parseIntMin0/parseIntMin1), so no consumer-side
        // fallback is needed; trusting them keeps the default in one place.
        let maxRetries = this.config['BOOTSTRAP_MAX_RETRIES'];
        let baseMs = this.config['BOOTSTRAP_RETRY_BASE_MS'];
        let maxMs  = this.config['BOOTSTRAP_RETRY_MAX_MS'];

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
        let snapshotData;
        try {
            snapshotData = JSON.parse(jsonStr.toString());
        } catch(parseErr){
            throw new Error('Snapshot download truncated or corrupt from ' + source +
                ' (JSON.parse failed; likely a network interruption mid-transfer): ' + parseErr.message);
        }

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

        // Pair lastHashes with the height set above (see _refreshTipHashes). start()
        // re-reads it anyway, but this path is also the oversized-catch-up fallback,
        // reached with live-follow already running.
        await this._refreshTipHashes();

        // 4. Verify the TERMINAL block (folds tip-1's committed hash, which is
        //    present in [base..tip]). The join block `base` is intentionally NOT
        //    recomputed here (no base-1 predecessor); _verifyRecompute skips it.
        if(this.config['VERIFY_RECOMPUTE'] && typeof this.lastAppliedBlock === 'number' &&
           this.lastAppliedBlock > this._bootstrapBase){
            if(await this._verifyRangeBoundary(this.lastAppliedBlock)){
                // Halted: leave lastAppliedBlock as-is so start()'s empty-replica
                // guard does not fire; the durable halt blocks all further applies.
                return true;
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
            // Cursor column is the monotonic AUTO_INCREMENT `id` for every lookup table,
            // including decoder pubkeys. pubkeys.address_id is non-monotonic (assigned at
            // first-SEEN, inserted at first-SPEND) and was retired as a cursor; see
            // lookupCursorColumn in replicatedTables.js.
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
                let page;
                try {
                    page = JSON.parse(jsonStr.toString());
                } catch(parseErr){
                    throw new Error('Snapshot download truncated or corrupt from ' + source +
                        ' (lookup page for ' + table + ' failed JSON.parse; likely a network interruption mid-transfer): ' +
                        parseErr.message);
                }
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

    // Re-read lastHashes for the tip a snapshot apply just advanced us to.
    //
    // lastAppliedBlock and lastHashes are a PAIR and must describe the same block:
    // _handleBlock's fork-at-head guards treat a re-delivery at blockIndex ===
    // lastAppliedBlock whose hashes differ from lastHashes as a lost 1-block reorg.
    // start(), _applyBlockEvent and _handleReorg all keep the pair in step; the three
    // snapshot-apply paths advanced only the height, leaving lastHashes on the
    // PRE-catch-up tip. The next delivery of the new tip (a second source serving the
    // same height, or a WS reconnect replaying it) then compared an honest block
    // against the wrong block's hashes and always mismatched, so the one log line that
    // means "a reorg event was lost" became routine noise plus a redundant catch-up
    // that 404s at since = tip+1.
    //
    // Call it AFTER any lookup re-page: until those index_* rows land, getBlockHashRow
    // resolves NULL through a LEFT JOIN miss. A null row is not stored - lastHashes is
    // left on the older block rather than nulled, because verifyChainContinuity treats
    // a null prevHashes as "nothing to chain to" and would stop detecting gaps.
    async _refreshTipHashes(){
        if(typeof this.lastAppliedBlock !== 'number') return;
        let hashes = await this.db.getBlockHashRow(this.lastAppliedBlock);
        if(hashes !== null) this.lastHashes = hashes;
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
        // Strict cross-source gate carried into catch-up (M-22). A block that timed
        // out cross-source confirmation under HASH_CONFIRM_STRICT was rejected at the
        // live path precisely so it would NOT be applied single-source. The catch-up
        // path pulls a range from ONE source, so running it here would re-apply that
        // rejected block single-source and defeat the gate. Any strict-pending height
        // is at or above the committed tip (it was never applied), so its resolution
        // must come from a second source over the live stream, not from a single-source
        // catch-up. Refuse the catch-up while one is outstanding. Scoped to strict mode
        // with 2+ sources; the set is otherwise always empty so this is inert.
        if(this._strictConfirmPending.size > 0){
            let heights = Array.from(this._strictConfirmPending).sort((a, b) => a - b);
            console.error('Refusing incremental catch-up since block ' + sinceBlock +
                '; HASH_CONFIRM_STRICT is on and block(s) ' + JSON.stringify(heights) +
                ' await cross-source confirmation. Single-source catch-up would bypass the strict gate; ' +
                'waiting for a second source to confirm over the live stream.');
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
        // Declared outside the try because the catch reports on sinceBlock.
        let dbTip = null;
        let sinceBlock = 0;
        try {
            // rethrow, not the fail-soft default: this read runs outside a transaction, where
            // doQuery logs a query error and returns [], so a transient fault would read as
            // dbTip === null - the same answer a genuinely empty replica gives - and collapse
            // sinceBlock to 1, re-requesting the WHOLE history into ledger tables that take a
            // plain INSERT (credits/debits/escrows are in neither ignoreTables nor
            // upsertFullDumpTables). Read INSIDE the try so the catch below aborts this pass
            // and the next status/gap trigger retries, rather than rejecting out of the
            // _catchUpInFlight runner into the WS event handlers, which do not catch.
            // Sitting inside the try also routes a tip-read schema fault (1146/1054) into
            // the catch's _healSchemaIfStale and its one debounce-bounded retry, an edge
            // the read could not reach while it sat outside.
            dbTip = await this.db.getLastBlock(null, { rethrow: true });
            sinceBlock = (dbTip === null ? 0 : dbTip) + 1;

            console.log('Incremental catch-up from block ' + sinceBlock + '...');
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

            let snapshotData;
            try {
                snapshotData = JSON.parse(jsonStr.toString());
            } catch(parseErr){
                throw new Error('Snapshot download truncated or corrupt from ' + source +
                    ' (JSON.parse failed; likely a network interruption mid-transfer): ' + parseErr.message);
            }
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

            // The height above moved; bring lastHashes to the same block before any
            // live event can read the pair (see _refreshTipHashes). After the re-page,
            // so the hash row resolves non-NULL.
            await this._refreshTipHashes();

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
            if(this.dbType === 'indexer' && this.config['VERIFY_RECOMPUTE']){
                // The join block MUST be the client's OWN resume point (sinceBlock =
                // dbTip+1), NEVER the server-echoed snapshotData.since_block. Recomputing
                // dbTip+1 folds the pre-existing replica tip's committed hash (dbTip),
                // which is the only check that catches a range stitched onto an orphaned
                // tip after a disconnect reorg. Trusting the server's echoed since_block
                // lets a hostile source (A) OMIT it to skip verification entirely, or
                // (B) INFLATE it so the audited boundary sits inside the served range and
                // the real join (dbTip+1, which folds the orphan) is never recomputed.
                let joinBlock = sinceBlock;
                // A source echoing a since_block that disagrees with our own resume point
                // is buggy or trying to shift the audited boundary; refuse the range.
                if(typeof snapshotData.since_block === 'number' && snapshotData.since_block !== sinceBlock){
                    await this._haltOnDivergence(joinBlock,
                        [{ field: 'since_block', a: sinceBlock, b: snapshotData.since_block }],
                        this.sources.slice(0, 1), 'catchup-since-block-mismatch');
                    return;
                }
                if(await this._verifyRangeBoundary(joinBlock)) return;
                // Also recompute the terminal block if the range spans more than one block.
                let terminalBlock = snapshotData.block_height;
                if(typeof terminalBlock === 'number' && terminalBlock > joinBlock){
                    if(await this._verifyRangeBoundary(terminalBlock)) return;
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
                let didReconcile = this._shouldReconcileDispensers(Date.now());
                if(didReconcile) await this._reconcileDispensers(source);
                // Include dispensers in the completeness check only on the cycles we
                // actually reconciled, else interim drift spams TABLE_COUNT_MISMATCH.
                await this._verifyDecoderCompleteness(source, this.lastAppliedBlock,
                    didReconcile ? null : new Set(['dispensers']));
            }
        } catch(e){
            // Content-length overflow: the since/:block payload exceeds the axios
            // cap (ERR_FR_MAX_CONTENT_LENGTH_EXCEEDED). A deeply-lagged full-history
            // replica can never shrink this below the wall, so incremental can make
            // no progress. Fall back to a full bootstrap (the same path start() uses
            // for an empty replica) so the node self-recovers instead of looping
            // forever and requiring a manual DB wipe.
            let isSizeError = this._isContentLengthOverflow(e);
            if(isSizeError){
                // A truncated replica (SYNC_BOOTSTRAP_DEPTH) exists precisely because
                // its full-history snapshot exceeds SNAPSHOT_MAX_CONTENT and cannot be
                // buffered+applied in one pass, so _bootstrapFromSnapshot would hit the
                // identical size wall, exhaust retries, throw BootstrapExhaustedError,
                // and process.exit(1) into a permanent crash loop. Route truncated chains
                // to the bounded height bootstrap instead (mirroring start()'s empty-DB
                // branch); reserve the full snapshot for full-history replicas.
                if(this._truncatedDepth >= 1){
                    console.warn('Incremental catch-up payload too large at sinceBlock ' + sinceBlock +
                        '; falling back to bounded height bootstrap (SYNC_BOOTSTRAP_DEPTH=' +
                        this._truncatedDepth + ', truncated replica).');
                    await this._bootstrapFromHeightRetry(this._truncatedDepth);
                    return;
                }
                // A full-history replica whose chain has GROWN past the wall lands
                // here and its full snapshot is oversized too; _bootstrapFromSnapshot
                // now halts on that rather than crash-looping, so this stays
                // the right call for the case it was written for (a payload window too
                // wide to fetch incrementally but a snapshot that still fits).
                console.warn('Incremental catch-up payload too large at sinceBlock ' + sinceBlock +
                    '; falling back to full bootstrap.');
                await this._bootstrapFromSnapshot();
                return;
            }
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
    // Returns a verdict string the bootstrap quorum loop counts:
    //   'agree'      confirmed same-height hash match against this source
    //   'diverge'    same-height mismatch under HALT_ON_DIVERGENCE=false (log-only)
    //   'halted'     divergence halted the replica (caller must stop)
    //   'skew'       tip skew; no same-height comparison was possible
    //   'unreachable' transport fault reaching the source
    //   'skip'       not applicable (decoder, or no local hash row)
    async _verifyAgainstSource(source, blockHeight){
        if(this.dbType !== 'indexer') return 'skip';
        let verdict = 'skip';
        try {
            let url = source + '/status/' + this.dbType + '/' + this.chain + '/' + this.network;
            let response = await axios.get(url, { timeout: 10000 });
            let remoteStatus = response.data;

            let localHashes = await this.db.getBlockHashRow(blockHeight);
            if(!localHashes) return 'skip';

            // Height gate: remoteStatus reports the source's CURRENT tip, whose
            // hashes describe that tip, not necessarily our bootstrap blockHeight.
            // Comparing across skewed heights would raise a spurious HASH MISMATCH
            // (alarm fatigue) and make genuine divergence indistinguishable from
            // skew. Only run the cross-source hash comparison when both sides are at
            // the same height (the same gate the advisory index-map check applies
            // below). A confirmed same-height mismatch is a real cross-source
            // divergence and must halt like the live dual-source path, not log-and-
            // continue: a replica bootstrapped from a forked/Byzantine source would
            // otherwise proceed to serve and extend forked state.
            if(remoteStatus.block_height != null && Number(remoteStatus.block_height) !== blockHeight){
                console.warn('Skipping cross-source hash check: tip skew (local height ' + blockHeight +
                    ', source ' + source + ' height ' + remoteStatus.block_height + ')');
                verdict = 'skew';
            } else {
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
                    verdict = 'diverge';
                    if(this.config['HALT_ON_DIVERGENCE']){
                        // Durable, alerting halt mirroring the live dual-source path:
                        // this source bulk-applied an entire replica and now disagrees
                        // at the same height, so it is on a forked/Byzantine chain.
                        await this._haltOnDivergence(blockHeight, result.mismatches, [source], 'cross-source-divergence');
                        return 'halted';
                    }
                } else {
                    console.log('Hash verification passed against ' + source);
                    verdict = 'agree';
                }
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
                    return 'halted';
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
            // At the same height, exact-parity tables are also checked for the
            // replica-AHEAD direction (an un-replicated source-side forward DELETE
            // leaves extra local rows the shortfall check cannot see); reported under
            // its own tag so operators can tell it from ordinary lag.
            let countMismatches = await this._verifyTableCounts(remoteStatus.table_counts, undefined,
                { remoteHeight: remoteStatus.block_height, localHeight: blockHeight });
            let shortfalls = countMismatches.filter(m => m.reason !== 'replica-ahead');
            let ahead      = countMismatches.filter(m => m.reason === 'replica-ahead');
            if(shortfalls.length){
                console.error('TABLE_COUNT_MISMATCH at block ' + blockHeight + ' against ' + source +
                    '; follower may be missing replicated rows:');
                console.error(JSON.stringify(shortfalls));
            }
            if(ahead.length){
                console.error('TABLE_COUNT_REPLICA_AHEAD at block ' + blockHeight + ' against ' + source +
                    '; follower holds rows the source deleted (un-replicated forward DELETE?):');
                console.error(JSON.stringify(ahead));
            }
            if(!countMismatches.length && remoteStatus.table_counts){
                console.log('Table-count verification passed against ' + source);
            }

            // Advisory id->address map parity (NON-consensus; never halts). The
            // ledger/actions/contract hashes resolve ids to canonical strings, so a
            // divergent id map (e.g. a local INSERT IGNORE that kept a pre-existing
            // colliding id and dropped the source's authoritative row) is invisible
            // both to them AND to the row-count check above (same count, different
            // content). Recompute the deterministic-subset checksum over our replica
            // and compare to the source's, but ONLY when:
            //   - both sides have the feature on (source published a non-null checksum;
            //     our INDEX_MAP_PARITY_CHECK is set), and
            //   - we are AT the source's published height, so both checksums use the
            //     same block_index<= bound, and
            //   - index_addresses is not itself short here: a row shortfall is a
            //     completeness gap already surfaced above, not a content divergence,
            //     and comparing then would only be incompleteness noise.
            // A mismatch is logged + durably counted, never halted on (string-based
            // consensus is unaffected).
            if(this.config['INDEX_MAP_PARITY_CHECK']
                    && remoteStatus.index_map_checksum != null
                    && Number(remoteStatus.block_height) === blockHeight
                    && !countMismatches.some(m => m.table === 'index_addresses')){
                try {
                    let localChecksum = await this.blockHasher.computeIndexMapChecksum(blockHeight);
                    let res = this.hashVerifier.compareIndexMap(blockHeight, localChecksum, remoteStatus.index_map_checksum);
                    if(!res.match){
                        console.warn('INDEX_MAP_PARITY mismatch at block ' + blockHeight + ' against ' + source +
                            ': local=' + localChecksum + ' source=' + remoteStatus.index_map_checksum +
                            ' (advisory, NOT halting; id->address map content diverged at equal row count)');
                        await this._recordIndexMapMismatch(blockHeight);
                    } else {
                        console.log('Index-map parity passed against ' + source);
                    }
                } catch(e){
                    console.error('Index-map parity check errored at block ' + blockHeight +
                        ' (advisory, ignoring):', e.message);
                }
            }

            await this._verifyTableContentParity(source, blockHeight, remoteStatus);
            return verdict;
        } catch(e){
            console.error('Hash verification failed against ' + source + ':', e);
            return 'unreachable';
        }
    }

    // Durably count advisory index-map parity mismatches (best-effort health signal,
    // NOT a consensus gate). Never throws. Stores a running count and the last
    // divergent block under dbType-namespaced sync-state keys, so an operator / the
    // dashboard can see id-map content divergence accumulating without a halt.
    async _recordIndexMapMismatch(blockIndex){
        try {
            if(!this.db || typeof this.db.setSyncState !== 'function') return;
            let countKey = 'index_map_mismatch_count:' + this.dbType;
            let cur = (typeof this.db.getSyncState === 'function') ? await this.db.getSyncState(countKey) : null;
            let n = (cur != null && Number.isFinite(Number(cur))) ? Number(cur) + 1 : 1;
            await this.db.setSyncState(countKey, String(n));
            await this.db.setSyncState('index_map_mismatch_last_block:' + this.dbType, String(blockIndex));
        } catch(e){
            // advisory; swallow
        }
    }

    // Advisory per-table CONTENT parity (NON-consensus; never halts).
    //
    // The index-map check proves the id->address map; this one proves the ROWS of
    // every replicated table the registry declares covered. Without it the three
    // block hashes covered the ledger/actions/contract projections, the state hashes
    // covered the in-place mutation classes, and the row-count check covered
    // cardinality only, so a substitution that kept the count equal in any other
    // replicated table passed everything a follower ran.
    //
    // Called from BOTH verification paths, and the decoder is the reason it is a
    // method rather than an inline block: _verifyAgainstSource returns early for
    // dbType 'decoder', whose tables have no synthetic hashes at all and so had no
    // content commitment of any kind.
    //
    // Preconditions mirror the index-map check, for the same reasons: both sides
    // opted in (the source published a non-null payload), and we are AT the source's
    // published height so the window bounds agree. The source's window and per-lookup
    // id ceilings are fed back into the local recompute, so the two sides read the
    // same rows rather than each hashing its own tail. Row-count differences are
    // SKIPPED inside compareTableContent (that is completeness, surfaced by the count
    // check); only equal-count content divergence is reported, logged and durably
    // counted, never halted on. Never throws: an advisory check must not be able to
    // break the verification pass that carries it.
    async _verifyTableContentParity(source, blockHeight, remoteStatus){
        if(!this.config['TABLE_CONTENT_PARITY_CHECK']) return null;
        if(!remoteStatus || !remoteStatus.table_content_parity) return null;
        if(Number(remoteStatus.block_height) !== Number(blockHeight)) return null;
        try {
            let remoteParity = remoteStatus.table_content_parity;
            let idBounds = {};
            for(let table in (remoteParity.tables || {})){
                let e = remoteParity.tables[table];
                if(e && e.id_max !== undefined && e.id_max !== null) idBounds[table] = e.id_max;
            }
            let localParity = await this.blockHasher.computeTableContentChecksums(blockHeight, {
                window:   remoteParity.window,
                idBounds: idBounds
            });
            let res = this.hashVerifier.compareTableContent(blockHeight, localParity, remoteParity);
            if(!res.match){
                console.warn('TABLE_CONTENT_PARITY mismatch at block ' + blockHeight + ' against ' + source +
                    ': ' + JSON.stringify(res.mismatches) +
                    ' (advisory, NOT halting; replicated table content diverged at equal row count)');
                await this._recordTableContentMismatch(blockHeight, res.mismatches);
            } else {
                console.log('Table-content parity passed against ' + source +
                    ' (' + res.compared + ' tables compared, ' + res.skipped.length + ' skipped)');
            }
            return res;
        } catch(e){
            console.error('Table-content parity check errored at block %s (advisory, ignoring):', blockHeight, e.message);
            return null;
        }
    }

    // Durably count advisory table-content parity mismatches, the twin of
    // _recordIndexMapMismatch above and never a consensus gate. Never throws. Also
    // stores the diverging TABLE NAMES, because unlike the index-map counter this
    // check spans ~93 tables and "which one" is the whole diagnostic; the list is
    // capped so a pathological all-tables divergence cannot write an unbounded value.
    async _recordTableContentMismatch(blockIndex, mismatches){
        try {
            if(!this.db || typeof this.db.setSyncState !== 'function') return;
            let countKey = 'table_content_mismatch_count:' + this.dbType;
            let cur = (typeof this.db.getSyncState === 'function') ? await this.db.getSyncState(countKey) : null;
            let n = (cur != null && Number.isFinite(Number(cur))) ? Number(cur) + 1 : 1;
            await this.db.setSyncState(countKey, String(n));
            await this.db.setSyncState('table_content_mismatch_last_block:' + this.dbType, String(blockIndex));
            let names = (mismatches || []).map(m => m.table).slice(0, 20).join(',');
            await this.db.setSyncState('table_content_mismatch_last_tables:' + this.dbType, names);
        } catch(e){
            // advisory; swallow
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
    // Block-windowed decoder tables: the block-scoped and tx-scoped tables a
    // truncated (SYNC_BOOTSTRAP_DEPTH) replica retains only for [base..tip]. These
    // are the tables whose local count legitimately falls short of the source's
    // full-history count on a truncated replica. The append-only lookup (`index`)
    // tables are fully paged in out of band via the id-cursor route, so they stay
    // under the strict count check. Derived from the topology, not a hardcoded copy,
    // so it tracks any change to the decoder block-scoped/tx-scoped sets.
    _truncatedWindowedTables(){
        let t = replicatedTables.getTopology('decoder');
        return new Set([].concat(t.blockScoped || [], t.txScoped || [], t.actionScoped || []));
    }

    async _verifyDecoderCompleteness(source, blockHeight, excludeTables){
        if(this.dbType !== 'decoder') return;
        try {
            let url = source + '/status/' + this.dbType + '/' + this.chain + '/' + this.network;
            let response = await axios.get(url, { timeout: 10000 });
            let remoteStatus = response.data;

            // On a truncated replica the block-windowed tables (blocks, transactions,
            // transaction_outputs) hold only [base..tip], so comparing their local
            // count against the source's full-history /status count is a guaranteed,
            // permanent TABLE_COUNT_MISMATCH by design of the truncation, burying the
            // real signal this check exists for (residual dispensers drift, failed
            // bootstrap dumps). Exclude them from the strict count check when truncated,
            // mirroring the dispensers exclusion, and note it once at info level so the
            // scoping is visible. Append-only lookups stay strict. (Follow-up: have the
            // source's /status expose window-scoped counts (block_index >= base) so a
            // truncated replica can strictly verify its retained window.)
            let effectiveExcludes = excludeTables;
            if(this.isTruncated()){
                effectiveExcludes = new Set(excludeTables || []);
                let windowed = this._truncatedWindowedTables();
                for(let tbl of windowed) effectiveExcludes.add(tbl);
                console.log('Truncated replica: skipping block-windowed tables from the decoder ' +
                    'completeness count check (' + [...windowed].join(', ') + '); append-only lookups stay strict.');
            }

            let countMismatches = await this._verifyTableCounts(remoteStatus.table_counts, effectiveExcludes);
            if(countMismatches.length){
                console.error('TABLE_COUNT_MISMATCH at block ' + blockHeight + ' against ' + source +
                    '; decoder snapshot may be truncated or incomplete:');
                console.error(JSON.stringify(countMismatches));
            } else if(remoteStatus.table_counts){
                console.log('Table-count verification passed against ' + source);
            }

            // Decoder content parity (advisory). The counts above are the
            // ONLY other signal this DB has: no ledger/actions/contract hash, no state
            // hash, so an equal-count content substitution in blocks, transactions,
            // transaction_outputs or the lookups was invisible here.
            await this._verifyTableContentParity(source, blockHeight, remoteStatus);
        } catch(e){
            console.error('Decoder completeness check failed against ' + source + ':', e);
        }
    }

    // Compare the source's published per-table row counts against this replica's
    // own counts. Returns an array of { table, sourceCount, localCount, delta } for
    // every table the source has MORE rows in than the follower (a shortfall that
    // indicates missing replicated data). Followers holding extra local rows are
    // ignored by default (decoder dispensers hard-purge, truncated windows, height
    // skew between the /status read and the local count), EXCEPT when the caller
    // passes `opts.remoteHeight` / `opts.localHeight` and they are equal: then, for
    // the tables whose registry class asserts exact row-set parity (indexer
    // stream:* with replicaRollback 'mirror'), a replica-AHEAD delta is reported too,
    // tagged reason 'replica-ahead' (delta negative). That is the shape an
    // un-replicated source-side forward DELETE leaves (the anchor-reward winner
    // collapse on validator_rewards was invisible here for exactly this reason,
    // #5610); it is advisory and never halts.
    // Best-effort: a table that can't be counted locally (absent in this replica's
    // schema) is reported as a full shortfall rather than silently skipped.
    async _verifyTableCounts(remoteCounts, excludeTables, opts){
        let mismatches = [];
        if(!remoteCounts || typeof remoteCounts !== 'object') return mismatches;
        let remoteHeight = opts && Number(opts.remoteHeight);
        let localHeight  = opts && Number(opts.localHeight);
        let sameHeight   = Number.isFinite(remoteHeight) && Number.isFinite(localHeight) && remoteHeight === localHeight;
        let exactParity  = (sameHeight && this.dbType === 'indexer') ? this._exactParityTables() : null;
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
                // This key came from the SOURCE's /status payload, so the source HAS the
                // table: a local errno 1146 means the source's schema moved ahead of this
                // replica after bootstrap. The apply-path heal (_applyBlockEvent) never
                // fires for a table the source has not yet written a row to, because
                // nothing ever streams for it, so a zero-row server-side addition would
                // error here on EVERY completeness check forever and never get created
                // (observed live: polls / poll_results / vote_delegations on the BTC
                // replicas). Route it through the same debounced schema heal, which CREATEs
                // missing tables; the count then reads correctly on the next pass. Advisory
                // path, so a heal failure must never fault the completeness check itself.
                try { await this._healSchemaIfStale(e); } catch(healErr){ /* advisory only */ }
                local = 0;
            }
            if(!Number.isFinite(local)) local = 0;
            if(remote > local)
                mismatches.push({ table: table, sourceCount: remote, localCount: local, delta: remote - local });
            else if(local > remote && exactParity && exactParity.has(table))
                mismatches.push({ table: table, sourceCount: remote, localCount: local, delta: remote - local, reason: 'replica-ahead' });
        }
        return mismatches;
    }

    // Periodic replica-completeness sweep against the PRIMARY source: the row-count
    // comparison is the only check that sees a follower short rows the consensus hashes
    // structurally cannot cover, since those hashes describe the source's computation
    // rather than what landed downstream. The bootstrap caller iterates sources[1..], so
    // a single-source replica reaches it from nowhere else; this hangs off the status
    // tick and covers both dbTypes.
    //
    // EQUAL HEIGHTS ONLY: while behind, the source legitimately holds more rows in every
    // streamed table, and a shortfall reported on ordinary lag trains operators to ignore
    // the one signal this exists to give them.
    //
    // Advisory, never halts (a hash-verified block is still a valid consensus result, the
    // bootstrap caller's posture) and best-effort, so an unreachable source logs and
    // returns rather than disturbing live following.
    async _maybeVerifyCompleteness(source, remoteHeight){
        let interval = this.config['COMPLETENESS_CHECK_INTERVAL'];
        if(!interval || !source) return;
        if(this._halted) return;                        // nothing to verify onto
        if(this.lastAppliedBlock === null) return;       // pre-bootstrap
        if(Number(remoteHeight) !== Number(this.lastAppliedBlock)) return;
        let now = Date.now();
        if(this._lastCompletenessSweepAt && (now - this._lastCompletenessSweepAt) < interval) return;
        // Stamp BEFORE the await: ticks keep arriving during a sweep that issues a
        // COUNT(*) per replicated table on both sides, and a stamp set afterwards lets
        // a second tick run one concurrently against the same source.
        this._lastCompletenessSweepAt = now;
        try {
            if(this.dbType === 'decoder'){
                // Delegate: the decoder variant carries the truncation exclusions its
                // counts need. dispensers converges only on a reconcile cycle, so it is
                // excluded here or every sweep reports drift.
                await this._verifyDecoderCompleteness(source, this.lastAppliedBlock, new Set(['dispensers']));
                return;
            }
            let url = source + '/status/' + this.dbType + '/' + this.chain + '/' + this.network;
            let response = await axios.get(url, { timeout: 10000 });
            let remoteStatus = response.data;
            // Re-check the height against the status we just fetched: the tick that
            // triggered this may be seconds old and the source may have advanced.
            if(remoteStatus.block_height != null &&
               Number(remoteStatus.block_height) !== Number(this.lastAppliedBlock)) return;
            let mismatches = await this._verifyTableCounts(remoteStatus.table_counts, undefined,
                { remoteHeight: remoteStatus.block_height, localHeight: this.lastAppliedBlock });
            let shortfalls = mismatches.filter(m => m.reason !== 'replica-ahead');
            let ahead      = mismatches.filter(m => m.reason === 'replica-ahead');
            if(shortfalls.length){
                console.error('TABLE_COUNT_MISMATCH at block ' + this.lastAppliedBlock + ' against ' + source +
                    '; follower may be missing replicated rows:');
                console.error(JSON.stringify(shortfalls));
            }
            if(ahead.length){
                console.error('TABLE_COUNT_REPLICA_AHEAD at block ' + this.lastAppliedBlock + ' against ' + source +
                    '; follower holds rows the source deleted (un-replicated forward DELETE?):');
                console.error(JSON.stringify(ahead));
            }
            if(!mismatches.length && remoteStatus.table_counts)
                console.log('Table-count verification passed against ' + source);
        } catch(e){
            console.error('Periodic completeness sweep failed against ' + source + ':', e.message || e);
        }
    }

    // Indexer tables whose registry class asserts exact row-set parity with the
    // source: streamed forward (stream:*) and mirrored on rollback. Derived from
    // tableLifecycle so there is no second hand-maintained list; snapshot / local /
    // hub-mirror / follower-derived / lookup classes (where extra local rows can be
    // legitimate) are excluded by construction.
    _exactParityTables(){
        if(!this._exactParityTableSet){
            this._exactParityTableSet = new Set(tableLifecycle.tablesWhere(t =>
                t.owner === 'indexer' && /^stream:/.test(t.replication) && t.replicaRollback === 'mirror'));
        }
        return this._exactParityTableSet;
    }

    // Re-fetch the decoder `dispensers` table in full and replace the local copy.
    // dispensers cannot ride the block stream or the id-cursor lookup paging (no
    // monotonic id; the decoder soft-expires then hard-purges rows), so a truncated
    // bootstrap never seeds it and an incremental catch-up lets it drift. This keyset-
    // paged re-dump + atomic replace (ClientApplier.applyDispensersReplace) is the
    // convergence path; _verifyDecoderCompleteness then verifies row counts without
    // false alarms. Decoder-only, best-effort: any fetch/parse failure aborts WITHOUT
    // touching the local table (the replace runs only once every page is in hand).
    // Decide whether to reconcile the decoder `dispensers` table on this catch-up cycle
    // (advances the per-process cycle counter as a side effect). Reconcile when:
    //   (a) firstResume  - nothing reconciled yet this process (a resume that skipped
    //       bootstrap), so a resumed replica converges dispensers promptly instead of
    //       serving up to `every` cycles of stale rows;
    //   (b) periodic     - every Nth catch-up in steady state (DISPENSERS_RECONCILE_EVERY,
    //       default 20), since dispensers cannot ride the block stream;
    //   (c) intervalDue  - the last reconcile is older than DISPENSERS_RECONCILE_MAX_INTERVAL_MS
    //       (default 30 min; 0 disables), so a slow/stalled catch-up cadence cannot let
    //       dispensers drift unbounded in wall-clock time.
    // `_lastDispenserReconcileAt` is stamped by _reconcileDispensers on success (covering
    // the bootstrap reconcile too), so firstResume is false once any reconcile has run.
    _shouldReconcileDispensers(nowMs){
        this._catchUpCount = (this._catchUpCount || 0) + 1;
        let every = parseInt(this.config['DISPENSERS_RECONCILE_EVERY'], 10);
        if(isNaN(every) || every < 1) every = 20;
        let maxIntervalMs = parseInt(this.config['DISPENSERS_RECONCILE_MAX_INTERVAL_MS'], 10);
        if(isNaN(maxIntervalMs) || maxIntervalMs < 0) maxIntervalMs = 1800000;
        let firstResume = (this._lastDispenserReconcileAt == null);
        let periodic    = (this._catchUpCount % every === 0);
        let intervalDue = (maxIntervalMs > 0 && this._lastDispenserReconcileAt != null &&
                           (nowMs - this._lastDispenserReconcileAt) >= maxIntervalMs);
        return firstResume || periodic || intervalDue;
    }

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
            // Stamp on success (covers bootstrap + catch-up reconciles) so the resume
            // and max-interval triggers in _incrementalCatchUp can tell when dispensers
            // were last converged.
            this._lastDispenserReconcileAt = Date.now();
            console.log('Dispensers reconcile: replaced ' + all.length + ' rows from ' + source +
                ' for ' + this.chain + '/' + this.network);
        } catch(e){
            // Best-effort: leave the existing local dispensers intact on any failure.
            console.error('Dispensers reconcile failed against ' + source +
                ' (local table left intact):', (e && e.message) ? e.message : e);
        }
    }

    _connectWebSockets(){
        for(let i = 0; i < this.sources.length; i++){
            this._connectWebSocket(this.sources[i], i);
        }
    }

    _connectWebSocket(source, sourceIndex){
        // Per-chain sync mode preference: 'full' (default) or 'infra-only', resolved
        // once in the constructor (SYNC_MODE_<CHAIN>), which also refuses the
        // infra-only + halting-verification combination before any connect.
        let syncMode = this._syncMode || 'full';
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

        ws.on('message', (data) => {
            // Serialize event processing in arrival order. ws does not await an async
            // listener, so without this a burst of block events runs _handleEvent
            // concurrently and races on lastAppliedBlock (the gap/continuity check runs
            // before _withApplyLock), firing spurious gaps that thrash the client into
            // bulk catch-up (which skips the apply-time state_hash recompute). Chaining
            // synchronously per message keeps gap-detection and apply atomic and ordered.
            let event;
            try {
                event = JSON.parse(data.toString());
                let check = validation.validateWsEvent(event);
                if(!check.valid){
                    console.error('Invalid WS event from ' + source + ': ' + check.reason);
                    return;
                }
            } catch(e){
                console.error('Error parsing WebSocket message:', e);
                return;
            }
            // Stamp liveness on receipt (before the serialized apply chain) so the
            // freshness signal reflects when the server last spoke, not when we
            // finished applying. Any valid event type counts, including the periodic
            // status heartbeat that arrives even when no new block is produced.
            this._lastWsEventAt = Date.now();
            this._wsEventChain = (this._wsEventChain || Promise.resolve())
                .then(() => this._handleEvent(event, sourceIndex))
                .catch(e => this._handleWsChainError(e));
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

    // Terminal-error gate for the serialized WS event chain. Ordinary handler
    // errors are logged and the chain stays alive (the next event repairs state),
    // but permanent bootstrap exhaustion reached mid-stream (the size-cap fallback
    // in _runIncrementalCatchUp) is unrecoverable: log-and-drop would keep the
    // process alive with running=true while the replica never advances, so the
    // supervisor never restarts it. Honor the documented restart contract
    // (SyncService sync.start().catch -> process.exit(1)) from this path too.
    _handleWsChainError(e){
        if(e instanceof BootstrapExhaustedError){
            console.error('Bootstrap exhausted mid-stream for ' + this.chain + '/' +
                this.network + '/' + this.dbType + '; exiting for supervised restart:', e);
            process.exit(1);
            return; // reached only when process.exit is stubbed (tests)
        }
        console.error('Error handling WebSocket message:', e);
    }

    _scheduleReconnect(source, sourceIndex){
        if(!this.running) return;
        // An evicted (Byzantine-suspected) source stays disconnected: reconnecting it
        // would re-admit it to the stream it was evicted from.
        if(this._evictedSources.has(sourceIndex)){
            console.warn('Not reconnecting evicted source ' + source + ' for ' +
                this.chain + '/' + this.network + '/' + this.dbType);
            return;
        }
        setTimeout(() => {
            if(this.running)
                this._connectWebSocket(source, sourceIndex);
        }, this.config['CLIENT_RECONNECT_DELAY']);
    }

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
            // Periodic replica-completeness sweep (throttled, equal-heights only). The
            // status tick is the one recurring signal a live client gets from its own
            // primary source, which is exactly the source the sweep could not reach.
            await this._maybeVerifyCompleteness(this.sources[sourceIndex], event.block_height);
        }
    }

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
            } else if(this.dbType === 'indexer' &&
               blockIndex === this.lastAppliedBlock &&
               this.lastHashes){
                // Indexer head-fork mirror of the decoder branch. The indexer event
                // carries no block_hash, but it does carry the three chain-of-state
                // hashes, and this.lastHashes holds the committed ones for the tip. A
                // tip re-delivery whose ledger/actions/contract hash differs is a
                // 1-block reorg whose `reorg` event was lost across a WS drop; without
                // this check it falls through to the silent skip, verifyChainContinuity
                // (index-ordering only, no hash linkage) then accepts block N+1 onto the
                // orphaned tip, and VERIFY_RECOMPUTE folds the orphaned predecessor into
                // a durable local-recompute-divergence halt (or, with VERIFY_RECOMPUTE
                // off, silently retains the orphaned block). Route the mismatch through
                // the same rollback/catch-up path the decoder uses. A true duplicate
                // (all three hashes equal) still skips silently.
                let lh = this.lastHashes;
                let mismatch =
                    (event.ledger_hash   != null && lh.ledger_hash   != null && event.ledger_hash   !== lh.ledger_hash) ||
                    (event.actions_hash  != null && lh.actions_hash  != null && event.actions_hash  !== lh.actions_hash) ||
                    (event.contract_hash != null && lh.contract_hash != null && event.contract_hash !== lh.contract_hash);
                if(mismatch){
                    console.error('Chain continuity error (indexer): fork at head block ' + blockIndex +
                        '; stored ledger/actions/contract hash != incoming; triggering catch-up');
                    await this._incrementalCatchUp(this.lastAppliedBlock + 1);
                }
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

        // Cross-source M-of-N quorum verification: indexer only (decoder has no
        // synthetic chain hashes). Apply a block once SOURCE_QUORUM active sources
        // publish the SAME hash tuple; strike (and eventually evict) dissenters instead
        // of halting on any disagreement; halt (no-source-quorum) only when every active
        // source has reported and no group can reach quorum. The 2-source case behaves
        // exactly as before (quorum 2; a 1-1 split has no majority and halts), while a
        // larger set tolerates a Byzantine minority.
        if(this.dbType === 'indexer' && this.config['VERIFY_HASHES'] && this._activeSourceCount() > 1){
            // An evicted source's in-flight delivery is ignored for the tally.
            if(this._evictedSources.has(sourceIndex)) return;

            // Record this source's hash tuple.
            if(!this.pendingHashes.has(blockIndex)) this.pendingHashes.set(blockIndex, {});
            this.pendingHashes.get(blockIndex)[sourceIndex] = {
                ledger_hash: event.ledger_hash,
                actions_hash: event.actions_hash,
                contract_hash: event.contract_hash
            };

            // Tally reported, non-evicted sources by hash tuple.
            let pending = this.pendingHashes.get(blockIndex);
            let groups = new Map(); // hashKey -> [sourceIndex...]
            let reportedCount = 0;
            for(let idxStr of Object.keys(pending)){
                let idx = Number(idxStr);
                if(this._evictedSources.has(idx)) continue;
                reportedCount++;
                let key = this._hashTupleKey(pending[idxStr]);
                if(!groups.has(key)) groups.set(key, []);
                groups.get(key).push(idx);
            }
            let quorum  = this._effectiveQuorum();
            let activeN = this._activeSourceCount();

            // The CURRENT arrival's own group. Applying is gated on IT reaching quorum,
            // so we only ever apply the block payload we actually hold, and (under the
            // majority default) the winning group is unique.
            let currentKey   = this._hashTupleKey(pending[sourceIndex]);
            let currentGroup = groups.get(currentKey) || [];

            if(currentGroup.length >= quorum){
                // Quorum reached on the current arrival's hash. Strike every reported
                // dissenter, record the applied majority, and apply this event.
                for(let idxStr of Object.keys(pending)){
                    let idx = Number(idxStr);
                    if(this._evictedSources.has(idx)) continue;
                    if(this._hashTupleKey(pending[idxStr]) !== currentKey) this._strikeSource(idx, blockIndex);
                }
                this._lastSourcesAgreeing = currentGroup.length;
                this.pendingHashes.delete(blockIndex);
                this._strictConfirmPending.delete(blockIndex);
                let t = this._applyTimers.get(blockIndex);
                if(t){ clearTimeout(t); this._applyTimers.delete(blockIndex); }
                // fall through to apply
            } else if(reportedCount >= activeN){
                // Every active source reported and no group reached quorum: genuinely
                // contested, the replica cannot determine truth. Fail-stop.
                this.pendingHashes.delete(blockIndex);
                this._strictConfirmPending.delete(blockIndex);
                let t = this._applyTimers.get(blockIndex);
                if(t){ clearTimeout(t); this._applyTimers.delete(blockIndex); }
                let summary = [...groups.entries()].map(([k, arr]) => ({ hash: k, sources: arr.map(i => this.sources[i]) }));
                if(this.config['HALT_ON_DIVERGENCE']){
                    await this._haltOnDivergence(blockIndex, summary,
                        [...groups.values()].reduce((a, arr) => a.concat(arr.map(i => this.sources[i])), []),
                        'no-source-quorum');
                    return;
                }
                console.error('NO-QUORUM ALERT: sources split with no majority at block ' + blockIndex +
                    '; not applying (HALT_ON_DIVERGENCE=false, log-only)');
                console.error('groups:', JSON.stringify(summary));
                return; // Don't apply contested blocks (log-only mode)
            } else {
                // Not enough sources have reported to reach quorum yet. Arm the fallback
                // timer once per block (liveness fallback for a silent/slow source; a
                // genuine split is caught above once all active sources report).
                if(!this._applyTimers.has(blockIndex)){
                    let timer = setTimeout(async () => {
                        this._applyTimers.delete(blockIndex);
                        if(this.pendingHashes.has(blockIndex) && this.lastAppliedBlock < blockIndex){
                            if(this.config['HASH_CONFIRM_STRICT']){
                                console.error('STRICT: Cross-source quorum timeout for block ' + blockIndex +
                                    ', rejecting and blocking single-source catch-up (HASH_CONFIRM_STRICT=true)');
                                // Retain the pending hashes so a later delivery can still
                                // complete quorum; block single-source catch-up meanwhile.
                                this._strictConfirmPending.add(blockIndex);
                            } else {
                                console.log('Cross-source quorum timeout for block ' + blockIndex + ', applying from primary');
                                try {
                                    await this._applyBlockEvent(event);
                                } catch(e){
                                    console.error('Error applying block ' + blockIndex + ' after cross-source timeout:', e);
                                }
                                this.pendingHashes.delete(blockIndex);
                            }
                        }
                    }, this.config['HASH_CONFIRM_TIMEOUT']);
                    this._applyTimers.set(blockIndex, timer);
                }
                return;
            }
        }

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
        } else if(this._halted.reason === 'recompute-error'){
            console.error('block ' + blockIndex + ': the bulk-range boundary recompute ERRORED after');
            console.error('retries. This recompute is the only verification of the applied range');
            console.error('(at the catch-up join it is what catches a reorg that crossed a');
            console.error('disconnect), so the replica cannot prove its state. HALTING (applying');
            console.error('no further blocks). Operator must fix the local fault (DB, schema) and');
            console.error('clear before this validator can resume.');
        } else if(this._halted.reason === 'max-rollback-depth-exceeded'){
            console.error('block ' + blockIndex + ': reorg too deep to roll back safely (exceeds');
            console.error('MAX_ROLLBACK_DEPTH). The replica is stranded on the orphaned fork and');
            console.error('cannot rewind to the new canonical base. HALTING (applying no further');
            console.error('blocks). Operator must investigate, resnapshot/rewind, and clear before');
            console.error('this validator can resume.');
        } else if(this._halted.reason === 'checkpoint-quorum-divergence'){
            console.error('block ' + blockIndex + ': the federation quorum-signed checkpoint does not');
            console.error('match this replica (quorum failed under the pinned validator set, or its');
            console.error('committed state_root disagrees with the replica\'s own recompute). The');
            console.error('source served state the federation did not sign. HALTING (applying no');
            console.error('further blocks). Operator must investigate and clear before resuming.');
        } else if(this._halted.reason === 'no-source-quorum'){
            console.error('block ' + blockIndex + ': the active sources split with NO majority reaching');
            console.error('SOURCE_QUORUM (' + this._effectiveQuorum() + ' of ' + this._activeSourceCount() +
                ' active). The replica cannot determine which chain is canonical, so it must not');
            console.error('pick one. HALTING (applying no further blocks). Operator must investigate');
            console.error('the contending sources and clear before this validator can resume.');
        } else if(this._halted.reason === 'checkpoint-freshness-stale'){
            console.error('block ' + blockIndex + ': the newest federation quorum checkpoint trails the');
            console.error('replica tip by more than CHECKPOINT_FRESHNESS_BLOCKS and CHECKPOINT_FRESHNESS_STRICT');
            console.error('is on. The tail past the last anchor is unverifiable against the federation, so');
            console.error('this replica refuses to serve it. HALTING (applying no further blocks). Operator');
            console.error('must restore a fresh anchor (or clear strict mode) and clear before resuming.');
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
        this._strictConfirmPending.clear();
        for(let [, timer] of this._applyTimers) clearTimeout(timer);
        this._applyTimers.clear();
    }

    // Recompute a block's consensus hashes from the replica's raw rows and compare
    // to the committed hashes (carried in the live block event, or read locally).
    // Returns an array of mismatches [{field, computed, committed}] or null on a
    // clean match. Indexer-only (decoder has no synthetic chain hashes).
    //
    // A recompute ERROR (transient DB hiccup, schema gap) is logged loudly and
    // treated as a non-halt on the LIVE path: a local infrastructure fault must
    // not fork this validator off the chain (halts are reserved for genuine DATA
    // divergence). The live block was individually delivered and will be folded
    // by the next block's recompute, so failing open there loses one check, not
    // the range. Bulk-range callers (bootstrap terminal, catch-up join/terminal)
    // pass opts.failClosed instead: there the recompute is the ONLY verification
    // of the whole applied range (the join recompute is what catches a
    // disconnect-spanning reorg stitched onto an orphaned tip), so an error is
    // retried briefly and then THROWN for the caller to halt on.
    async _verifyRecompute(event, committedOverride, opts = {}){
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
        let attempts = opts.failClosed ? 3 : 1;
        for(let attempt = 1; attempt <= attempts; attempt++){
            try {
                // chain/network drive the state_key collation flag-day so the replica
                // gates identically to the source indexer (state_key_collation_activation.js).
                computed = await this.blockHasher.computeBlockHashes(event.block_index, this.network, this.chain);
                break;
            } catch(e){
                if(attempt < attempts){
                    console.error('Recompute verification errored at block %s (attempt %s/%s, retrying):',
                        (event && event.block_index), attempt, attempts, e);
                    await this.util.sleep(1000 * attempt);
                    continue;
                }
                if(opts.failClosed) throw e;
                console.error('Recompute verification errored at block %s (NOT halting on a recompute error):',
                    (event && event.block_index), e);
                return null;
            }
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

    // Fail-CLOSED recompute of one boundary block of a bulk-applied range
    // (bootstrap terminal, catch-up join, catch-up terminal). Halts durably on a
    // hash mismatch AND on a recompute error that survives the retries: unlike
    // the live path, this recompute is the only verification of the applied
    // range, so an unverifiable range must not be served. Returns true
    // when it halted (caller must stop), false when the block verified or the
    // committed hash is not yet resolvable (NULL ledger_hash / missing row, the
    // pre-existing skip the lookup re-page minimizes).
    async _verifyRangeBoundary(blockIndex){
        let committed = await this.db.getBlockHashRow(blockIndex);
        if(!(committed && committed.ledger_hash)) return false;
        let mismatches;
        try {
            mismatches = await this._verifyRecompute({ block_index: blockIndex }, {
                ledger_hash:   committed.ledger_hash,
                actions_hash:  committed.actions_hash,
                contract_hash: committed.contract_hash
            }, { failClosed: true });
        } catch(e){
            await this._haltOnDivergence(blockIndex,
                [{ field: 'recompute_error', computed: null, committed: String((e && e.message) || e) }],
                this.sources.slice(0, 1), 'recompute-error');
            return true;
        }
        if(mismatches){
            await this._haltOnDivergence(blockIndex, mismatches,
                this.sources.slice(0, 1), 'local-recompute-divergence');
            return true;
        }
        return false;
    }

    isHalted(){ return this._halted !== null; }
    getHaltInfo(){ return this._halted; }

    // Returns the truncation join floor (the lowest block this replica holds),
    // or null when the replica holds full history.
    getBootstrapBase(){ return this._bootstrapBase; }

    // True when this replica was seeded from a recent height and therefore
    // cannot answer pre-base history queries.
    isTruncated(){ return typeof this._bootstrapBase === 'number' && this._bootstrapBase > 0; }

    // Whether the live source signal has gone stale: no WS event (block, reorg, or
    // the periodic status heartbeat) within CLIENT_SOURCE_STALE_MS. When true,
    // lastKnownServerBlock can no longer be trusted as the current source tip, so a
    // lag_blocks of 0 may be hiding a silently dropped WebSocket. Returns null (not
    // false) before the first event is seen, where staleness is genuinely unknown.
    isSourceHeightStale(){
        if(this._lastWsEventAt === null) return null;
        return (Date.now() - this._lastWsEventAt) > this.config['CLIENT_SOURCE_STALE_MS'];
    }

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

    // Clear the truncation join floor, in-memory and durable. Called after a
    // successful full-history snapshot apply, which restores complete state and
    // makes any prior floor stale. Fail-soft on db instances without the durable
    // store (mirrors _persistBootstrapBase): the in-memory reset always happens.
    async _clearBootstrapBase(){
        this._bootstrapBase = null;
        if(!this.db || typeof this.db.deleteSyncState !== 'function') return;
        await this.db.deleteSyncState(this._bootstrapBaseKey());
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
                    event.block_index, (delay === undefined) ? null : delay, gasTickSymbol(), this.network, this.coinTicker);
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
            // surfaces as a state_root mismatch. A null _lastComputedRoots (block before
            // the flag-day per the replica's OWN map, or a skipped/duplicate apply already
            // verified when first applied) is skipped, never a divergence; a NULL
            // event.balances_root while _lastComputedRoots is set is the opposite, a
            // withheld commitment, and halts (see the fail-closed block below).
            // Skip on a truncated replica: buildFullBalancesRoot sums the full credits/debits
            // history, which a truncated base does not retain, so its recomputed root cannot
            // match the source's full-history root and would false-halt every block. The
            // truncation state is surfaced on /status (SyncService truncated flag) so operators
            // can see the replica is not running the apply-time commitment check.
            if(this.dbType === 'indexer' && this.config['VERIFY_STATE_COMMITMENT'] !== false
                    && !this.isTruncated()
                    && this.applier._lastComputedRoots){
                let computed   = this.applier._lastComputedRoots;
                // Fail closed on WITHHELD roots (uuid:4b95ddef). Reaching here means the
                // replica's OWN bundled flag-day map says the commitment is live at this
                // height: ClientApplier.applyBlock sets _lastComputedRoots solely under
                // isStateCommitmentActive and clears it at entry, so it is never stale.
                // state_tree_roots.balances_root and .block_merkle_root are NOT NULL
                // columns, so a null on the wire means the source served no roots row at
                // an ACTIVE height. Reading that as "nothing to check" let a failed,
                // re-seeded, or hostile source skip the one control that writes a
                // sync_halt marker. Same posture as checkpoint.js commitmentMissing().
                // state_root stays exempt: ServerPoller deliberately NULLs it for
                // catch-up-burst blocks (viewTip > B), where the follower would otherwise
                // recompute it over tip-state stakes and halt on a value the source never
                // committed at B. VERIFY_STATE_COMMITMENT=false is the operator override.
                let missing = [];
                if(event.balances_root == null)
                    missing.push({ field: 'balances_root', a: null, b: computed.balances_root });
                if(event.block_merkle_root == null)
                    missing.push({ field: 'block_merkle_root', a: null, b: computed.block_merkle_root });
                if(missing.length){
                    await this._haltOnDivergence(event.block_index, missing,
                        this.sources.slice(0, 1), 'state-commitment-missing');
                    return; // halted: do not advance lastAppliedBlock
                }
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

            // Clean up old pending hashes and any armed fallback timers for
            // blocks we have now applied (or that are older than our applied tip).
            for(let [key] of this.pendingHashes){
                if(key <= this.lastAppliedBlock)
                    this.pendingHashes.delete(key);
            }
            for(let [key, timer] of this._applyTimers){
                if(key <= this.lastAppliedBlock){
                    clearTimeout(timer);
                    this._applyTimers.delete(key);
                }
            }

            // SPV checkpoint-quorum anchor (opt-in, throttled): confirm the replica's
            // OWN recomputed state_root matches a federation-quorum-signed checkpoint,
            // verified against an out-of-band pinned set. Transport errors never halt;
            // only a genuine quorum / state_root divergence does.
            if(this.dbType === 'indexer' && this.config['VERIFY_CHECKPOINT_QUORUM']
                    && (this.lastAppliedBlock - (this._lastCheckpointVerifyBlock || 0)) >= this.config['CHECKPOINT_VERIFY_INTERVAL']){
                this._lastCheckpointVerifyBlock = this.lastAppliedBlock;
                await this._verifyCheckpointQuorum();
            }
        } catch(e){
            console.error('Error applying block %s:', event.block_index, e);
            // Heal a schema gap but don't re-apply the block inline: the
            // skipped block leaves a gap that the next status event's gap
            // detection closes via incremental catch-up, post-heal.
            await this._healSchemaIfStale(e);
        }
    }

    // SPV: fetch the source's latest signed checkpoint, verify its quorum against the
    // OUT-OF-BAND pinned validator set, and assert its committed state_root equals the
    // replica's OWN recomputed state_tree_roots row at that height. This anchors the
    // replica to the federation quorum: a single lying source cannot forge a quorum of
    // signatures, so it cannot make a fabricated state_root pass. Skips silently (INERT)
    // when there is no pinned set, no checkpoint yet, or the replica has not reached the
    // checkpoint height. NEVER halts on a transport error (404 / network), only on a
    // real quorum failure or state_root divergence.
    async _verifyCheckpointQuorum(){
        if(this._halted) return;
        let validators = getPinnedValidators(this.chain, this.network);
        if(!validators || !validators.length) return;            // inert: no out-of-band trust root
        // Fetch the anchor out-of-band when configured (hub/federation), NOT from the very
        // source we audit, so a single source cannot withhold the checkpoints that would
        // catch a forged tail. Falls back to sources[0] when no out-of-band URL is set.
        let source = this.config['CHECKPOINT_ANCHOR_URL'] || this.sources[0];
        if(!source) return;
        let cp;
        try {
            let url = source + '/checkpoint/indexer/' + this.chain + '/' + this.network + '/latest';
            let resp = await axios.get(url, { timeout: 10000 });
            cp = resp && resp.data;
        } catch(e){
            // Transport fault / 404 is not proof of divergence, so it never halts. But it
            // is no longer SWALLOWED: a source that withholds the anchor must be visible,
            // otherwise a forged tail past the last served checkpoint goes unanchored.
            console.warn('Checkpoint-quorum anchor: failed to fetch checkpoint for ' + this.chain + '/' +
                this.network + ' from ' + source + ' (' + e.message + '); anchor not refreshed this cycle');
            return;
        }
        if(!cp) return;
        // A ROOTLESS checkpoint. Below the commitment flag-day that is normal and there
        // is nothing to anchor. At or above it the federation never signs one, so a
        // rootless row served there is stale, forged, or a source withholding the
        // anchorable material (uuid:c9dfc3d9). Deciding that on the WIRE's missing
        // state_root alone also returned before the seq-regression and freshness guards
        // below, leaving no trace at all. Decide it on the replica's OWN bundled
        // flag-day map instead, via the same predicate checkpoint.verifyCheckpoint
        // fail-closes on, and make the active-height case visible. Still a return and
        // never a halt: absence is not proof of forgery, matching the freshness guard's
        // documented advisory stance a few lines down.
        if(checkpointVerifier.commitmentMissing(cp)){
            console.warn('Checkpoint-quorum anchor: source served a ROOTLESS checkpoint for ' +
                this.chain + '/' + this.network + ' at block ' + cp.block_index +
                ' (seq ' + cp.checkpoint_seq + ', snapshot_block ' + cp.snapshot_block +
                '), at/above the checkpoint-commitment flag-day where the federation never signs one; ' +
                'source may be withholding anchorable checkpoints, not anchoring this cycle');
            return;
        }
        if(cp.state_root == null) return;                        // pre-commitment: nothing to anchor
        if(typeof cp.block_index !== 'number') return;           // malformed: no height to anchor
        // Reject a checkpoint_seq regression: a genuine federation sequence only advances,
        // so a lower seq than one already anchored means the source rewound (withholding
        // newer checkpoints). Do not anchor it; surface the rewind.
        if(this._lastVerifiedCheckpointSeq !== null && typeof cp.checkpoint_seq === 'number'
                && cp.checkpoint_seq < this._lastVerifiedCheckpointSeq){
            console.warn('Checkpoint-quorum anchor: seq regression for ' + this.chain + '/' + this.network +
                ' (served seq ' + cp.checkpoint_seq + ' < last verified ' + this._lastVerifiedCheckpointSeq +
                '); source may be withholding newer checkpoints, not anchoring');
            return;
        }
        // Freshness: if the newest quorum checkpoint trails the tip by more than the bound,
        // the anchor cannot catch a forged tail near the tip. Advisory by default (never a
        // halt): withholding is not proof of forgery and halting on absence is a DoS vector.
        // CHECKPOINT_FRESHNESS_STRICT promotes it to enforced: a replica that has
        // opted in refuses to serve the unanchored tail and HALTs. Only enforced once at
        // least one checkpoint has been verified (the federation is demonstrably live), so a
        // replica that has never anchored is not halted at startup.
        if(this.lastAppliedBlock - cp.block_index > this.config['CHECKPOINT_FRESHNESS_BLOCKS']){
            console.warn('Checkpoint-quorum anchor: stale anchor for ' + this.chain + '/' + this.network +
                ' (latest checkpoint at ' + cp.block_index + ', replica tip ' + this.lastAppliedBlock +
                ', >' + this.config['CHECKPOINT_FRESHNESS_BLOCKS'] + ' blocks behind); tail past it is unanchored');
            if(this.config['CHECKPOINT_FRESHNESS_STRICT'] && this._lastVerifiedCheckpointSeq !== null){
                await this._haltOnDivergence(cp.block_index,
                    [{ field: 'checkpoint_freshness', a: 'tip ' + this.lastAppliedBlock,
                       b: 'newest anchor ' + cp.block_index + ' (>' + this.config['CHECKPOINT_FRESHNESS_BLOCKS'] + ' behind)' }],
                    this.sources.slice(0, 1), 'checkpoint-freshness-stale');
                return;
            }
        }
        if(cp.block_index > this.lastAppliedBlock) return;       // not caught up to this checkpoint yet

        // 1. The checkpoint must meet the federation quorum. Try the PINNED launch set
        //    first (the launch epoch). If it no longer signs (the federation rotated
        //    its keys), roll the trust root FORWARD from the pinned SEED checkpoint over
        //    BTC's stakes_root (spec §7.3) IF a seed is configured; otherwise preserve
        //    the original behavior (a quorum failure is a divergence).
        let q = checkpointVerifier.verifyCheckpoint(cp, validators);
        if(!q.valid){
            let seed = getPinnedCheckpoint(this.chain, this.network);
            if(seed){
                let r = await this._followCheckpointForward(cp, seed);
                if(r.verdict === 'ok'){
                    this._recordVerifiedCheckpointSeq(cp.checkpoint_seq);
                    console.log('Checkpoint-quorum anchor OK (rotation-followed): ' + this.chain + '/' +
                        this.network + ' block ' + cp.block_index + ' (seq ' + cp.checkpoint_seq + ')');
                    return;
                }
                if(r.verdict === 'divergence'){
                    await this._haltOnDivergence(cp.block_index, r.mismatches,
                        this.sources.slice(0, 1), 'checkpoint-quorum-divergence');
                    return;
                }
                return;                                          // 'wait': cannot anchor across rotation yet
            }
            await this._haltOnDivergence(cp.block_index,
                [{ field: 'checkpoint_quorum', a: 'quorum-signed', b: 'INVALID under pinned set' }],
                this.sources.slice(0, 1), 'checkpoint-quorum-divergence');
            return;
        }
        // 2. Its committed roots must equal the replica's OWN recomputed roots at that height.
        let cmp = await this._checkpointRootsMatchLocal(cp);
        if(cmp.status === 'missing') return;                     // height not recomputed here (truncated bootstrap)
        if(cmp.status === 'mismatch'){
            await this._haltOnDivergence(cp.block_index, cmp.mismatches,
                this.sources.slice(0, 1), 'checkpoint-quorum-divergence');
            return;
        }
        this._recordVerifiedCheckpointSeq(cp.checkpoint_seq);
        console.log('Checkpoint-quorum anchor OK: ' + this.chain + '/' + this.network +
            ' block ' + cp.block_index + ' (seq ' + cp.checkpoint_seq + ', ' + q.validSigs +
            ' valid sigs, weighted=' + q.weighted + ')');
    }

    // Advance the high-water mark of verified checkpoint sequences. Monotonic: a later
    // verify never lowers it, so a subsequent regressed seq is rejected by the anchor.
    _recordVerifiedCheckpointSeq(seq){
        if(typeof seq !== 'number') return;
        if(this._lastVerifiedCheckpointSeq === null || seq > this._lastVerifiedCheckpointSeq)
            this._lastVerifiedCheckpointSeq = seq;
    }

    // Compare a checkpoint's committed roots to the replica's OWN recomputed
    // state_tree_roots row. Returns { status: 'match'|'mismatch'|'missing', mismatches }.
    async _checkpointRootsMatchLocal(c){
        let rows = await this.db.doQuery(
            'SELECT state_root, block_merkle_root FROM state_tree_roots WHERE block_index=? LIMIT 1', [c.block_index]);
        if(!rows || !rows.length) return { status: 'missing', mismatches: [] };
        let local = rows[0], mism = [];
        if(String(local.state_root).toLowerCase() !== String(c.state_root).toLowerCase())
            mism.push({ field: 'state_root', a: c.state_root, b: local.state_root });
        if(c.block_merkle_root != null && local.block_merkle_root != null
                && String(local.block_merkle_root).toLowerCase() !== String(c.block_merkle_root).toLowerCase())
            mism.push({ field: 'block_merkle_root', a: c.block_merkle_root, b: local.block_merkle_root });
        return { status: mism.length ? 'mismatch' : 'match', mismatches: mism };
    }

    // The oracle_publish validator set [{pubkey, weight, source}] at a BTC snapshot
    // height, computed from the replica's OWN staking tables. Uses the AS-OF variant
    // (getStakeWeightsByCapabilityAsOf), not the live getStakeWeightsByCapability:
    // a SLASH zeroes stakes.amount in place, so the live query run at the current tip
    // would understate the weight that stakes_root[snapshotBlock] committed in order
    // and could false-drop a source -> false-halt on a legitimate rotation. The as-of
    // variant adds back post-snapshot slash debits, reproducing the committed set so
    // it cannot drift from what stateCommitment.gatherStakeEntries committed at S.
    // checkpoint.verifyCheckpoint source-dedupes it for the quorum.
    async _oraclePublishSetAt(snapshotBlock){
        const caps = btcStakeCapabilities();
        const cap  = 'oracle_publish';
        const rows = await this.db.getStakeWeightsByCapabilityAsOf(cap, snapshotBlock, caps[cap], VALIDATOR_QUERY_LIMIT, this.chain, this.network);
        const set  = [], ZERO = M.canonicalAmount('0');
        for(const r of (rows || [])){
            if(!r || r.pubkey == null) continue;
            if(M.canonicalAmount(String(r.weight == null ? '0' : r.weight)) === ZERO) continue;   // zero cannot qualify
            set.push({ pubkey: String(r.pubkey), weight: String(r.weight), source: String(r.source) });
        }
        return set;
    }

    // Roll the pinned trust root FORWARD to `cp` across validator rotation (spec §7.3),
    // seeded by the out-of-band pinned checkpoint. BTC-only: the signer set for every
    // chain is the oracle_publish set in BTC's stakes_root (§4.1). Returns:
    //   { verdict: 'ok' }                 cp's quorum verified against the forward-
    //                                     followed authoritative set AND its committed
    //                                     roots equal the replica's recompute.
    //   { verdict: 'wait' }               inconclusive (transport / incomplete range /
    //                                     a step whose signer set is not yet attested /
    //                                     a height not yet recomputed here). No halt.
    //   { verdict: 'divergence', mismatches }
    //                                     a checkpoint failed quorum under an
    //                                     AUTHORITATIVE set, or its roots disagree with
    //                                     the recompute. Caller halts.
    // Each step's signer set is computed from the replica's own staking tables at the
    // step's snapshot_block, trusted only once that height is attested (covered by an
    // already-adopted checkpoint whose committed state_root == the recompute). Trust
    // flows forward from the pinned seed; the set that signs N+1 is the one committed
    // in the previous trusted checkpoint's (attested) state, never N+1's own.
    async _followCheckpointForward(cp, seed){
        if(this.chain !== 'BTC') return { verdict: 'wait' };     // stakes (signer sets) are BTC-only
        if(!seed || seed.state_root == null || typeof seed.block_index !== 'number') return { verdict: 'wait' };
        if(cp.block_index <= seed.block_index) return { verdict: 'wait' };

        // Bootstrap: the seed is the out-of-band trust root; the replica's own recompute
        // at seed.block_index must match it, else the replica is on a different chain.
        let seedCmp = await this._checkpointRootsMatchLocal(seed);
        if(seedCmp.status === 'missing') return { verdict: 'wait' };
        if(seedCmp.status === 'mismatch') return { verdict: 'divergence', mismatches: seedCmp.mismatches };

        let trusted = seed, from = seed.block_index + 1, guard = 0;
        while(trusted.block_index < cp.block_index){
            if(++guard > 10000) return { verdict: 'wait' };      // runaway guard
            let chain;
            try {
                let url = this.sources[0] + '/checkpoint/indexer/' + this.chain + '/' + this.network +
                          '/range?from=' + from + '&to=' + cp.block_index;
                let resp = await axios.get(url, { timeout: 10000 });
                chain = resp && resp.data && resp.data.checkpoints;
            } catch(e){ return { verdict: 'wait' }; }            // transport: not a divergence
            if(!Array.isArray(chain) || !chain.length) return { verdict: 'wait' };   // cannot reach cp

            let advanced = false;
            for(let next of chain){
                if(typeof next.block_index !== 'number' || next.block_index <= trusted.block_index) continue;
                if(next.block_index > this.lastAppliedBlock) return { verdict: 'wait' };   // not recomputed here yet
                if(next.state_root == null) return { verdict: 'wait' };                    // pre-commitment in range
                // The set that signs `next` is the oracle_publish set at next.snapshot_block;
                // trust it only once that height is attested by the current trust root.
                if(typeof next.snapshot_block !== 'number' || next.snapshot_block > trusted.block_index)
                    return { verdict: 'wait' };
                let vset = await this._oraclePublishSetAt(next.snapshot_block);
                if(!checkpointVerifier.verifyCheckpoint(next, vset).valid)
                    return { verdict: 'divergence', mismatches: [{ field: 'checkpoint_quorum',
                        a: 'quorum-signed (federation)',
                        b: 'INVALID at block ' + next.block_index + ' under the authoritative oracle_publish set at snapshot ' + next.snapshot_block }] };
                // Attest `next` so its rows extend the trusted frontier for the next step.
                let cmp = await this._checkpointRootsMatchLocal(next);
                if(cmp.status === 'missing') return { verdict: 'wait' };
                if(cmp.status === 'mismatch') return { verdict: 'divergence', mismatches: cmp.mismatches };
                trusted = next; from = next.block_index + 1; advanced = true;
                if(trusted.block_index >= cp.block_index) break;
            }
            if(!advanced) return { verdict: 'wait' };            // range had nothing usable past `trusted`
        }

        if(trusted.block_index === cp.block_index
                && String(trusted.state_root).toLowerCase() === String(cp.state_root).toLowerCase())
            return { verdict: 'ok' };
        return { verdict: 'wait' };
    }

    async _handleReorg(event){
        console.log('Reorg event received for ' + this.chain + '/' + this.network + ' at block ' + event.block_index);

        // Ignore a reorg while the replica has NO committed tip. A reorg presupposes
        // blocks to invalidate; a null tip means getLastBlock was null at start and the
        // DB holds no data, so there is nothing to roll back - and setting
        // lastAppliedBlock = block_index - 1 from purely server-supplied data would
        // inflate the in-memory tip past an empty DB, wedging the replica exactly like
        // the above-tip case below (_handleBlock then drops every canonical block
        // <= the inflated tip, halted:false). This is defense in depth: the live path
        // cannot reach here with a null tip (start() throws "Refusing to enter
        // live-follow" before _connectWebSockets if the replica is still empty after
        // bootstrap, and lastAppliedBlock is never reset to null once set), but guarding
        // here hardens against a future change that opens the WS earlier.
        if(this.lastAppliedBlock === null){
            console.warn('Ignoring reorg for ' + this.chain + '/' + this.network +
                ': no committed tip yet (replica empty); a reorg has nothing to roll back');
            return;
        }

        // Ignore a reorg that targets a block ABOVE our current tip. A reorg can only
        // invalidate a block we already hold; a target > lastAppliedBlock is either a
        // bogus/hostile event or one for data we have not reached. The depth guard below
        // computes depth = lastAppliedBlock - block_index + 1, which is <= 0 for an
        // above-tip target and so never trips MAX_ROLLBACK_DEPTH; rollback() would then
        // be a no-op and the cursor advance at the end of this method would push
        // lastAppliedBlock PAST the data actually in the DB. Every canonical block the
        // source then streams is <= the inflated tip and silently dropped by
        // _handleBlock, wedging the replica (halted:false) until an operator restarts it.
        // A reorg to an unapplied block is a no-op, never a cursor advance. (Guarded only
        // when we have a tip; a null tip is a distinct early-sync state the rollback path
        // handles on its own.)
        if(this.lastAppliedBlock !== null && event.block_index > this.lastAppliedBlock){
            console.warn('Ignoring reorg for ' + this.chain + '/' + this.network +
                ': target block ' + event.block_index + ' is above the replica tip (' +
                this.lastAppliedBlock + '); a reorg to an unapplied block is a no-op');
            return;
        }

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
            // FAIL CLOSED, consistent with the MAX_ROLLBACK_DEPTH branch above. A failed
            // rollback (lock timeout, deadlock, connection drop mid-rewind) otherwise leaves
            // lastAppliedBlock at the now-orphaned tip: _handleBlock's `blockIndex <=
            // lastAppliedBlock` guard then silently drops every canonical block the source
            // re-streams from event.block_index upward, so the replica serves the orphaned
            // fork with halted:false on /status (the decoder track has no recompute net to
            // self-halt). Record a durable halt via the same contract used for consensus
            // divergence and let the operator investigate/clear, rather than wedging silently.
            console.error('Reorg rollback failed for %s/%s (%s) rewinding to block %s:',
                this.chain, this.network, this.dbType, event.block_index, e);
            await this._haltOnDivergence(event.block_index,
                [{ field: 'reorg_rollback_failed', error: String(e && e.message ? e.message : e) }],
                this.sources.slice(0, 1), 'reorg-rollback-failed');
            return; // halted: do not leave lastAppliedBlock advanced onto the orphaned fork
        }
    }
}

module.exports = ClientSync;
// Exposed for the WS-chain escalation tests and any caller that needs to
// distinguish permanent bootstrap exhaustion from transient sync errors.
module.exports.BootstrapExhaustedError = BootstrapExhaustedError;
