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
const fs          = require('fs');
const path        = require('path');
const validation  = require('../util/validation');
const trainActivation = require('../consensus/gates/train_gate');
const BlockHasher = require('./block_hasher');
const RollbackGuard = require('./rollback_guard');
const { decoderLinkBroken, decoderLinkState } = require('./decoder_link');
const { classDigests } = require('./state_hash_classes');
const replicatedTables = require('../schema/replicated_tables');
const tableLifecycle = require('../table_lifecycle');
const { SCHEMA_VERSION } = require('../schema/version');
const { activationDelayBlocks, gasTickSymbol, coinTicker, btcStakeCapabilities, VALIDATOR_QUERY_LIMIT } = require('../consensus-constants');
const { bootstrapDepthKey } = require('../config');
const checkpointVerifier = require('../checkpoint');
const M = require('../merkle');
const { getPinnedValidators, getPinnedCheckpoint } = require('./pinned_validators');
// Resolved at each call site rather than bound once: the shim is installed by the
// entry file after this module is required, and getLogger() hands back a lazy
// façade that reaches the real sink once that has happened.
const { getLogger } = require('../observability');
const util = require('node:util');
const envConfig = require('../config');

// Tables whose row counts cannot converge between source and replica, and so are
// never a completeness signal. See the exclusion in verifyTableCounts for the
// mechanism. Declared once in replicated_tables.js, which the content-parity plan
// also reads, so the count and content checks cannot disagree about a table.
const OPERATIONAL_LOG_TABLES = new Set(replicatedTables.OPERATIONAL_LOG_TABLES);
const REISSUING_LOOKUPS = new Set(['index_addresses', 'index_tickers']);
// Lock-wait timeouts on schema apply retry with exponential backoff before the
// table counts as a persistent failure.
const SCHEMA_TRANSIENT_ERRNO = 1205;  // ER_LOCK_WAIT_TIMEOUT
const SCHEMA_TRANSIENT_MAX_RETRIES = 3;
const SCHEMA_TRANSIENT_BASE_MS = 2000;

// Permanent bootstrap exhaustion. start()-time throws already unwind to
// SyncService's sync.start().catch(... process.exit(1)) restart contract on their
// own, but the same exhaustion is also reachable MID-STREAM (the size-cap fallback
// in runIncrementalCatchUp re-runs bootstrapFromSnapshot from live WS handling),
// where the serialized WS event chain's catch would otherwise swallow it and leave
// the process alive but permanently stalled (running=true, tip never advances, no
// supervisor restart). A dedicated error type lets that catch distinguish the
// unrecoverable case (handleWsChainError) and escalate it to the same exit.
class BootstrapExhaustedError extends Error {}

// Is the decoder `dispensers` table due a wall-clock reconcile? The one term of the
// reconcile decision that carries no cycle-counter side effect, so the recurring status
// tick can sample it without corrupting the every-Nth catch-up cadence. Measured from the
// later of the last success and `since` (the tick's last attempt, else when live-follow
// began), so a snapshot-bootstrapped replica that never reconciled is still bounded and a
// failing re-dump retries once per interval, not once per tick. No time known: not due.
function dispenserIntervalDue(config, lastReconcileAt, nowMs, since){
    let maxIntervalMs = parseInt(config['DISPENSERS_RECONCILE_MAX_INTERVAL_MS'], 10);
    if(isNaN(maxIntervalMs) || maxIntervalMs < 0) maxIntervalMs = 1800000;
    if(maxIntervalMs === 0) return false;            // explicitly disabled
    let from = latestTime(lastReconcileAt, since);
    if(from == null) return false;
    return (nowMs - from) >= maxIntervalMs;
}

// Return the later of two optional epoch-ms times, or null when neither is set.
function latestTime(a, b){
    if(a == null) return (b == null) ? null : b;
    if(b == null) return a;
    return Math.max(a, b);
}

function blockHashFields(row){
    return {
        ledger_hash: row.ledger_hash,
        actions_hash: row.actions_hash,
        contract_hash: row.contract_hash
    };
}

// Compare hashes only at equal heights so tip skew cannot masquerade as divergence.
function compareSourceBlockHashes(client, source, blockHeight, remoteStatus, localHashes){
    if(remoteStatus.block_height != null && Number(remoteStatus.block_height) !== blockHeight){
        getLogger().warn('Skipping cross-source hash check: tip skew (local height ' + blockHeight +
            ', source ' + source + ' height ' + remoteStatus.block_height + ')');
        return { verdict: 'skew', mismatches: null };
    }
    let result = client.hashVerifier.compareBlockHashes(blockHeight,
        blockHashFields(localHashes), blockHashFields(remoteStatus));
    if(result.match){
        getLogger().info('Hash verification passed against ' + source);
        return { verdict: 'agree', mismatches: null };
    }
    getLogger().error('HASH MISMATCH at block ' + blockHeight + ' against ' + source);
    getLogger().error(util.format('Mismatches:', JSON.stringify(result.mismatches)));
    return { verdict: 'diverge', mismatches: result.mismatches };
}

// Report completeness gaps without changing the hash verdict or halting the replica.
function reportTableCountResults(source, blockHeight, remoteStatus, countMismatches){
    let shortfalls = countMismatches.filter(m => m.reason !== 'replica-ahead');
    let ahead = countMismatches.filter(m => m.reason === 'replica-ahead');
    if(shortfalls.length){
        getLogger().error('TABLE_COUNT_MISMATCH at block ' + blockHeight + ' against ' + source +
            '; follower may be missing replicated rows:');
        getLogger().error(JSON.stringify(shortfalls));
    }
    if(ahead.length){
        getLogger().error('TABLE_COUNT_REPLICA_AHEAD at block ' + blockHeight + ' against ' + source +
            '; follower holds rows the source deleted (un-replicated forward DELETE?):');
        getLogger().error(JSON.stringify(ahead));
    }
    if(!countMismatches.length && remoteStatus.table_counts){
        getLogger().info('Table-count verification passed against ' + source);
    }
}

// Check map content only when both peers expose comparable same-height data.
function shouldCompareIndexMap(client, blockHeight, remoteStatus, countMismatches){
    return client.config['INDEX_MAP_PARITY_CHECK']
        && remoteStatus.index_map_checksum != null
        && Number(remoteStatus.block_height) === blockHeight
        && !countMismatches.some(m => m.table === 'index_addresses');
}

// Report advisory map divergence and tell the caller whether to record it.
function reportIndexMapResult(client, source, blockHeight, localChecksum, remoteChecksum){
    let result = client.hashVerifier.compareIndexMap(blockHeight, localChecksum, remoteChecksum);
    if(result.match){
        getLogger().info('Index-map parity passed against ' + source);
        return false;
    }
    getLogger().warn('INDEX_MAP_PARITY mismatch at block ' + blockHeight + ' against ' + source +
        ': local=' + localChecksum + ' source=' + remoteChecksum +
        ' (advisory, NOT halting; id->address map content diverged at equal row count)');
    return true;
}

function buildDivergenceHalt(blockIndex, mismatches, sources, reason){
    return {
        blockIndex, reason: reason || 'cross-source-divergence',
        mismatches: mismatches || [], sources: sources || [],
        at: new Date().toISOString()
    };
}

function logDivergenceReason(client, blockIndex, mismatches){
    if(client._halted.reason === 'local-recompute-divergence'){
        getLogger().error('block ' + blockIndex + ': local recompute diverged from committed hash. Replica');
        getLogger().error('integrity failure. HALTING (applying no further blocks). Operator must');
        getLogger().error('investigate replica state and clear before this validator can resume.');
    } else if(client._halted.reason === 'recompute-error'){
        getLogger().error('block ' + blockIndex + ': the bulk-range boundary recompute ERRORED after');
        getLogger().error('retries. This recompute is the only verification of the applied range');
        getLogger().error('(at the catch-up join it is what catches a reorg that crossed a');
        getLogger().error('disconnect), so the replica cannot prove its state. HALTING (applying');
        getLogger().error('no further blocks). Operator must fix the local fault (DB, schema) and');
        getLogger().error('clear before this validator can resume.');
    } else if(client._halted.reason === 'boundary-read-error'){
        getLogger().error('block ' + blockIndex + ': the committed boundary hash could not be READ after');
        getLogger().error('retries, so the bulk-range verification could not run at all. An unreadable');
        getLogger().error('hash is not an absent one: treating it as absent would skip the only check');
        getLogger().error('the applied range gets. HALTING (applying no further blocks). Operator must');
        getLogger().error('fix the local database fault and clear before this validator can resume.');
    } else if(client._halted.reason === 'max-rollback-depth-exceeded'){
        getLogger().error('block ' + blockIndex + ': reorg too deep to roll back safely (exceeds');
        getLogger().error('MAX_ROLLBACK_DEPTH). The replica is stranded on the orphaned fork and');
        getLogger().error('cannot rewind to the new canonical base. HALTING (applying no further');
        getLogger().error('blocks). Operator must investigate, resnapshot/rewind, and clear before');
        getLogger().error('this validator can resume.');
    } else if(client._halted.reason === 'checkpoint-quorum-divergence'){
        getLogger().error('block ' + blockIndex + ': the federation quorum-signed checkpoint does not');
        getLogger().error('match this replica (quorum failed under the pinned validator set, or its');
        getLogger().error('committed state_root disagrees with the replica\'s own recompute). The');
        getLogger().error('source served state the federation did not sign. HALTING (applying no');
        getLogger().error('further blocks). Operator must investigate and clear before resuming.');
    } else if(client._halted.reason === 'no-source-quorum'){
        getLogger().error('block ' + blockIndex + ': the active sources split with NO majority reaching');
        getLogger().error('SOURCE_QUORUM (' + client.effectiveQuorum() + ' of ' + client.activeSourceCount() +
            ' active). The replica cannot determine which chain is canonical, so it must not');
        getLogger().error('pick one. HALTING (applying no further blocks). Operator must investigate');
        getLogger().error('the contending sources and clear before this validator can resume.');
    } else if(client._halted.reason === 'checkpoint-freshness-stale'){
        getLogger().error('block ' + blockIndex + ': the newest federation quorum checkpoint trails the');
        getLogger().error('replica tip by more than CHECKPOINT_FRESHNESS_BLOCKS and CHECKPOINT_FRESHNESS_STRICT');
        getLogger().error('is on. The tail past the last anchor is unverifiable against the federation, so');
        getLogger().error('this replica refuses to serve it. HALTING (applying no further blocks). Operator');
        getLogger().error('must restore a fresh anchor (or clear strict mode) and clear before resuming.');
    } else if(client._halted.reason === 'train-activation'){
        let m = (mismatches && mismatches[0]) || {};
        getLogger().error('block ' + blockIndex + ': TRAIN ACTIVATION HALT. The signed release manifest requires');
        getLogger().error('rule set ' + m.required + ' from BTC height ' + m.at_height + ' on ' + m.network +
            ', which this build does');
        getLogger().error('not implement. Applying this block under the old rules would fork. HALTING');
        getLogger().error('(applying no further blocks). REQUIRED OPERATOR ACTION: update this node to the');
        getLogger().error('platform version that carries the rule set, then clear. Clearing without the');
        getLogger().error('update is not a supported path.');
        if(m.reason) getLogger().error(m.reason);
    } else {
        getLogger().error('block ' + blockIndex + ': sources disagree on the consensus hash. One is on a');
        getLogger().error('forked/Byzantine chain. HALTING (applying no further blocks). Operator must');
        getLogger().error('investigate and clear before this validator can resume.');
    }
}

function logDivergenceHalt(client, blockIndex, mismatches, sources){
    getLogger().error('================================================================');
    getLogger().error('CONSENSUS DIVERGENCE HALT: ' + client.chain + '/' + client.network + '/' + client.dbType);
    logDivergenceReason(client, blockIndex, mismatches);
    getLogger().error('mismatches: ' + JSON.stringify(mismatches));
    getLogger().error('sources: ' + JSON.stringify(sources));
    getLogger().error('================================================================');
}

function stopDivergenceApply(client){
    // Stop the live apply path; pending cross-source hashes are now moot.
    client.pendingHashes.clear();
    client._strictConfirmPending.clear();
    for(let [, timer] of client._applyTimers) clearTimeout(timer);
    client._applyTimers.clear();
}

// Keep the highest numeric height a server reported; null until one arrives.
function updateLastKnownServerBlock(client, blockIndex){
    if(typeof blockIndex === 'number' &&
       (client.lastKnownServerBlock === null || blockIndex > client.lastKnownServerBlock)){
        client.lastKnownServerBlock = blockIndex;
    }
}

// Log a proven gap and return its catch-up start, or null when the status proves none.
function statusGapStart(client, blockHeight){
    if(client.lastAppliedBlock === null || !(blockHeight > client.lastAppliedBlock + 1))
        return null;
    client.logGap('Block gap detected: local=' + client.lastAppliedBlock + ' remote=' + blockHeight);
    return client.lastAppliedBlock + 1;
}

// Decoder-only status-tick reconcile gate; needs an applied block and no reconcile in flight.
// A rollback that left dispensers stale makes it due at once, even with the interval at 0.
function shouldReconcileDispensersOnStatus(client){
    return client.dbType === 'decoder' && !client._halted && client.lastAppliedBlock !== null &&
        !client._dispenserReconcileInFlight &&
        (client._dispenserReconcileAfterReorg === true || client.dispenserReconcileIntervalDue(Date.now()));
}

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
        this.maxRollbackDepth = envConfig.resolveMaxRollbackDepth(
            this.chain, this.network, this.config['MAX_ROLLBACK_DEPTH'],
            this.config['MAX_ROLLBACK_DEPTH_EXPLICIT']);
        this.rollbackGuard = new RollbackGuard(this.maxRollbackDepth);
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
            getLogger().error('================================================================');
            getLogger().error('WARNING: VERIFY_RECOMPUTE is DISABLED for ' + this.chain + '/' +
                this.network + '/indexer. This mode is UNSAFE for consensus-relevant');
            getLogger().error('replicas: a reorg occurring while this client is disconnected or');
            getLogger().error('restarting will be stitched onto the orphaned tip UNVERIFIED and');
            getLogger().error('the replica will silently follow the forked chain. Use only for');
            getLogger().error('throwaway read-only mirrors whose state nothing downstream trusts.');
            getLogger().error('================================================================');
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
        this._syncMode = envConfig.envValueByName(modeKey) || this.config[modeKey] || 'full';
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

        // Truncated-replica join block. Set by bootstrapFromHeight when this chain
        // is seeded from a recent height (SYNC_BOOTSTRAP_DEPTH_*) rather than full
        // history. The join block has no in-replica predecessor, so its chained
        // previous_hash cannot be recomputed; verifyRecompute skips ONLY this block.
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
        // per chain, see config.resolveMaxRollbackDepth) is the deepest reorg the client will roll back, so a depth <=
        // MAX_ROLLBACK_DEPTH lets an in-window reorg request a rollback past the floor.
        // Clamp the effective depth up to MAX_ROLLBACK_DEPTH + 1 and warn loudly so a
        // misconfigured small depth can't quietly strand the replica. (depth 0 = full-
        // history replica, not truncated, so it is exempt.)
        if(this._truncatedDepth >= 1){
            let maxRollback = Number(this.maxRollbackDepth);
            if(!Number.isFinite(maxRollback) || maxRollback < 1) maxRollback = 100;
            if(this._truncatedDepth <= maxRollback){
                let clamped = maxRollback + 1;
                getLogger().warn('SYNC_BOOTSTRAP_DEPTH for ' + this.chain + '/' + this.network +
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

        // Upstream replication evidence, per CONNECTED source index. A server's status
        // event carries its own DB tip (source_block_height) and the verdict its
        // ServerPoller reached on its own database (replica_stale, replica_seconds_behind).
        // Discarding it left this follower publishing lag_blocks 0 against a server whose
        // SQL replica had stopped applying hours earlier: both of that server's heights
        // freeze together, so we catch up to the frozen tip while the heartbeats keep
        // source_height_stale false. Rewritten on EVERY status event, not only when the
        // height advances, because a stalled upstream is exactly the case where it never
        // advances again; dropped when the socket closes, so a disconnected source's last
        // verdict is never mistaken for current evidence.
        this._upstreamStatus = new Map();   // sourceIndex -> { sourceHeight, stale, secondsBehind }

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
        this.validatorId = envConfig.validatorIdFromEnv() || require('os').hostname() || 'unknown';

        // Divergence halt. Set (in-memory + durably in sync_halt) when a confirmed
        // cross-source consensus-hash divergence is detected. Once halted the client
        // applies NO further blocks and stays halted across restarts until an
        // operator clears it. null = healthy.
        this._halted = null; // { blockIndex, reason, mismatches, sources, at }

        // Platform-train activation verdict for the block (or snapshot tip) this
        // follower is about to apply (src/consensus/gates/train_gate.js), or null before the
        // first evaluation. `pending` means the signed release manifest names a rule
        // set this build does not implement and the boundary is still ahead, which
        // /status carries so the halt is announced before it fires; `halt` means the
        // boundary is reached and the follower recorded a durable train-activation
        // halt through the same sync_halt path as a divergence. The log tick keeps
        // the periodic pending/halt reminder from firing on every apply.
        this.trainActivation          = null;
        this._trainActivationLogTick  = 0;
        // The manifest's trainActivation block, resolved once and cached: undefined
        // until the first resolution, null when the manifest carries none (every MINOR
        // and PATCH train). A resolution fault is not cached so a manifest that becomes
        // readable later is picked up on the next apply.
        this._trainActivationRequired = undefined;

        // SMT roots computed for a block that is COMMITTED but not yet past the
        // post-commit verification gates, carried so a retry of that height still runs
        // the commitment comparison (see applyBlockEvent). null = nothing outstanding.
        this._unverifiedRoots = null; // { blockIndex, roots }

        // Throttle stamp for the periodic replica-completeness sweep against the
        // primary source (see maybeVerifyCompleteness). 0 = never swept, so the
        // first equal-height status tick of a process runs one baseline sweep.
        this._lastCompletenessSweepAt = 0;

        // Persistent replica-gap state (see trackReplicaGaps). Keyed by table name,
        // one entry per table this replica has been found short of the source at
        // EQUAL heights, carrying how long and across how many sweeps the shortfall
        // has survived. A shortfall seen on one sweep can still be a race (the local
        // count and the source's /status read are seconds apart); a shortfall that
        // survives consecutive equal-height sweeps is replicated rows this follower
        // will never receive on its own, which is what this state exists to escalate.
        this._replicaGaps = new Map();
        // Whether a non-empty gap set has been written to sync state, so the keys get
        // cleared exactly once on convergence instead of rewritten every sweep (and
        // so a stale "short N rows" value cannot outlive the gap it described).
        this._replicaGapRecorded = false;
        // Sweeps a table must stay short before the loud alert fires, and how often
        // that alert may repeat afterwards. Defaults: escalate on the second
        // consecutive sweep, then at most every 6 h. A gap that GROWS re-alerts
        // immediately regardless of the repeat window, because a widening gap is a
        // new fault rather than the known one.
        this._replicaGapAlertSweeps   = this.numericSetting('REPLICA_GAP_ALERT_SWEEPS', 2, 1);
        this._replicaGapAlertRepeatMs = this.numericSetting('REPLICA_GAP_ALERT_REPEAT_MS', 21600000, 0);

        // Validated table names returned by the primary source's /schema endpoint.
        // null means no source schema has been observed, so a missing-table verdict
        // would be unknown rather than empty.
        this._sourceTables = null;

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
    logGap(message, now){
        now = (typeof now === 'number') ? now : Date.now();
        if(this._gapLogLastAt && (now - this._gapLogLastAt) < this._gapLogIntervalMs){
            this._gapLogSuppressed++;
            return;
        }
        let suffix = this._gapLogSuppressed > 0
            ? ' (+' + this._gapLogSuppressed + ' similar in last ' +
              Math.round((now - this._gapLogLastAt) / 1000) + 's)'
            : '';
        getLogger().info(message + suffix);
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
    warnTrustPosture(){
        if(this.sources.length < 2){
            getLogger().warn(
                'SECURITY: ' + this.dbType + ' replica is running SINGLE-SOURCE (' +
                (this.sources[0] || '<none>') + '). Content integrity rests entirely on TLS trust ' +
                'of that one server. Cross-source divergence detection is INACTIVE, and the local ' +
                'recompute only checks rows against hashes published by the same server. Configure ' +
                '2+ independent SYNC_SOURCES for Byzantine integrity.'
            );
        }
        if(this.dbType === 'decoder'){
            getLogger().warn(
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
                getLogger().warn(
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
    // lag_blocks:0 while entire tables never arrive, `verifyTableCounts` cannot see
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
    async warnMissingTables(){
        try {
            let present = await this.db.listExistingTables();
            let missing = replicatedTables.missingReplicatedTables(
                present, this.dbType, this._sourceTables
            );
            this._missingTables = missing;
            if(missing && missing.length){
                getLogger().warn('MISSING_REPLICATED_TABLES: ' + this.chain + '/' + this.network + '/' +
                    this.dbType + ' replica schema is missing ' + missing.length +
                    ' source table(s) that this build replicates per block: ' + missing.join(', ') +
                    '. Rows for these tables are SKIPPED (errno 1146 is tolerated so a schema gap ' +
                    'cannot wedge the replica), so replication is partial while /status still ' +
                    'reports halted:false. Migrate this replica to the source schema; the same ' +
                    'list is published as /status missing_tables.');
            }
        } catch(e){
            this._missingTables = null;
            getLogger().error(util.format('Missing-table check failed for ' + this.chain + '/' + this.network + '/' +
                this.dbType + ' (advisory, continuing):', e.message));
        }
    }

    // Source-side per-block replicated tables absent from this replica's schema,
    // or null when the check has not run or either table listing is unknown.
    getMissingTables(){ return this._missingTables === undefined ? null : this._missingTables; }

    // Multi-source Byzantine quorum helpers.

    // Stable comparison key for a source's committed hash tuple.
    hashTupleKey(h){
        if(!h) return 'null';
        return String(h.ledger_hash) + '|' + String(h.actions_hash) + '|' + String(h.contract_hash) +
            '|' + String(h.state_hash == null ? null : h.state_hash);
    }

    // Sources still eligible to vote (configured minus evicted).
    activeSourceCount(){ return this.sources.length - this._evictedSources.size; }

    // Effective quorum, clamped to the number of active (non-evicted) sources so an
    // eviction lowers the denominator rather than making quorum permanently unreachable.
    effectiveQuorum(){ return Math.min(this.sourceQuorum, Math.max(1, this.activeSourceCount())); }

    // Record a divergence strike against a source for a block, prune the sliding
    // window, and evict once the threshold is reached (subject to the keep-quorum-
    // viable guard). Idempotent per (source, block).
    strikeSource(sourceIndex, blockIndex){
        if(this._evictedSources.has(sourceIndex)) return;
        let strikes = this._sourceStrikes.get(sourceIndex) || [];
        if(!strikes.length || strikes[strikes.length - 1] !== blockIndex) strikes.push(blockIndex);
        // Prune strikes older than the sliding window.
        let floor = blockIndex - this._sourceStrikeWindow;
        strikes = strikes.filter(b => b > floor);
        this._sourceStrikes.set(sourceIndex, strikes);
        getLogger().warn('SOURCE STRIKE: ' + (this.sources[sourceIndex] || ('#' + sourceIndex)) +
            ' dissented from the quorum majority at block ' + blockIndex + ' (' + strikes.length + '/' +
            this._sourceEvictThreshold + ' within ' + this._sourceStrikeWindow + ' blocks) for ' +
            this.chain + '/' + this.network + '/' + this.dbType);
        if(strikes.length >= this._sourceEvictThreshold) this.evictSource(sourceIndex);
    }

    // Evict a Byzantine-suspected source: remove it from the active quorum denominator,
    // close its WebSocket (reconnect is suppressed for evicted sources), and alert.
    // Never evicts below two active sources, or cross-source verification collapses to
    // a single-source posture.
    evictSource(sourceIndex){
        if(this._evictedSources.has(sourceIndex)) return;
        let label = this.sources[sourceIndex] || ('#' + sourceIndex);
        if(this.activeSourceCount() - 1 < 2){
            getLogger().error('SOURCE EVICTION SUPPRESSED: ' + label + ' reached the strike threshold but ' +
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
        getLogger().error('================================================================');
        getLogger().error('SOURCE EVICTED: ' + label + ' for ' + this.chain + '/' + this.network + '/' + this.dbType);
        getLogger().error('It reached ' + this._sourceEvictThreshold + ' divergence strikes within ' +
            this._sourceStrikeWindow + ' blocks (dissented from the quorum majority). Its WebSocket is');
        getLogger().error('closed and it is removed from the active quorum denominator (now ' +
            this.activeSourceCount() + ' active source(s)). A quorum still stands behind every applied');
        getLogger().error('block. Investigate the evicted source for a fork/Byzantine fault.');
        getLogger().error('================================================================');
    }

    // /status getters for the Byzantine quorum surface.
    getSourceQuorum(){ return this.effectiveQuorum(); }
    getConfiguredSourceCount(){ return this.sources.length; }
    getActiveSourceCount(){ return this.activeSourceCount(); }
    getEvictedSources(){ return [...this._evictedSources].map(i => this.sources[i] || ('#' + i)); }
    getSourcesAgreeing(){ return this._lastSourcesAgreeing; }

    installStartupHalt(prior){
        this._halted = {
            blockIndex: Number(prior.block_index), reason: prior.reason,
            mismatches: this.safeParse(prior.mismatches), sources: this.safeParse(prior.sources),
            at: prior.detected_at
        };
    }

    async waitForStartupHalt(){
        while(this.running && this._halted){ await this.util.sleep(5000); }
        return this._halted ? 'stop' : 'restart';
    }

    async retryStartupHaltRead(){
        let haltRetryDelay = 5000;
        while(this.running && this._halted && this._halted.reason === 'halt-state-check-failed'){
            await this.util.sleep(haltRetryDelay);
            if(!(this.running && this._halted && this._halted.reason === 'halt-state-check-failed')) break;
            try {
                const prior = await this.db.getActiveHalt(this.dbType);
                if(!prior){
                    getLogger().info('halt-state check recovered for ' + this.chain + '/' + this.network +
                        '/' + this.dbType + ': sync_halt holds no active halt; resuming replication');
                    this._halted = null;
                    break;
                }
                this.installStartupHalt(prior);
                getLogger().error('halt-state check recovered for ' + this.chain + '/' + this.network +
                    '/' + this.dbType + ': HALTED on a prior consensus divergence at block ' +
                    prior.block_index + '. Not resuming until cleared. Detected at ' + prior.detected_at + '.');
                break;
            } catch(e2){
                haltRetryDelay = Math.min(haltRetryDelay * 2, 60000);
            }
        }
    }

    async resolveStartupHalt(){
        // A persisted divergence halt keeps the client idle until an operator clears it.
        try {
            const prior = await this.db.getActiveHalt(this.dbType);
            if(!prior) return 'continue';
            this.installStartupHalt(prior);
            getLogger().error('ClientSync is HALTED on a prior consensus divergence at block ' +
                prior.block_index + ' (' + this.chain + '/' + this.network + '/' + this.dbType +
                '). Not resuming until cleared. Detected at ' + prior.detected_at + '.');
            return await this.waitForStartupHalt();
        } catch(e){
            // An uncertain halt-state read blocks replication until an authoritative read succeeds.
            getLogger().error(util.format('halt-state check failed; staying HALTED (fail-closed) until it can be read:', e));
            this._halted = {
                blockIndex: -1, reason: 'halt-state-check-failed',
                mismatches: [], sources: [], at: null
            };
            await this.retryStartupHaltRead();
            return this.waitForStartupHalt();
        }
    }

    async synchronizeStoredReplica(){
        this.lastAppliedBlock = await this.db.getLastBlock();
        if(this.lastAppliedBlock === null){
            // A configured truncation depth selects recent-height bootstrap.
            if(this._truncatedDepth >= 1){
                getLogger().info('No local data found; bootstrapping ' + this.chain + '/' + this.network +
                    ' from recent height (SYNC_BOOTSTRAP_DEPTH=' + this._truncatedDepth + ', truncated replica)...');
                await this.bootstrapFromHeightRetry(this._truncatedDepth);
            } else {
                getLogger().info('No local data found, bootstrapping from full snapshot...');
                await this.bootstrapFromSnapshot();
            }
            return;
        }
        // Resume applies the source schema before incremental rows reach the replica.
        await this.fetchAndApplySchema(this.sources[0]);
        getLogger().info('Resuming from block ' + this.lastAppliedBlock);
        await this.incrementalCatchUp(this.lastAppliedBlock + 1);
    }

    ensureLiveFollowTip(){
        if(this.lastAppliedBlock === null){
            throw new Error('Refusing to enter live-follow: replica still empty after bootstrap for ' +
                this.chain + '/' + this.network + '/' + this.dbType);
        }
    }

    async prepareLiveFollow(){
        // Missing-table warnings run after bootstrap or catch-up applies the source schema.
        await this.warnMissingTables();
        this.lastHashes = await this.db.getBlockHashRow(this.lastAppliedBlock);
    }

    beginLiveFollow(){
        // The dispensers wall clock begins when live-follow begins.
        if(this.dbType === 'decoder') this._dispenserClockArmedAt = Date.now();
        this.connectWebSockets();
    }

    async start(){
        this.running = true;
        getLogger().info('ClientSync starting for ' + this.chain + '/' + this.network + '/' + this.dbType);
        this.warnTrustPosture();

        const haltAction = await this.resolveStartupHalt();
        if(haltAction === 'restart') return this.start();
        if(haltAction === 'stop') return;

        // The persisted truncation floor loads before the resume branch and its depth guard.
        await this.loadBootstrapBase();
        await this.synchronizeStoredReplica();
        this.ensureLiveFollowTip();
        await this.prepareLiveFollow();
        this.beginLiveFollow();

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
    scheduleHeartbeat(){
        if(this._hbLastSentBlock === null ||
           (this.lastAppliedBlock - this._hbLastSentBlock) >= 10){
            this.flushHeartbeat();
        } else if(!this._hbTimer){
            this._hbTimer = setTimeout(() => {
                this._hbTimer = null;
                this.flushHeartbeat();
            }, 5000);
        }
    }

    // Send the current applied height to every open source connection. Best-effort:
    // a server running an older build simply ignores the message, and a failed send
    // is swallowed (the next heartbeat will carry the latest height anyway).
    // Also fires a REST POST /validator-heartbeat for named-validator tracking.
    flushHeartbeat(){
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
            this.sendRestHeartbeat(source).catch(() => {});
        }
    }

    // Credential for calls we make OUT to a source server, on every REST call and the
    // WebSocket handshake. SYNC_UPSTREAM_KEY, never SYNC_API_KEY: the latter guards
    // THIS process's own API, and using one value for both meant a host could not be
    // a guarded server and a client of a differently-keyed source at the same time.
    // Unset returns no header, which is what an unkeyed source expects, so nothing
    // changes on a fleet that has not armed its servers yet.
    upstreamHeaders(){
        let key = this.config['SYNC_UPSTREAM_KEY'];
        return key ? { Authorization: 'Bearer ' + key } : {};
    }

    // POST the current applied height to a source server's /validator-heartbeat endpoint.
    // Best-effort: errors are suppressed at the call site.
    async sendRestHeartbeat(source){
        let url = source + '/validator-heartbeat/' + this.dbType + '/' + this.chain + '/' + this.network;
        let body = {
            validator_id:       this.validatorId,
            applied_height:     this.lastAppliedBlock,
            applied_block_time: this.lastAppliedBlockTime
        };
        await axios.post(url, body, { timeout: 5000, headers: this.upstreamHeaders() });
    }

    async fetchAndApplySchema(source){
        getLogger().info('Fetching schema from ' + source + '...');
        let schema;
        try {
            let response = await this.fetchSchema(source);
            schema = response.data;
        } catch(e){
            // A fetch/transport failure is not a schema fault: the source may be
            // briefly unreachable. Leave it to the bootstrap retry/rotate loop.
            getLogger().error(util.format('Failed to fetch schema from ' + source + ':', e));
            return;
        }
        if(!schema || !schema.tables) return;

        let pending = this.collectSchemaTables(schema.tables);
        let lastErr = new Map();
        while(pending.length){
            let stillPending = [];
            let progressed = false;
            for(let { tableName, createSql } of pending){
                let attempt = 0;
                let succeeded = false;
                while(attempt <= SCHEMA_TRANSIENT_MAX_RETRIES){
                    try {
                        let exists = await this.db.findTableInSchema(tableName);
                        if(exists.length === 0){
                            await this.db.doQuery(createSql);
                            getLogger().info('  Created table: ' + tableName);
                        } else {
                            // Propagates columns added upstream since bootstrap, outside any snapshot transaction.
                            await this.db.addMissingColumns(tableName, createSql);
                        }
                        lastErr.delete(tableName);
                        succeeded = true;
                        break;
                    } catch(e){
                        if(e.errno !== SCHEMA_TRANSIENT_ERRNO || attempt >= SCHEMA_TRANSIENT_MAX_RETRIES){
                            lastErr.set(tableName, e);
                            break;
                        }
                        let delay = SCHEMA_TRANSIENT_BASE_MS * Math.pow(2, attempt);
                        this.logSchemaRetry(tableName, delay, attempt);
                        await this.util.sleep(delay);
                        attempt++;
                    }
                }
                if(succeeded) progressed = true;
                else stillPending.push({ tableName, createSql });
            }
            if(!progressed) break;
            pending = stillPending;
        }
        if(pending.length){
            await this.haltOnSchemaFailure(source, this.schemaFailureDetails(pending, lastErr));
            return;
        }
        getLogger().info('Schema applied from ' + source);
    }

    fetchSchema(source){
        let url = source + '/schema/' + this.dbType + '/' + this.chain + '/' + this.network;
        return axios.get(url, { headers: this.upstreamHeaders(), timeout: 30000 });
    }

    // Validates every name and DDL up front. The validated name set is kept for
    // missing-table checks because an older source legitimately omits newer tables.
    collectSchemaTables(tables){
        let sourceTables = new Set();
        let pending = [];
        for(let tableName in tables){
            let createSql = tables[tableName];
            let idCheck = validation.validateIdentifier(tableName);
            if(!idCheck.valid){
                getLogger().error('Rejected table name from schema: ' + tableName + ' (' + idCheck.reason + ')');
                continue;
            }
            sourceTables.add(tableName);
            if(!createSql) continue;
            let ddlCheck = validation.validateDdl(createSql);
            if(!ddlCheck.valid){
                getLogger().error('Rejected DDL for table ' + tableName + ': ' + ddlCheck.reason);
                continue;
            }
            pending.push({ tableName, createSql });
        }
        this._sourceTables = sourceTables;
        return pending;
    }

    logSchemaRetry(tableName, delay, attempt){
        getLogger().warn('Schema apply lock-timeout on ' + tableName +
            ' (errno 1205), retrying in ' + delay + 'ms (attempt ' +
            (attempt + 1) + '/' + SCHEMA_TRANSIENT_MAX_RETRIES + ')');
    }

    schemaFailureDetails(pending, lastErr){
        return pending.map(p => {
            let e = lastErr.get(p.tableName) || {};
            return { table: p.tableName, errno: e.errno || null, message: e.message || null };
        });
    }

    // Durable halt for an unrecoverable schema apply (distinct from the
    // consensus-divergence halt: same persistence + /status surface via
    // recordHalt/isHalted, but its own reason and messaging so operators are not
    // misled into chasing a forked chain). Reached only after the ordering
    // fixpoint AND after transient lock-timeouts have been retried with backoff,
    // so FK-ordering misses and brief ALTER contention never trigger it. Only
    // persistent faults (disk-full, permissions, malformed DDL, lock-timeout
    // that outlasts the retry cap) reach here.
    async haltOnSchemaFailure(source, failedTables){
        if(this._halted) return;
        let blockIndex = (this.lastAppliedBlock != null) ? this.lastAppliedBlock : 0;
        this._halted = {
            blockIndex, reason: 'schema-apply-failed',
            mismatches: failedTables || [], sources: [source],
            at: new Date().toISOString()
        };
        try { await this.db.recordHalt(this.dbType, blockIndex, this._halted.reason, failedTables, [source]); }
        catch(e){ getLogger().error(util.format('CRITICAL: failed to persist schema-apply halt (still halting in-memory):', e)); }
        getLogger().error('================================================================');
        getLogger().error('SCHEMA APPLY HALT: ' + this.chain + '/' + this.network + '/' + this.dbType);
        getLogger().error('after the multi-pass apply these tables still could not be created');
        getLogger().error('or altered (a genuine DDL fault, not FK ordering): ' + JSON.stringify(failedTables));
        getLogger().error('the replica cannot build a complete schema, so a snapshot apply would');
        getLogger().error('loop forever on errno 1146/1054. HALTING (applying no further blocks).');
        getLogger().error('Operator must fix the DDL fault (disk, permissions, lock, malformed');
        getLogger().error('DDL) and clear the halt before this replica can resume.');
        getLogger().error('================================================================');
        this.pendingHashes.clear();
        this._strictConfirmPending.clear();
        for(let [, timer] of this._applyTimers) clearTimeout(timer);
        this._applyTimers.clear();
    }

    // Table and key of an upsert full-dump duplicate-key failure (errno 1062), or null
    // for any other error. An upsert absorbs its own row's key collision, so a 1062 means
    // the UPDATE leg moved a key (markets.id) onto a value another replica row holds.
    upsertDuplicateKeyTarget(e){
        if(!e || e.errno !== 1062 || typeof e.upsertTable !== 'string') return null;
        let m = /Duplicate entry '(.*?)' for key '([^']*)'/.exec(String(e.sqlMessage || e.message || ''));
        return { table: e.upsertTable, entry: m ? m[1] : null, key: m ? m[2] : null };
    }

    // Halt when an upsert full-dump fails on the same table and key twice with no
    // successful apply between (id-space skew no retry can clear); a first hit retries.
    async noteUpsertDuplicateKey(source, e){
        let target = this.upsertDuplicateKeyTarget(e);
        if(!target) return false;
        let id = target.table + '|' + target.key + '|' + target.entry;
        let repeat = (this._upsertDupKeyTarget === id);
        this._upsertDupKeyTarget = id;
        if(!repeat){
            getLogger().warn('Upsert full-dump of ' + target.table + ' hit a duplicate key (' +
                target.entry + ' on ' + target.key + '); retrying once before halting');
            return false;
        }
        // Reset so a replica resumed after an operator clear gets its one retry again.
        this._upsertDupKeyTarget = null;
        await this.haltOnUpsertDuplicateKey(source, target);
        return true;
    }

    // Durable halt for a repeating upsert duplicate key: same recordHalt/isHalted
    // persistence and /status surface as the schema-apply halt, its own reason.
    async haltOnUpsertDuplicateKey(source, target){
        if(this._halted) return;
        let blockIndex = (this.lastAppliedBlock != null) ? this.lastAppliedBlock : 0;
        let detail = [target];
        this._halted = {
            blockIndex, reason: 'apply-duplicate-key',
            mismatches: detail, sources: [source],
            at: new Date().toISOString()
        };
        try { await this.db.recordHalt(this.dbType, blockIndex, this._halted.reason, detail, [source]); }
        catch(e){ getLogger().error(util.format('CRITICAL: failed to persist duplicate-key halt (still halting in-memory):', e)); }
        getLogger().error('================================================================');
        getLogger().error('UPSERT DUPLICATE KEY HALT: ' + this.chain + '/' + this.network + '/' + this.dbType);
        getLogger().error('the ' + target.table + ' full-dump from ' + source + ' failed twice on duplicate entry ' +
            target.entry + ' for key ' + target.key + ': a replica row already holds a key the');
        getLogger().error('source assigns to a different row (id-space skew from a source re-index, a');
        getLogger().error('cross-source bootstrap, or a row the source deleted and this replica kept).');
        getLogger().error('Retrying the same payload cannot clear it. HALTING (applying no further blocks).');
        getLogger().error('Operator must re-bootstrap this replica or reconcile the colliding row, then clear the halt.');
        getLogger().error('================================================================');
        this.pendingHashes.clear();
        this._strictConfirmPending.clear();
        for(let [, timer] of this._applyTimers) clearTimeout(timer);
        this._applyTimers.clear();
    }

    // True when a snapshot download aborted because the body outgrew the axios
    // ceiling (SNAPSHOT_MAX_CONTENT). One definition for both the incremental
    // fallback and the bootstrap halt, so the two can never disagree on what the
    // size wall looks like.
    isContentLengthOverflow(e){
        if(!e) return false;
        return e.code === 'ERR_FR_MAX_CONTENT_LENGTH_EXCEEDED' ||
               !!(e.message && e.message.includes('maxContentLength'));
    }

    // Seconds the source says to wait after a 429, or null when the error is not
    // a rate-limit. Reads Retry-After first, then express-rate-limit's
    // RateLimit-Reset; returns 0 when neither header is present so the caller can
    // still report the 429 itself.
    rateLimitRetryAfterSeconds(e){
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
    async haltOnSnapshotTooLarge(source, cause){
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
        catch(e){ getLogger().error(util.format('CRITICAL: failed to persist snapshot-too-large halt (still halting in-memory):', e)); }
        getLogger().error('================================================================');
        getLogger().error('SNAPSHOT TOO LARGE HALT: ' + this.chain + '/' + this.network + '/' + this.dbType);
        getLogger().error('the full-history snapshot from ' + source + ' exceeds SNAPSHOT_MAX_CONTENT (' +
            (this.config['SNAPSHOT_MAX_CONTENT'] || 'unset') + ' bytes).');
        getLogger().error('every source serves the same payload, so retrying, rotating sources, or');
        getLogger().error('restarting the process cannot get past this. HALTING (applying no further');
        getLogger().error('blocks) rather than crash-looping and exhausting the source snapshot budget.');
        getLogger().error('Operator must either reseed this replica as a truncated one');
        getLogger().error('(SYNC_BOOTSTRAP_DEPTH) or raise SNAPSHOT_MAX_CONTENT, then clear the halt.');
        getLogger().error('================================================================');
        this.pendingHashes.clear();
        this._strictConfirmPending.clear();
        for(let [, timer] of this._applyTimers) clearTimeout(timer);
        this._applyTimers.clear();
    }

    // A missing table (errno 1146) or missing column (1054) during an apply
    // means the source's schema moved ahead of this replica since the last
    // reconciliation, so schema fetch/apply cannot be bootstrap-only.
    // fetchAndApplySchema runs at four call sites: the full-snapshot
    // bootstrap (bootstrapRotateSources), bootstrap-from-height
    // (bootstrapFromHeight), resume (start()), and this apply-time heal
    // itself. A server-side table addition can still wedge an
    // already-bootstrapped, not-yet-resumed replica on the first snapshot
    // carrying rows for it until this heal runs. Re-apply the source schema
    // (it CREATEs missing tables and ALTERs in missing columns) so the next
    // apply attempt can proceed, debounced to one heal per minute
    // (this._lastSchemaHeal below) so a failure the schema can't fix (e.g.
    // rejected DDL) can't hammer the /schema endpoint.
    async healSchemaIfStale(e){
        let errno = e ? e.errno : null;
        if(errno !== 1146 && errno !== 1054) return false;
        let now = Date.now();
        if(this._lastSchemaHeal && (now - this._lastSchemaHeal) < 60000) return false;
        this._lastSchemaHeal = now;
        getLogger().info('Apply failed on a schema gap (errno ' + errno + ') for ' +
            this.chain + '/' + this.network + '; re-applying source schema');
        await this.fetchAndApplySchema(this.sources[0]);
        return true;
    }

    // Bootstrap from a full snapshot.
    //
    // Drives a bounded retry-with-backoff loop around bootstrapRotateSources (one
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
    async bootstrapFromSnapshot(){
        if(!this.sources[0]){
            getLogger().error('No sync sources configured');
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
            if(await this.bootstrapRotateSources()) return; // success: tip committed
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
            getLogger().warn('Bootstrap round ' + (round + 1) + ' exhausted all sources for ' +
                this.chain + '/' + this.network + '/' + this.dbType + '; retrying in ' + delay + 'ms');
            await this.util.sleep(delay);
        }
    }

    async bootstrapDownloadSnapshot(source){
        let url = source + '/snapshot/' + this.dbType + '/' + this.chain + '/' + this.network;
        let response = await axios.get(url, {
            headers: this.upstreamHeaders(),
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
                // Axios may already have decompressed the response.
            }
        }
        try {
            return JSON.parse(jsonStr.toString());
        } catch(parseErr){
            throw new Error('Snapshot download truncated or corrupt from ' + source +
                ' (JSON.parse failed; likely a network interruption mid-transfer): ' + parseErr.message);
        }
    }

    async bootstrapApplySnapshot(snapshotData){
        if(await this.checkTrainActivation(snapshotData.block_height)) return false;
        await this.withApplyLock(() => this.applier.applyFullSnapshot(snapshotData));
        this.lastAppliedBlock = snapshotData.block_height;
        await this.refreshTipHashes();
        await this.clearBootstrapBase();
        return true;
    }

    async bootstrapVerifyIndexerQuorum(){
        if(!this.config['VERIFY_HASHES']) return true;
        let need = Math.max(0, this.effectiveQuorum() - 1);
        let agreed = 0;
        for(let i = 1; i < this.sources.length && agreed < need; i++){
            let verdict = await this.verifyAgainstSource(this.sources[i], this.lastAppliedBlock);
            if(this._halted) return false;
            if(verdict === 'agree') agreed++;
        }
        if(agreed < need){
            getLogger().warn('SECURITY: bootstrap cross-check reached only ' + (agreed + 1) +
                ' agreeing source(s) of the ' + this.effectiveQuorum() + ' required for quorum for ' +
                this.chain + '/' + this.network + '/indexer; proceeding on reachable sources, but the ' +
                'bootstrap tip is under-verified until live quorum forms.');
        }
        return true;
    }

    async bootstrapVerifySnapshot(){
        if(this.sources.length <= 1) return true;
        if(this.dbType === 'indexer') return this.bootstrapVerifyIndexerQuorum();
        await this.verifyDecoderCompleteness(this.sources[1], this.lastAppliedBlock);
        return true;
    }

    async bootstrapHandleSourceFailure(source, attempt, e){
        if(this.isContentLengthOverflow(e)){
            await this.haltOnSnapshotTooLarge(source, e);
            return false;
        }
        let retryAfter = this.rateLimitRetryAfterSeconds(e);
        if(retryAfter !== null){
            getLogger().error('Bootstrap rate-limited (HTTP 429) by ' + source + ' for ' +
                this.chain + '/' + this.network + '/' + this.dbType +
                '; the source will not serve another full snapshot for ' + retryAfter + 's.');
        }
        getLogger().error(util.format('Bootstrap failed:', e));
        if(this.sources.length > 1 && attempt < this.sources.length - 1){
            getLogger().info('Trying secondary source...');
            this.sources.push(this.sources.shift());
            return this.bootstrapRotateSources(attempt + 1);
        }
        getLogger().error('All sync sources exhausted after ' + (attempt + 1) + ' attempt(s)');
        return false;
    }

    // Runs one bounded pass over the configured sources and rotates after failures.
    async bootstrapRotateSources(attempt){
        attempt = attempt || 0;
        let source = this.sources[0];
        if(!source) return false;

        await this.fetchAndApplySchema(source);
        if(this._halted) return false;

        getLogger().info('Downloading full snapshot from ' + source + '...');
        try {
            let snapshotData = await this.bootstrapDownloadSnapshot(source);
            if(!await this.bootstrapApplySnapshot(snapshotData)) return false;
            if(!await this.bootstrapVerifySnapshot()) return false;
            getLogger().info('Bootstrap complete at block ' + this.lastAppliedBlock);
            return true;
        } catch(e){
            return this.bootstrapHandleSourceFailure(source, attempt, e);
        }
    }

    // Bounded retry-with-backoff wrapper around bootstrapFromHeight, mirroring
    // bootstrapFromSnapshot's contract: a truncated bootstrap that keeps failing
    // must NEVER fall through to live-follow on an empty replica. Success returns;
    // exhaustion THROWS, propagating to start() and on to the supervisor for a
    // clean container restart. Reuses BOOTSTRAP_* tuning.
    async bootstrapFromHeightRetry(depth){
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
                if(await this.bootstrapFromHeight(depth)) return; // success: tip committed
            } catch(e){
                getLogger().error(util.format('Bootstrap-from-height round ' + (round + 1) + ' failed for ' +
                    this.chain + '/' + this.network + ':', e));
            }
            if(this._halted){
                // A halt recorded before the window landed (the platform-train gate).
                // Retrying re-fetches the same window into the same refusal, so stop
                // burning rounds: throw so start()/SyncService restarts and start()
                // lands in the durable idle-halted state until an operator clears it.
                throw new Error('Bootstrap-from-height aborted by ' + this._halted.reason + ' halt for ' +
                    this.chain + '/' + this.network + '/' + this.dbType + '; operator must clear the halt');
            }
            if(round >= maxRetries){
                throw new Error('Bootstrap-from-height failed: exhausted after ' + (round + 1) +
                    ' round(s) for ' + this.chain + '/' + this.network + '/' + this.dbType);
            }
            let delay = Math.min(maxMs, baseMs * Math.pow(2, round));
            getLogger().warn('Bootstrap-from-height round ' + (round + 1) + ' failed for ' +
                this.chain + '/' + this.network + '; retrying in ' + delay + 'ms');
            await this.util.sleep(delay);
        }
    }

    async discoverBootstrapRange(source, depth){
        const statusUrl = source + '/status/' + this.dbType + '/' + this.chain + '/' + this.network;
        const statusResp = await axios.get(statusUrl, { headers: this.upstreamHeaders(), timeout: 30000 });
        const status = statusResp.data || {};
        // The incremental snapshot comes from the DB tip, with the last broadcast
        // position as a fallback when the source does not expose source_height.
        const tip = (typeof status.source_height === 'number') ? status.source_height
                : (typeof status.block_height === 'number') ? status.block_height : null;
        if(typeof tip !== 'number'){
            throw new Error('Bootstrap-from-height: source tip unavailable from ' + statusUrl);
        }
        const base = Math.max(0, tip - depth);
        getLogger().info('Bootstrap-from-height: source tip=' + tip + ', depth=' + depth +
            ', base=' + base + ' for ' + this.chain + '/' + this.network);
        return { tip, base };
    }

    async fetchBootstrapWindow(source, base){
        const url = source + '/snapshot/' + this.dbType + '/' + this.chain + '/' + this.network + '/since/' + base + '?skip_lookups=1';
        const response = await axios.get(url, {
            headers: this.upstreamHeaders(),
            responseType: 'arraybuffer',
            timeout: 600000,
            decompress: true,
            maxContentLength: this.config['SNAPSHOT_MAX_CONTENT']
        });
        let jsonStr = response.data;
        if(Buffer.isBuffer(jsonStr)){
            try { jsonStr = zlib.gunzipSync(jsonStr); } catch(e){}
        }
        try {
            return JSON.parse(jsonStr.toString());
        } catch(parseErr){
            throw new Error('Snapshot download truncated or corrupt from ' + source +
                ' (JSON.parse failed; likely a network interruption mid-transfer): ' + parseErr.message);
        }
    }

    async applyBootstrapWindow(snapshotData, base, tip){
        const activationHeight = (typeof snapshotData.block_height === 'number')
            ? snapshotData.block_height : tip;
        if(await this.checkTrainActivation(activationHeight)) return false;

        // since_block is the exact bound served; base is the requested fallback.
        this._bootstrapBase = (typeof snapshotData.since_block === 'number') ? snapshotData.since_block : base;
        await this.persistBootstrapBase(this._bootstrapBase);
        await this.withApplyLock(() => this.applier.applyIncrementalSnapshot(snapshotData));
        if(typeof snapshotData.block_height === 'number'){
            this.lastAppliedBlock = snapshotData.block_height;
        }
        return true;
    }

    async verifyBootstrapTerminalBlock(){
        if(this.config['VERIFY_RECOMPUTE'] && typeof this.lastAppliedBlock === 'number' &&
           this.lastAppliedBlock > this._bootstrapBase){
            return await this.verifyRangeBoundary(this.lastAppliedBlock);
        }
        return false;
    }

    async reconcileBootstrapDecoder(source){
        // A truncated bootstrap never seeds dispensers, so after a FAILED reconcile the
        // table is still empty: leave it out of the count check rather than blame the snapshot.
        let reconciled = await this.reconcileDispensers(source) === true;
        await this.verifyDecoderCompleteness(source, this.lastAppliedBlock,
            reconciled ? undefined : new Set(['dispensers']));
    }

    logBootstrapFromHeightComplete(){
        getLogger().info('Bootstrap-from-height complete: ' + this.chain + '/' + this.network +
            ' replica holds [' + this._bootstrapBase + '..' + this.lastAppliedBlock + ']' +
            ' (truncated; pre-' + this._bootstrapBase + ' history and full-history aggregates unavailable)');
    }

    // Seed a truncated replica from a recent height instead of full history.
    // Both DB types live-follow from the immediate predecessor in the window.

    // The indexer join block has no predecessor, so recompute skips only that block.
    // Later blocks fold their committed predecessors and retain chained verification.
    async bootstrapFromHeight(depth){
        const source = this.sources[0];
        if(!source) throw new Error('Bootstrap-from-height: no sync source');

        const { tip, base } = await this.discoverBootstrapRange(source, depth);
        await this.fetchAndApplySchema(source);
        await this.syncLookupTablesPaged(source);

        const snapshotData = await this.fetchBootstrapWindow(source, base);
        if(!await this.applyBootstrapWindow(snapshotData, base, tip)) return false;

        // Re-page lookups so rows added during the window request exist locally.
        await this.syncLookupTablesPaged(source);
        await this.refreshTipHashes();

        if(await this.verifyBootstrapTerminalBlock()) return true;
        if(this.dbType === 'decoder'){
            await this.reconcileBootstrapDecoder(source);
        }

        this.logBootstrapFromHeightComplete();
        return true;
    }

    // Per-page row count for syncLookupTablesPaged. Bounded so no single request
    // approaches SNAPSHOT_MAX_CONTENT (the server clamps to its own ceiling too).
    lookupPageSize(){
        let n = parseInt(this.config['LOOKUP_PAGE_SIZE'], 10);
        if(isNaN(n) || n < 1) n = 50000;
        return Math.min(100000, n);
    }

    fetchLookupPage(source, table, afterId, pageSize, maxBlock){
        let url = source + '/snapshot-rows/' + this.dbType + '/' + this.chain + '/' +
            this.network + '/' + table + '?after_id=' + afterId + '&limit=' + pageSize;
        if(maxBlock != null) url += '&max_block=' + maxBlock;
        return axios.get(url, {
            headers: this.upstreamHeaders(),
            responseType: 'arraybuffer',
            timeout: 600000,
            decompress: true,
            maxContentLength: this.config['SNAPSHOT_MAX_CONTENT']
        });
    }

    parseLookupPage(response, source, table, expected){
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
        return page;
    }

    applyLookupPage(table, rows, expected, repairing){
        return this.withApplyLock(() => this.applier.applyIncrementalSnapshot({
            schema_version: expected,
            tables: { [table]: rows }
        }, repairing ? { strictIgnoreCheck: true } : undefined));
    }

    advanceLookupCursor(table, page, afterId){
        let nextAfter = (typeof page.max_id === 'number') ? page.max_id : afterId;
        if(nextAfter <= afterId){
            getLogger().warn('Lookup paging for ' + table + ' made no progress past id ' +
                afterId + '; stopping');
            return null;
        }
        return nextAfter;
    }

    // Pages append-only lookup tables from their current high-water cursors.
    // A from-zero pass fills holes below a table's high-water mark. Tables that
    // reissue ids on a reorg are paged only up to the replica's applied tip, so the
    // replica never holds a row its own reorg rollback would not delete and reissue.
    async syncLookupTablesPaged(source, opts){
        let tables = replicatedTables.getTopology(this.dbType).index || [];
        let pageSize = this.lookupPageSize();
        let expected = SCHEMA_VERSION[this.dbType];
        let fromZero = (opts && opts.fromZero) || null;
        for(let table of tables){
            let col = replicatedTables.lookupCursorColumn(table);
            let afterId = 0;
            let repairing = !!(fromZero && fromZero.has(table));
            let tip = this.lastAppliedBlock;
            let maxBlock = (REISSUING_LOOKUPS.has(table) && Number.isInteger(tip) && tip >= 0) ? tip : null;
            if(repairing){
                getLogger().info('Lookup repair: paging ' + table + ' from id 0 to fill a hole ' +
                    'below the high-water mark (a cursor-seeded page cannot reach it).');
            } else {
                try {
                    let r = await this.db.getMaxColumnValue(table, col);
                    if(r && r[0] && r[0].m != null) afterId = Number(r[0].m);
                } catch(e){
                    afterId = 0;
                }
            }
            let pages = 0;
            while(true){
                let response = await this.fetchLookupPage(source, table, afterId, pageSize, maxBlock);
                let page = this.parseLookupPage(response, source, table, expected);
                let rows = page.rows || [];
                if(rows.length){
                    await this.applyLookupPage(table, rows, expected, repairing);
                }
                pages++;
                if(!page.has_more) break;
                let nextAfter = this.advanceLookupCursor(table, page, afterId);
                if(nextAfter == null) break;
                afterId = nextAfter;
            }
            if(pages > 1) getLogger().info('Lookup-sync ' + table + ': ' + pages + ' page(s) up to id ' + afterId);
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
    async withApplyLock(fn){
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
    // handleBlock's fork-at-head guards treat a re-delivery at blockIndex ===
    // lastAppliedBlock whose hashes differ from lastHashes as a lost 1-block reorg.
    // start(), applyBlockEvent and handleReorg all keep the pair in step; the three
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
    async refreshTipHashes(){
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
    async incrementalCatchUp(sinceBlock){
        // Refuse to advance once halted on a divergence (same contract as
        // applyBlockEvent). The live apply path has carried this guard since the
        // halts were made durable, but gap detection (handleBlock) and status
        // events still triggered catch-ups while halted, and the catch-up apply
        // path would happily advance the replica past the divergence (the same
        // half-enforced-halt failure mode the live-path guard closed).
        if(this._halted){
            getLogger().error('Refusing incremental catch-up since block ' + sinceBlock +
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
            getLogger().error('Refusing incremental catch-up since block ' + sinceBlock +
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
                await this.runIncrementalCatchUp();
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

    async downloadIncrementalCatchUp(source, sinceBlock){
        getLogger().info('Incremental catch-up from block ' + sinceBlock + '...');
        // Sync append-only lookups separately for bounded replicas so the block
        // snapshot stays below the content limit. The local maximum id acts as
        // the cursor, so this request transfers only lookup rows not held locally.

        // Keep lookup tables in the bundled snapshot for full-history replicas.
        // Their normal incremental window stays bounded without a separate paging
        // pass, preserving the single-request path for those chains.
        let skipLookups = this._truncatedDepth >= 1;
        if(skipLookups){
            await this.syncLookupTablesPaged(source);
        }

        // Tell a bounded source to omit the multi-million-row lookup tables after
        // paging them independently. Re-dumping those append-only tables on every
        // catch-up can exceed the configured response cap.
        let url = source + '/snapshot/' + this.dbType + '/' + this.chain + '/' + this.network + '/since/' + sinceBlock +
            (skipLookups ? '?skip_lookups=1' : '');

        // Request a binary response because sources may return a compressed payload.
        // Keep decompression enabled for transport encoding while retaining explicit
        // gzip handling for snapshot bodies delivered as raw buffers.
        let response = await axios.get(url, {
            headers: this.upstreamHeaders(),
            responseType: 'arraybuffer',
            timeout: 300000,
            decompress: true,
            maxContentLength: this.config['SNAPSHOT_MAX_CONTENT']
        });

        // Accept both plain JSON buffers and gzip-compressed buffers. A failed gzip
        // probe leaves the original bytes intact so the JSON parser can decide
        // whether the source returned a valid uncompressed snapshot.
        let jsonStr = response.data;
        if(Buffer.isBuffer(jsonStr)){
            try { jsonStr = zlib.gunzipSync(jsonStr); } catch(e){}
        }

        // Convert parse failures into a source-specific transfer error. A response
        // interrupted in transit often reaches this point as a syntactically
        // incomplete buffer rather than an axios transport exception.
        let snapshotData;
        try {
            snapshotData = JSON.parse(jsonStr.toString());
        } catch(parseErr){
            throw new Error('Snapshot download truncated or corrupt from ' + source +
                ' (JSON.parse failed; likely a network interruption mid-transfer): ' + parseErr.message);
        }
        return { snapshotData, skipLookups };
    }

    async applyIncrementalCatchUpSnapshot(source, snapshotData, sinceBlock, dbTip, skipLookups){
        // Refuse a range whose tip crosses the active platform-train boundary.
        // The whole window lands in one transaction, so no part of a crossing
        // window may apply before the activation gate completes.
        if(await this.checkTrainActivation(
                (typeof snapshotData.block_height === 'number') ? snapshotData.block_height : sinceBlock))
            return true;

        // Rewind a decoder tip when the downloaded range does not build on it.
        // This handles a reorg that occurs while the client is disconnected and
        // therefore cannot arrive through the live block event stream.
        if(await this.rewindIfCatchUpForked(snapshotData, sinceBlock, dbTip, skipLookups ? source : null)) return true;

        // Apply the entire incremental snapshot under the same lock as live events.
        // The lock prevents an event from interleaving writes with this range and
        // exposing a partially advanced replica.
        await this.withApplyLock(() => this.applier.applyIncrementalSnapshot(snapshotData));

        // Clear duplicate-key attribution after a successful apply and advance the
        // in-memory cursor only when the source supplies a numeric terminal height.
        // This keeps diagnostics and live-event gap checks aligned with the database.
        this._upsertDupKeyTarget = null;
        if(typeof snapshotData.block_height === 'number')
            this.lastAppliedBlock = snapshotData.block_height;

        // Fill lookup rows created while the source prepared the block snapshot.
        // The first paging pass ends at high-water T1 while the snapshot may observe
        // a later source tip T2, so referenced rows can exist in the interval.

        // Run this second pass before hash refresh and boundary verification. Missing
        // index rows can make the local block-hash lookup null, which would otherwise
        // skip the range audit instead of proving the applied state.
        if(skipLookups){
            await this.syncLookupTablesPaged(source);
        }

        // Keep the cached hashes at the applied height before live events read them.
        // Refresh only after the lookup re-page so every hash input referenced by the
        // newly applied range is locally available.
        await this.refreshTipHashes();
        return false;
    }

    async verifyIncrementalCatchUpRange(snapshotData, sinceBlock){
        // Audit the local resume point because it folds the committed predecessor.
        // The live path recomputes every block, while catch-up applies a whole range
        // without a per-block recompute.

        // Derive the join from this replica's committed tip rather than trusting the
        // source echo. Recomputing that boundary is what detects a range joined onto
        // a locally orphaned predecessor after a disconnected reorg.
        let joinBlock = sinceBlock;

        // Reject a source that shifts the audited range boundary. Omitting the echo
        // still audits the local boundary, while an explicit disagreement indicates
        // a buggy source or an attempt to move the check inside the served range.
        if(typeof snapshotData.since_block === 'number' && snapshotData.since_block !== sinceBlock){
            await this.haltOnDivergence(joinBlock,
                [{ field: 'since_block', a: sinceBlock, b: snapshotData.since_block }],
                this.sources.slice(0, 1), 'catchup-since-block-mismatch');
            return true;
        }

        // Recompute the join before considering the terminal block. Its chained
        // hashes include the committed predecessor, so an orphan stitch cannot
        // reproduce the stored boundary and triggers the durable halt contract.
        if(await this.verifyRangeBoundary(joinBlock)) return true;

        // Audit the terminal block when chained hashes cover more than the join.
        // Its committed hashes fold the applied range through the chained metadata,
        // detecting interior corruption that leaves the first block intact.
        let terminalBlock = snapshotData.block_height;
        if(typeof terminalBlock === 'number' && terminalBlock > joinBlock){
            if(await this.verifyRangeBoundary(terminalBlock)) return true;
        }
        return false;
    }

    async verifyIncrementalCatchUpDecoder(source){
        // Reconcile dispensers on the configured cadence because this table cannot
        // converge through the block stream or append-only lookup paging. Its bounded
        // purge depth keeps the periodic atomic replacement affordable.
        let reconciled = this.shouldReconcileDispensers(Date.now()) &&
            await this.reconcileDispensers(source) === true;

        // Check dispensers only after a SUCCESSFUL reconciliation to avoid interim drift
        // reports: a failed one leaves the drifted table in place and is logged on its own.
        // Other decoder tables converge through streamed blocks or full dumps and
        // remain part of every completeness check.
        await this.verifyDecoderCompleteness(source, this.lastAppliedBlock,
            reconciled ? null : new Set(['dispensers']));
    }

    async handleIncrementalCatchUpFailure(source, sinceBlock, error){
        // Separate response-cap failures from ordinary transport and apply failures.
        // A deeply lagged replica cannot reduce an already oversized catch-up window
        // by retrying the identical request.
        let isSizeError = this.isContentLengthOverflow(error);
        if(isSizeError){
            // Use the bounded bootstrap for replicas whose full snapshot exceeds the cap.
            // Their configured depth exists because buffering and applying complete
            // chain history cannot fit beneath the snapshot limit.

            // Avoid routing bounded replicas through the full bootstrap. That path can
            // hit the same cap, exhaust retries, and enter a permanent process restart
            // loop without making synchronization progress.
            if(this._truncatedDepth >= 1){
                getLogger().warn('Incremental catch-up payload too large at sinceBlock ' + sinceBlock +
                    '; falling back to bounded height bootstrap (SYNC_BOOTSTRAP_DEPTH=' +
                    this._truncatedDepth + ', truncated replica).');
                await this.bootstrapFromHeightRetry(this._truncatedDepth);
                return;
            }

            // Use the full bootstrap when a full-history catch-up window exceeds the cap.
            // A full snapshot can still fit when the incremental response covering a
            // very wide range is the object that crosses the limit.
            getLogger().warn('Incremental catch-up payload too large at sinceBlock ' + sinceBlock +
                '; falling back to full bootstrap.');
            await this.bootstrapFromSnapshot();
            return;
        }

        // A 404 means the source holds no rows at or past this height yet (the resume
        // tip is already current). That is expected and self-clearing, so it logs one
        // warn line instead of an error and skips duplicate-key and schema recovery.
        if(error && error.response && error.response.status === 404){
            getLogger().warn('Incremental catch-up from block ' + sinceBlock +
                ' found nothing newer on ' + source + ' (404); replica is current or source is behind.');
            return;
        }

        // Attribute duplicate-key failures before attempting schema recovery. The
        // duplicate-key path may halt or classify the source and therefore owns the
        // failure when it recognizes one.
        getLogger().error(util.format('Incremental catch-up failed:', error));
        if(await this.noteUpsertDuplicateKey(source, error)) return;

        // Retry once after a schema repair; the repair debounce bounds recursion.
        // A second schema-shaped failure inside that window falls through instead of
        // nesting another catch-up attempt.
        if(await this.healSchemaIfStale(error))
            return this.runIncrementalCatchUp();
    }

    async runIncrementalCatchUp(){
        // Use the first configured source for the entire pass so download, recovery,
        // and optional verification share one upstream identity. An empty source list
        // leaves the replica untouched and lets later triggers retry.
        let source = this.sources[0];
        if(!source) return;

        // Keep the resume values outside the try body because failure handling reports
        // the attempted boundary even when reading the database tip itself fails.
        let dbTip = null;
        let sinceBlock = 0;
        try {
            // Rethrow tip-read faults so recovery handles them instead of resuming at one.
            // The database helper normally logs and returns an empty result, which is
            // indistinguishable here from a genuinely empty replica.

            // Read the committed tip again instead of trusting the in-memory cursor.
            // A lagging cursor could request already applied rows and collide with
            // ledger tables that accept plain inserts rather than ignores or upserts.

            // Keep this read inside the recovery boundary so transient query faults
            // abort the pass and schema faults reach the debounced repair path. WebSocket
            // event callers do not catch a rejection escaping the in-flight runner.
            dbTip = await this.db.getLastBlock(null, { rethrow: true });
            sinceBlock = (dbTip === null ? 0 : dbTip) + 1;

            // Preserve the server's inclusive since-bound by starting one block after
            // the committed tip. A null tip maps to block one through the zero seed.
            let { snapshotData, skipLookups } = await this.downloadIncrementalCatchUp(source, sinceBlock);

            // Stop immediately when activation or fork handling consumes the pass.
            // Both checks occur before any later verification or decoder reconciliation.
            if(await this.applyIncrementalCatchUpSnapshot(
                    source, snapshotData, sinceBlock, dbTip, skipLookups)) return;

            // Gate consensus recomputation to indexers that explicitly enable it.
            // Boundary verification can durably halt the replica, so its true result
            // prevents all remaining work in this pass.
            if(this.dbType === 'indexer' && this.config['VERIFY_RECOMPUTE']){
                if(await this.verifyIncrementalCatchUpRange(snapshotData, sinceBlock)) return;
            }

            // Run decoder completeness work only after snapshot application and any
            // indexer-only range audit. The database type makes these paths exclusive.
            if(this.dbType === 'decoder') await this.verifyIncrementalCatchUpDecoder(source);
        } catch(error){
            // Await recovery through the returned promise so fallback bootstraps and a
            // possible schema-heal retry retain their original completion semantics.
            return this.handleIncrementalCatchUpFailure(source, sinceBlock, error);
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
    async verifyAgainstSource(source, blockHeight){
        if(this.dbType !== 'indexer') return 'skip';
        let verdict = 'skip';
        try {
            let url = source + '/status/' + this.dbType + '/' + this.chain + '/' + this.network;
            let response = await axios.get(url, { headers: this.upstreamHeaders(), timeout: 10000 });
            let remoteStatus = response.data;
            let localHashes = await this.db.getBlockHashRow(blockHeight);
            if(!localHashes) return 'skip';
            let comparison = compareSourceBlockHashes(this, source, blockHeight, remoteStatus, localHashes);
            verdict = comparison.verdict;
            if(comparison.verdict === 'diverge' && this.config['HALT_ON_DIVERGENCE']){
                await this.haltOnDivergence(blockHeight, comparison.mismatches,
                    [source], 'cross-source-divergence');
                return 'halted';
            }
            if(this.config['VERIFY_RECOMPUTE']){
                let recomputeMismatches = await this.verifyRecompute(
                    { block_index: blockHeight }, blockHashFields(localHashes));
                if(recomputeMismatches){
                    await this.haltOnDivergence(blockHeight, recomputeMismatches, [source], 'local-recompute-divergence');
                    return 'halted';
                }
            }
            let countMismatches = await this.verifyTableCounts(remoteStatus.table_counts, undefined,
                { remoteHeight: remoteStatus.block_height, localHeight: blockHeight });
            reportTableCountResults(source, blockHeight, remoteStatus, countMismatches);
            if(shouldCompareIndexMap(this, blockHeight, remoteStatus, countMismatches)){
                try {
                    let localChecksum = await this.blockHasher.computeIndexMapChecksum(blockHeight);
                    if(reportIndexMapResult(this, source, blockHeight, localChecksum,
                        remoteStatus.index_map_checksum)){
                        await this.recordIndexMapMismatch(blockHeight);
                    }
                } catch(e){
                    getLogger().error(util.format('Index-map parity check errored at block ' + blockHeight +
                        ' (advisory, ignoring):', e.message));
                }
            }
            await this.verifyTableContentParity(source, blockHeight, remoteStatus);
            await this.verifyTokenFoldParity(source, blockHeight, remoteStatus);
            return verdict;
        } catch(e){
            getLogger().error(util.format('Hash verification failed against ' + source + ':', e));
            return 'unreachable';
        }
    }

    // Durably count advisory index-map parity mismatches (best-effort health signal,
    // NOT a consensus gate). Never throws. Stores a running count and the last
    // divergent block under dbType-namespaced sync-state keys, so an operator / the
    // dashboard can see id-map content divergence accumulating without a halt.
    async recordIndexMapMismatch(blockIndex){
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
    // method rather than an inline block: verifyAgainstSource returns early for
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
    async verifyTableContentParity(source, blockHeight, remoteStatus){
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
                getLogger().warn('TABLE_CONTENT_PARITY mismatch at block ' + blockHeight + ' against ' + source +
                    ': ' + JSON.stringify(res.mismatches) +
                    ' (advisory, NOT halting; replicated table content diverged at equal row count)');
                await this.recordTableContentMismatch(blockHeight, res.mismatches);
            } else {
                getLogger().info('Table-content parity passed against ' + source +
                    ' (' + res.compared + ' tables compared, ' + res.skipped.length + ' skipped)');
            }
            return res;
        } catch(e){
            getLogger().error(util.format('Table-content parity check errored at block %s (advisory, ignoring):', blockHeight, e.message));
            return null;
        }
    }

    // Advisory tokens fold-column parity (NON-consensus; never halts, never throws).
    // Same preconditions as the two checks above: both sides opted in and we are AT
    // the source's published height. The source's `ahead` ticks (edited after that
    // height on its live table) are fed back as the exclusion, so both digests cover
    // the same ticks. A mismatch means this replica holds an ISSUE edit the source
    // does not, or lacks one it has: the forward class-7 carry or the rollback refold
    // went wrong.
    async verifyTokenFoldParity(source, blockHeight, remoteStatus){
        if(!this.config['TOKEN_FOLD_PARITY_CHECK']) return null;
        let remote = remoteStatus && remoteStatus.token_fold_parity;
        if(!remote || typeof remote.h !== 'string') return null;
        if(Number(remoteStatus.block_height) !== Number(blockHeight)) return null;
        try {
            let local = await this.blockHasher.computeTokenFoldChecksum(blockHeight, { exclude: remote.ahead || [] });
            if(local.h !== remote.h){
                getLogger().warn('TOKEN_FOLD_PARITY mismatch at block ' + blockHeight + ' against ' + source +
                    ': local=' + local.h + ' (' + local.n + ' rows) source=' + remote.h + ' (' + remote.n + ' rows)' +
                    ' (advisory, NOT halting; tokens owner/lock/callback/list/mint-window/bridge columns diverged)');
                await this.recordSyncStateCounter('token_fold_mismatch', blockHeight);
                return false;
            }
            getLogger().info('Token fold parity passed against ' + source + ' (' + local.n + ' rows)');
            return true;
        } catch(e){
            getLogger().error(util.format('Token fold parity check errored at block %s (advisory, ignoring):', blockHeight, e.message));
            return null;
        }
    }

    // Durable running count plus last block for one advisory mismatch kind, under
    // dbType-namespaced sync-state keys. Best-effort health signal; never throws.
    async recordSyncStateCounter(kind, blockIndex){
        try {
            if(!this.db || typeof this.db.setSyncState !== 'function') return;
            let countKey = kind + '_count:' + this.dbType;
            let cur = (typeof this.db.getSyncState === 'function') ? await this.db.getSyncState(countKey) : null;
            let n = (cur != null && Number.isFinite(Number(cur))) ? Number(cur) + 1 : 1;
            await this.db.setSyncState(countKey, String(n));
            await this.db.setSyncState(kind + '_last_block:' + this.dbType, String(blockIndex));
        } catch(e){
            // advisory; swallow
        }
    }

    // Durably count advisory table-content parity mismatches, the twin of
    // recordIndexMapMismatch above and never a consensus gate. Never throws. Also
    // stores the diverging TABLE NAMES, because unlike the index-map counter this
    // check spans ~93 tables and "which one" is the whole diagnostic; the list is
    // capped so a pathological all-tables divergence cannot write an unbounded value.
    async recordTableContentMismatch(blockIndex, mismatches){
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
    // follower with fewer rows than the source. verifyAgainstSource (indexer-only)
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
    truncatedWindowedTables(){
        let t = replicatedTables.getTopology('decoder');
        return new Set([].concat(t.blockScoped || [], t.txScoped || [], t.actionScoped || []));
    }

    // Returns the shortfall rows it found (an ARRAY, possibly empty) when the check
    // completed, and null when it could not run or errored, so the periodic caller can
    // tell "no gaps" from "no reading" before aging its persistent-gap state.
    // opts.requireEqualHeight (periodic sweep only): a source that moved past
    // blockHeight is "no reading", since its counts would read as a hole to repair.
    async verifyDecoderCompleteness(source, blockHeight, excludeTables, opts){
        if(this.dbType !== 'decoder') return null;
        try {
            let url = source + '/status/' + this.dbType + '/' + this.chain + '/' + this.network;
            let response = await axios.get(url, { headers: this.upstreamHeaders(), timeout: 10000 });
            let remoteStatus = response.data;
            if(opts && opts.requireEqualHeight && remoteStatus && remoteStatus.block_height != null &&
               Number(remoteStatus.block_height) !== Number(blockHeight)){
                getLogger().info('Decoder completeness sweep skipped: source at ' + remoteStatus.block_height +
                    ', replica at ' + blockHeight);
                return null;
            }

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
                let windowed = this.truncatedWindowedTables();
                for(let tbl of windowed) effectiveExcludes.add(tbl);
                getLogger().info('Truncated replica: skipping block-windowed tables from the decoder ' +
                    'completeness count check (' + [...windowed].join(', ') + '); append-only lookups stay strict.');
            }

            let countMismatches = await this.verifyTableCounts(remoteStatus.table_counts, effectiveExcludes);
            if(countMismatches.length){
                getLogger().error('TABLE_COUNT_MISMATCH at block ' + blockHeight + ' against ' + source +
                    '; decoder snapshot may be truncated or incomplete:');
                getLogger().error(JSON.stringify(countMismatches));
            } else if(remoteStatus.table_counts){
                getLogger().info('Table-count verification passed against ' + source);
            }

            // Decoder content parity (advisory). The counts above are the
            // ONLY other signal this DB has: no ledger/actions/contract hash, no state
            // hash, so an equal-count content substitution in blocks, transactions,
            // transaction_outputs or the lookups was invisible here.
            await this.verifyTableContentParity(source, blockHeight, remoteStatus);
            return countMismatches;
        } catch(e){
            getLogger().error(util.format('Decoder completeness check failed against ' + source + ':', e));
            return null;
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
    // True when this client prunes its own sync_meta, i.e. SYNC_META_RETENTION_BLOCKS is
    // a positive window and the DB is writable. Mirrors the conditions
    // SyncService.startSyncMetaRetention starts its timer under, so the count check and
    // the sweep cannot disagree about whether local pruning is happening.
    syncMetaWindowArmed(){
        if(!this.config) return false;
        if(this.config['SYNC_MODE'] === 'server') return false;
        if(this.config['REPLICA_DB_READONLY']) return false;
        const keep = parseInt(this.config['SYNC_META_RETENTION_BLOCKS'], 10);
        return Number.isFinite(keep) && keep > 0;
    }

    shouldCompareTableCount(table, excludeTables){
        if(excludeTables && excludeTables.has(table)) return false;
        if(OPERATIONAL_LOG_TABLES.has(table)) return false;
        if(table === 'sync_meta' && this.syncMetaWindowArmed()) return false;
        return true;
    }

    validatedRemoteTableCount(table, value){
        let idCheck = validation.validateIdentifier(table);
        if(!idCheck.valid){
            getLogger().error('Rejected table name in remote table_counts: ' + table + ' (' + idCheck.reason + ')');
            return undefined;
        }
        let remote = Number(value);
        return Number.isFinite(remote) ? remote : undefined;
    }

    async localTableCountForVerification(table){
        let local;
        try {
            local = Number(await this.db.getTableCount(table));
        } catch(e){
            try { await this.healSchemaIfStale(e); } catch(healErr){ /* advisory only */ }
            local = 0;
        }
        return Number.isFinite(local) ? local : 0;
    }

    appendTableCountMismatch(mismatches, table, remote, local, exactParity){
        if(remote > local){
            mismatches.push({ table: table, sourceCount: remote, localCount: local, delta: remote - local });
        } else if(local > remote && exactParity && exactParity.has(table)){
            mismatches.push({ table: table, sourceCount: remote, localCount: local,
                delta: remote - local, reason: 'replica-ahead' });
        }
    }

    async verifyTableCounts(remoteCounts, excludeTables, opts){
        let mismatches = [];
        if(!remoteCounts || typeof remoteCounts !== 'object') return mismatches;
        let remoteHeight = opts && Number(opts.remoteHeight);
        let localHeight  = opts && Number(opts.localHeight);
        let sameHeight   = Number.isFinite(remoteHeight) && Number.isFinite(localHeight) && remoteHeight === localHeight;
        let exactParity  = (sameHeight && this.dbType === 'indexer') ? this.exactParityTables() : null;
        for(let table of Object.keys(remoteCounts)){
            if(!this.shouldCompareTableCount(table, excludeTables)) continue;
            let remote = this.validatedRemoteTableCount(table, remoteCounts[table]);
            if(remote === undefined) continue;
            let local = await this.localTableCountForVerification(table);
            this.appendTableCountMismatch(mismatches, table, remote, local, exactParity);
        }
        return mismatches;
    }

    // Re-page the short append-only lookups from id 0 and return the Set it tried.
    // The ordinary pager seeds at MAX(id), so a hole below the high-water mark
    // survives every sweep (see the HOLE note in syncLookupTablesPaged); detecting it
    // and never acting is what let the BTC mainnet index_transactions gap sit for
    // four weeks. INSERT IGNORE makes a re-page idempotent, a table short for another
    // reason comes back short next sweep, and operational logs never converge, so
    // they are never re-paged. Advisory: a failed pass logs and never throws.
    async repairShortLookups(source, shortfalls){
        let lookups = new Set(replicatedTables.getTopology(this.dbType).index || []);
        let shortLookups = new Set((shortfalls || []).map(m => m && m.table)
            .filter(t => lookups.has(t) && !OPERATIONAL_LOG_TABLES.has(t)));
        if(!shortLookups.size) return shortLookups;
        try {
            await this.syncLookupTablesPaged(source, { fromZero: shortLookups });
        } catch(repairErr){
            getLogger().error(util.format('Lookup repair pass failed against ' + source + ':',
                repairErr.message || repairErr));
        }
        return shortLookups;
    }

    beginCompletenessSweep(source, remoteHeight){
        let interval = this.config['COMPLETENESS_CHECK_INTERVAL'];
        if(!interval || !source) return false;
        if(this._halted) return false;                        // nothing to verify onto
        if(this.lastAppliedBlock === null) return false;       // pre-bootstrap
        if(Number(remoteHeight) !== Number(this.lastAppliedBlock)) return false;
        let now = Date.now();
        if(this._lastCompletenessSweepAt && (now - this._lastCompletenessSweepAt) < interval) return false;
        // Stamp BEFORE the await: ticks keep arriving during a sweep that issues a
        // COUNT(*) per replicated table on both sides, and a stamp set afterwards lets
        // a second tick run one concurrently against the same source.
        this._lastCompletenessSweepAt = now;
        return true;
    }

    remoteStatusMatchesCompletenessHeight(remoteStatus){
        // Re-check the height against the status we just fetched: the tick that
        // triggered this may be seconds old and the source may have advanced.
        return remoteStatus.block_height == null ||
            Number(remoteStatus.block_height) === Number(this.lastAppliedBlock);
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
    async maybeVerifyCompleteness(source, remoteHeight){
        if(!this.beginCompletenessSweep(source, remoteHeight)) return;
        try {
            if(this.dbType === 'decoder'){
                let decoderShortfalls = await this.verifyCompletenessDecoder(source);
                if(Array.isArray(decoderShortfalls)){
                    let decoderRepairTried = await this.repairShortLookups(source, decoderShortfalls);
                    await this.trackCompletenessGaps(source, decoderShortfalls, decoderRepairTried);
                }
                return;
            }
            let remoteStatus = (await this.fetchCompletenessStatus(source)).data;
            if(!this.remoteStatusMatchesCompletenessHeight(remoteStatus)) return;
            let mismatches = await this.verifyCompletenessCounts(remoteStatus);
            let { shortfalls, ahead } = this.partitionCompletenessMismatches(mismatches);
            let repairTried = new Set();
            if(shortfalls.length){
                this.reportCompletenessShortfalls(source, shortfalls);
                repairTried = await this.repairShortLookups(source, shortfalls);
            }
            if(ahead.length) this.reportCompletenessAhead(source, ahead);
            await this.trackCompletenessGaps(source, shortfalls, repairTried);
            if(!mismatches.length && remoteStatus.table_counts)
                this.reportCompletenessPass(source);
        } catch(e){
            getLogger().error(util.format('Periodic completeness sweep failed against ' + source + ':', e.message || e));
        }
    }

    // Dispensers converges only on a reconcile cycle, so the decoder sweep excludes it.
    verifyCompletenessDecoder(source){
        return this.verifyDecoderCompleteness(source, this.lastAppliedBlock,
            new Set(['dispensers']), { requireEqualHeight: true });
    }

    fetchCompletenessStatus(source){
        let url = source + '/status/' + this.dbType + '/' + this.chain + '/' + this.network;
        return axios.get(url, { headers: this.upstreamHeaders(), timeout: 10000 });
    }

    verifyCompletenessCounts(remoteStatus){
        return this.verifyTableCounts(remoteStatus.table_counts, undefined,
            { remoteHeight: remoteStatus.block_height, localHeight: this.lastAppliedBlock });
    }

    partitionCompletenessMismatches(mismatches){
        return {
            shortfalls: mismatches.filter(m => m.reason !== 'replica-ahead'),
            ahead: mismatches.filter(m => m.reason === 'replica-ahead')
        };
    }

    reportCompletenessShortfalls(source, shortfalls){
        getLogger().error('TABLE_COUNT_MISMATCH at block ' + this.lastAppliedBlock + ' against ' + source +
            '; follower may be missing replicated rows:');
        getLogger().error(JSON.stringify(shortfalls));
    }

    reportCompletenessAhead(source, ahead){
        getLogger().error('TABLE_COUNT_REPLICA_AHEAD at block ' + this.lastAppliedBlock + ' against ' + source +
            '; follower holds rows the source deleted (un-replicated forward DELETE?):');
        getLogger().error(JSON.stringify(ahead));
    }

    // A clean completed sweep clears tracked gaps, so this runs even without shortfalls.
    trackCompletenessGaps(source, shortfalls, repairTried){
        return this.trackReplicaGaps(shortfalls, {
            source: source, blockIndex: this.lastAppliedBlock, repaired: repairTried
        });
    }

    reportCompletenessPass(source){
        getLogger().info('Table-count verification passed against ' + source);
    }

    // Read a numeric tunable from config, falling back to the environment and then to
    // a default, clamped at `min`. Config-first so a test or an embedding service can
    // set it directly, env second so an operator can set it on a build whose config
    // loader predates the key, and never NaN (a NaN threshold would either alert on
    // every sweep or never).
    numericSetting(key, fallback, min){
        let raw = (this.config && this.config[key] != null && this.config[key] !== '')
            ? this.config[key] : envConfig.envValueByName(key);
        let n = Number(raw);
        if(!Number.isFinite(n)) n = fallback;
        if(min != null && n < min) n = min;
        return n;
    }

    // Age the equal-height shortfall set and escalate the ones that will not close.
    //
    // The row-count sweep already detects a follower short replicated rows, but it
    // reported every detection identically at error level, so the single line that
    // means "this replica is permanently missing data" was indistinguishable from the
    // same line emitted by a count read that raced the source's /status. Operators
    // learn to skip a line that repeats forever, which is precisely how a mainnet
    // follower stayed ~2k index_transactions rows short for weeks under a green
    // status endpoint while this check fired hourly.
    //
    // So: carry per-table state across sweeps, and when a table stays short for
    // _replicaGapAlertSweeps consecutive EQUAL-HEIGHT sweeps, emit a distinct,
    // rate-limited REPLICA_GAP_PERSISTENT alert that names the age, the sweep count,
    // the delta trend and whether the client's own repair pass already failed to
    // close it, plus a durable sync-state record a monitor can read without scraping
    // logs. A gap that GROWS re-alerts immediately; a gap that closes clears its
    // state and says so, so the durable record can never outlive the gap.
    //
    // Advisory and never throws: this is a reporting layer over a check that itself
    // never halts.
    async trackReplicaGaps(shortfalls, opts){
        try {
            let o    = opts || {};
            let now  = Number.isFinite(o.now) ? o.now : Date.now();
            let seen = new Set();
            let escalate = [];
            for(let m of (shortfalls || [])){
                if(!m || !m.table || seen.has(m.table)) continue;
                seen.add(m.table);
                let entry = this._replicaGaps.get(m.table);
                if(!entry){
                    entry = {
                        table: m.table, firstSeenAt: now, firstDelta: m.delta, worstDelta: m.delta,
                        sweeps: 0, alerts: 0, lastAlertAt: 0, repairAttempts: 0
                    };
                    this._replicaGaps.set(m.table, entry);
                }
                entry.sweeps      += 1;
                entry.lastSeenAt   = now;
                entry.lastDelta    = m.delta;
                entry.sourceCount  = m.sourceCount;
                entry.localCount   = m.localCount;
                entry.lastBlock    = (o.blockIndex != null) ? o.blockIndex : entry.lastBlock;
                if(o.repaired && typeof o.repaired.has === 'function' && o.repaired.has(m.table))
                    entry.repairAttempts += 1;
                let grew = m.delta > entry.worstDelta;
                if(grew) entry.worstDelta = m.delta;
                if(entry.sweeps < this._replicaGapAlertSweeps) continue;
                let due = !entry.lastAlertAt || (now - entry.lastAlertAt) >= this._replicaGapAlertRepeatMs;
                if(grew || due) escalate.push(entry);
            }
            // Anything absent from a COMPLETED sweep has converged. Say so at the same
            // volume the alert used, so an operator who was paged sees the recovery in
            // the same journal rather than inferring it from silence.
            for(let table of [...this._replicaGaps.keys()]){
                if(seen.has(table)) continue;
                let closed = this._replicaGaps.get(table);
                this._replicaGaps.delete(table);
                if(closed.alerts || closed.sweeps >= this._replicaGapAlertSweeps)
                    getLogger().warn('REPLICA_GAP_CLOSED: ' + this.replicaLabel() + ' table ' + table +
                        ' now matches the source (was short ' + closed.lastDelta + ' row(s) for ' +
                        this.gapAgeMinutes(closed, now) + ' min across ' + closed.sweeps + ' sweep(s)).');
            }
            if(escalate.length) this.alertPersistentReplicaGaps(escalate, now, o.source);
            await this.recordReplicaGaps(now);
        } catch(e){
            // Advisory reporting layer; a failure here must not disturb the sweep.
            getLogger().error(util.format('Replica-gap tracking failed (advisory, continuing):', e.message || e));
        }
    }

    // The loud line. Separate tag from TABLE_COUNT_MISMATCH on purpose: the mismatch
    // line is a detection, this one is a verdict, and a monitor keyed on the verdict
    // tag cannot be desensitised by the detections.
    alertPersistentReplicaGaps(entries, now, source){
        let detail = entries.map(e => {
            let trend = e.lastDelta > e.firstDelta ? 'GROWING from ' + e.firstDelta
                      : (e.lastDelta < e.firstDelta ? 'closing from ' + e.firstDelta : 'unchanged');
            return e.table + ' short ' + e.lastDelta + ' row(s) (source ' + e.sourceCount +
                ' vs local ' + e.localCount + ', delta ' + trend + ', first seen ' +
                this.gapAgeMinutes(e, now) + ' min ago across ' + e.sweeps + ' equal-height sweep(s)' +
                (e.repairAttempts ? ', ' + e.repairAttempts + ' self-repair pass(es) did NOT close it' : '') + ')';
        }).join('; ');
        getLogger().error('REPLICA_GAP_PERSISTENT: ' + this.replicaLabel() +
            ' is missing replicated rows that repeated sweeps are not closing at block ' +
            (entries[0].lastBlock != null ? entries[0].lastBlock : 'unknown') +
            (source ? ' against ' + source : '') + ': ' + detail +
            '. The consensus hashes describe the source computation and structurally cannot see this, ' +
            'so the replica keeps reporting healthy while serving incomplete data: operator action required ' +
            '(re-dump the named table(s) from the source).');
        for(let e of entries){ e.alerts += 1; e.lastAlertAt = now; }
    }

    // Durable, monitorable twin of the alert above (the same best-effort sync-state
    // channel the parity counters use, never a consensus gate, never throws). Written
    // only while persistent gaps exist, and cleared exactly once when the last one
    // closes so no key can report a gap that is gone.
    async recordReplicaGaps(now){
        let persistent = this.getReplicaGaps();
        try {
            if(!this.db || typeof this.db.setSyncState !== 'function') return;
            if(!persistent.length){
                if(!this._replicaGapRecorded) return;
                this._replicaGapRecorded = false;
                await this.db.setSyncState('replica_gap_tables:' + this.dbType, '');
                await this.db.setSyncState('replica_gap_since:' + this.dbType, '');
                await this.db.setSyncState('replica_gap_sweeps:' + this.dbType, '0');
                return;
            }
            // table:delta pairs, capped: a pathological all-tables shortfall must not
            // write an unbounded value into sync state.
            let names = persistent.slice(0, 20).map(g => g.table + ':' + g.delta).join(',');
            let oldest = Math.min(...persistent.map(g => g.firstSeenAt));
            let sweeps = Math.max(...persistent.map(g => g.sweeps));
            await this.db.setSyncState('replica_gap_tables:' + this.dbType, names);
            await this.db.setSyncState('replica_gap_since:' + this.dbType, String(oldest));
            await this.db.setSyncState('replica_gap_sweeps:' + this.dbType, String(sweeps));
            await this.db.setSyncState('replica_gap_last_block:' + this.dbType,
                String(persistent[0].last_block != null ? persistent[0].last_block : ''));
            this._replicaGapRecorded = true;
        } catch(e){
            // advisory; swallow (mirrors the parity counters)
        }
    }

    // Persistent gaps only, worst delta first, in a shape a status row can publish so
    // a monitor alerts on a non-empty array instead of log-scraping. A shortfall that
    // has not yet crossed the sweep threshold is deliberately absent: it may still be
    // a count/status race, and publishing it would rebuild the desensitising signal
    // this whole path exists to replace.
    getReplicaGaps(){
        return [...this._replicaGaps.values()]
            .filter(e => e.sweeps >= this._replicaGapAlertSweeps)
            .sort((a, b) => b.lastDelta - a.lastDelta)
            .map(e => ({
                table: e.table, delta: e.lastDelta, source_count: e.sourceCount,
                local_count: e.localCount, sweeps: e.sweeps, first_seen_at: e.firstSeenAt,
                alerts: e.alerts, repair_attempts: e.repairAttempts, last_block: e.lastBlock
            }));
    }

    replicaLabel(){
        return this.chain + '/' + this.network + '/' + this.dbType + ' follower';
    }

    gapAgeMinutes(entry, now){
        return Math.round(Math.max(0, now - entry.firstSeenAt) / 60000);
    }

    // Indexer tables whose registry class asserts exact row-set parity with the
    // source: streamed forward (stream:*) and mirrored on rollback. Derived from
    // tableLifecycle so there is no second hand-maintained list; snapshot / local /
    // hub-mirror / follower-derived / lookup classes (where extra local rows can be
    // legitimate) are excluded by construction.
    exactParityTables(){
        if(!this._exactParityTableSet){
            this._exactParityTableSet = new Set(tableLifecycle.tablesWhere(t =>
                t.owner === 'indexer' && /^stream:/.test(t.replication) && t.replicaRollback === 'mirror'));
        }
        return this._exactParityTableSet;
    }

    // Decide whether to reconcile the decoder `dispensers` table on this catch-up cycle
    // (advances the per-process cycle counter as a side effect). Reconcile when:
    //   (a) firstResume  - nothing reconciled yet this process (a resume that skipped
    //       bootstrap), so a resumed replica converges dispensers promptly instead of
    //       serving up to `every` cycles of stale rows;
    //   (b) periodic     - every Nth catch-up in steady state (DISPENSERS_RECONCILE_EVERY,
    //       default 20), since dispensers cannot ride the block stream;
    //   (c) intervalDue  - the last reconcile is older than DISPENSERS_RECONCILE_MAX_INTERVAL_MS
    //       (default 30 min; 0 disables), so a slow/stalled catch-up cadence cannot let
    //       dispensers drift unbounded in wall-clock time. Sampled from the recurring
    //       status tick as well as from this catch-up path (see
    //       dispenserReconcileIntervalDue and its caller in handleEvent), because a
    //       healthy live-following replica never enters a catch-up at all, which is
    //       precisely the cadence this clause claims to bound.
    //   (d) afterReorg   - a reorg rollback ran since the last attempt: the source rewrote
    //       dispensers off-stream for the orphaned blocks, and the follower's rollback
    //       leaves them alone, so the table is known stale (see applyReorgRollback).
    // `_lastDispenserReconcileAt` is stamped by reconcileDispensers on success (covering
    // the from-height bootstrap reconcile too), so firstResume is false once any has run.
    shouldReconcileDispensers(nowMs){
        this._catchUpCount = (this._catchUpCount || 0) + 1;
        let every = parseInt(this.config['DISPENSERS_RECONCILE_EVERY'], 10);
        if(isNaN(every) || every < 1) every = 20;
        let firstResume = (this._lastDispenserReconcileAt == null);
        let periodic    = (this._catchUpCount % every === 0);
        let afterReorg  = (this._dispenserReconcileAfterReorg === true);
        // Free function, not this.dispenserReconcileIntervalDue: this method is exercised
        // through prototype.call with hand-built contexts, which carry config and the stamp
        // and nothing else.
        return firstResume || periodic || afterReorg ||
               dispenserIntervalDue(this.config, this._lastDispenserReconcileAt, nowMs);
    }

    // Wall-clock term of the reconcile decision, WITHOUT shouldReconcileDispensers'
    // cycle-counter side effect, so a recurring caller can sample the same bound without
    // corrupting the every-Nth catch-up cadence. Falls back to the last attempt, else the
    // live-follow start, so a replica that never reconciled is bounded too.
    dispenserReconcileIntervalDue(nowMs){
        let since = latestTime(this._lastDispenserReconcileAttemptAt, this._dispenserClockArmedAt);
        return dispenserIntervalDue(this.config, this._lastDispenserReconcileAt, nowMs, since);
    }

    // Re-fetch the decoder `dispensers` table in full and replace the local copy.
    // dispensers cannot ride the block stream or the id-cursor lookup paging (no
    // monotonic id; the decoder soft-expires then hard-purges rows), so a truncated
    // bootstrap never seeds it and an incremental catch-up lets it drift. The source
    // serves the whole table in one statement-consistent response (the has_more walk
    // stays so a source that still pages completes too), then an atomic replace
    // (ClientApplier.applyDispensersReplace) converges it. Decoder-only, best-effort:
    // any fetch/parse failure aborts WITHOUT touching the local table. Every page is
    // checked against SCHEMA_VERSION like the lookup-page and snapshot channels, so a
    // code-version mismatch aborts before the replace (the status-tick caller has no
    // earlier version-checked apply in front of it).
    // Returns true only once the replace has committed, false on every skip or failure.
    async reconcileDispensers(source){
        if(this.dbType !== 'decoder') return false;
        if(!source) return false;
        // Stamp the attempt so a failing re-dump is retried once per interval, not per tick.
        this._lastDispenserReconcileAttemptAt = Date.now();
        // Clear the post-reorg trigger on attempt, not success: a later reorg re-arms it,
        // and a failing source then falls back to the interval instead of every tick.
        this._dispenserReconcileAfterReorg = false;
        let expected = SCHEMA_VERSION[this.dbType];
        try {
            let all = [];
            let afterTx = null, afterAddr = null;
            for(let guard = 0; guard < 1000000; guard++){
                let url = source + '/snapshot-dispensers/' + this.dbType + '/' + this.chain + '/' + this.network +
                    (afterTx !== null ? '?after_tx=' + afterTx + '&after_addr=' + afterAddr : '');
                let response = await axios.get(url, {
                    headers: this.upstreamHeaders(),
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
                if(page.schema_version !== expected){
                    throw new Error('Dispensers page schema mismatch: server=' +
                        page.schema_version + ' client=' + expected);
                }
                let rows = Array.isArray(page.rows) ? page.rows : [];
                for(let r of rows) all.push(r);
                if(!page.has_more || rows.length === 0) break;
                afterTx = page.max_tx; afterAddr = page.max_addr;
            }
            await this.withApplyLock(() => this.applier.applyDispensersReplace(all));
            // Stamp on success (covers bootstrap + catch-up reconciles) so the resume
            // and max-interval triggers in incrementalCatchUp can tell when dispensers
            // were last converged.
            this._lastDispenserReconcileAt = Date.now();
            getLogger().info('Dispensers reconcile: replaced ' + all.length + ' rows from ' + source +
                ' for ' + this.chain + '/' + this.network);
            return true;
        } catch(e){
            // Best-effort: leave the existing local dispensers intact on any failure.
            getLogger().error(util.format('Dispensers reconcile failed against ' + source +
                ' (local table left intact):', (e && e.message) ? e.message : e));
            return false;
        }
    }

    connectWebSockets(){
        for(let i = 0; i < this.sources.length; i++){
            this.connectWebSocket(this.sources[i], i);
        }
    }

    connectWebSocket(source, sourceIndex){
        let ws = this.createWebSocket(source, sourceIndex);
        if(!ws) return;

        this.registerWebSocketHandlers(ws, source, sourceIndex);
        this.wsConns[sourceIndex] = ws;
    }

    createWebSocket(source, sourceIndex){
        // Per-chain sync mode preference: 'full' (default) or 'infra-only', resolved
        // once in the constructor (SYNC_MODE_<CHAIN>), which also refuses the
        // infra-only + halting-verification combination before any connect.
        let syncMode = this._syncMode || 'full';
        let modeQs   = (syncMode === 'infra-only') ? '?sync_mode=infra-only' : '';
        let wsUrl    = source.replace(/^http/, 'ws') + '/subscribe/' + this.dbType + '/' + this.chain + '/' + this.network + modeQs;
        getLogger().info('Connecting WebSocket to ' + wsUrl + ' (sync_mode=' + syncMode + ')');

        let ws;
        try {
            // The server's upgrade handler runs the same Bearer check as the REST
            // routes, so a keyed source drops a headerless handshake with a bare 401
            // and the client falls into its reconnect loop with no streaming sync.
            ws = new WebSocket(wsUrl, {
                maxPayload: this.config['WS_MAX_PAYLOAD'],
                headers:    this.upstreamHeaders()
            });
        } catch(e){
            getLogger().error(util.format('WebSocket connection error:', e));
            this.scheduleReconnect(source, sourceIndex);
            return;
        }

        return ws;
    }

    registerWebSocketHandlers(ws, source, sourceIndex){
        ws.on('open', () => this.handleWebSocketOpen(source));
        ws.on('message', data => this.handleWebSocketMessage(data, source, sourceIndex));
        ws.on('close', () => this.handleWebSocketClose(source, sourceIndex));
        ws.on('error', err => this.handleWebSocketError(err, source));
    }

    handleWebSocketOpen(source){
        getLogger().info('WebSocket connected to ' + source + ' for ' + this.chain + '/' + this.network);
    }

    handleWebSocketMessage(data, source, sourceIndex){
        // Synchronous chaining keeps gap detection and apply work atomic and ordered
        // because ws does not await async listeners.
        let event;
        try {
            event = JSON.parse(data.toString());
            let check = validation.validateWsEvent(event);
            if(!check.valid){
                getLogger().error('Invalid WS event from ' + source + ': ' + check.reason);
                return;
            }
        } catch(e){
            getLogger().error(util.format('Error parsing WebSocket message:', e));
            return;
        }
        // Stamp liveness on receipt so freshness reflects when the server last speaks,
        // not when apply work finishes. Every valid event type counts.
        this._lastWsEventAt = Date.now();
        this._wsEventChain = (this._wsEventChain || Promise.resolve())
            .then(() => this.handleEvent(event, sourceIndex))
            .catch(e => this.handleWsChainError(e));
    }

    handleWebSocketClose(source, sourceIndex){
        getLogger().info('WebSocket disconnected from ' + source);
        // A disconnected source provides no evidence about upstream freshness, so its
        // last verdict cannot continue certifying that source.
        this._upstreamStatus.delete(sourceIndex);
        this.scheduleReconnect(source, sourceIndex);
    }

    handleWebSocketError(err, source){
        getLogger().error(util.format('WebSocket error from ' + source + ':', err.message));
    }

    // Terminal-error gate for the serialized WS event chain. Ordinary handler
    // errors are logged and the chain stays alive (the next event repairs state),
    // but permanent bootstrap exhaustion reached mid-stream (the size-cap fallback
    // in runIncrementalCatchUp) is unrecoverable: log-and-drop would keep the
    // process alive with running=true while the replica never advances, so the
    // supervisor never restarts it. Honor the documented restart contract
    // (SyncService sync.start().catch -> process.exit(1)) from this path too.
    handleWsChainError(e){
        if(e instanceof BootstrapExhaustedError){
            getLogger().error(util.format('Bootstrap exhausted mid-stream for ' + this.chain + '/' +
                this.network + '/' + this.dbType + '; exiting for supervised restart:', e));
            process.exit(1);
            return; // reached only when process.exit is stubbed (tests)
        }
        getLogger().error(util.format('Error handling WebSocket message:', e));
    }

    scheduleReconnect(source, sourceIndex){
        if(!this.running) return;
        // An evicted (Byzantine-suspected) source stays disconnected: reconnecting it
        // would re-admit it to the stream it was evicted from.
        if(this._evictedSources.has(sourceIndex)){
            getLogger().warn('Not reconnecting evicted source ' + source + ' for ' +
                this.chain + '/' + this.network + '/' + this.dbType);
            return;
        }
        setTimeout(() => {
            if(this.running)
                this.connectWebSocket(source, sourceIndex);
        }, this.config['CLIENT_RECONNECT_DELAY']);
    }

    async handleEvent(event, sourceIndex){
        if(event.type === 'block'){
            updateLastKnownServerBlock(this, event.block_index);
            await this.handleBlock(event, sourceIndex);
        } else if(event.type === 'reorg'){
            await this.handleReorg(event);
        } else if(event.type === 'status'){
            updateLastKnownServerBlock(this, event.block_height);
            // Keep the server's replication verdict even when its height stalls.
            // Preserve evidence that a frozen upstream reports on later status ticks.
            // Record the verdict before any gap recovery starts.
            this.recordUpstreamStatus(sourceIndex, event);
            let catchUpStart = statusGapStart(this, event.block_height);
            if(catchUpStart !== null)
                await this.incrementalCatchUp(catchUpStart);
            if(shouldReconcileDispensersOnStatus(this)){
                this._dispenserReconcileInFlight = true;
                try { await this.reconcileDispensers(this.sources[sourceIndex]); }
                finally { this._dispenserReconcileInFlight = false; }
            }
            // Sweep completeness on the recurring signal from the primary source.
            // Apply its own throttle and equal-height policy inside the helper.
            // Query the same source that sends this status event.
            await this.maybeVerifyCompleteness(this.sources[sourceIndex], event.block_height);
        }
    }

    // Unwind the orphaned tip a head-fork detector just found, then re-fetch it.
    //
    // A bare incrementalCatchUp(tip + 1) on either fork branch cannot reach the fork:
    // the sinceBlock argument is inert because runIncrementalCatchUp re-reads the DB
    // tip itself and resolves since = dbTip + 1. With the orphan still committed that
    // asks the source for /since/<orphan + 1>, which either 404s at the source's own
    // tip or streams later blocks on top of the orphaned row, leaving the decoder
    // silently diverged and the indexer wedged on the orphaned predecessor.
    // Route the rewind through handleReorg, the one proven unwind path (depth guard,
    // lastAppliedBlock/lastHashes kept in step, durable fail-closed halt when the
    // rollback itself fails), so the DB tip actually moves and the follow-up catch-up
    // resolves since = <forked height>. incrementalCatchUp refuses while _halted, so a
    // rollback that failed closed can never be followed by an advance.
    async rewindForkedHead(blockIndex){
        await this.handleReorg({ block_index: blockIndex });
        await this.incrementalCatchUp(this.lastAppliedBlock + 1);
    }

    // True when a live decoder block at tip + 1 names a parent other than the committed
    // tip: a reorg replaced the tip while its `reorg` broadcast was lost (it is never
    // replayed on reconnect). Unresolvable linkage reads as linked, never as a fork.
    async decoderTipReplaced(event){
        let rows = (event && event.data) || {};
        let blockRow = (rows.blocks || []).find(b => b && Number(b.block_index) === Number(event.block_index));
        return await decoderLinkBroken(blockRow, rows.index_transactions,
            this.lastHashes && this.lastHashes.block_hash, this.db);
    }

    // Catch-up twin of decoderTipReplaced: true, after rewinding the tip, when a decoder
    // window's first block does not build on the committed tip, so the window must not
    // land. It runs inside incrementalCatchUp's in-flight runner, so it flags a re-run
    // rather than awaiting a catch-up (which would wait on itself).
    //
    // Fail-closed: a skip_lookups window is snapshotted after the lookups were paged, so
    // its join parent id can name a row the replica lacks. `lookupSource` (set for that
    // case) re-pages the lookups first, and a parent or tip hash that still does not
    // resolve throws, aborting this pass for the next trigger to retry, never "linked".
    async rewindIfCatchUpForked(snapshotData, sinceBlock, dbTip, lookupSource){
        if(this.dbType !== 'decoder' || dbTip === null || dbTip !== this.lastAppliedBlock) return false;
        let tables = (snapshotData && snapshotData.tables) || {};
        let joinRow = (tables.blocks || []).find(b => b && Number(b.block_index) === sinceBlock);
        if(!joinRow) return false;
        if(lookupSource) await this.syncLookupTablesPaged(lookupSource);
        let tip = await this.db.getBlockHashRow(dbTip, null, { rethrow: true });
        let state = await decoderLinkState(joinRow, tables.index_transactions, tip && tip.block_hash, this.db);
        if(state === 'unresolved'){
            throw new Error('Decoder catch-up join block ' + sinceBlock + ' parent or committed tip ' + dbTip +
                ' hash is unresolved; aborting this pass to retry');
        }
        if(state !== 'broken') return false;
        getLogger().error('Chain continuity error (decoder): catch-up block ' + sinceBlock +
            ' does not build on the committed tip ' + dbTip + '; rewinding the orphaned tip before applying');
        await this.handleReorg({ block_index: dbTip });
        this._catchUpPending = true;
        return true;
    }

    refuseLiveBlockOnEmptyReplica(event){
        let blockIndex = event.block_index;

        // Refuse a live block above genesis when the replica has no committed tip.
        // start() bootstraps before live-follow, so this state means bootstrap is
        // skipped or fails without establishing a tip.

        // Allow block_index 0 as the only legitimate from-empty application.
        if(this.lastAppliedBlock !== null || blockIndex <= 0) return null;

        // Applying here leaves every lower block missing because the duplicate,
        // continuity, and fork guards only run after a tip exists.
        // Rebuild from the source instead of orphaning the lower blocks.

        // Trigger catch-up at the incoming height so the source can fill the replica.
        getLogger().error('Refusing to apply block ' + blockIndex + ' onto an empty replica (' +
            this.chain + '/' + this.network + '/' + this.dbType + '). Bootstrap did not complete; ' +
            'triggering catch-up instead of orphaning blocks below it');
        return { completion: this.incrementalCatchUp(blockIndex) };
    }

    handleDecoderHeadDuplicate(event){
        let blockIndex = event.block_index;

        // Treat a different hash at the committed tip as a short reorg that the live
        // stream does not report. A silent skip pins the replica to the orphaned tip.
        // The next block's previous-hash link catches a fork with no tip re-delivery.

        // Match the indexer's head-fork protection through the decoder's block hash.
        if(blockIndex !== this.lastAppliedBlock ||
           !this.lastHashes || !this.lastHashes.block_hash ||
           !event.block_hash || event.block_hash === this.lastHashes.block_hash) return null;

        getLogger().error('Chain continuity error (decoder): fork at head block ' + blockIndex +
            '; stored block_hash ' + this.lastHashes.block_hash +
            ' != incoming ' + event.block_hash + '; rewinding the orphaned tip and catching up');
        return this.rewindForkedHead(blockIndex);
    }

    handleIndexerHeadDuplicate(event){
        let blockIndex = event.block_index;

        // Restrict hash comparison to a re-delivery of the committed head.
        // Leave older duplicate heights on the ordinary silent-skip path.
        if(blockIndex !== this.lastAppliedBlock || !this.lastHashes) return null;

        // Compare the three chain-of-state hashes because indexer events carry no
        // block_hash. A mismatch identifies a one-block reorg across a socket drop.
        // Keep null fields out of the comparison because they do not supply evidence.
        let lh = this.lastHashes;
        let mismatch =
            (event.ledger_hash   != null && lh.ledger_hash   != null && event.ledger_hash   !== lh.ledger_hash) ||
            (event.actions_hash  != null && lh.actions_hash  != null && event.actions_hash  !== lh.actions_hash) ||
            (event.contract_hash != null && lh.contract_hash != null && event.contract_hash !== lh.contract_hash);
        if(!mismatch) return null;

        // Rewind before catch-up so the orphaned predecessor cannot enter local hash
        // recomputation. Matching duplicates remain silent skips.
        // Route both database types through the same rollback and catch-up sequence.
        getLogger().error('Chain continuity error (indexer): fork at head block ' + blockIndex +
            '; stored ledger/actions/contract hash != incoming; rewinding the orphaned tip and catching up');
        return this.rewindForkedHead(blockIndex);
    }

    handlePreviouslyAppliedBlock(event){
        let blockIndex = event.block_index;
        if(this.lastAppliedBlock === null || blockIndex > this.lastAppliedBlock) return null;

        // Check a duplicate at the current head for a fork before skipping it.
        // Skip older heights without a fork check because they cannot replace the tip.
        let completion = null;
        if(this.dbType === 'decoder') completion = this.handleDecoderHeadDuplicate(event);
        else if(this.dbType === 'indexer') completion = this.handleIndexerHeadDuplicate(event);
        return { completion };
    }

    handleIndexerBlockContinuity(event){
        let continuity = this.hashVerifier.verifyChainContinuity(
            this.lastAppliedBlock, this.lastHashes, event
        );
        if(continuity.valid) return null;

        // Treat ordinary trailing-tip lag as an informational gap; the separate head
        // duplicate check reports a genuine head fork as an error.
        // Catch up from the first height after the committed tip.
        this.logGap('Catch-up lag (indexer): ' + continuity.reason);
        return { completion: this.incrementalCatchUp(this.lastAppliedBlock + 1) };
    }

    handleDecoderBlockGap(event){
        let blockIndex = event.block_index;
        if(blockIndex <= this.lastAppliedBlock + 1) return null;

        // Detect dropped decoder blocks even though decoder rows have no synthetic
        // chain hashes for the indexer continuity verifier.
        // Catch up before applying the incoming block over the missing range.
        this.logGap('Block gap detected (decoder): local=' + this.lastAppliedBlock + ' incoming=' + blockIndex);
        return { completion: this.incrementalCatchUp(this.lastAppliedBlock + 1) };
    }

    recordPendingBlockHash(event, sourceIndex){
        let blockIndex = event.block_index;

        // Record the current source's chain-of-state tuple for this height.
        // Keep each source's report available until quorum resolves or times out.
        // Replace a source's earlier report when the same height arrives again.
        if(!this.pendingHashes.has(blockIndex)) this.pendingHashes.set(blockIndex, {});
        this.pendingHashes.get(blockIndex)[sourceIndex] = {
            ledger_hash: event.ledger_hash,
            actions_hash: event.actions_hash,
            contract_hash: event.contract_hash,
            state_hash: event.state_hash
        };
        return this.pendingHashes.get(blockIndex);
    }

    tallyPendingBlockHashes(pending){
        // Group reported, non-evicted sources by hash tuple.
        // Count only active reports when deciding whether every source has answered.
        let groups = new Map(); // hashKey -> [sourceIndex...]
        let reportedCount = 0;
        for(let idxStr of Object.keys(pending)){
            let idx = Number(idxStr);
            if(this._evictedSources.has(idx)) continue;
            reportedCount++;
            let key = this.hashTupleKey(pending[idxStr]);
            if(!groups.has(key)) groups.set(key, []);
            groups.get(key).push(idx);
        }
        return { groups, reportedCount };
    }

    acceptBlockQuorum(blockIndex, pending, currentKey, currentGroup){
        // Strike every reported dissenter and record the applied majority.
        // Clear pending confirmation state and cancel its liveness timer.
        for(let idxStr of Object.keys(pending)){
            let idx = Number(idxStr);
            if(this._evictedSources.has(idx)) continue;
            if(this.hashTupleKey(pending[idxStr]) !== currentKey) this.strikeSource(idx, blockIndex);
        }
        this._lastSourcesAgreeing = currentGroup.length;
        this.pendingHashes.delete(blockIndex);
        this._strictConfirmPending.delete(blockIndex);
        let timer = this._applyTimers.get(blockIndex);
        if(timer){ clearTimeout(timer); this._applyTimers.delete(blockIndex); }

        // Continue to block application only after confirmation state is cleared.
    }

    rejectBlockWithoutQuorum(blockIndex, groups){
        // Reject a contested block after every active source reports and no group
        // reaches quorum, because the replica cannot determine the true payload.
        // Clear the pending confirmation state before halting or logging the split.
        this.pendingHashes.delete(blockIndex);
        this._strictConfirmPending.delete(blockIndex);
        let timer = this._applyTimers.get(blockIndex);
        if(timer){ clearTimeout(timer); this._applyTimers.delete(blockIndex); }
        let summary = [...groups.entries()].map(([key, sources]) => ({
            hash: key,
            sources: sources.map(index => this.sources[index])
        }));

        // Halt fail-closed when divergence handling is enabled.
        if(this.config['HALT_ON_DIVERGENCE']){
            return this.haltOnDivergence(blockIndex, summary,
                [...groups.values()].reduce((all, sources) => all.concat(sources.map(index => this.sources[index])), []),
                'no-source-quorum');
        }

        // Keep log-only mode from applying a payload with no source quorum.
        getLogger().error('NO-QUORUM ALERT: sources split with no majority at block ' + blockIndex +
            '; not applying (HALT_ON_DIVERGENCE=false, log-only)');
        getLogger().error(util.format('groups:', JSON.stringify(summary)));
        return null;
    }

    armBlockQuorumTimer(event){
        let blockIndex = event.block_index;
        if(this._applyTimers.has(blockIndex)) return;

        // Arm one liveness fallback while another source is silent or slow.
        // A split is rejected when all active sources report.
        // Apply the available payload only when strict confirmation is disabled.
        let timer = setTimeout(async () => {
            // Remove the timer marker before examining confirmation state so a later
            // arrival can create a fresh timer when the block remains unresolved.
            this._applyTimers.delete(blockIndex);

            // Skip fallback after another path applies the height or clears its tuple.
            if(this.pendingHashes.has(blockIndex) && this.lastAppliedBlock < blockIndex){
                if(this.config['HASH_CONFIRM_STRICT']){
                    getLogger().error('STRICT: Cross-source quorum timeout for block ' + blockIndex +
                        ', rejecting and blocking single-source catch-up (HASH_CONFIRM_STRICT=true)');

                    // Retain pending hashes so a later delivery can complete quorum,
                    // and block single-source catch-up in the meantime.
                    // Mark the height strict-pending until another source confirms it.
                    this._strictConfirmPending.add(blockIndex);
                } else {
                    getLogger().info('Cross-source quorum timeout for block ' + blockIndex + ', applying from primary');
                    try {
                        await this.applyBlockEvent(event);
                    } catch(e){
                        getLogger().error(util.format('Error applying block ' + blockIndex + ' after cross-source timeout:', e));
                    }
                    this.pendingHashes.delete(blockIndex);
                }
            }
        }, this.config['HASH_CONFIRM_TIMEOUT']);
        this._applyTimers.set(blockIndex, timer);
    }

    handleBlockQuorum(event, sourceIndex){
        // Require tuple quorum only for a multi-source indexer with hash verification.
        // Keep decoder, disabled-verification, and single-source application direct.
        // Preserve the two-source unanimous case and larger-set majority behavior.
        if(this.dbType !== 'indexer' || !this.config['VERIFY_HASHES'] || this.activeSourceCount() <= 1)
            return null;

        // Ignore an evicted source's in-flight delivery.
        // Exclude decoder and single-source paths because they need no tuple quorum.
        if(this._evictedSources.has(sourceIndex)) return {};

        let blockIndex = event.block_index;
        let pending = this.recordPendingBlockHash(event, sourceIndex);
        let { groups, reportedCount } = this.tallyPendingBlockHashes(pending);

        // Gate application on the current arrival's group so the accepted tuple always
        // belongs to the payload held by this call. The majority winner is unique.
        // Strike dissenters only after the current tuple reaches the effective quorum.
        let currentKey = this.hashTupleKey(pending[sourceIndex]);
        let currentGroup = groups.get(currentKey) || [];
        if(currentGroup.length >= this.effectiveQuorum()){
            this.acceptBlockQuorum(blockIndex, pending, currentKey, currentGroup);
            return null;
        }
        if(reportedCount >= this.activeSourceCount()){
            return { completion: this.rejectBlockWithoutQuorum(blockIndex, groups) };
        }
        // Wait when more active reports can still produce a quorum.
        this.armBlockQuorumTimer(event);
        return {};
    }

    async handleBlock(event, sourceIndex){
        let action = this.refuseLiveBlockOnEmptyReplica(event);
        if(action){
            if(action.completion) await action.completion;
            return;
        }

        action = this.handlePreviouslyAppliedBlock(event);
        if(action){
            if(action.completion) await action.completion;
            return;
        }

        if(this.lastAppliedBlock !== null){
            action = this.dbType === 'indexer' ? this.handleIndexerBlockContinuity(event) :
                this.handleDecoderBlockGap(event);
            if(action){
                if(action.completion) await action.completion;
                return;
            }
            if(this.dbType !== 'indexer' && await this.decoderTipReplaced(event)){
                getLogger().error('Chain continuity error (decoder): previous-hash mismatch at block ' + event.block_index +
                    '; the committed tip ' + this.lastAppliedBlock + ' was replaced by a reorg this replica missed; ' +
                    'rewinding the orphaned tip and catching up');
                await this.rewindForkedHead(this.lastAppliedBlock);
                return;
            }
        }

        action = this.handleBlockQuorum(event, sourceIndex);
        if(action){
            if(action.completion) await action.completion;
            return;
        }
        await this.applyBlockEvent(event);
    }

    // The trainActivation block of the signed release manifest this follower was
    // installed from. The carrier ships the manifest and this component ships the
    // vendored TRAIN_ACTIVATION map, which is exactly why the comparison is worth
    // making: a partial upgrade (new carrier, stale sync image) is the shape a
    // post-launch MAJOR train forks in, and it is invisible to every per-feature
    // flag day. Resolved from config RELEASE_MANIFEST_PATH when the deployment sets
    // one, otherwise from the carrier's copy beside this checkout. NO manifest is
    // not a fault and not a halt: nothing then names a rule set, which is the honest
    // reading of an install that has no manifest to require one. A manifest that
    // exists and cannot be read or parsed is reported as malformed, which
    // evaluateTrainActivation halts on fail-closed.
    resolveTrainActivationRequirement(){
        if(this._trainActivationRequired !== undefined) return this._trainActivationRequired;
        let candidates = [];
        if(this.config && this.config['RELEASE_MANIFEST_PATH'])
            candidates.push(String(this.config['RELEASE_MANIFEST_PATH']));
        candidates.push(path.join(__dirname, '../../../xchain-node/src/release-manifest.json'));
        for(const file of candidates){
            let raw;
            try {
                if(!fs.existsSync(file)) continue;
                raw = fs.readFileSync(file, 'utf8');
            } catch(e){
                // Present but unreadable. Do NOT cache: a permissions fix or a
                // completed atomic rename should be picked up on the next apply.
                return { malformed: 'release manifest at ' + file + ' could not be read (' + (e && e.message) + ')' };
            }
            let parsed;
            try { parsed = JSON.parse(raw); }
            catch(e){ return { malformed: 'release manifest at ' + file + ' is not valid JSON' }; }
            this._trainActivationRequired = trainActivation.readManifestTrainActivation(parsed);
            return this._trainActivationRequired;
        }
        this._trainActivationRequired = null;
        return null;
    }

    // Pre-apply platform-train gate. `blockIndex` is the block about to be applied
    // on the live path, or the TIP of a snapshot range on the bootstrap and catch-up
    // paths: a range lands in one transaction, so if any block in it is at or above
    // the boundary the whole range is refused. Returns true when the follower halted
    // (the caller must stop without applying), false when it may apply. Also keeps
    // this.trainActivation current for /status, which is how the halt is announced
    // BEFORE it fires: the `pending` verdict is published from the moment the
    // manifest names an unimplemented rule set, not at the boundary.
    //
    // The clock is the BTC height, which on a BTC follower is the block index itself.
    // Off BTC there is no BTC height in this path, so null is passed and the gate
    // treats an unimplemented requirement as fail-closed (see the header of
    // src/consensus/gates/train_gate.js). The halt rides haltOnDivergence so it is durable in
    // sync_halt, re-read by start(), and cleared only by an operator, exactly like a
    // divergence halt: a follower that forgot the halt across a restart would apply
    // the forked block. Never throws into the apply path: a fault in the gate itself
    // halts, because a gate that cannot decide must not wave the block through.
    async checkTrainActivation(blockIndex){
        let verdict;
        try {
            verdict = trainActivation.evaluateTrainActivation({
                height:   (this.coinTicker === 'BTC') ? blockIndex : null,
                network:  this.network,
                required: this.resolveTrainActivationRequirement()
            });
        } catch(e){
            verdict = {
                status: 'halt', activeRuleSet: null, requiredRuleSet: null, requiredAtHeight: null,
                network: this.network, height: blockIndex, classification: null,
                reason: 'train_activation: the activation gate itself failed to evaluate (' +
                        (e && e.message) + '); refusing to advance'
            };
        }
        this.trainActivation = verdict;

        if(verdict.status === 'clear') return false;

        if(verdict.status === 'pending'){
            // Loud on the transition, then periodic, so the announcement cannot be
            // missed and cannot drown the log during a long rolling-upgrade window.
            if((this._trainActivationLogTick++ % 60) === 0)
                getLogger().error('ClientSync: TRAIN ACTIVATION PENDING for ' + this.chain + '/' + this.network +
                    '/' + this.dbType + ' - ' + verdict.reason);
            return false;
        }

        // The mismatch record carries the machine-readable fields plus the prose so
        // /status (haltInfo) and sync_halt name the missing rule set and the height
        // without a log lookup.
        await this.haltOnDivergence(blockIndex, [{
            field:     'rule_set',
            required:  verdict.requiredRuleSet,
            at_height: verdict.requiredAtHeight,
            network:   verdict.network,
            height:    verdict.height,
            reason:    verdict.reason
        }], [], 'train-activation');
        return true;
    }

    getTrainActivation(){ return this.trainActivation; }

    // Durable HALT on a confirmed cross-source consensus divergence. Two honest
    // sources committed different ledger/actions/contract hashes for the SAME
    // block: one is on a forked/Byzantine chain. We must NOT pick one and apply
    // it (that risks replicating a forked chain), and must NOT silently stall.
    // Stop applying, record the halt durably (survives restart), and alert loudly
    // until an operator investigates and clears it. The platform-train gate
    // (checkTrainActivation) halts through here too, so every halt shares one
    // marker, one startup check and one operator clear.
    async haltOnDivergence(blockIndex, mismatches, sources, reason){
        if(this._halted) return; // already halted
        this._halted = buildDivergenceHalt(blockIndex, mismatches, sources, reason);
        try { await this.db.recordHalt(this.dbType, blockIndex, this._halted.reason, mismatches, sources); }
        catch(e){ getLogger().error(util.format('CRITICAL: failed to persist divergence halt (still halting in-memory):', e)); }
        logDivergenceHalt(this, blockIndex, mismatches, sources);
        stopDivergenceApply(this);
    }

    // Recompute a block's consensus hashes from the replica's raw rows and compare
    // to the committed hashes (carried in the live block event, or read locally).
    // Returns an array of mismatches [{field, computed, committed}] or null on a
    // clean match. Indexer-only (decoder has no synthetic chain hashes).
    //
    // A recompute ERROR (transient DB hiccup, schema gap) is logged loudly and
    // never halts: a local infrastructure fault must not fork this validator off
    // the chain (halts are reserved for genuine DATA divergence). Callers that
    // pass opts.holdTip (the live block path) get the error rethrown so the tip
    // stays put for redelivery, the same as a state_hash read error; the default
    // returns null. Bulk-range callers (bootstrap terminal, catch-up join/terminal)
    // pass opts.failClosed instead: there the recompute is the ONLY verification
    // of the whole applied range (the join recompute is what catches a
    // disconnect-spanning reorg stitched onto an orphaned tip), so an error is
    // retried briefly and then THROWN for the caller to halt on.
    async verifyRecompute(event, committedOverride, opts = {}){
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
                // Pass the ticker: the state_key collation gate is keyed '<TICKER>:<network>'
                // as the source indexer keys it, and the full name misses it (reads OFF).
                computed = await this.blockHasher.computeBlockHashes(event.block_index, this.network, this.coinTicker);
                break;
            } catch(e){
                if(attempt < attempts){
                    getLogger().error(util.format('Recompute verification errored at block %s (attempt %s/%s, retrying):',
                        (event && event.block_index), attempt, attempts, e));
                    await this.util.sleep(1000 * attempt);
                    continue;
                }
                if(opts.failClosed) throw e;
                getLogger().error(util.format('Recompute verification errored at block %s (holding the tip, not halting):',
                    (event && event.block_index), e));
                if(opts.recordUnverified) await this.recordSyncStateCounter('unverified_recompute', event && event.block_index);
                if(opts.holdTip) throw e;
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
    // hash mismatch AND on a committed-hash READ error or a recompute error that
    // survives the retries: unlike
    // the live path, this recompute is the only verification of the applied
    // range, so an unverifiable range must not be served. Returns true
    // when it halted (caller must stop), false when the block verified or the
    // committed hash is not yet resolvable (NULL ledger_hash / missing row, the
    // pre-existing skip the lookup re-page minimizes).
    async verifyRangeBoundary(blockIndex){
        // Read the committed hash FAIL-CLOSED, on the same retry-then-halt terms as
        // the recompute below. getBlockHashRow's default is fail-soft (doQuery
        // collapses a non-transactional query error into [], then null), and null
        // here means "not yet resolvable" -> return false -> the three callers (the
        // bootstrapFromHeight terminal, and the join and terminal boundaries in
        // runIncrementalCatchUp) read that as "range verified, continue". A transient
        // DB fault would therefore skip the ONLY verification of a bulk-applied range,
        // and the join check is what catches a disconnect-spanning reorg stitched onto
        // an orphaned tip.
        // { rethrow: true } separates a failed read from a genuinely absent row.
        let committed;
        let readAttempts = 3;
        for(let attempt = 1; attempt <= readAttempts; attempt++){
            try {
                committed = await this.db.getBlockHashRow(blockIndex, null, { rethrow: true });
                break;
            } catch(e){
                if(attempt < readAttempts){
                    getLogger().error('Boundary hash read errored at block ' + blockIndex +
                        ' (attempt ' + attempt + '/' + readAttempts + ', retrying): ' +
                        ((e && e.message) ? e.message : e));
                    await this.util.sleep(1000 * attempt);
                    continue;
                }
                await this.haltOnDivergence(blockIndex,
                    [{ field: 'boundary_hash_read_error', computed: null, committed: String((e && e.message) || e) }],
                    this.sources.slice(0, 1), 'boundary-read-error');
                return true;
            }
        }
        if(!(committed && committed.ledger_hash)) return false;
        let mismatches;
        try {
            mismatches = await this.verifyRecompute({ block_index: blockIndex }, {
                ledger_hash:   committed.ledger_hash,
                actions_hash:  committed.actions_hash,
                contract_hash: committed.contract_hash
            }, { failClosed: true });
        } catch(e){
            await this.haltOnDivergence(blockIndex,
                [{ field: 'recompute_error', computed: null, committed: String((e && e.message) || e) }],
                this.sources.slice(0, 1), 'recompute-error');
            return true;
        }
        if(mismatches){
            await this.haltOnDivergence(blockIndex, mismatches,
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

    // Record one source's self-reported replication evidence off its status event.
    // A server that predates the fields reports nothing, which stays UNKNOWN here
    // rather than being read as healthy: `undefined` is not `false`.
    recordUpstreamStatus(sourceIndex, event){
        let stale = (typeof event.replica_stale === 'boolean') ? event.replica_stale : null;
        let secondsBehind = (typeof event.replica_seconds_behind === 'number'
                             && Number.isFinite(event.replica_seconds_behind))
                                ? event.replica_seconds_behind : null;
        let sourceHeight = (typeof event.source_block_height === 'number'
                            && Number.isFinite(event.source_block_height))
                               ? event.source_block_height : null;
        this._upstreamStatus.set(sourceIndex, { sourceHeight, stale, secondsBehind });
    }

    // The upstream replication verdict this follower's own /status must carry, so
    // lag_blocks is never read as a clean bill of health for a source whose database
    // said otherwise. Separate from the transport-liveness signal isSourceHeightStale:
    // that one says whether the server is still SPEAKING, this one says whether what it
    // said certifies its data.
    //
    // stale is TRI-STATE, and the third state is the point: null means no connected
    // source has reported the field at all (nothing heard yet, or an older server), which
    // is unknown and must not read as fresh. true means at least one connected source
    // reported its own DB stale; the follower applies from all of them, so any stale
    // source qualifies the row. secondsBehind is the WORST reported lag and sourceHeight
    // the HIGHEST reported upstream DB tip, which is what makes broadcaster-vs-source lag
    // visible on the follower.
    getUpstreamReplicaState(){
        let stale = null, secondsBehind = null, sourceHeight = null;
        for(let [sourceIndex, seen] of this._upstreamStatus){
            if(this._evictedSources.has(sourceIndex)) continue;
            if(seen.stale === true) stale = true;
            else if(seen.stale === false && stale === null) stale = false;
            if(seen.secondsBehind !== null && (secondsBehind === null || seen.secondsBehind > secondsBehind))
                secondsBehind = seen.secondsBehind;
            if(seen.sourceHeight !== null && (sourceHeight === null || seen.sourceHeight > sourceHeight))
                sourceHeight = seen.sourceHeight;
        }
        return { stale, secondsBehind, sourceHeight };
    }

    safeParse(s){ try { return JSON.parse(s); } catch(e){ return s; } }

    // Durable-marker key for this client's truncation join floor. Namespaced by
    // dbType so the indexer and decoder replicas of one chain don't clobber each
    // other (they share neither DB nor floor, but the key space is shared if they
    // ever did).
    bootstrapBaseKey(){ return 'bootstrap_base:' + this.dbType; }

    // Persist the truncation join floor durably so it survives a restart. Guarded:
    // the durable store is optional (older db instances / test mocks may not expose
    // it), so a missing setSyncState degrades to in-memory-only behaviour rather
    // than throwing. Fail-soft (the db helper itself swallows persistence errors).
    async persistBootstrapBase(base){
        if(base === null || base === undefined) return;
        if(!this.db || typeof this.db.setSyncState !== 'function') return;
        await this.db.setSyncState(this.bootstrapBaseKey(), String(base));
    }

    // Clear the truncation join floor, in-memory and durable. Called after a
    // successful full-history snapshot apply, which restores complete state and
    // makes any prior floor stale. Fail-soft on db instances without the durable
    // store (mirrors persistBootstrapBase): the in-memory reset always happens.
    async clearBootstrapBase(){
        this._bootstrapBase = null;
        if(!this.db || typeof this.db.deleteSyncState !== 'function') return;
        await this.db.deleteSyncState(this.bootstrapBaseKey());
    }

    // Reload the persisted truncation join floor at startup. Only overwrites the
    // in-memory _bootstrapBase when a durable value exists AND the field is not
    // already set (a fresh bootstrap in this same process already set it). A
    // full-history replica never wrote one, so this is a no-op there and the floor
    // stays null (every block recomputes). Guarded for db instances without the
    // durable store.
    async loadBootstrapBase(){
        if(this._bootstrapBase !== null && this._bootstrapBase !== undefined) return;
        if(!this.db || typeof this.db.getSyncState !== 'function') return;
        let v = await this.db.getSyncState(this.bootstrapBaseKey());
        if(v === null || v === undefined) return;
        let n = Number(v);
        if(Number.isFinite(n)){
            this._bootstrapBase = n;
            getLogger().info('Reloaded truncation join floor _bootstrapBase=' + n + ' for ' +
                this.chain + '/' + this.network + '/' + this.dbType + ' (survives restart)');
        }
    }

    // Operator clear: acknowledge an investigated divergence and allow resume.
    // Never automatic: a halted validator must not self-resume onto a contested
    // chain. Caller is responsible for restarting the sync loop afterwards.
    async clearHalt(){
        // A synthetic 'halt-state-check-failed' state says the durable table could not be
        // READ, not that a divergence was adjudicated. Deleting sync_halt from here would
        // destroy a genuine uncleared divergence row this process never managed to see,
        // and clearing the stall is the one workaround an operator has, so it is exactly
        // the path that would do it. Drop the in-memory flag only and let start()'s
        // re-read decide what the table actually holds.
        if(this._halted && this._halted.reason === 'halt-state-check-failed'){
            const wasSynthetic = this._halted;
            this._halted = null;
            getLogger().info('halt-state-check-failed state cleared for ' + this.chain + '/' + this.network +
                '/' + this.dbType + '; sync_halt left untouched (it was never successfully read)');
            return wasSynthetic;
        }
        try { await this.db.clearHalt(this.dbType); } catch(e){ getLogger().error(util.format('clearHalt persistence failed:', e)); }
        const was = this._halted;
        this._halted = null;
        getLogger().info('Divergence halt CLEARED for ' + this.chain + '/' + this.network + '/' + this.dbType +
            (was ? ' (was halted at block ' + was.blockIndex + ')' : ''));
        return was;
    }

    carryUnverifiedRoots(event){
        // Preserve computed roots across retryable verification errors without
        // treating snapshot roots as a live block's commitments.
        let computedRoots = this.applier._lastComputedRoots;
        if(!computedRoots && this._unverifiedRoots
                && this._unverifiedRoots.blockIndex === event.block_index)
            computedRoots = this._unverifiedRoots.roots;
        this._unverifiedRoots = computedRoots
            ? { blockIndex: event.block_index, roots: computedRoots }
            : null;
        return computedRoots;
    }

    addStateHashClassDetails(mismatch, preimage){
        // Record each mutable-state class digest for mismatch diagnosis.
        mismatch.local_classes = classDigests(
            preimage, data => this.util.getDataHash(data));
        for(let detail of mismatch.local_classes){
            getLogger().error('state_hash local class ' + detail.class +
                ': rows=' + (detail.rows === null ? 'n/a' : detail.rows) +
                ' digest=' + detail.digest);
        }
    }

    stateCommitmentDivergence(event, computed){
        // Treat missing required roots as divergence while allowing an omitted
        // state root for catch-up blocks whose stake state is already newer.
        let missing = [];
        if(event.balances_root == null)
            missing.push({ field: 'balances_root', a: null, b: computed.balances_root });
        if(event.block_merkle_root == null)
            missing.push({ field: 'block_merkle_root', a: null, b: computed.block_merkle_root });
        if(missing.length)
            return { mismatches: missing, reason: 'state-commitment-missing' };

        let mismatches = [];
        if(computed.balances_root !== event.balances_root)
            mismatches.push({ field: 'balances_root', a: event.balances_root, b: computed.balances_root });
        if(event.block_merkle_root != null && computed.block_merkle_root !== event.block_merkle_root)
            mismatches.push({ field: 'block_merkle_root', a: event.block_merkle_root, b: computed.block_merkle_root });
        if(event.state_root != null && computed.state_root !== event.state_root)
            mismatches.push({ field: 'state_root', a: event.state_root, b: computed.state_root });
        if(mismatches.length)
            return { mismatches, reason: 'state-commitment-divergence' };
        return null;
    }

    finishAppliedBlock(event){
        // Advance the verified tip and publish its hashes to local status consumers.
        this._unverifiedRoots     = null;
        this.lastAppliedBlock     = event.block_index;
        this.lastAppliedBlockTime = (typeof event.block_time === 'number') ? event.block_time : null;
        this.scheduleHeartbeat();
        if(this.dbType === 'decoder'){
            this.lastHashes = { block_hash: event.block_hash };
        } else {
            this.lastHashes = {
                ledger_hash: event.ledger_hash,
                actions_hash: event.actions_hash,
                contract_hash: event.contract_hash
            };
        }
    }

    clearAppliedBlockWork(){
        // Remove pending hashes and fallback timers at or below the verified tip.
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
    }

    checkpointQuorumIsDue(){
        // Check whether the verified tip reaches the next checkpoint interval.
        return this.dbType === 'indexer' && this.config['VERIFY_CHECKPOINT_QUORUM']
            && (this.lastAppliedBlock - (this._lastCheckpointVerifyBlock || 0)) >= this.config['CHECKPOINT_VERIFY_INTERVAL'];
    }

    async applyBlockEvent(event){
        // Refuse new blocks while the client is halted on a consensus divergence.
        if(this._halted){
            getLogger().error('Refusing to apply block ' + (event && event.block_index) +
                '; client is HALTED on a consensus divergence at block ' + this._halted.blockIndex);
            return;
        }
        // Check train activation before writing any part of the block.
        if(await this.checkTrainActivation(event.block_index)) return;
        try {
            await this.withApplyLock(() => this.applier.applyBlock(event));
            let computedRoots = this.carryUnverifiedRoots(event);
            if(this.dbType === 'indexer' && this.config['VERIFY_RECOMPUTE']){
                let mismatches = await this.verifyRecompute(event, null, { recordUnverified: true, holdTip: true });
                if(mismatches){
                    await this.haltOnDivergence(event.block_index, mismatches, this.sources.slice(0, 1), 'local-recompute-divergence');
                    return;
                }
            }
            if(this.dbType === 'indexer' && this.config['VERIFY_STATE_HASH'] !== false && event.state_hash != null){
                let delay = activationDelayBlocks(this.chain);
                let localState = await this.blockHasher.computeStateHash(
                    event.block_index, (delay === undefined) ? null : delay, gasTickSymbol(), this.network, this.coinTicker);
                if(localState !== event.state_hash){
                    let mismatch = { field: 'state_hash', a: event.state_hash, b: localState };
                    try {
                        let preimage = await this.blockHasher.computeStateHashPreimage(
                            event.block_index, (delay === undefined) ? null : delay, gasTickSymbol(), this.network, this.coinTicker);
                        this.addStateHashClassDetails(mismatch, preimage);
                    } catch(e){
                        getLogger().error(util.format('state_hash local class detail failed at block ' + event.block_index + ':', e));
                    }
                    await this.haltOnDivergence(event.block_index, [mismatch], this.sources.slice(0, 1), 'state-hash-divergence');
                    return;
                }
            }
            if(this.dbType === 'indexer' && this.config['VERIFY_STATE_COMMITMENT'] !== false
                    && !this.isTruncated() && computedRoots){
                let divergence = this.stateCommitmentDivergence(event, computedRoots);
                if(divergence){
                    if(divergence.reason === 'state-commitment-missing')
                        await this.haltOnDivergence(event.block_index, divergence.mismatches, this.sources.slice(0, 1), 'state-commitment-missing');
                    else
                        await this.haltOnDivergence(event.block_index, divergence.mismatches,
                            this.sources.slice(0, 1), 'state-commitment-divergence');
                    return;
                }
            }
            this.finishAppliedBlock(event);
            this.clearAppliedBlockWork();
            if(this.checkpointQuorumIsDue()){
                this._lastCheckpointVerifyBlock = this.lastAppliedBlock;
                await this.verifyCheckpointQuorum();
            }
        } catch(e){
            getLogger().error(util.format('Error applying block %s:', event.block_index, e));
            // Heal schema gaps and let later status events drive catch-up.
            await this.healSchemaIfStale(e);
        }
    }

    // SPV: verify the source's latest signed checkpoint quorum against the pinned validator
    // set and assert its state_root equals the replica's own recomputed root at that height.
    // Inert without a pinned set or checkpoint; halts only on a real quorum or root failure.
    async verifyCheckpointQuorum(){
        if(this._halted) return;
        let validators = getPinnedValidators(this.chain, this.network);
        if(!validators || !validators.length) return;            // inert: no out-of-band trust root
        // The anchor comes from the out-of-band URL when set, so the audited source cannot withhold it.
        let source = this.config['CHECKPOINT_ANCHOR_URL'] || this.sources[0];
        if(!source) return;
        let cp;
        try {
            let url = source + '/checkpoint/indexer/' + this.chain + '/' + this.network + '/latest';
            let resp = await axios.get(url, { headers: this.upstreamHeaders(), timeout: 10000 });
            cp = resp && resp.data;
        } catch(e){
            this.warnCheckpointFetchFailure(source, e);
            return;
        }
        if(!cp) return;
        if(this.checkpointSkipsAnchoring(cp)) return;
        let freshnessMismatches = this.checkpointFreshnessMismatches(cp);
        if(freshnessMismatches){
            await this.haltOnDivergence(cp.block_index, freshnessMismatches,
                this.sources.slice(0, 1), 'checkpoint-freshness-stale');
            return;
        }
        if(cp.block_index > this.lastAppliedBlock) return;       // not caught up to this checkpoint yet
        let q = checkpointVerifier.verifyCheckpoint(cp, validators);
        if(!q.valid){
            let seed = getPinnedCheckpoint(this.chain, this.network);
            if(seed){
                let r = await this.followCheckpointForward(cp, seed, source);
                let rotationMismatches = this.processRotationFollow(cp, source, r);
                if(!rotationMismatches) return;
                await this.haltOnDivergence(cp.block_index, rotationMismatches,
                    this.sources.slice(0, 1), 'checkpoint-quorum-divergence');
                return;
            }
            await this.haltOnDivergence(cp.block_index,
                [{ field: 'checkpoint_quorum', a: 'quorum-signed', b: 'INVALID under pinned set' }],
                this.sources.slice(0, 1), 'checkpoint-quorum-divergence');
            return;
        }
        // Its committed roots must equal the replica's OWN recomputed roots at that height.
        let cmp = await this.checkpointRootsMatchLocal(cp);
        if(cmp.status === 'missing') return;                     // height not recomputed here (truncated bootstrap)
        if(cmp.status === 'mismatch'){
            await this.haltOnDivergence(cp.block_index, cmp.mismatches,
                this.sources.slice(0, 1), 'checkpoint-quorum-divergence');
            return;
        }
        this.recordVerifiedCheckpointSeq(cp.checkpoint_seq);
        getLogger().info('Checkpoint-quorum anchor OK: ' + this.chain + '/' + this.network +
            ' block ' + cp.block_index + ' (seq ' + cp.checkpoint_seq + ', ' + q.validSigs +
            ' valid sigs, weighted=' + q.weighted + ')');
    }

    // A transport fault or 404 never halts, but the failed refresh stays visible.
    warnCheckpointFetchFailure(source, error){
        getLogger().warn('Checkpoint-quorum anchor: failed to fetch checkpoint for ' + this.chain + '/' +
            this.network + ' from ' + source + ' (' + error.message + '); anchor not refreshed this cycle');
    }

    // True when the checkpoint cannot be anchored this cycle: rootless, malformed, or a seq
    // regression. Rootless at or above the flag-day is logged; none of these ever halts.
    checkpointSkipsAnchoring(cp){
        if(checkpointVerifier.commitmentMissing(cp)){
            getLogger().warn('Checkpoint-quorum anchor: source served a ROOTLESS checkpoint for ' +
                this.chain + '/' + this.network + ' at block ' + cp.block_index +
                ' (seq ' + cp.checkpoint_seq + ', snapshot_block ' + cp.snapshot_block +
                '), at/above the checkpoint-commitment flag-day where the federation never signs one; ' +
                'source may be withholding anchorable checkpoints, not anchoring this cycle');
            return true;
        }
        if(cp.state_root == null) return true;                   // pre-commitment: nothing to anchor
        if(typeof cp.block_index !== 'number') return true;      // malformed: no height to anchor
        // A genuine federation sequence only advances, so a lower seq means the source rewound.
        if(this._lastVerifiedCheckpointSeq !== null && typeof cp.checkpoint_seq === 'number'
                && cp.checkpoint_seq < this._lastVerifiedCheckpointSeq){
            getLogger().warn('Checkpoint-quorum anchor: seq regression for ' + this.chain + '/' + this.network +
                ' (served seq ' + cp.checkpoint_seq + ' < last verified ' + this._lastVerifiedCheckpointSeq +
                '); source may be withholding newer checkpoints, not anchoring');
            return true;
        }
        return false;
    }

    // Freshness is advisory by default. Strict mode returns a mismatch after one checkpoint
    // verifies so the caller can halt on the stale anchor.
    checkpointFreshnessMismatches(cp){
        if(!(this.lastAppliedBlock - cp.block_index > this.config['CHECKPOINT_FRESHNESS_BLOCKS'])) return null;
        getLogger().warn('Checkpoint-quorum anchor: stale anchor for ' + this.chain + '/' + this.network +
            ' (latest checkpoint at ' + cp.block_index + ', replica tip ' + this.lastAppliedBlock +
            ', >' + this.config['CHECKPOINT_FRESHNESS_BLOCKS'] + ' blocks behind); tail past it is unanchored');
        if(this.config['CHECKPOINT_FRESHNESS_STRICT'] && this._lastVerifiedCheckpointSeq !== null){
            return [{ field: 'checkpoint_freshness', a: 'tip ' + this.lastAppliedBlock,
                b: 'newest anchor ' + cp.block_index + ' (>' + this.config['CHECKPOINT_FRESHNESS_BLOCKS'] + ' behind)' }];
        }
        return null;
    }

    // Record a successful rotation follow, return divergence mismatches, or log a wait.
    processRotationFollow(cp, source, result){
        if(result.verdict === 'ok'){
            this.recordVerifiedCheckpointSeq(cp.checkpoint_seq);
            getLogger().info('Checkpoint-quorum anchor OK (rotation-followed): ' + this.chain + '/' +
                this.network + ' block ' + cp.block_index + ' (seq ' + cp.checkpoint_seq + ')');
            return null;
        }
        if(result.verdict === 'divergence') return result.mismatches;
        getLogger().warn('Checkpoint-quorum anchor: cannot follow validator rotation to ' + this.chain + '/' +
            this.network + ' block ' + cp.block_index + ' (seq ' + cp.checkpoint_seq + ') via ' + source +
            ': ' + (result.reason || 'inconclusive') + '; anchor not refreshed this cycle');
        return null;
    }

    // Advance the high-water mark of verified checkpoint sequences. Monotonic: a later
    // verify never lowers it, so a subsequent regressed seq is rejected by the anchor.
    recordVerifiedCheckpointSeq(seq){
        if(typeof seq !== 'number') return;
        if(this._lastVerifiedCheckpointSeq === null || seq > this._lastVerifiedCheckpointSeq)
            this._lastVerifiedCheckpointSeq = seq;
    }

    // Compare a checkpoint's committed roots to the replica's OWN recomputed
    // state_tree_roots row. Returns { status: 'match'|'mismatch'|'missing', mismatches }.
    async checkpointRootsMatchLocal(c){
        let rows = await this.db.getStateTreeRootByBlock(c.block_index);
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
    async oraclePublishSetAt(snapshotBlock){
        const caps = btcStakeCapabilities();
        const cap  = 'oracle_publish';
        // Pass the ticker: the source-cap and collation gates are keyed '<TICKER>:<network>'.
        const rows = await this.db.getStakeWeightsByCapabilityAsOf(cap, snapshotBlock, caps[cap], VALIDATOR_QUERY_LIMIT, this.coinTicker, this.network);
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
    //   { verdict: 'wait', reason }       inconclusive (transport / incomplete range /
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
    // Fetch the range from the same out-of-band anchor as /latest (`source`), so the
    // audited source cannot withhold the rotation chain either.
    async followCheckpointForward(cp, seed, source){
        let rangeSource = source || this.config['CHECKPOINT_ANCHOR_URL'] || this.sources[0];
        // Signer sets live only in BTC's stakes, so other chains cannot follow rotation
        if(this.coinTicker !== 'BTC') return { verdict: 'wait', reason: 'rotation following is BTC-only' };
        if(!seed || seed.state_root == null || typeof seed.block_index !== 'number')
            return { verdict: 'wait', reason: 'pinned seed checkpoint is malformed' };
        if(cp.block_index <= seed.block_index) return { verdict: 'wait', reason: 'checkpoint is not past the pinned seed' };

        // Bootstrap: the seed is the out-of-band trust root; the replica's own recompute
        // at seed.block_index must match it, else the replica is on a different chain.
        let seedCmp = await this.checkpointRootsMatchLocal(seed);
        if(seedCmp.status === 'missing')
            return { verdict: 'wait', reason: 'seed height ' + seed.block_index + ' not recomputed locally yet' };
        if(seedCmp.status === 'mismatch') return { verdict: 'divergence', mismatches: seedCmp.mismatches };

        let trusted = seed, from = seed.block_index + 1, guard = 0;
        while(trusted.block_index < cp.block_index){
            if(++guard > 10000) return { verdict: 'wait', reason: 'runaway guard tripped' };
            let fetched = await this.fetchCheckpointRange(rangeSource, from, cp.block_index);
            if(fetched.verdict) return fetched;

            let advanced = false;
            for(let next of fetched.chain){
                if(typeof next.block_index !== 'number' || next.block_index <= trusted.block_index) continue;
                let stop = await this.attestRotationStep(next, trusted);
                if(stop) return stop;
                trusted = next; from = next.block_index + 1; advanced = true;
                if(trusted.block_index >= cp.block_index) break;
            }
            // Stop when the range held nothing usable past the trusted frontier
            if(!advanced) return { verdict: 'wait', reason: 'range had nothing usable past block ' + trusted.block_index };
        }

        if(trusted.block_index === cp.block_index
                && String(trusted.state_root).toLowerCase() === String(cp.state_root).toLowerCase())
            return { verdict: 'ok' };
        return { verdict: 'wait', reason: 'walk ended at block ' + trusted.block_index + ', short of the checkpoint' };
    }

    // Fetch the signed-checkpoint chain over [from, to] from the anchor source.
    // Returns { chain } with at least one row, or a 'wait' verdict (never a divergence).
    async fetchCheckpointRange(rangeSource, from, to){
        let chain;
        try {
            let url = rangeSource + '/checkpoint/indexer/' + this.chain + '/' + this.network +
                      '/range?from=' + from + '&to=' + to;
            let resp = await axios.get(url, { headers: this.upstreamHeaders(), timeout: 10000 });
            chain = resp && resp.data && resp.data.checkpoints;
        } catch(e){                                              // transport: not a divergence
            return { verdict: 'wait', reason: 'range fetch failed (' + e.message + ')' };
        }
        // An empty range cannot reach the checkpoint (the source may be withholding it)
        if(!Array.isArray(chain) || !chain.length)
            return { verdict: 'wait', reason: 'empty checkpoint range from ' + from };
        return { chain };
    }

    // Verify one rotation step `next` against the current trust root `trusted`.
    // Returns null when `next` is attested and extends the frontier, else the verdict to stop on.
    async attestRotationStep(next, trusted){
        // Stop at a checkpoint past the replica tip, since it is not recomputed here yet
        if(next.block_index > this.lastAppliedBlock)
            return { verdict: 'wait', reason: 'range checkpoint ' + next.block_index + ' is past the replica tip' };
        // Stop at a rootless (pre-commitment) checkpoint, since it anchors nothing
        if(next.state_root == null)
            return { verdict: 'wait', reason: 'pre-commitment checkpoint ' + next.block_index + ' in range' };
        // The set that signs `next` is the oracle_publish set at next.snapshot_block;
        // trust it only once that height is attested by the current trust root.
        if(typeof next.snapshot_block !== 'number' || next.snapshot_block > trusted.block_index)
            return { verdict: 'wait', reason: 'signer set at snapshot ' + next.snapshot_block + ' not yet attested' };
        let vset = await this.oraclePublishSetAt(next.snapshot_block);
        if(!checkpointVerifier.verifyCheckpoint(next, vset).valid)
            return { verdict: 'divergence', mismatches: [{ field: 'checkpoint_quorum',
                a: 'quorum-signed (federation)',
                b: 'INVALID at block ' + next.block_index + ' under the authoritative oracle_publish set at snapshot ' + next.snapshot_block }] };
        // Attest `next` so its rows extend the trusted frontier for the next step.
        let cmp = await this.checkpointRootsMatchLocal(next);
        if(cmp.status === 'missing')
            return { verdict: 'wait', reason: 'step height ' + next.block_index + ' not recomputed locally yet' };
        if(cmp.status === 'mismatch') return { verdict: 'divergence', mismatches: cmp.mismatches };
        return null;
    }

    ignoreReorgWithoutTip(event){
        if(this.lastAppliedBlock === null){
            getLogger().warn('Ignoring reorg for ' + this.chain + '/' + this.network +
                ': no committed tip yet (replica empty); a reorg has nothing to roll back');
            return true;
        }
        return false;
    }

    ignoreReorgAboveTip(event){
        if(this.lastAppliedBlock !== null && event.block_index > this.lastAppliedBlock){
            getLogger().warn('Ignoring reorg for ' + this.chain + '/' + this.network +
                ': target block ' + event.block_index + ' is above the replica tip (' +
                this.lastAppliedBlock + '); a reorg to an unapplied block is a no-op');
            return true;
        }
        return false;
    }

    async haltForExcessiveRollback(event, depth){
        await this.haltOnDivergence(event.block_index,
            [{ field: 'rollback_depth', depth, max: this.maxRollbackDepth }],
            this.sources.slice(0, 1), 'max-rollback-depth-exceeded');
    }

    async applyReorgRollback(event){
        await this.withApplyLock(() => this.rollback.rollback(event.block_index));
        // The source rewrote dispensers off-stream for the orphaned blocks, so arm a reconcile
        // for the next status tick or catch-up rather than waiting out the interval.
        if(this.dbType === 'decoder') this._dispenserReconcileAfterReorg = true;
        this.lastAppliedBlock = event.block_index - 1;
        // Reload the new tip's hashes, genesis block 0 included (a null tip turns off the gap check).
        if(this.lastAppliedBlock >= 0)
            this.lastHashes = await this.db.getBlockHashRow(this.lastAppliedBlock);
        else
            this.lastHashes = null;
    }

    async haltForReorgRollbackFailure(event, error){
        getLogger().error(util.format('Reorg rollback failed for %s/%s (%s) rewinding to block %s:',
            this.chain, this.network, this.dbType, event.block_index, error));
        await this.haltOnDivergence(event.block_index,
            [{ field: 'reorg_rollback_failed', error: String(error && error.message ? error.message : error) }],
            this.sources.slice(0, 1), 'reorg-rollback-failed');
    }

    async handleReorg(event){
        getLogger().info('Reorg event received for ' + this.chain + '/' + this.network + ' at block ' + event.block_index);

        if(this.ignoreReorgWithoutTip(event)) return;
        if(this.ignoreReorgAboveTip(event)) return;

        let depth = this.rollbackGuard.depthFor(this.lastAppliedBlock, event.block_index);
        if(depth > this.maxRollbackDepth){
            await this.haltForExcessiveRollback(event, depth);
            return;
        }

        try {
            this.rollbackGuard.record(this.lastAppliedBlock, event.block_index);
            await this.applyReorgRollback(event);
        } catch(e){
            await this.haltForReorgRollbackFailure(event, e);
        }
    }
}

// Exposed for the WS-chain escalation tests and any caller that needs to
// distinguish permanent bootstrap exhaustion from transient sync errors. Hung on
// the class rather than on module.exports so the file has ONE export shape.
ClientSync.BootstrapExhaustedError = BootstrapExhaustedError;

module.exports = ClientSync;
