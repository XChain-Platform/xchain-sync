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
 * chain-of-state hashes).
 *
 ********************************************************************/

const dotenv      = require('dotenv');
const express     = require('express');
const helmet      = require('helmet');
const cors        = require('cors');
const http        = require('http');
const WebSocket   = require('ws');
const { rateLimit, ipKeyGenerator } = require('express-rate-limit');
const config      = require('./config');
const { computeArmedMapFingerprint } = require('./consensus/armed_map/fingerprint');
const { carrierLogicDigest } = require('./health/carrier_logic');
const SyncService = require('./sync_service');
const Utility     = require('./util');
const BlockHasher = require('./client/block_hasher');
const { createApiKeyMiddleware, safeEqual } = require('./http/middleware');
const { createShutdown, createSyncDrain } = require('./http/shutdown');
const { getReplicatedTables, missingReplicatedTables } = require('./schema/replicated_tables');
const coins       = require('./coins');

// Stateless helper for the advisory index-map parity checksum published on
// /status (server mode). getDataHash holds no per-call state, so one shared
// instance is safe. See BlockHasher.computeIndexMapChecksum (NON-consensus).
const statusUtil = new Utility();

// The armed-map identity every /health body carries: fingerprint v2 in the legacy field,
// the version that names the algorithm, and the logic digest beside it. The _v2 alias of
// the W1 to W4 window is gone since W5 (activation-registry C4).
function consensusIdentityFields(){ return { armed_map_fingerprint: computeArmedMapFingerprint().hex, armed_map_fingerprint_version: 2, carrier_logic_digest: carrierLogicDigest() }; }

function hubConsensusHashFields(syncService){
    let hub = syncService.hubClient;
    return {
        hub_consensus_hash_mismatch: hub && typeof hub.hubConsensusHashMismatch === 'boolean'
            ? hub.hubConsensusHashMismatch : null,
        hub_consensus_hash_mismatch_details: hub && Array.isArray(hub.hubConsensusHashMismatchDetails)
            ? hub.hubConsensusHashMismatchDetails : []
    };
}

dotenv.config();

// Before anything else logs. checkStartupEnv's console.error calls are exactly
// the lines an operator needs levelled and timestamped, and installObservability
// does not run until well past module load, so the patch has to land here.
const { patchConsole, getLogger } = require('./observability');
patchConsole({ service: 'xchain-sync', version: require('../package.json').version });

const REQUIRED_ENV = ['HUB_API_HOST'];

const cfg = config.getConfig();

// Startup-only environment gate. Kept out of module scope so that requiring
// this file (the security suite does, to reach the seams below) cannot exit the
// process; it runs from the entry-point guard at the bottom instead.
function checkStartupEnv(){
    for(const key of REQUIRED_ENV){
        if(!config.readEnvNow(key)){
            console.error('Missing required environment variable: ' + key);
            process.exit(1);
        }
    }

    // SYNC_API_KEY is optional, matching the other services: unset leaves the
    // REST/WS replication endpoints open (single-host / regtest / managed
    // deployments; xchain-node injects no key) and is warned about loudly here;
    // when configured, every endpoint fails closed (401) without it. The
    // destructive /halt/clear admin route additionally refuses to run at all
    // while no key is configured.
    if(!cfg['SYNC_API_KEY'])
        console.warn('WARNING: SYNC_API_KEY is not set. The REST/WS API is UNAUTHENTICATED and /halt/clear is disabled. Set a key for any shared or public-facing deployment.');
}

// How many reverse-proxy hops express should believe when it resolves req.ip.
// ONE hop, never `true`: the only proxy in front of this process is the
// co-located Apache that terminates TLS on the same box and appends the real
// client address to X-Forwarded-For, so counting back exactly one entry lands
// on an address Apache observed rather than one the client typed. `true`
// trusts the whole chain, which lets any caller prepend a fake X-Forwarded-For
// and mint an unlimited number of rate-limit buckets; express-rate-limit
// rejects that setting outright (ERR_ERL_PERMISSIVE_TRUST_PROXY). With
// TRUST_PROXY unset the header is ignored entirely and req.ip is the socket
// peer, which is right for a directly-exposed process.
function trustProxyHops(trustProxy){
    return trustProxy ? 1 : false;
}

// Key snapshot limits per (client IP + chain/network/dbType), NOT per IP alone.
// A single replica bootstraps every chain it follows from one IP, so a global
// per-IP bucket would let one chain's snapshot exhaust the budget and 429 all the
// others. block-height (incremental /since/:blockHeight) is intentionally excluded
// so the bucket is stable per resource across catch-ups.
// ipKeyGenerator collapses an IPv6 address to its /56 network so a single
// allocation cannot rotate through addresses for a fresh budget each time; it is
// a no-op for IPv4 and for IPv4-mapped peers.
const snapshotKey = (req) => ipKeyGenerator(req.ip) + '|' + req.params.dbType + '/' + req.params.chain + '/' + req.params.network;

// Built here rather than inline so the security suite can exercise the same
// limiter instances the service runs with, instead of a re-declaration of them.
function createRateLimiters(cfg){

    // App-wide per-IP backstop limiter, mirroring the peer services (explorer,
    // hub, indexer, decoder, encoder, utxo-tracker all front-load one). Without
    // it the non-snapshot routes (/schema, /status, /catalog, /health,
    // /validator-status) are unbounded and, when SYNC_API_KEY is unset (the
    // documented default), unauthenticated - so anonymous traffic can drive
    // information_schema / COUNT(*) scans on the source MariaDB the replication
    // poller depends on. The tighter per-route snapshot buckets below still
    // apply on top of this. Generous default so legitimate replica polling is
    // unaffected; override with SYNC_RATE_LIMIT_RPM.
    const backstopLimiter = rateLimit({
        windowMs:        60 * 1000,
        limit:           parseInt(config.readEnvNow('SYNC_RATE_LIMIT_RPM'), 10) || 500,
        standardHeaders: true,
        legacyHeaders:   false,
        message:         { error: 'Too many requests', code: 'RATE_LIMITED' },
    });

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

    return { backstopLimiter, fullSnapshotLimiter, incrSnapshotLimiter, transparencyLimiter, heartbeatLimiter };
}

// Carry the poller's replication verdict onto a /status row.
// source_height comes from the SERVED database, so on a node fronting a native
// SQL replica the source and served heights freeze together when replication
// stops applying and lag_blocks computes 0 on an hours-behind node. Withhold the
// number rather than publish a zero the replication engine contradicts: every
// consumer keying off `lag_blocks === 0` would otherwise certify the node.
// Fail closed: only an explicit boolean verdict is a measurement, so a missing
// poller status (before the first poll) or one without the field reads stale.
function applyReplicaFreshness(row, pollerStatus){
    row.replica_seconds_behind = (pollerStatus && pollerStatus.replica_seconds_behind !== undefined)
                                     ? pollerStatus.replica_seconds_behind : null;
    let measured = !!pollerStatus && typeof pollerStatus.replica_stale === 'boolean';
    row.replica_stale = measured ? pollerStatus.replica_stale : true;
    if(row.replica_stale) row.lag_blocks = null;
    return row;
}

async function applyProtocolHaltFreshness(row, db, dbType){
    if(!db || typeof db.getActiveHalt !== 'function'){
        row.replica_halted = null;
        return row;
    }
    try {
        row.replica_halted = !!(await db.getActiveHalt(dbType));
    } catch(e){
        row.replica_halted = null;
        row.replica_stale = true;
        row.lag_blocks = null;
        return row;
    }
    if(row.replica_halted){
        row.replica_stale = true;
        row.lag_blocks = null;
    }
    return row;
}

// One /health databases[] row for a chain, and the verdict it implies.
//
// Module-scope and exported for the same reason buildStatusRow is: this is the
// shape the Docker probe judges the container on, so it has to be checkable
// without binding a port.
//
// The circuit breaker only opens after circuitThreshold (10) consecutive
// acquisition failures (db.js), so a dead origin DB would otherwise read
// 'healthy' for up to 10 BLOCK_POLL_INTERVAL cycles while every snapshot request
// is already 500ing. ServerPoller's pollErrorCount resets to 0 on the next
// successful poll, so a non-zero count means the poller is currently in a failing
// streak: the earliest reliable outage signal.
//
// A halted CLIENT applies no blocks at all, and neither of those signals can see
// it: getPoller is server-mode only, so pollErrorCount is a constant 0 on a
// client, and a durable halt leaves the database perfectly healthy with its
// circuit closed. So this route, the one ModuleService points the sync
// container's probe at, answered 200 'healthy' for a replica that had stopped
// replicating and stayed stopped across reboots. Read the halt through
// getClientSyncState, the accessor /status already trusts. Degrading on an
// INTENTIONAL halt is safe too: the sync healthcheck entry carries no autoheal
// flag, so an unhealthy verdict marks the container and never restarts it out
// from under the operator investigating the divergence.
function buildHealthEntry(syncService, mode, db, coin, network, dbType){
    let poller = syncService.getPoller(coin, network, dbType);
    let entry = {
        chain: coin, network: network, dbType: dbType,
        circuit: (db && db.circuitState) || null,
        poll_error_count: poller ? poller.pollErrorCount : 0
    };
    if(mode !== 'server' && typeof syncService.getClientSyncState === 'function'){
        let clientState = syncService.getClientSyncState(coin, network, dbType);
        let halted = !!(clientState && clientState.halted);
        entry.halted      = halted;
        entry.halt_reason = (halted && clientState.haltInfo) ? clientState.haltInfo.reason : null;
        entry.halt_block  = (halted && clientState.haltInfo) ? clientState.haltInfo.blockIndex : null;
    }
    return entry;
}

// Degraded verdict for one /health row. Kept beside the builder so the probe's
// contract is one readable predicate rather than a condition spread over a loop.
function healthEntryDegraded(entry){
    return entry.circuit === 'open' || entry.poll_error_count > 0 || entry.halted === true;
}

function clientMissingTables(syncService, chain, network, dbType){
    let sync = (typeof syncService.getClientSync === 'function')
        ? syncService.getClientSync(chain, network, dbType) : null;
    return (sync && typeof sync.getMissingTables === 'function')
        ? sync.getMissingTables() : null;
}

// Read through getStatus so the accessor can expire stale measurements.
// Reject a cached freshness verdict after SYNC_STATUS_MAX_AGE_MS.
// Avoid reading statusData directly because it cannot perform that expiry.

// Preserve the two heights stored by ServerPoller.updateStatus.
// Separate its polled block position from the source database tip.
function getServerPollerStatus(broadcaster, chain, network, dbType){
    return (broadcaster && typeof broadcaster.getStatus === 'function')
        ? broadcaster.getStatus(chain, network, dbType || 'indexer') : null;
}

// Treat block_height as the broadcaster's last-polled position.
// Keep it distinct from the source database tip in server mode.
// Expose how far the poller has actually broadcast.

// Avoid hiding a wedged or catching-up poller behind a climbing source tip.
// Match the WebSocket updateStatus semantics used by ServerPoller.
// Separate lastPolledBlock from the source tip on the REST surface.
function createServerStatusRow(pollerStatus, polledBlock, sourceBlock, hashRow){
    return {
        block_height:  polledBlock,
        source_height: sourceBlock,
        lag_blocks:    (sourceBlock !== null && polledBlock !== null)
                           ? Math.max(0, sourceBlock - polledBlock) : null,
        block_time:    hashRow ? Number(hashRow.block_time) : null,
        poll_error_count: (pollerStatus && pollerStatus.poll_error_count != null)
                           ? pollerStatus.poll_error_count : 0
    };
}

function applyServerHashFields(row, hashRow, dbType){
    if(dbType === 'decoder'){
        row.block_hash = hashRow ? hashRow.block_hash : null;
    } else {
        row.ledger_hash   = hashRow ? hashRow.ledger_hash : null;
        row.actions_hash  = hashRow ? hashRow.actions_hash : null;
        row.contract_hash = hashRow ? hashRow.contract_hash : null;
    }
}

// Compute advisory id-to-address parity over deterministic source rows.
// Bound the calculation to the polled height published by this status row.
// Let a follower recompute the checksum at that exact height.

// Detect divergent maps that resolved-string hashes and row counts miss.
// Catch equal-size maps whose address content differs.
// Keep this non-consensus signal independent of the three primary hashes.

// Leave the subset scan disabled by default because of its cost.
// Return null so followers skip the comparison when the check is disabled.
function getIndexMapChecksum(db, polledBlock){
    return new BlockHasher(db, statusUtil).computeIndexMapChecksum(polledBlock);
}

// Advisory tokens fold-column parity (NON-consensus, default off), the only
// check that sees an ISSUE edit or its reorg reversal missing on a replica.
// See BlockHasher.computeTokenFoldChecksum; null => follower skips.
function getTokenFoldChecksum(db, polledBlock){
    return new BlockHasher(db, statusUtil).computeTokenFoldChecksum(polledBlock);
}

// Publish advisory table-content parity for both database types.
// Cover decoder tables that have no synthetic content hashes.
// Keep this non-consensus check disabled by default.

// Bound parity to the same polled height published by this status row.
// Carry the window and lookup-id ceilings used by the source.
// Let followers recompute over identical bounds rather than their own tails.

// Return null when disabled so followers skip the comparison.
// Preserve a content commitment for replicated decoder tables when enabled.
// Complement the primary hashes and the index-map checksum.
function getTableContentParity(db, polledBlock){
    return new BlockHasher(db, statusUtil)
        .computeTableContentChecksums(polledBlock, { window: cfg['TABLE_CONTENT_PARITY_WINDOW'] });
}

function getExistingTables(db){
    return db.listExistingTables();
}

function applyServerMissingTables(row, presentTables, dbType){
    // Companion to table_counts, which can only omit a table it cannot count:
    // an absent table looks exactly like a table nobody asked about.
    row.missing_tables = missingReplicatedTables(presentTables, dbType);
}

function applySnapshotStatus(row, syncService){
    // Lifetime full-snapshot serve count (incremented by SnapshotBuilder
    // on each successful streamFullSnapshot completion; 0 until first serve).
    let builder = syncService.getSnapshotBuilder();
    row.snapshots_served = builder ? (builder.snapshotsServed || 0) : 0;
    // Lifetime count of snapshot requests rejected 503 by the per-Database
    // concurrency cap; a growing value flags a bootstrap stampede.
    row.snapshots_rejected = builder ? (builder.snapshotsRejected || 0) : 0;
}

async function buildServerStatusRow(syncService, db, dbType, chain, network){
    let broadcaster = syncService.getBroadcaster();
    let pollerStatus = getServerPollerStatus(broadcaster, chain, network, dbType);
    let polledBlock = (pollerStatus && pollerStatus.block_height != null)
        ? pollerStatus.block_height : null;
    let sourceBlock = (pollerStatus && pollerStatus.source_block_height != null)
        ? pollerStatus.source_block_height : (await db.getLastBlock());
    let hashRow = polledBlock !== null ? await db.getBlockHashRow(polledBlock) : null;
    let row = createServerStatusRow(pollerStatus, polledBlock, sourceBlock, hashRow);
    applyReplicaFreshness(row, pollerStatus);
    await applyProtocolHaltFreshness(row, db, dbType);
    applyServerHashFields(row, hashRow, dbType);
    if(dbType !== 'decoder'){
        row.index_map_checksum = null;
        if(cfg['INDEX_MAP_PARITY_CHECK'] && polledBlock !== null){
            try { row.index_map_checksum = await getIndexMapChecksum(db, polledBlock); }
            catch(e){ console.error('[API] index_map_checksum compute failed for %s/%s at block %s (advisory, returning null):', chain, network, polledBlock, e.message); }
        }
        row.token_fold_parity = null;
        if(cfg['TOKEN_FOLD_PARITY_CHECK'] && polledBlock !== null){
            try { row.token_fold_parity = await getTokenFoldChecksum(db, polledBlock); }
            catch(e){ getLogger().error('[API] token_fold_parity compute failed for ' + chain + '/' + network +
                ' at block ' + polledBlock + ' (advisory, returning null): ' + e.message); }
        }
    }
    row.table_content_parity = null;
    if(cfg['TABLE_CONTENT_PARITY_CHECK'] && polledBlock !== null){
        try { row.table_content_parity = await getTableContentParity(db, polledBlock); }
        catch(e){ console.error('[API] table_content_parity compute failed for %s/%s at block %s (advisory, returning null):', chain, network, polledBlock, e.message); }
    }
    // Expose per-subscriber applied-block lag so operators can see a
    // validator falling behind before the backpressure limit force-closes it.
    row.subscribers = broadcaster ? broadcaster.getSubscribers(chain, network, dbType) : [];
    // Per-table row counts (same logic as client-mode path below)
    row.table_counts = {};
    let presentA = null;
    try { presentA = await getExistingTables(db); } catch(e){ /* fall back to probing */ }
    for(let table of getReplicatedTables(dbType)){
        if(presentA && !presentA.has(table)) continue;
        try {
            row.table_counts[table] = await db.getTableCount(table);
        } catch(e){
            // Raced away between the listing and the count; omit rather than fail.
        }
    }
    applyServerMissingTables(row, presentA, dbType);
    applySnapshotStatus(row, syncService);
    return row;
}

function createClientStatusRow(hashRow, dbType){
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
    return row;
}

function applyClientSourceStatus(row, clientState){
    let sourceHeight = clientState.lastKnownServerBlock;
    row.source_height = sourceHeight;
    row.lag_blocks    = (sourceHeight !== null && row.block_height !== null)
        ? Math.max(0, sourceHeight - row.block_height)
        : null;
    // Mark source_height and lag_blocks stale after live events stop.
    // Treat lastKnownServerBlock as frozen across a silent disconnect.
    // Avoid reporting a trustworthy zero lag after catching that stale tip.

    // Use null when no live event has established freshness.
    // Tell operators when the source height lacks recent confirmation.
    row.source_height_stale = clientState.sourceHeightStale;

    // Relay the upstream server's own replication verdict under distinct names.
    // Reserve replica_stale on a server row for that node's database.
    // Avoid giving a different client-side claim the same key.

    // Separate transport liveness from the upstream data-authority verdict.
    // Use source_height_stale to show whether the server is still speaking.
    // Use upstream_replica_stale to show whether its heights remain authoritative.

    // Preserve both signals when a stalled SQL replica keeps heartbeating.
    // Expose the frozen source even if its follower catches up and reports zero lag.
    // Keep source_height_stale false while reporting the upstream database stale.

    // Treat upstream_replica_stale as a tri-state value.
    // Return null when no connected source reports the field.
    // Avoid treating a server without the field or an unheard source as healthy.

    // Return true when any connected source marks its database stale.
    // Publish the upstream database tip through upstream_source_height.
    // Reveal broadcaster lag relative to the upstream source database.

    // Default callers without these fields to an unknown verdict.
    let upstream = clientState.upstreamReplica || {};
    row.upstream_replica_stale          = (upstream.stale === true || upstream.stale === false)
                                              ? upstream.stale : null;
    row.upstream_replica_seconds_behind = (upstream.secondsBehind != null) ? upstream.secondsBehind : null;
    row.upstream_source_height          = (upstream.sourceHeight != null) ? upstream.sourceHeight : null;
}

function applyClientStateStatus(row, clientState){
    // Consensus-divergence halt: a halted client has STOPPED applying and
    // requires operator clearance. Surfaced so the dashboard monitor and
    // peers see a forked/Byzantine validator immediately.
    row.halted = clientState.halted || false;
    if(clientState.halted) row.halt = clientState.haltInfo;
    // Publish the next-apply activation verdict as clear, pending, or halt.
    // Use pending when this build lacks a rule set required by the manifest.
    // Name the future halt height so monitors can alert before the boundary.

    // Return null until the follower evaluates or when the caller lacks the field.
    // Preserve unknown state rather than reporting a clear verdict.
    row.train_activation = clientState.trainActivation || null;
    // Truncated-replica visibility: lets an explorer or operator know
    // this replica cannot answer pre-base history queries.
    row.truncated      = clientState.truncated || false;
    row.bootstrap_base = clientState.bootstrapBase != null ? clientState.bootstrapBase : null;
}

function applyClientQuorumStatus(row, clientState){
    // Publish the multi-source Byzantine quorum and its M-of-N threshold.
    // Include configured, active, agreeing, and evicted source counts.

    // Let monitors detect weak operator diversity and source evictions.
    // Expose agreement on the last applied block.
    row.source_quorum      = clientState.sourceQuorum != null ? clientState.sourceQuorum : null;
    row.sources_configured = clientState.sourcesConfigured != null ? clientState.sourcesConfigured : null;
    row.sources_active     = clientState.sourcesActive != null ? clientState.sourcesActive : null;
    row.sources_agreeing   = clientState.sourcesAgreeing != null ? clientState.sourcesAgreeing : null;
    row.sources_evicted    = Array.isArray(clientState.sourcesEvicted) ? clientState.sourcesEvicted : [];
}

function applyClientGapStatus(row, syncService, chain, network, dbType){
    // Report persistent replica gaps beside the missing-table verdict.
    // Treat a non-empty array as replicated rows missing from this follower.
    // Preserve the empty array as the healthy and unsupported default.

    // Distinguish these gaps from source-computed hashes, which are replicated
    // verbatim and can still agree when follower rows are missing.
    // Surface the gap even when halted is false and lag_blocks is zero.

    // Publish only the count sweep's verdict after consecutive equal-height
    // sweeps confirm the shortfall.
    // Avoid alerting on transient count races against the source status row.

    // Read the verdict from ClientSync through the same accessor used by the
    // halt-clear endpoint.
    // Leave getClientSyncState unchanged because callers assert its exact shape.
    let liveSync = (typeof syncService.getClientSync === 'function')
        ? syncService.getClientSync(chain, network, dbType) : null;

    // Default to no alert when the caller does not support gap reporting.
    // Normalize non-array responses to the same empty verdict.
    // Keep the public field present for every client row.
    let gaps = (liveSync && typeof liveSync.getReplicaGaps === 'function')
        ? liveSync.getReplicaGaps() : [];
    row.replica_gaps = Array.isArray(gaps) ? gaps : [];
}

// Publish per-table row counts for replica-completeness verification.
// Expose an independent signal for rows that never reached the follower.
// Detect entire missing tables even when the committed hashes still agree.

// Distinguish these counts from hashes computed on the source.
// Describe the source's blockchain computation with those replicated hashes.
// Avoid treating them as proof of what actually landed downstream.

// Let ClientSync compare source counts against its own database.
// Flag tables that contain source rows but no follower rows.
// Keep the count object present before any database reads.

// Restrict the caller to the per-block replicated table set.
// Exclude snapshot-only and operator-local tables that may differ legitimately.
// Avoid false alarms from those deliberately divergent tables.

// Accept COUNT(*) here because status is an operator-polled endpoint.
function initializeClientTableCounts(row){
    row.table_counts = {};
}

function applyClientMissingTables(row, syncService, chain, network, dbType){
    // ClientSync owns the source-scoped verdict; null means either schema is unknown.
    row.missing_tables = clientMissingTables(syncService, chain, network, dbType);
}

// Live recompute errors hold the tip and are counted durably; publish the count
// so a replica stuck on a persistent recompute fault is visible.
async function applyUnverifiedRecompute(row, db, dbType){
    row.unverified_recompute_count = null;
    row.unverified_recompute_last_block = null;
    if(typeof db.getSyncState !== 'function') return;
    try {
        let count = await db.getSyncState('unverified_recompute_count:' + dbType);
        let last  = await db.getSyncState('unverified_recompute_last_block:' + dbType);
        row.unverified_recompute_count = count != null ? Number(count) : 0;
        row.unverified_recompute_last_block = last != null ? Number(last) : null;
    } catch(e){
        row.unverified_recompute_count = null;
    }
}

async function buildClientStatusRow(syncService, db, dbType, chain, network){
    // Client mode: block_height is whatever the replica DB has applied.
    let lastBlock = await db.getLastBlock();
    let hashRow = lastBlock !== null ? await db.getBlockHashRow(lastBlock) : null;
    let row = createClientStatusRow(hashRow, dbType);
    let clientState = syncService.getClientSyncState(chain, network, dbType);
    applyClientSourceStatus(row, clientState);
    applyClientStateStatus(row, clientState);
    applyClientQuorumStatus(row, clientState);
    applyClientGapStatus(row, syncService, chain, network, dbType);
    initializeClientTableCounts(row);
    // List tables once and avoid one failing query per absent table.
    // Apply the same missing-table rationale as the server path above.
    let present = null;
    try { present = await getExistingTables(db); } catch(e){ /* fall back to probing */ }
    for(let table of getReplicatedTables(dbType)){
        if(present && !present.has(table)) continue;
        try {
            row.table_counts[table] = await db.getTableCount(table);
        } catch(e){
            // Table absent in this schema (older replica, or decoder vs
            // indexer split); omit rather than fail the whole status.
        }
    }
    applyClientMissingTables(row, syncService, chain, network, dbType);
    await applyUnverifiedRecompute(row, db, dbType);
    return row;
}

// Build one status row for each database, type, chain, and network tuple.
// Keep the builder at module scope and outside startApi.
// Test the row shape against mock service and database providers.

// Expose the fields that monitoring consumers use as their contract.
// Preserve missing_tables as an alertable field on that public row.
// Verify the contract without starting the service.
async function buildStatusRow(syncService, db, dbType, chain, network){
    if(cfg['SYNC_MODE'] === 'server')
        return buildServerStatusRow(syncService, db, dbType, chain, network);
    return buildClientStatusRow(syncService, db, dbType, chain, network);
}

function createHealthHandler(syncService, cfg){
    return (req, res) => {
        if(typeof syncService.isReady === 'function' && !syncService.isReady()){
            res.status(503);
            return res.json({
                status:       'starting',
                mode:         cfg['SYNC_MODE'],
                databases:    [],
                hub_config_age_seconds: syncService.getHubConfigAgeSeconds(),
                ...consensusIdentityFields(),
                last_updated: new Date().toISOString(),
                ...hubConsensusHashFields(syncService)
            });
        }
        let databases = [];
        let degraded = false;
        for(let { coin, network, dbType } of syncService.getChains()){
            let db = syncService.getDatabase(coin, network, dbType);
            if(!db) continue;
            let entry = buildHealthEntry(syncService, cfg['SYNC_MODE'], db, coin, network, dbType);
            if(healthEntryDegraded(entry)) degraded = true;
            databases.push(entry);
        }
        if(degraded) res.status(503);
        res.json({
            status:       degraded ? 'degraded' : 'healthy',
            mode:         cfg['SYNC_MODE'],
            databases:    databases,
            hub_config_age_seconds: syncService.getHubConfigAgeSeconds(),
            ...consensusIdentityFields(),
            last_updated: new Date().toISOString(),
            ...hubConsensusHashFields(syncService)
        });
    };
}

// The REST surface, built over a SyncService-shaped provider and a config so the
// running service and the e2e harness mount the same middleware order, limiters
// and routes. Listening, the WebSocket upgrade path and process lifecycle stay in
// startApi.
function createApp(syncService, cfg, app = express()){

    // Must precede every limiter: they read req.ip, which express only derives
    // from X-Forwarded-For once this is set. Left unset (the state this service
    // shipped in), every request behind Apache resolves to 127.0.0.1 and the
    // snapshot buckets below become one shared global budget that any single
    // caller can drain for everyone.
    app.set('trust proxy', trustProxyHops(cfg['TRUST_PROXY']));
    app.use(helmet());
    app.use(cors({ origin: cfg['CORS_ORIGIN'], methods: ['GET', 'POST'] }));
    app.use(express.json({ limit: '16kb' }));
    app.use(createApiKeyMiddleware(cfg['SYNC_API_KEY']));

    const { backstopLimiter, fullSnapshotLimiter, incrSnapshotLimiter,
            transparencyLimiter, heartbeatLimiter } = createRateLimiters(cfg);

    app.use(backstopLimiter);

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

    // GET /health : lightweight liveness + DB circuit-breaker visibility.
    // /status reports per-chain block heights and lag, but not whether a
    // database connection has tripped its circuit breaker open after repeated
    // failures. When that happens the replicator stops applying blocks while the
    // process stays up, so a bare liveness probe still looks fine. This endpoint
    // surfaces the per-database circuit state so monitoring can tell a healthy
    // replicator apart from one stalled on a database outage.
    app.get('/health', createHealthHandler(syncService, cfg));

    // GET /status : all chains, nested by coin/network/dbType
    app.get('/status', (req, res) => {
        let chains = syncService.getChains();
        let result = {};
        let promises = chains.map(async ({ coin, network, dbType }) => {
            let db = syncService.getDatabase(coin, network, dbType);
            if(!db) return;
            let row = await buildStatusRow(syncService, db, dbType, coin, network);
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
            let row = await buildStatusRow(syncService, db, dbType, chain, network);
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

    app.get('/checkpoint/:dbType/:chain/:network/latest', incrSnapshotLimiter, async (req, res) => {
        let dbType = validateDbType(req.params.dbType);
        if(!dbType) return res.status(400).json({ error: "Invalid dbType. Must be 'indexer' or 'decoder'", code: 'BAD_REQUEST' });
        if(dbType !== 'indexer') return res.status(400).json({ error: 'Checkpoints exist only for indexer DBs', code: 'BAD_REQUEST' });
        let { chain, network } = req.params;
        let db = syncService.getDatabase(chain, network, dbType);
        if(!db) return res.status(404).json({ error: 'Chain/network/dbType not found', code: 'NOT_FOUND' });
        try {
            let rows = await db.getLatestCheckpoint();
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
            let rows = await db.findCheckpointsInRange(from, to, CHECKPOINT_RANGE_LIMIT);
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
            let rows = await db.getCheckpointAtHeight(h);
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
            let tables = await db.findBaseTableNames();
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

    // GET /snapshot-dispensers/:dbType/:chain/:network?after_tx=&after_addr=
    // The WHOLE decoder `dispensers` table in one statement-consistent response
    // (has_more always false) for the client's replace-table reconcile. dispensers
    // rides neither the block stream nor the id-cursor lookup paging (no monotonic id;
    // the decoder soft-expires/hard-purges rows), so the client periodically re-dumps
    // it and swaps it in atomically; see SnapshotBuilder.streamDispensers +
    // ClientSync.reconcileDispensers. The cursor params filter within that one query;
    // a `limit` param from an older client is ignored, and the response's only size
    // ceiling is the client's SNAPSHOT_MAX_CONTENT. Decoder-only; rate-limited as an
    // incremental fetch.
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

        let db = syncService.getDatabase(chain, network, dbType);
        if(!db) return res.status(404).json({ error: 'Chain/network/dbType not found', code: 'NOT_FOUND' });

        let builder = syncService.getSnapshotBuilder();
        if(!builder) return res.status(500).json({ error: 'Snapshot builder not initialized', code: 'INTERNAL_ERROR' });

        try {
            await builder.streamDispensers(db, afterTx, afterAddr, res);
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
            console.error('[API error] /transparency/.../proof/%s:', block_index, e);
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

    return app;
}

// API key check for WebSocket upgrades, enforced only when a key is configured
// because managed validators replicate keyless. Returns false after rejecting.
function authorizeUpgrade(request, socket){
    let apiKey = cfg['SYNC_API_KEY'];
    if(!apiKey) return true;
    let authHeader = request.headers['authorization'];
    if(!authHeader || !safeEqual(authHeader, 'Bearer ' + apiKey)){
        socket.write('HTTP/1.1 401 Unauthorized\r\n\r\n');
        socket.destroy();
        return false;
    }
    return true;
}

// Parses /subscribe/:dbType/:chain/:network[?sync_mode=full|infra-only].
// 'infra-only' is indexer-only; the decoder always serves the full table set.
function parseSubscribePath(url){
    let match = url.match(/^\/subscribe\/([^\/]+)\/([^\/]+)\/([^\/\?]+)(?:\?(.*))?/);
    if(!match) return null;

    let dbType  = match[1];
    let chain   = match[2];
    let network = match[3];
    if(dbType !== 'indexer' && dbType !== 'decoder') return null;

    let syncMode = 'full';
    if(match[4]){
        let qs = new URLSearchParams(match[4]);
        let mode = qs.get('sync_mode');
        if(mode === 'infra-only' && dbType === 'indexer') syncMode = 'infra-only';
    }
    return { dbType, chain, network, syncMode };
}

// A manual handleUpgrade does not emit 'connection', so the emit below runs the
// keepalive wiring that the ping interval depends on.
function createUpgradeHandler(syncService, wss){
    return function handleWebSocketUpgrade(request, socket, head){
        if(!authorizeUpgrade(request, socket)) return;

        let sub = parseSubscribePath(request.url);
        if(!sub){
            socket.destroy();
            return;
        }
        let { dbType, chain, network, syncMode } = sub;

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
            wss.emit('connection', ws, request);
            broadcaster.addSubscription(ws, request, chain, network, syncMode, dbType);
        });
    };
}

function startPingInterval(wss){
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
}

// Server-mode status broadcasts and stale validator-heartbeat eviction. The
// handles are returned so the shutdown drain can clear them.
function startBackgroundTimers(syncService){
    const backgroundTimers = [];
    if(cfg['SYNC_MODE'] === 'server'){
        backgroundTimers.push(setInterval(() => {
            let broadcaster = syncService.getBroadcaster();
            if(!broadcaster) return;
            let chains = syncService.getChains();
            for(let { coin, network, dbType } of chains){
                broadcaster.broadcastStatus(coin, network, dbType);
            }
        }, cfg['WS_STATUS_INTERVAL']));

        backgroundTimers.push(setInterval(() => {
            let broadcaster = syncService.getBroadcaster();
            if(broadcaster) broadcaster.evictStaleValidators(cfg['VALIDATOR_HEARTBEAT_TTL']);
        }, 30000));
    }
    return backgroundTimers;
}

// node is PID 1 in the image, so SIGTERM from `docker stop` reaches this handler.
// Its hard-exit timer (src/http/shutdown.js) bounds a hung drain.
function installShutdownHandlers(syncService, server, wss, backgroundTimers){
    const shutdown = createShutdown({
        drain: createSyncDrain({
            syncService: syncService,
            server:      server,
            wss:         wss,
            timers:      backgroundTimers
        })
    });
    process.on('SIGTERM', () => shutdown('SIGTERM'));
    process.on('SIGINT',  () => shutdown('SIGINT'));
}

// An uncaughtException leaves shared state unknown, so the process exits after
// logging; an unhandledRejection logs and continues.
function installCrashHandlers(){
    process.on('uncaughtException', (err) => {
        getLogger().error('CRASH', { kind: 'uncaughtException', err: err && err.message, stack: err && err.stack });
        process.exit(1);
    });
    process.on('unhandledRejection', (reason) => {
        const err = reason instanceof Error ? reason : new Error(String(reason));
        getLogger().error('CRASH', { kind: 'unhandledRejection', err: err.message, stack: err.stack });
    });
}

async function startApi(){
    // Pin check precedes any port, poller or source-DB handle; a null pin skips,
    // a mismatch on an armed network throws, uncaught.
    for(const net of coins.NETWORKS) coins.verifyConsensusPin(net);

    const syncService = new SyncService(cfg);
    const app = express();
    createApp(syncService, cfg, app);
    const server = http.createServer(app);

    const wss = new WebSocket.Server({ noServer: true });

    server.on('upgrade', createUpgradeHandler(syncService, wss));

    startPingInterval(wss);

    const backgroundTimers = startBackgroundTimers(syncService);

    server.listen(cfg['SYNC_API_PORT'], () => {
        console.log('xchain-sync API listening on port ' + cfg['SYNC_API_PORT']);
    });

    syncService.start().catch((error) => {
        console.error('Fatal SyncService error:', error);
        process.exit(1);
    });

    installShutdownHandlers(syncService, server, wss, backgroundTimers);
    installCrashHandlers();
}

// Only boot when this file IS the process entry point (`npm run api`, the
// Dockerfile CMD). Requiring it as a module - which the security suite does to
// reach the proxy-trust and rate-limit seams above - must not open ports or
// start polling a source database.
if(require.main === module){
    checkStartupEnv();
    startApi();
}

module.exports = { trustProxyHops, snapshotKey, createRateLimiters, applyReplicaFreshness, applyProtocolHaltFreshness,
                   buildHealthEntry, healthEntryDegraded, buildStatusRow, createApp, startApi };
