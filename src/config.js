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
 * XChain Indexer Sync - Configuration
 *
 * This file reads environment variables and returns a config object
 *
 ********************************************************************/

// Parse an integer from an env var, returning defaultVal when the value is
// absent, empty, or non-numeric.  Unlike `parseInt(x) || default`, this
// correctly preserves 0 as a valid value.
function parseIntSafe(val, defaultVal){
    if(val === undefined || val === null || val === '') return defaultVal;
    let parsed = parseInt(val, 10);
    return isNaN(parsed) ? defaultVal : parsed;
}

// Parse a non-negative integer, clamping to 0 at minimum.
function parseIntMin0(val, defaultVal){
    return Math.max(0, parseIntSafe(val, defaultVal));
}

// Parse a positive integer (>= 1).
function parseIntMin1(val, defaultVal){
    return Math.max(1, parseIntSafe(val, defaultVal));
}

module.exports = {

    getConfig: function(){
        let config = {};

        // Operating mode
        config['SYNC_MODE']     = process.env.SYNC_MODE || 'server';

        // API port
        config['SYNC_API_PORT'] = parseIntMin0(process.env.SYNC_API_PORT, 3006);

        // Hub connection (HUB_VALIDATORS takes priority over HUB_API_HOST:HUB_PORT)
        config['HUB_VALIDATORS'] = process.env.HUB_VALIDATORS || '';
        config['HUB_API_HOST']   = process.env.HUB_API_HOST;
        config['HUB_PORT']       = parseIntMin1(process.env.HUB_PORT, 10000);

        // Max time to wait for the hub at startup before giving up and
        // exiting non-zero (so a process supervisor can restart/alert).
        // Defaults to 5 minutes.
        config['MAX_HUB_WAIT_MS'] = parseIntMin0(process.env.MAX_HUB_WAIT_MS, 300000);

        // CORS
        config['CORS_ORIGIN']   = process.env.CORS_ORIGIN || false;

        // Server mode settings
        config['BLOCK_POLL_INTERVAL'] = parseIntMin0(process.env.BLOCK_POLL_INTERVAL, 3000);
        // A replica/validator follows EVERY chain a server hosts over its own WS, all
        // from one IP; node-host-b alone serves 12 chain/network/dbType streams. The old
        // default of 3 closed 9 of them with 1008 "Too many connections", causing a
        // permanent reconnect storm that also broke gap-detection-driven catch-up.
        config['WS_MAX_PER_IP']       = parseIntMin1(process.env.WS_MAX_PER_IP, 100);
        // Per-resource (IP + chain/network/dbType) limits; see snapshotKey in api.js.
        // Defaults give a multi-chain replica headroom for bootstrap retries (full) and
        // frequent gap catch-ups (incremental) instead of a single global-per-IP bucket
        // that starves other chains.
        config['SNAPSHOT_RATE_FULL']  = parseIntMin0(process.env.SNAPSHOT_RATE_FULL, 12);
        config['SNAPSHOT_RATE_INCR']  = parseIntMin0(process.env.SNAPSHOT_RATE_INCR, 600);

        // Per-chain replication exclude (client mode). Comma-separated list of
        // `coin:network:dbType` keys (e.g. `DOGE:testnet:indexer`) that the client
        // must NOT replicate. A discovered chain whose key is listed is skipped in
        // _discoverChains, so no ClientSync is started for it and it can never
        // crash-loop the process. Used to drop a chain that cannot full-snapshot
        // bootstrap (fast chains with tens of millions of blocks) until the
        // start-from-recent-height bootstrap (SYNC_BOOTSTRAP_DEPTH_*) is deployed.
        // Trimmed + deduplicated; empty/unset -> [] (no chain excluded).
        config['SYNC_EXCLUDE'] = [...new Set(
            (process.env.SYNC_EXCLUDE || '')
                .split(',')
                .map(s => s.trim())
                .filter(s => s.length > 0)
        )];

        // Per-chain start-from-recent-height bootstrap (client mode). Opt-in via
        // SYNC_BOOTSTRAP_DEPTH_<CHAIN>_<NETWORK>=N (e.g.
        // SYNC_BOOTSTRAP_DEPTH_DOGE_TESTNET=50000). When set for a chain, an empty
        // replica bootstraps from (sourceTip - N) using one incremental snapshot
        // instead of a full-history snapshot. This is the only way to seed a fast
        // chain whose full snapshot (tens of millions of blocks) cannot be buffered
        // and applied in one pass on the client. Absent -> unchanged full bootstrap
        // with full history + full verification.
        //
        // A truncated replica CANNOT answer pre-base history and its aggregate
        // balances are recent-window only; it is acceptable ONLY for a non-consensus
        // explorer mirror, never a trusted validator. Parsed into a map keyed by
        // 'CHAIN:NETWORK' (uppercased); values clamped to >= 1.
        let bootstrapDepth = {};
        for(let envKey in process.env){
            if(envKey.indexOf('SYNC_BOOTSTRAP_DEPTH_') !== 0) continue;
            let rest = envKey.slice('SYNC_BOOTSTRAP_DEPTH_'.length); // e.g. DOGE_TESTNET
            let sep = rest.lastIndexOf('_');
            if(sep <= 0 || sep >= rest.length - 1) continue; // need CHAIN_NETWORK
            let chainKey = rest.slice(0, sep) + ':' + rest.slice(sep + 1);
            let depth = parseIntSafe(process.env[envKey], 0);
            if(depth >= 1) bootstrapDepth[chainKey.toUpperCase()] = depth;
        }
        config['SYNC_BOOTSTRAP_DEPTH'] = bootstrapDepth;

        // Client mode settings
        config['SYNC_SOURCES']   = process.env.SYNC_SOURCES || '';
        config['VERIFY_HASHES']  = (process.env.VERIFY_HASHES || '').toLowerCase() !== 'false';
        config['REPLICA_DB_HOST'] = process.env.REPLICA_DB_HOST;
        config['REPLICA_DB_PORT'] = parseIntMin1(process.env.REPLICA_DB_PORT, 3306);
        config['REPLICA_DB_USER'] = process.env.REPLICA_DB_USER;
        config['REPLICA_DB_PASS'] = process.env.REPLICA_DB_PASS;

        // Hub re-poll interval (default 5 minutes; override via HUB_REPOLL_INTERVAL)
        config['HUB_REPOLL_INTERVAL'] = parseIntMin0(process.env.HUB_REPOLL_INTERVAL, 300000);

        // Merkle tree epoch size (blocks per epoch)
        config['MERKLE_EPOCH_SIZE'] = parseInt(process.env.MERKLE_EPOCH_SIZE) || 100;

        // Transparency endpoint rate limit (requests per minute per IP)
        config['TRANSPARENCY_RATE_LIMIT'] = parseInt(process.env.TRANSPARENCY_RATE_LIMIT) || 10;

        // WebSocket backpressure limit (consecutive buffered sends before a slow
        // subscriber is force-disconnected). Env-configurable so operators can tune
        // tolerance for validators with heterogeneous replica DB speeds.
        config['WS_BACKPRESSURE_LIMIT'] = parseIntMin1(process.env.WS_BACKPRESSURE_LIMIT, 50);

        // WebSocket status broadcast interval (default 60 seconds; override via WS_STATUS_INTERVAL)
        config['WS_STATUS_INTERVAL'] = parseIntMin0(process.env.WS_STATUS_INTERVAL, 60000);

        // WebSocket ping interval (default 30 seconds; override via WS_PING_INTERVAL)
        config['WS_PING_INTERVAL'] = parseIntMin0(process.env.WS_PING_INTERVAL, 30000);

        // Security: API key authentication (disabled when not set)
        config['SYNC_API_KEY'] = process.env.SYNC_API_KEY || '';

        // Security: Hub protocol (http or https)
        config['HUB_PROTOCOL'] = (process.env.HUB_PROTOCOL || '').toLowerCase() === 'https' ? 'https' : 'http';

        // Security: Trust x-forwarded-for header (only enable behind a reverse proxy)
        config['TRUST_PROXY'] = (process.env.TRUST_PROXY || '').toLowerCase() === 'true';

        // Security: Maximum rollback depth from a single source (blocks)
        config['MAX_ROLLBACK_DEPTH'] = parseIntMin1(process.env.MAX_ROLLBACK_DEPTH, 100);

        // Security: Reject blocks on cross-source verification timeout (instead of applying from primary)
        config['HASH_CONFIRM_STRICT'] = (process.env.HASH_CONFIRM_STRICT || '').toLowerCase() === 'true';

        // Consensus safety: on a CONFIRMED cross-source hash divergence (two honest
        // sources committed different ledger/actions/contract hashes for the same
        // block; one is on a forked/Byzantine chain), HALT durably instead of just
        // logging and silently stalling. Default ON. Set HALT_ON_DIVERGENCE=false to
        // revert to log-only (not recommended for validators).
        config['HALT_ON_DIVERGENCE'] = (process.env.HALT_ON_DIVERGENCE || 'true').toLowerCase() !== 'false';

        // Validator track: independently RECOMPUTE each indexer block's consensus
        // hashes (ledger/actions/contract) from the replicated raw rows and confirm
        // they match the committed hash. Catches a replica whose DATA does not hash
        // to the committed hash (replication corruption / partial apply / a source
        // serving rows inconsistent with its committed hash), which the verbatim
        // hash comparison cannot. A mismatch HALTs durably. Default ON.
        //
        // VERIFY_RECOMPUTE=false is DECLARED UNSAFE (operator decision 2026-06-12)
        // for any consensus-relevant replica: the recompute is the ONLY mechanism
        // that verifies the catch-up JOIN block, so with it off a reorg that
        // happens while this client is disconnected/restarting is stitched onto
        // the orphaned pre-reorg tip and the replica SILENTLY follows the forked
        // chain, keeping the orphaned blocks forever (proven by the
        // parity-interleave e2e property suite). Disable only for throwaway,
        // read-only convenience mirrors whose state nothing downstream trusts.
        config['VERIFY_RECOMPUTE'] = (process.env.VERIFY_RECOMPUTE || 'true').toLowerCase() !== 'false';

        // VERIFY_STATE_HASH: apply-time recompute of the per-block state_hash (the
        // in-place mutations + backdated refund credits the three consensus hashes
        // can't cover). Default ON; a mismatch HALTs durably, the same as a recompute
        // divergence. Additive + fail-soft: a NULL state_hash (block indexed before the
        // source had the feature) is skipped, so enabling it can never false-halt a
        // back-level source. Set VERIFY_STATE_HASH=false only for throwaway mirrors.
        config['VERIFY_STATE_HASH'] = (process.env.VERIFY_STATE_HASH || 'true').toLowerCase() !== 'false';

        // VERIFY_STATE_COMMITMENT: apply-time recompute of the per-block light-client
        // SMT roots (SPV spec sec.4-5) over the replica, HALTing on divergence. Default
        // ON; NULL roots (blocks before the flag-day) are skipped (additive + fail-soft).
        // Phase 1 verifies balances_root + block_merkle_root only (stakes_root/state_root
        // are deferred until the BTC stake-weight query is ported). Set
        // VERIFY_STATE_COMMITMENT=false on truncated / incremental-bootstrapped replicas
        // (their balances history is incomplete, so the SMT full-build would be wrong)
        // and on throwaway mirrors.
        config['VERIFY_STATE_COMMITMENT'] = (process.env.VERIFY_STATE_COMMITMENT || 'true').toLowerCase() !== 'false';

        // INDEX_MAP_PARITY_CHECK: advisory id->address map parity. Default OFF, and
        // UNLIKE the VERIFY_* gates above it NEVER halts: a mismatch is logged + counted
        // only. It catches a replica whose index_addresses id->address map content
        // diverged from the source's (e.g. a local INSERT IGNORE that kept a colliding
        // id and dropped the source's row), which the resolved-string consensus hashes
        // and the row-count check structurally cannot see (equal count, different
        // content). Read on BOTH sides: a server (SYNC_MODE=server) publishes the
        // deterministic-subset checksum on /status; a client recomputes + compares in
        // _verifyAgainstSource. OFF by default because computing it scans the
        // deterministic subset of index_addresses (an index on block_index is advisable
        // before enabling on a high-volume chain). See
        // claude/reports/2026-06-19_index-map-soft-parity-proposal.md.
        config['INDEX_MAP_PARITY_CHECK'] = (process.env.INDEX_MAP_PARITY_CHECK || '').toLowerCase() === 'true';

        // VERIFY_CHECKPOINT_QUORUM (SPV): anchor the replica's INDEPENDENTLY-recomputed
        // state_root to the federation quorum instead of trusting the source's claimed
        // value. The client fetches the source's signed checkpoint, verifies its
        // signatures against an OUT-OF-BAND pinned validator set (pinnedValidators.js),
        // and HALTs if the quorum fails or the checkpoint's state_root disagrees with the
        // replica's own state_tree_roots row. Closes the single-source trust gap that the
        // recompute checks (which only compare against the same source) cannot.
        // Default OFF: consensus-sensitive and INERT without a pinned set. When on but no
        // pinned set is configured for the (chain, network), the step is skipped (never
        // bypassed). CHECKPOINT_VERIFY_INTERVAL bounds how often the /latest checkpoint is
        // probed (in applied blocks; default 50).
        config['VERIFY_CHECKPOINT_QUORUM'] = (process.env.VERIFY_CHECKPOINT_QUORUM || 'false').toLowerCase() === 'true';
        config['CHECKPOINT_VERIFY_INTERVAL'] = parseIntMin1(process.env.CHECKPOINT_VERIFY_INTERVAL, 50);

        // Security: WebSocket max incoming message size in bytes (default 1 MB)
        config['WS_MAX_PAYLOAD'] = parseIntMin1(process.env.WS_MAX_PAYLOAD, 1048576);

        // Security: Max HTTP response size for snapshot downloads in bytes (default 512 MB)
        config['SNAPSHOT_MAX_CONTENT'] = parseIntMin1(process.env.SNAPSHOT_MAX_CONTENT, 536870912);

        // Client reconnect delay (default 5 seconds; override via CLIENT_RECONNECT_DELAY)
        config['CLIENT_RECONNECT_DELAY'] = parseIntMin0(process.env.CLIENT_RECONNECT_DELAY, 5000);

        // Bootstrap retry-with-backoff (client mode). A full snapshot bootstrap that
        // exhausts every configured source must NOT fall through to live-follow on an
        // empty replica; instead it retries the whole source rotation with bounded
        // exponential backoff, then propagates failure (start() throws, and the process
        // supervisor restarts the container). This is the only recovery for the
        // single-source topology, where there is no second source to rotate to.
        //   BOOTSTRAP_MAX_RETRIES: extra full-rotation rounds after the first (0 = no retry)
        //   BOOTSTRAP_RETRY_BASE_MS: first backoff delay; doubles each round
        //   BOOTSTRAP_RETRY_MAX_MS: backoff ceiling
        config['BOOTSTRAP_MAX_RETRIES']   = parseIntMin0(process.env.BOOTSTRAP_MAX_RETRIES, 5);
        config['BOOTSTRAP_RETRY_BASE_MS'] = parseIntMin1(process.env.BOOTSTRAP_RETRY_BASE_MS, 2000);
        config['BOOTSTRAP_RETRY_MAX_MS']  = parseIntMin1(process.env.BOOTSTRAP_RETRY_MAX_MS, 60000);

        // Hash confirm timeout for cross-source verification (default 5 seconds; override via HASH_CONFIRM_TIMEOUT)
        config['HASH_CONFIRM_TIMEOUT'] = parseIntMin0(process.env.HASH_CONFIRM_TIMEOUT, 5000);

        // Validator heartbeat TTL: entries not seen within this window (ms) are
        // transitioned to a 'stale' status (kept visible in /validator-status, not
        // hard-deleted) so a validator that restarted or briefly dropped off the
        // network remains observable rather than silently vanishing.
        config['VALIDATOR_HEARTBEAT_TTL'] = parseIntMin1(process.env.VALIDATOR_HEARTBEAT_TTL, 60000);

        // Optional expected-validator roster: a comma-separated list of validator
        // ids/pubkeys. When set, /validator-status reports an `expected_total`
        // denominator alongside the observed count and flags roster members that
        // have never reported a heartbeat as status 'absent'. This is the only
        // in-band signal for a validator that silently fell off the federation,
        // e.g. a replaced machine configured with the wrong sync server URL, or a
        // node network-partitioned before it ever POSTed. Empty/unset -> [] (the
        // service runs exactly as before, with no roster anchor). Deduplicated and
        // trimmed so the denominator is accurate.
        config['EXPECTED_VALIDATORS'] = [...new Set(
            (process.env.EXPECTED_VALIDATORS || '')
                .split(',')
                .map(s => s.trim())
                .filter(s => s.length > 0)
        )];

        return config;
    }
};
