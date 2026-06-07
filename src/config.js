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
        // from one IP — node-host-b alone serves 12 chain/network/dbType streams. The old
        // default of 3 closed 9 of them with 1008 "Too many connections", causing a
        // permanent reconnect storm that also broke gap-detection-driven catch-up.
        config['WS_MAX_PER_IP']       = parseIntMin1(process.env.WS_MAX_PER_IP, 100);
        // Per-resource (IP + chain/network/dbType) limits — see snapshotKey in api.js.
        // Defaults give a multi-chain replica headroom for bootstrap retries (full) and
        // frequent gap catch-ups (incremental) instead of a single global-per-IP bucket
        // that starves other chains.
        config['SNAPSHOT_RATE_FULL']  = parseIntMin0(process.env.SNAPSHOT_RATE_FULL, 12);
        config['SNAPSHOT_RATE_INCR']  = parseIntMin0(process.env.SNAPSHOT_RATE_INCR, 600);

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
        // block — one is on a forked/Byzantine chain), HALT durably instead of just
        // logging and silently stalling. Default ON. Set HALT_ON_DIVERGENCE=false to
        // revert to log-only (not recommended for validators).
        config['HALT_ON_DIVERGENCE'] = (process.env.HALT_ON_DIVERGENCE || 'true').toLowerCase() !== 'false';

        // Security: WebSocket max incoming message size in bytes (default 1 MB)
        config['WS_MAX_PAYLOAD'] = parseIntMin1(process.env.WS_MAX_PAYLOAD, 1048576);

        // Security: Max HTTP response size for snapshot downloads in bytes (default 512 MB)
        config['SNAPSHOT_MAX_CONTENT'] = parseIntMin1(process.env.SNAPSHOT_MAX_CONTENT, 536870912);

        // Client reconnect delay (default 5 seconds; override via CLIENT_RECONNECT_DELAY)
        config['CLIENT_RECONNECT_DELAY'] = parseIntMin0(process.env.CLIENT_RECONNECT_DELAY, 5000);

        // Hash confirm timeout for cross-source verification (default 5 seconds; override via HASH_CONFIRM_TIMEOUT)
        config['HASH_CONFIRM_TIMEOUT'] = parseIntMin0(process.env.HASH_CONFIRM_TIMEOUT, 5000);

        // Validator heartbeat TTL: entries not seen within this window (ms) are evicted
        config['VALIDATOR_HEARTBEAT_TTL'] = parseIntMin1(process.env.VALIDATOR_HEARTBEAT_TTL, 60000);

        return config;
    }
};
