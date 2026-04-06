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

        // CORS
        config['CORS_ORIGIN']   = process.env.CORS_ORIGIN || '*';

        // Server mode settings
        config['BLOCK_POLL_INTERVAL'] = parseIntMin0(process.env.BLOCK_POLL_INTERVAL, 3000);
        config['WS_MAX_PER_IP']       = parseIntMin1(process.env.WS_MAX_PER_IP, 3);
        config['SNAPSHOT_RATE_FULL']  = parseIntMin0(process.env.SNAPSHOT_RATE_FULL, 1);
        config['SNAPSHOT_RATE_INCR']  = parseIntMin0(process.env.SNAPSHOT_RATE_INCR, 10);

        // Client mode settings
        config['SYNC_SOURCES']   = process.env.SYNC_SOURCES || '';
        config['VERIFY_HASHES']  = (process.env.VERIFY_HASHES || '').toLowerCase() !== 'false';
        config['REPLICA_DB_HOST'] = process.env.REPLICA_DB_HOST;
        config['REPLICA_DB_PORT'] = parseIntMin1(process.env.REPLICA_DB_PORT, 3306);
        config['REPLICA_DB_USER'] = process.env.REPLICA_DB_USER;
        config['REPLICA_DB_PASS'] = process.env.REPLICA_DB_PASS;

        // Hub re-poll interval (5 minutes)
        config['HUB_REPOLL_INTERVAL'] = 300000;

        // WebSocket backpressure limit
        config['WS_BACKPRESSURE_LIMIT'] = 50;

        // WebSocket status broadcast interval (60 seconds)
        config['WS_STATUS_INTERVAL'] = 60000;

        // WebSocket ping interval (30 seconds)
        config['WS_PING_INTERVAL'] = 30000;

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

        // Security: WebSocket max incoming message size in bytes (default 1 MB)
        config['WS_MAX_PAYLOAD'] = parseIntMin1(process.env.WS_MAX_PAYLOAD, 1048576);

        // Security: Max HTTP response size for snapshot downloads in bytes (default 512 MB)
        config['SNAPSHOT_MAX_CONTENT'] = parseIntMin1(process.env.SNAPSHOT_MAX_CONTENT, 536870912);

        // Client reconnect delay (5 seconds)
        config['CLIENT_RECONNECT_DELAY'] = 5000;

        // Hash confirm timeout for cross-source verification (5 seconds)
        config['HASH_CONFIRM_TIMEOUT'] = 5000;

        return config;
    }
};
