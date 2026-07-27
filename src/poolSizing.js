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
 * XChain Sync - Per-dbType connection pool sizing
 *
 * One `Database` (and therefore one mariadb pool) exists per
 * chain:network:dbType. The two dbTypes have very different connection
 * demand, so a single global DB_POOL_SIZE cannot size both well:
 *
 *   indexer - ServerPoller assembles every block payload from ~113
 *             sequential queries (block-scoped rows over 8+ tables,
 *             action-scoped rows over 80+ tables, index-id pooling), on
 *             top of which each in-flight /snapshot stream pins one
 *             connection for its whole duration. At the old default of 5
 *             the fan-out serialized ~23 deep and a small bootstrap
 *             cohort could pin every connection, starving the poller's
 *             ~3s getLastBlock acquire and stalling live broadcast.
 *   decoder - replicates 8 narrow tables with no action fan-out, so it
 *             needs far fewer connections and should not pay the
 *             indexer's footprint on every chain.
 *
 * Resolution order per parameter, most specific first:
 *   1. <NAME>_<DBTYPE>   e.g. DB_POOL_SIZE_INDEXER, DB_POOL_SIZE_DECODER
 *   2. <NAME>            e.g. DB_POOL_SIZE (legacy global, all dbTypes)
 *   3. the per-dbType default below
 *
 * Sizing budget: a source serving 3 chains x 2 dbTypes opens 6 pools, so
 * the defaults cost 3*(12+6) = 54 connections, comfortably under
 * MariaDB's default max_connections.
 *
 ********************************************************************/

// Per-dbType defaults. Unknown dbTypes fall back to the indexer profile:
// an unrecognized type is more likely a new fan-out-heavy schema than a
// narrow one, and over-sizing degrades gracefully while under-sizing
// serializes.
const DEFAULT_POOL_SIZE = {
    indexer: 12,
    decoder: 6
};

// A pool of 1 cannot work: ServerPoller needs a connection while a snapshot
// stream holds its own, and SnapshotBuilder's cap is derived from poolSize - 2.
// The ceiling keeps a typo (DB_POOL_SIZE_INDEXER=5000) from exhausting
// MariaDB's max_connections across every pool on the host.
const MIN_POOL_SIZE = 2;
const MAX_POOL_SIZE = 100;

// Per-dbType defaults for the other pool timings. Kept here so every
// pool knob resolves through the same <NAME>_<DBTYPE> mechanism.
const DEFAULT_CONNECT_TIMEOUT = 10000;
const DEFAULT_ACQUIRE_TIMEOUT = 10000;
const DEFAULT_QUERY_TIMEOUT   = 30000;

function normalizeDbType(dbType){
    return String(dbType || 'indexer').trim().toLowerCase();
}

// Read `<name>_<DBTYPE>` first, then the bare `<name>`. Returns undefined
// when neither is set (or both are empty), so the caller can apply its
// own default rather than guessing here.
function readEnvOverride(name, dbType, env){
    let source = env || process.env;
    let scoped = source[name + '_' + normalizeDbType(dbType).toUpperCase()];
    if(scoped !== undefined && String(scoped).trim() !== '') return scoped;
    let global = source[name];
    if(global !== undefined && String(global).trim() !== '') return global;
    return undefined;
}

// Parse an override into a positive integer, ignoring anything that is not
// one (a malformed value must not silently become NaN in the pool config).
function parsePositiveInt(raw){
    if(raw === undefined) return null;
    let parsed = parseInt(raw, 10);
    if(!Number.isFinite(parsed) || parsed <= 0) return null;
    return parsed;
}

/**
 * Resolve the connection limit for one dbType.
 * Out-of-range values are clamped (not rejected) so an operator tuning a
 * hot source can never accidentally deadlock the poller or blow past
 * MariaDB's connection ceiling.
 */
function resolvePoolSize(dbType, env){
    let type    = normalizeDbType(dbType);
    let parsed  = parsePositiveInt(readEnvOverride('DB_POOL_SIZE', type, env));
    let wanted  = parsed === null ? (DEFAULT_POOL_SIZE[type] || DEFAULT_POOL_SIZE.indexer) : parsed;
    return Math.max(MIN_POOL_SIZE, Math.min(MAX_POOL_SIZE, wanted));
}

/**
 * Resolve every pool timing/sizing knob for one dbType in one shot.
 * Returned keys match the mariadb pool option names so db.js can spread
 * them straight into createPool's config.
 */
function resolvePoolParams(dbType, env){
    let type = normalizeDbType(dbType);
    return {
        connectionLimit: resolvePoolSize(type, env),
        connectTimeout:  parsePositiveInt(readEnvOverride('DB_CONNECT_TIMEOUT', type, env)) || DEFAULT_CONNECT_TIMEOUT,
        acquireTimeout:  parsePositiveInt(readEnvOverride('DB_ACQUIRE_TIMEOUT', type, env)) || DEFAULT_ACQUIRE_TIMEOUT,
        queryTimeout:    parsePositiveInt(readEnvOverride('DB_QUERY_TIMEOUT',   type, env)) || DEFAULT_QUERY_TIMEOUT
    };
}

module.exports = {
    DEFAULT_POOL_SIZE,
    MIN_POOL_SIZE,
    MAX_POOL_SIZE,
    normalizeDbType,
    resolvePoolSize,
    resolvePoolParams
};
