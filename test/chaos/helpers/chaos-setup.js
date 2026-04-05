/**
 * Chaos test setup — bootstraps the full sync stack against MariaDB
 * instances routed through Toxiproxy, and provides utilities for
 * seeding data, monitoring sync progress, and measuring recovery.
 *
 * Reuses the e2e test helpers for database management, server/client
 * process control, fixtures, and assertions.
 *
 * Requires:
 *   - Source MariaDB proxied on port 33060, direct on port 33065
 *   - Replica MariaDB proxied on port 33061
 *   - Toxiproxy API on port 8474
 *   - See docker-compose.chaos.yml
 */

'use strict';

const http = require('http');
const testDb        = require('../../e2e/helpers/testDb');
const fixtures      = require('../../e2e/helpers/fixtures');
const ServerProcess = require('../../e2e/helpers/serverProcess');
const ClientProcess = require('../../e2e/helpers/clientProcess');

// -------------------------------------------------------------------------
// Connection constants — proxied ports from docker-compose.chaos.yml
// -------------------------------------------------------------------------

const CHAOS_DB_HOST      = process.env.CHAOS_DB_HOST || '127.0.0.1';
const SOURCE_PROXY_PORT  = parseInt(process.env.SOURCE_PROXY_PORT  || '33060', 10);
const REPLICA_PROXY_PORT = parseInt(process.env.REPLICA_PROXY_PORT || '33061', 10);
const SOURCE_DIRECT_PORT = parseInt(process.env.SOURCE_DIRECT_PORT || '33065', 10);
const CHAOS_DB_USER      = 'xchain-node';
const CHAOS_DB_PASS      = 'XChain4life';

const SOURCE_DB_NAME  = 'xchain_chaos_source';
const REPLICA_DB_NAME = 'xchain_chaos_replica';

// -------------------------------------------------------------------------
// Database singletons
// -------------------------------------------------------------------------

let sourceDb       = null;   // through proxy (33060) — used by ServerProcess
let replicaDb      = null;   // through proxy (33061) — used by ClientProcess
let sourceDbDirect = null;   // direct (33065) — for seeding while proxy is down

// -------------------------------------------------------------------------
// Database lifecycle
// -------------------------------------------------------------------------

async function bootstrapDatabases() {
    console.log('    [chaos setup] Creating databases through proxied ports...');

    sourceDb = await testDb.createDatabase(
        SOURCE_DB_NAME, CHAOS_DB_HOST, SOURCE_PROXY_PORT, CHAOS_DB_USER, CHAOS_DB_PASS
    );
    await testDb.seedSchema(sourceDb);
    console.log('    [chaos setup] Source database ready (proxied): ' + SOURCE_DB_NAME);

    replicaDb = await testDb.createDatabase(
        REPLICA_DB_NAME, CHAOS_DB_HOST, REPLICA_PROXY_PORT, CHAOS_DB_USER, CHAOS_DB_PASS
    );
    await testDb.seedSchema(replicaDb);
    console.log('    [chaos setup] Replica database ready (proxied): ' + REPLICA_DB_NAME);

    // Direct connection for admin seeding while proxy is disabled
    sourceDbDirect = await testDb.createDatabase(
        SOURCE_DB_NAME, CHAOS_DB_HOST, SOURCE_DIRECT_PORT, CHAOS_DB_USER, CHAOS_DB_PASS
    );
    console.log('    [chaos setup] Source database direct connection ready (port ' + SOURCE_DIRECT_PORT + ')');
}

async function teardownDatabases() {
    console.log('    [chaos teardown] Dropping databases...');
    if (sourceDb)       await sourceDb.close();
    if (replicaDb)      await replicaDb.close();
    if (sourceDbDirect) await sourceDbDirect.close();

    await testDb.dropDatabase(SOURCE_DB_NAME, CHAOS_DB_HOST, SOURCE_DIRECT_PORT, CHAOS_DB_USER, CHAOS_DB_PASS);
    await testDb.dropDatabase(REPLICA_DB_NAME, CHAOS_DB_HOST, REPLICA_PROXY_PORT, CHAOS_DB_USER, CHAOS_DB_PASS);
    console.log('    [chaos teardown] Cleanup complete');

    sourceDb       = null;
    replicaDb      = null;
    sourceDbDirect = null;
}

async function resetDatabases() {
    if (sourceDb)  await testDb.truncateAll(sourceDb);
    if (replicaDb) await testDb.truncateAll(replicaDb);
}

// -------------------------------------------------------------------------
// Server / Client process creation
// -------------------------------------------------------------------------

function createServer(port, chain, network) {
    return new ServerProcess(sourceDb, port, chain || 'bitcoin', network || 'mainnet');
}

function createClient(serverUrl, opts = {}) {
    return new ClientProcess(
        replicaDb,
        serverUrl,
        opts.chain || 'bitcoin',
        opts.network || 'mainnet',
        {
            syncSources:      opts.syncSources || serverUrl,
            verifyHashes:     opts.verifyHashes !== undefined ? opts.verifyHashes : false,
            reconnectDelay:   opts.reconnectDelay || 500,
            hashConfirmTimeout: opts.hashConfirmTimeout || 2000
        }
    );
}

// -------------------------------------------------------------------------
// Data seeding
// -------------------------------------------------------------------------

/**
 * Seed blocks into the source DB through the proxied connection.
 * Use this when the source proxy is enabled (normal case).
 */
async function seedSourceBlocks(startBlock, endBlock, opts) {
    return fixtures.seedBlocks(sourceDb, startBlock, endBlock, opts);
}

/**
 * Seed blocks into the source DB through the direct connection.
 * Use this when the source proxy is disabled (e.g., CE-SRC-05, CE-SYNC-04).
 */
async function seedSourceDirect(startBlock, endBlock, opts) {
    return fixtures.seedBlocks(sourceDbDirect, startBlock, endBlock, opts);
}

/**
 * Delete blocks from a given height in the source DB.
 * Simulates a chain reorganization on the source side.
 */
async function deleteSourceBlocksFrom(blockIndex) {
    return fixtures.deleteBlocksFrom(sourceDb, blockIndex);
}

// -------------------------------------------------------------------------
// HTTP helpers for server status checks
// -------------------------------------------------------------------------

function httpGet(urlPath, opts = {}) {
    const base = opts.baseUrl;
    return new Promise((resolve, reject) => {
        const url = new URL(urlPath, base);
        const req = http.get(url.href, { timeout: opts.timeout || 10000 }, (res) => {
            let body = '';
            res.on('data', (chunk) => { body += chunk; });
            res.on('end', () => {
                resolve({
                    statusCode: res.statusCode,
                    headers:    res.headers,
                    body
                });
            });
        });
        req.on('error', reject);
        req.on('timeout', () => { req.destroy(); reject(new Error('Request timeout')); });
    });
}

/**
 * Check if the server process is alive by hitting its status endpoint.
 * Returns true if the server responds (any HTTP status), false on ECONNREFUSED.
 */
async function isServerAlive(serverUrl) {
    try {
        const res = await httpGet('/status', { baseUrl: serverUrl, timeout: 5000 });
        return typeof res.statusCode === 'number';
    } catch (e) {
        return !e.message.includes('ECONNREFUSED');
    }
}

// -------------------------------------------------------------------------
// Recovery and monitoring helpers
// -------------------------------------------------------------------------

/**
 * Wait until the replica DB reaches a given block height.
 * Returns elapsed ms, or -1 if timeout expires.
 */
async function waitForSyncRecovery(expectedBlock, timeoutMs = 60000) {
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
        try {
            const block = await replicaDb.getLastBlock();
            if (block !== null && block >= expectedBlock) {
                return Date.now() - start;
            }
        } catch {
            // DB may be temporarily unreachable — keep trying
        }
        await new Promise(r => setTimeout(r, 250));
    }
    return -1;
}

/**
 * Wait until the replica DB reaches a given block height.
 * Uses the direct source connection to check the source as well.
 */
async function waitForSyncRecoveryDirect(expectedBlock, timeoutMs = 60000) {
    const start = Date.now();
    const directReplica = await testDb.createDb(
        REPLICA_DB_NAME, CHAOS_DB_HOST, REPLICA_PROXY_PORT, CHAOS_DB_USER, CHAOS_DB_PASS
    );
    try {
        while (Date.now() - start < timeoutMs) {
            try {
                const block = await directReplica.getLastBlock();
                if (block !== null && block >= expectedBlock) {
                    return Date.now() - start;
                }
            } catch {
                // keep trying
            }
            await new Promise(r => setTimeout(r, 250));
        }
        return -1;
    } finally {
        await directReplica.close();
    }
}

/**
 * Poll replica block height at an interval and collect results.
 * Returns a stop function. Results are pushed to the provided array.
 */
function startReplicaPoller(results, intervalMs = 500) {
    const poll = async () => {
        try {
            const block = await replicaDb.getLastBlock();
            results.push({
                ts:    Date.now(),
                block: block,
                ok:    block !== null
            });
        } catch (e) {
            results.push({
                ts:    Date.now(),
                block: null,
                ok:    false,
                error: e.message
            });
        }
    };
    const timer = setInterval(poll, intervalMs);
    poll();
    return () => clearInterval(timer);
}

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

// -------------------------------------------------------------------------
// Exports
// -------------------------------------------------------------------------

module.exports = {
    bootstrapDatabases,
    teardownDatabases,
    resetDatabases,
    createServer,
    createClient,
    seedSourceBlocks,
    seedSourceDirect,
    deleteSourceBlocksFrom,
    httpGet,
    isServerAlive,
    waitForSyncRecovery,
    waitForSyncRecoveryDirect,
    startReplicaPoller,
    sleep,
    getSourceDb:       () => sourceDb,
    getReplicaDb:      () => replicaDb,
    getSourceDbDirect: () => sourceDbDirect,
    SOURCE_PROXY_PORT,
    REPLICA_PROXY_PORT,
    SOURCE_DIRECT_PORT,
    SOURCE_DB_NAME,
    REPLICA_DB_NAME
};
