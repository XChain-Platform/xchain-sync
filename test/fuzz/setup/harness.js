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
 * Fuzz test harness: shared helpers for all fuzz suites
 *
 * Provides mock factories and NUM_RUNS configuration for property-based testing.
 */

const sinon = require('sinon');
const Utility = require('../../../src/utility');

const NUM_RUNS = parseInt(process.env.FUZZ_RUNS || '1000');

/**
 * Create a mock Database with stubs for every method used across all target modules.
 */
function createMockDb() {
    return {
        dbName: 'test_db',
        doQuery: sinon.stub().resolves([]),
        getBlockHashRow: sinon.stub().resolves(null),
        getLastBlock: sinon.stub().resolves(null),
        getFirstActionIndex: sinon.stub().resolves(null),
        // ClientRollback._rollbackIndexer resolves the 'completed'/'valid' status ids
        // (for cooldown-credit + status-scoped rebuilds) via getStatusId; a stable
        // integer is enough here since the follow-on doQuery results are stubbed.
        getStatusId: sinon.stub().resolves(1),
        getBlockScopedRows: sinon.stub().resolves([]),
        getActionScopedRows: sinon.stub().resolves([]),
        getTransactions: sinon.stub().resolves([]),
        getActions: sinon.stub().resolves([]),
        beginTransaction: sinon.stub().resolves(),
        commitTransaction: sinon.stub().resolves(),
        rollbackTransaction: sinon.stub().resolves(),
        truncateTable: sinon.stub().resolves(),
        getTablePage: sinon.stub().resolves([]),
        getTableCount: sinon.stub().resolves(0),
    };
}

/**
 * Create a real Utility instance (no I/O, safe for direct use).
 */
function createMockUtil() {
    return new Utility();
}

/**
 * Create a mock BlockBroadcaster.
 */
function createMockBroadcaster() {
    return {
        broadcast: sinon.stub(),
        updateStatus: sinon.stub(),
    };
}

/**
 * Create a mock TransparencyLog.
 */
function createMockTransparencyLog() {
    return {
        recordBlock: sinon.stub().resolves(),
    };
}

/**
 * Create a config object with safe defaults matching config.getConfig() shape.
 */
function createMockConfig() {
    return {
        SYNC_MODE: 'server',
        SYNC_API_PORT: 3006,
        HUB_API_HOST: 'localhost',
        HUB_PORT: 10000,
        CORS_ORIGIN: '*',
        BLOCK_POLL_INTERVAL: 3000,
        WS_MAX_PER_IP: 3,
        SNAPSHOT_RATE_FULL: 1,
        SNAPSHOT_RATE_INCR: 10,
        SYNC_SOURCES: '',
        VERIFY_HASHES: true,
        REPLICA_DB_HOST: 'localhost',
        REPLICA_DB_PORT: 3306,
        REPLICA_DB_USER: 'root',
        REPLICA_DB_PASS: '',
        HUB_REPOLL_INTERVAL: 300000,
        WS_BACKPRESSURE_LIMIT: 50,
        WS_STATUS_INTERVAL: 60000,
        WS_PING_INTERVAL: 30000,
        CLIENT_RECONNECT_DELAY: 5000,
        HASH_CONFIRM_TIMEOUT: 5000,
    };
}

module.exports = {
    NUM_RUNS,
    createMockDb,
    createMockUtil,
    createMockBroadcaster,
    createMockTransparencyLog,
    createMockConfig,
};
