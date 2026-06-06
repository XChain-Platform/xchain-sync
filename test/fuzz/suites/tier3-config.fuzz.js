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
 * Tier 3 Fuzz: config.getConfig()
 *
 * Tests that getConfig() never crashes regardless of process.env contents.
 * Verifies that all returned fields have correct types and constraints.
 * Each iteration mutates process.env with a finally block for restoration.
 */

const assert = require('assert');
const fc = require('fast-check');
const { NUM_RUNS } = require('../setup/harness');
const { envVarString } = require('../generators/values');

// All env vars that config.getConfig() reads
const CONFIG_ENV_KEYS = [
    'SYNC_MODE', 'SYNC_API_PORT', 'HUB_API_HOST', 'HUB_PORT',
    'CORS_ORIGIN', 'BLOCK_POLL_INTERVAL', 'WS_MAX_PER_IP',
    'SNAPSHOT_RATE_FULL', 'SNAPSHOT_RATE_INCR',
    'SYNC_SOURCES', 'VERIFY_HASHES',
    'REPLICA_DB_HOST', 'REPLICA_DB_PORT', 'REPLICA_DB_USER', 'REPLICA_DB_PASS',
];

// All keys that getConfig() must return
const REQUIRED_KEYS = [
    'SYNC_MODE', 'SYNC_API_PORT', 'HUB_API_HOST', 'HUB_PORT',
    'CORS_ORIGIN', 'BLOCK_POLL_INTERVAL', 'WS_MAX_PER_IP',
    'SNAPSHOT_RATE_FULL', 'SNAPSHOT_RATE_INCR',
    'SYNC_SOURCES', 'VERIFY_HASHES',
    'REPLICA_DB_HOST', 'REPLICA_DB_PORT', 'REPLICA_DB_USER', 'REPLICA_DB_PASS',
    'HUB_REPOLL_INTERVAL', 'WS_BACKPRESSURE_LIMIT', 'WS_STATUS_INTERVAL',
    'WS_PING_INTERVAL', 'CLIENT_RECONNECT_DELAY', 'HASH_CONFIRM_TIMEOUT',
];

// Numeric keys that must be integers and their minimum values
const NUMERIC_KEYS = {
    SYNC_API_PORT: 0,
    HUB_PORT: 1,
    BLOCK_POLL_INTERVAL: 0,
    WS_MAX_PER_IP: 1,
    SNAPSHOT_RATE_FULL: 0,
    SNAPSHOT_RATE_INCR: 0,
    REPLICA_DB_PORT: 1,
};

// Hardcoded constants that must never change
const HARDCODED = {
    HUB_REPOLL_INTERVAL: 300000,
    WS_BACKPRESSURE_LIMIT: 50,
    WS_STATUS_INTERVAL: 60000,
    WS_PING_INTERVAL: 30000,
    CLIENT_RECONNECT_DELAY: 5000,
    HASH_CONFIRM_TIMEOUT: 5000,
};

describe('Tier 3 - config.getConfig @tier3', function () {
    this.timeout(0);
    let config;

    before(function () {
        config = require('../../../src/config');
    });

    // Save/restore env vars around each property iteration
    function withFuzzedEnv(envObj, fn) {
        let saved = {};
        for (let key of CONFIG_ENV_KEYS) {
            saved[key] = process.env[key];
        }
        try {
            // Apply fuzzed values
            for (let key of CONFIG_ENV_KEYS) {
                let val = envObj[key];
                if (val === undefined || val === null) {
                    delete process.env[key];
                } else {
                    process.env[key] = String(val);
                }
            }
            return fn();
        } finally {
            // Restore original values
            for (let key of CONFIG_ENV_KEYS) {
                if (saved[key] === undefined) {
                    delete process.env[key];
                } else {
                    process.env[key] = saved[key];
                }
            }
        }
    }

    // Arbitrary for a full env var object
    function fuzzedEnv() {
        let entries = {};
        for (let key of CONFIG_ENV_KEYS) {
            entries[key] = envVarString();
        }
        return fc.record(entries);
    }

    describe('crash safety', function () {

        it('never throws for any combination of env var values', function () {
            fc.assert(fc.property(
                fuzzedEnv(),
                (envObj) => {
                    withFuzzedEnv(envObj, () => {
                        config.getConfig();
                    });
                }
            ), { numRuns: NUM_RUNS });
        });
    });

    describe('return shape', function () {

        it('always returns an object with all required keys', function () {
            fc.assert(fc.property(
                fuzzedEnv(),
                (envObj) => {
                    withFuzzedEnv(envObj, () => {
                        let result = config.getConfig();
                        for (let key of REQUIRED_KEYS) {
                            assert.ok(key in result, 'Missing key: ' + key);
                        }
                    });
                }
            ), { numRuns: NUM_RUNS });
        });

        it('numeric fields are always integers', function () {
            fc.assert(fc.property(
                fuzzedEnv(),
                (envObj) => {
                    withFuzzedEnv(envObj, () => {
                        let result = config.getConfig();
                        for (let key in NUMERIC_KEYS) {
                            assert.ok(Number.isInteger(result[key]),
                                key + ' should be integer, got: ' + typeof result[key] + ' (' + result[key] + ')');
                        }
                    });
                }
            ), { numRuns: NUM_RUNS });
        });

        it('numeric fields respect minimum values', function () {
            fc.assert(fc.property(
                fuzzedEnv(),
                (envObj) => {
                    withFuzzedEnv(envObj, () => {
                        let result = config.getConfig();
                        for (let key in NUMERIC_KEYS) {
                            assert.ok(result[key] >= NUMERIC_KEYS[key],
                                key + ' should be >= ' + NUMERIC_KEYS[key] + ', got: ' + result[key]);
                        }
                    });
                }
            ), { numRuns: NUM_RUNS });
        });
    });

    describe('VERIFY_HASHES', function () {

        it('is always a boolean', function () {
            fc.assert(fc.property(
                fuzzedEnv(),
                (envObj) => {
                    withFuzzedEnv(envObj, () => {
                        let result = config.getConfig();
                        assert.strictEqual(typeof result.VERIFY_HASHES, 'boolean');
                    });
                }
            ), { numRuns: NUM_RUNS });
        });

        it('is false only when env var is exactly "false" (case-insensitive)', function () {
            fc.assert(fc.property(
                envVarString(),
                (val) => {
                    let envObj = {};
                    for (let key of CONFIG_ENV_KEYS) envObj[key] = undefined;
                    envObj.VERIFY_HASHES = val;

                    withFuzzedEnv(envObj, () => {
                        let result = config.getConfig();
                        let strVal = val === undefined || val === null ? '' : String(val);
                        if (strVal.toLowerCase() === 'false') {
                            assert.strictEqual(result.VERIFY_HASHES, false,
                                'Expected false for VERIFY_HASHES="' + val + '"');
                        } else {
                            assert.strictEqual(result.VERIFY_HASHES, true,
                                'Expected true for VERIFY_HASHES="' + val + '"');
                        }
                    });
                }
            ), { numRuns: NUM_RUNS });
        });
    });

    describe('hardcoded constants', function () {

        it('are never overridden by env vars', function () {
            fc.assert(fc.property(
                fuzzedEnv(),
                (envObj) => {
                    withFuzzedEnv(envObj, () => {
                        let result = config.getConfig();
                        for (let key in HARDCODED) {
                            assert.strictEqual(result[key], HARDCODED[key],
                                key + ' should always be ' + HARDCODED[key] + ', got: ' + result[key]);
                        }
                    });
                }
            ), { numRuns: NUM_RUNS });
        });
    });
});
