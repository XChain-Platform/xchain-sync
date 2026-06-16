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
 * Tier 2 Fuzz: HubClient
 *
 * Tests that getIndexerConfigs and _parsePort never crash regardless of
 * hub response shape, network errors, or input types.
 * Uses proxyquire to inject a faked axios without real HTTP calls.
 */

const assert = require('assert');
const fc = require('fast-check');
const sinon = require('sinon');
const proxyquire = require('proxyquire').noCallThru();
const { NUM_RUNS } = require('../setup/harness');
const { portValue } = require('../generators/values');
const { hubConfigResponse, adversarialHubResponse } = require('../generators/payloads');

describe('Tier 2 - HubClient @tier2', function () {
    this.timeout(0);
    let axiosStub, HubClient, hub;

    before(function () {
        axiosStub = { post: sinon.stub() };
        HubClient = proxyquire('../../../src/HubClient', { axios: axiosStub });
    });

    beforeEach(function () {
        hub = new HubClient('localhost', 10000);
        axiosStub.post.reset();
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });

    afterEach(function () {
        sinon.restore();
    });

    describe('getIndexerConfigs', function () {

        it('never throws for any hub response shape', function () {
            return fc.assert(fc.asyncProperty(
                adversarialHubResponse(),
                async (response) => {
                    axiosStub.post.resolves({ data: { result: response } });
                    let result = await hub.getIndexerConfigs();
                    // Should always return an array, never throw
                    assert.ok(Array.isArray(result));
                }
            ), { numRuns: NUM_RUNS });
        });

        it('always returns an array', function () {
            return fc.assert(fc.asyncProperty(
                fc.oneof(
                    adversarialHubResponse(),
                    fc.constant(null),
                    fc.constant(undefined),
                ),
                async (response) => {
                    axiosStub.post.resolves({ data: { result: response } });
                    let result = await hub.getIndexerConfigs();
                    assert.ok(Array.isArray(result));
                }
            ), { numRuns: NUM_RUNS });
        });

        it('handles network errors gracefully', function () {
            return fc.assert(fc.asyncProperty(
                fc.string({ minLength: 0, maxLength: 100 }),
                async (msg) => {
                    axiosStub.post.rejects(new Error(msg));
                    let result = await hub.getIndexerConfigs();
                    assert.ok(Array.isArray(result));
                    assert.strictEqual(result.length, 0);
                }
            ), { numRuns: NUM_RUNS });
        });

        it('each returned entry has correct shape', function () {
            return fc.assert(fc.asyncProperty(
                hubConfigResponse(),
                async (response) => {
                    axiosStub.post.resolves({ data: { result: response } });
                    let result = await hub.getIndexerConfigs();

                    for (let entry of result) {
                        assert.strictEqual(typeof entry.coin, 'string');
                        assert.ok(entry.coin.length > 0, 'coin must be non-empty');
                        assert.strictEqual(typeof entry.network, 'string');
                        assert.strictEqual(typeof entry.db_port, 'number');
                        assert.ok(Number.isInteger(entry.db_port), 'db_port must be integer');
                        assert.ok(entry.db_port >= 0, 'db_port must be non-negative');
                        assert.ok('db_host' in entry);
                        assert.ok('db_name' in entry);
                        assert.ok('db_user' in entry);
                        assert.ok('db_pass' in entry);
                    }
                }
            ), { numRuns: NUM_RUNS });
        });

        it('skips entries with empty coin keys', function () {
            return fc.assert(fc.asyncProperty(
                hubConfigResponse(),
                async (response) => {
                    axiosStub.post.resolves({ data: { result: response } });
                    let result = await hub.getIndexerConfigs();
                    for (let entry of result) {
                        assert.ok(entry.coin !== '', 'Empty coin key should be filtered');
                    }
                }
            ), { numRuns: NUM_RUNS });
        });

        it('handles completely broken response data structure', function () {
            return fc.assert(fc.asyncProperty(
                fc.anything(),
                async (data) => {
                    axiosStub.post.resolves({ data: data });
                    let result = await hub.getIndexerConfigs();
                    assert.ok(Array.isArray(result));
                }
            ), { numRuns: NUM_RUNS });
        });
    });

    describe('_parsePort', function () {

        it('never throws and always returns a non-negative integer', function () {
            fc.assert(fc.property(
                portValue(), portValue(),
                (primary, fallback) => {
                    let result = HubClient._parsePort(primary, fallback);
                    assert.strictEqual(typeof result, 'number');
                    assert.ok(Number.isInteger(result), 'Expected integer, got: ' + result);
                    assert.ok(result >= 0, 'Expected non-negative, got: ' + result);
                }
            ), { numRuns: NUM_RUNS });
        });

        it('returns 3306 as fallback for non-parseable inputs', function () {
            fc.assert(fc.property(
                fc.constantFrom(undefined, null, '', 'abc', NaN),
                fc.constantFrom(undefined, null, '', 'abc', NaN),
                (primary, fallback) => {
                    let result = HubClient._parsePort(primary, fallback);
                    assert.strictEqual(result, 3306);
                }
            ), { numRuns: 50 });
        });

        it('returns the parsed primary when valid', function () {
            fc.assert(fc.property(
                fc.integer({ min: 0, max: 65535 }),
                (port) => {
                    let result = HubClient._parsePort(port, undefined);
                    assert.strictEqual(result, port);
                }
            ), { numRuns: NUM_RUNS });
        });

        it('returns the parsed primary even as string', function () {
            fc.assert(fc.property(
                fc.integer({ min: 0, max: 65535 }),
                (port) => {
                    let result = HubClient._parsePort(String(port), undefined);
                    assert.strictEqual(result, port);
                }
            ), { numRuns: NUM_RUNS });
        });

        it('falls back to secondary when primary is empty/null/undefined', function () {
            fc.assert(fc.property(
                fc.constantFrom(undefined, null, ''),
                fc.integer({ min: 0, max: 65535 }),
                (primary, fallback) => {
                    let result = HubClient._parsePort(primary, fallback);
                    assert.strictEqual(result, fallback);
                }
            ), { numRuns: NUM_RUNS });
        });

        it('returns 3306 for negative values', function () {
            fc.assert(fc.property(
                fc.integer({ min: -10000, max: -1 }),
                (port) => {
                    let result = HubClient._parsePort(port, undefined);
                    assert.strictEqual(result, 3306);
                }
            ), { numRuns: NUM_RUNS });
        });
    });
});
