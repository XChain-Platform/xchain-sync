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
 * Tier 1 Fuzz: HashVerifier
 *
 * Pure synchronous module with no I/O — ideal for exhaustive property assertions.
 * Tests crash safety and correctness invariants for cross-source hash comparison
 * and hash chain continuity verification.
 */

const assert = require('assert');
const fc = require('fast-check');
const { NUM_RUNS } = require('../setup/harness');
const { blockHashPair, continuityPayload } = require('../generators/payloads');
const { blockIndex } = require('../generators/values');

describe('Tier 1 - HashVerifier @tier1', function () {
    this.timeout(0);
    let HashVerifier, verifier;

    before(function () {
        HashVerifier = require('../../../src/HashVerifier');
    });

    beforeEach(function () {
        verifier = new HashVerifier();
    });

    describe('compareBlockHashes', function () {

        it('never throws for any inputs', function () {
            fc.assert(fc.property(
                fc.anything(), blockHashPair(), blockHashPair(),
                (height, a, b) => {
                    verifier.compareBlockHashes(height, a, b);
                }
            ), { numRuns: NUM_RUNS });
        });

        it('always returns { match: boolean, blockHeight, mismatches: array }', function () {
            fc.assert(fc.property(
                fc.integer(), blockHashPair(), blockHashPair(),
                (height, a, b) => {
                    let result = verifier.compareBlockHashes(height, a, b);
                    assert.strictEqual(typeof result.match, 'boolean');
                    assert.ok(Array.isArray(result.mismatches));
                    assert.strictEqual(result.blockHeight, height);
                }
            ), { numRuns: NUM_RUNS });
        });

        it('match is true iff mismatches is empty', function () {
            fc.assert(fc.property(
                fc.integer(), blockHashPair(), blockHashPair(),
                (height, a, b) => {
                    let result = verifier.compareBlockHashes(height, a, b);
                    assert.strictEqual(result.match, result.mismatches.length === 0);
                }
            ), { numRuns: NUM_RUNS });
        });

        it('mismatches length is always 0–3', function () {
            fc.assert(fc.property(
                fc.integer(), blockHashPair(), blockHashPair(),
                (height, a, b) => {
                    let result = verifier.compareBlockHashes(height, a, b);
                    assert.ok(result.mismatches.length >= 0 && result.mismatches.length <= 3);
                }
            ), { numRuns: NUM_RUNS });
        });

        it('same object reference always matches', function () {
            fc.assert(fc.property(
                fc.integer(), blockHashPair(),
                (height, hashes) => {
                    let result = verifier.compareBlockHashes(height, hashes, hashes);
                    assert.strictEqual(result.match, true);
                    assert.strictEqual(result.mismatches.length, 0);
                }
            ), { numRuns: NUM_RUNS });
        });

        it('each mismatch entry has { field, a, b }', function () {
            fc.assert(fc.property(
                fc.integer(), blockHashPair(), blockHashPair(),
                (height, a, b) => {
                    let result = verifier.compareBlockHashes(height, a, b);
                    for (let m of result.mismatches) {
                        assert.strictEqual(typeof m.field, 'string');
                        assert.ok('a' in m);
                        assert.ok('b' in m);
                    }
                }
            ), { numRuns: NUM_RUNS });
        });

        it('mismatch fields are always from the known set', function () {
            let validFields = new Set(['ledger_hash', 'actions_hash', 'contract_hash']);
            fc.assert(fc.property(
                fc.integer(), blockHashPair(), blockHashPair(),
                (height, a, b) => {
                    let result = verifier.compareBlockHashes(height, a, b);
                    for (let m of result.mismatches) {
                        assert.ok(validFields.has(m.field), 'unexpected field: ' + m.field);
                    }
                }
            ), { numRuns: NUM_RUNS });
        });
    });

    describe('verifyChainContinuity', function () {

        it('never throws for any inputs', function () {
            fc.assert(fc.property(
                fc.anything(), fc.anything(), fc.anything(),
                (prev, hashes, payload) => {
                    verifier.verifyChainContinuity(prev, hashes, payload);
                }
            ), { numRuns: NUM_RUNS });
        });

        it('always returns { valid: boolean, reason: string|null }', function () {
            fc.assert(fc.property(
                fc.option(fc.integer()), fc.option(blockHashPair()), continuityPayload(),
                (prev, hashes, payload) => {
                    let result = verifier.verifyChainContinuity(
                        prev === undefined ? null : prev,
                        hashes === undefined ? null : hashes,
                        payload
                    );
                    assert.strictEqual(typeof result.valid, 'boolean');
                    assert.ok(result.reason === null || typeof result.reason === 'string');
                }
            ), { numRuns: NUM_RUNS });
        });

        it('valid is true when prevBlockIndex is null', function () {
            fc.assert(fc.property(
                fc.anything(), continuityPayload(),
                (hashes, payload) => {
                    let result = verifier.verifyChainContinuity(null, hashes, payload);
                    assert.strictEqual(result.valid, true);
                }
            ), { numRuns: NUM_RUNS });
        });

        it('valid is true for exactly sequential blocks', function () {
            fc.assert(fc.property(
                fc.integer({ min: 1, max: 9000000 }),
                (n) => {
                    let result = verifier.verifyChainContinuity(
                        n,
                        { ledger_hash: 'a', actions_hash: 'b', contract_hash: 'c' },
                        { block_index: n + 1 }
                    );
                    assert.strictEqual(result.valid, true);
                }
            ), { numRuns: NUM_RUNS });
        });

        it('valid is false when there is a block gap', function () {
            fc.assert(fc.property(
                fc.integer({ min: 1, max: 9000000 }),
                fc.integer({ min: 2, max: 100 }),
                (n, gap) => {
                    let result = verifier.verifyChainContinuity(
                        n,
                        { ledger_hash: 'a', actions_hash: 'b', contract_hash: 'c' },
                        { block_index: n + gap }
                    );
                    assert.strictEqual(result.valid, false);
                    assert.ok(typeof result.reason === 'string');
                    assert.ok(result.reason.length > 0);
                }
            ), { numRuns: NUM_RUNS });
        });
    });
});
