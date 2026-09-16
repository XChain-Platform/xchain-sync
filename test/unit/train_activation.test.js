/*********************************************************************
 *
 * Copyright © 2025-2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC - https://dankest.llc
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This file is part of XChain Platform. Licensed under the GNU Affero
 * General Public License v3.0 or later; see LICENSE.md. A commercial
 * license (without AGPL source-disclosure terms) is available -
 * contact legal@dankest.llc.
 *
 *********************************************************************/

'use strict';

// Unit coverage for src/train_activation.js, the platform-train pre-apply gate
// vendored byte-identically from xchain-indexer. The copy is not wired into a
// sync path yet, so this suite is the only thing standing between an edit to it
// and a follower that either halts when it should not or advances past a train
// boundary it cannot apply. Behaviour cases run against LOCAL fixture maps, so a
// legitimate train cut changes the shipped map without reddening them; the one
// case that pins the shipped map pins it exactly, because a loose bound there is
// how a one-sided height edit passes green.

const assert = require('assert');
const {
    TRAIN_ACTIVATION,
    parseRuleSetVersion,
    compareRuleSetVersions,
    implementedRuleSets,
    activationHeightFor,
    asHeight,
    resolveRuleSet,
    readManifestTrainActivation,
    evaluateTrainActivation
} = require('../../src/train_activation.js');

// The launch floor alone: a build that implements only rule set 1.0.0.
const FLOOR = { '1.0.0': { mainnet: 0, testnet: 0, regtest: 0 } };
// A build that also implements the next train, for the ordering cases.
const TWO_ARM = {
    '1.0.0': { mainnet: 0, testnet: 0, regtest: 0 },
    '2.0.0': { mainnet: 970000, testnet: 150000, regtest: 0 }
};

// A manifest requiring `version`, optionally naming activation heights.
function manifestRequiring(version, heights) {
    return { trainActivation: { ruleSetVersion: version, heights: heights === undefined ? {} : heights } };
}

describe('train_activation', function () {

    describe('TRAIN_ACTIVATION (the shipped map)', function () {
        it('pins the launch floor at zero on every network', function () {
            assert.deepStrictEqual(TRAIN_ACTIVATION['1.0.0'], { mainnet: 0, testnet: 0, regtest: 0 });
        });

        it('names every row as a bare X.Y.Z version, so nothing in it is unorderable', function () {
            const rows = Object.keys(TRAIN_ACTIVATION);
            assert.ok(rows.length >= 1, 'the shipped map is empty; resolveRuleSet would answer null everywhere');
            for (const v of rows) assert.ok(parseRuleSetVersion(v), 'unparseable rule-set row: ' + v);
        });
    });

    describe('parseRuleSetVersion', function () {
        it('accepts a bare three-part version and nothing else', function () {
            assert.deepStrictEqual(parseRuleSetVersion('1.2.3'), [1, 2, 3]);
            assert.deepStrictEqual(parseRuleSetVersion(' 1.2.3 '), [1, 2, 3]);
            for (const bad of ['1.2', '1.2.3.4', '1.2.3-rc.1', '1.2.3+build', 'v1.2.3', '', null, undefined, {}])
                assert.strictEqual(parseRuleSetVersion(bad), null, 'accepted ' + JSON.stringify(bad));
        });
    });
});

describe('train_activation', function () {
    describe('compareRuleSetVersions', function () {
        it('orders numerically, not lexically', function () {
            assert.strictEqual(compareRuleSetVersions('2.0.0', '10.0.0'), -1);
            assert.strictEqual(compareRuleSetVersions('1.2.3', '1.2.3'), 0);
            assert.strictEqual(compareRuleSetVersions('1.3.0', '1.2.9'), 1);
        });

        it('throws on an unparseable side rather than ordering it somewhere', function () {
            assert.throws(() => compareRuleSetVersions('1.0', '1.0.0'), /unparseable rule-set version/);
            assert.throws(() => compareRuleSetVersions('1.0.0', 'nonsense'), /unparseable rule-set version/);
        });
    });

    describe('implementedRuleSets', function () {
        it('returns the map keys ascending', function () {
            assert.deepStrictEqual(implementedRuleSets({ '10.0.0': {}, '2.0.0': {}, '1.0.0': {} }),
                ['1.0.0', '2.0.0', '10.0.0']);
        });

        it('falls back to the shipped map when given nothing', function () {
            assert.deepStrictEqual(implementedRuleSets(), Object.keys(TRAIN_ACTIVATION).sort(compareRuleSetVersions));
        });
    });
});

describe('train_activation', function () {
    describe('activationHeightFor', function () {
        it('reads the height a row names', function () {
            assert.strictEqual(activationHeightFor('2.0.0', 'mainnet', TWO_ARM), 970000);
            assert.strictEqual(activationHeightFor('1.0.0', 'regtest', TWO_ARM), 0);
        });

        it('answers null, never zero, for a network or version the map does not name', function () {
            assert.strictEqual(activationHeightFor('2.0.0', 'signet', TWO_ARM), null);
            assert.strictEqual(activationHeightFor('9.9.9', 'mainnet', TWO_ARM), null);
        });
    });

    describe('asHeight', function () {
        // A bare Number() here reads null/''/false as 0, which is "we are at genesis"
        // on a service that has no BTC clock at all. That coercion is the difference
        // between a fail-closed halt and a silent fork, so it gets its own case.
        it('refuses every value that Number() would silently turn into zero', function () {
            for (const v of [null, undefined, '', false, true, 'nonsense', NaN, Infinity])
                assert.strictEqual(asHeight(v), null, 'coerced ' + JSON.stringify(String(v)));
        });

        it('passes a finite height through, including a genuine zero', function () {
            assert.strictEqual(asHeight(0), 0);
            assert.strictEqual(asHeight(970000), 970000);
            assert.strictEqual(asHeight('970000'), 970000);
        });
    });
});

describe('train_activation', function () {
    describe('resolveRuleSet', function () {
        it('returns the greatest entry at or below the height on that network', function () {
            assert.strictEqual(resolveRuleSet(969999, 'mainnet', TWO_ARM), '1.0.0');
            assert.strictEqual(resolveRuleSet(970000, 'mainnet', TWO_ARM), '2.0.0');
            assert.strictEqual(resolveRuleSet(0, 'regtest', TWO_ARM), '2.0.0');
        });

        it('returns null when the clock is unusable or the network is unnamed', function () {
            assert.strictEqual(resolveRuleSet(null, 'mainnet', TWO_ARM), null);
            assert.strictEqual(resolveRuleSet('', 'mainnet', TWO_ARM), null);
            assert.strictEqual(resolveRuleSet(100, 'signet', TWO_ARM), null);
        });
    });
});

describe('train_activation', function () {
    describe('readManifestTrainActivation', function () {
        it('reads a well-formed block into a normalised record', function () {
            const rec = readManifestTrainActivation({
                trainActivation: {
                    ruleSetVersion: ' 2.0.0 ',
                    heights: { mainnet: 970000 },
                    classification: 'consensus',
                    computedFromBtcTip: { height: 969000 }
                }
            });
            assert.strictEqual(rec.ruleSetVersion, '2.0.0');
            assert.deepStrictEqual(rec.heights, { mainnet: 970000 });
            assert.strictEqual(rec.classification, 'consensus');
            assert.deepStrictEqual(rec.computedFromBtcTip, { height: 969000 });
        });

        it('answers null only for a manifest that carries no block at all', function () {
            assert.strictEqual(readManifestTrainActivation(null), null);
            assert.strictEqual(readManifestTrainActivation('not a manifest'), null);
            assert.strictEqual(readManifestTrainActivation({}), null);
            assert.strictEqual(readManifestTrainActivation({ trainActivation: null }), null);
        });

        it('answers a malformed record, never null, for a block it cannot read', function () {
            // Null here would reach the verdict as "the manifest says nothing", which is
            // the silent-fork reading of "the manifest says something unreadable".
            const cases = [
                { trainActivation: [] },
                { trainActivation: 'soon' },
                { trainActivation: { ruleSetVersion: '2.0', heights: {} } },
                { trainActivation: { ruleSetVersion: '2.0.0-rc.1', heights: {} } },
                { trainActivation: { ruleSetVersion: '2.0.0' } },
                { trainActivation: { ruleSetVersion: '2.0.0', heights: [] } }
            ];
            for (const m of cases) {
                const rec = readManifestTrainActivation(m);
                assert.ok(rec && typeof rec.malformed === 'string',
                    'read as non-malformed: ' + JSON.stringify(m));
            }
        });
    });
});

describe('train_activation', function () {
    describe('evaluateTrainActivation', function () {

        it('clears when the manifest requires a rule set this build implements, clock or no clock', function () {
            for (const height of [0, 969999, 12345678, null]) {
                const v = evaluateTrainActivation({
                    manifest: manifestRequiring('1.0.0', { mainnet: 0 }),
                    network: 'mainnet', height, activation: FLOOR
                });
                assert.strictEqual(v.status, 'clear', 'height ' + height + ' did not clear');
                assert.strictEqual(v.requiredRuleSet, null);
            }
        });

        it('clears when the manifest carries no trainActivation block', function () {
            const v = evaluateTrainActivation({ manifest: {}, network: 'mainnet', height: 969999, activation: FLOOR });
            assert.strictEqual(v.status, 'clear');
            assert.strictEqual(v.activeRuleSet, '1.0.0');
        });

        it('is pending below the boundary for an unimplemented rule set, and names the countdown', function () {
            const v = evaluateTrainActivation({
                manifest: manifestRequiring('2.0.0', { mainnet: 970000 }),
                network: 'mainnet', height: 969999, activation: FLOOR
            });
            assert.strictEqual(v.status, 'pending');
            assert.strictEqual(v.requiredRuleSet, '2.0.0');
            assert.strictEqual(v.requiredAtHeight, 970000);
            assert.ok(/2\.0\.0/.test(v.reason) && /970000/.test(v.reason) && /1 block\(s\)/.test(v.reason), v.reason);
        });

        it('halts at or above the boundary for an unimplemented rule set', function () {
            for (const height of [970000, 970001]) {
                const v = evaluateTrainActivation({
                    manifest: manifestRequiring('2.0.0', { mainnet: 970000 }),
                    network: 'mainnet', height, activation: FLOOR
                });
                assert.strictEqual(v.status, 'halt', 'height ' + height + ' did not halt');
                assert.strictEqual(v.requiredAtHeight, 970000);
            }
        });
    });
});

describe('train_activation', function () {
    describe('evaluateTrainActivation', function () {
        it('halts when the manifest names no activation height for this network', function () {
            const v = evaluateTrainActivation({
                manifest: manifestRequiring('2.0.0', { testnet: 150000 }),
                network: 'mainnet', height: 1, activation: FLOOR
            });
            assert.strictEqual(v.status, 'halt');
            assert.strictEqual(v.requiredRuleSet, '2.0.0');
            assert.strictEqual(v.requiredAtHeight, null);
            assert.ok(/names no activation height/.test(v.reason), v.reason);
        });

        it('halts, rather than clearing, when there is no BTC clock to prove the boundary is ahead', function () {
            // Deliberately stricter than the BTC case: a node that cannot prove the
            // boundary is still ahead of it must not advance.
            const v = evaluateTrainActivation({
                manifest: manifestRequiring('2.0.0', { mainnet: 970000 }),
                network: 'mainnet', height: null, activation: FLOOR
            });
            assert.strictEqual(v.status, 'halt');
            assert.strictEqual(v.requiredAtHeight, 970000);
            assert.ok(/no .*BTC height/.test(v.reason), v.reason);
        });

        it('halts on a malformed trainActivation block', function () {
            const v = evaluateTrainActivation({
                manifest: { trainActivation: { ruleSetVersion: '2.0', heights: {} } },
                network: 'mainnet', height: 1, activation: FLOOR
            });
            assert.strictEqual(v.status, 'halt');
            assert.ok(/cannot read/.test(v.reason), v.reason);
        });

        it('reports the active rule set and the network it judged on every verdict', function () {
            const v = evaluateTrainActivation({
                manifest: manifestRequiring('2.0.0', { mainnet: 970000 }),
                network: 'mainnet', height: 969999, activation: TWO_ARM
            });
            // TWO_ARM implements 2.0.0, so the same manifest that is pending on FLOOR clears here.
            assert.strictEqual(v.status, 'clear');
            assert.strictEqual(v.activeRuleSet, '1.0.0');
            assert.strictEqual(v.network, 'mainnet');
            assert.strictEqual(v.height, 969999);
        });
    });
});
