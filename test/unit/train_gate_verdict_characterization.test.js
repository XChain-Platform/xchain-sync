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

// Characterization of the whole verdict evaluateTrainActivation returns today:
// all eight fields, with each reason spelled out in full. The other train gate
// suite asserts on fragments of a verdict, which lets a reworded reason or a
// dropped field pass; this one pins the exact value so any byte-level change to
// the gate or to a copy of it shows up as a failed deepStrictEqual. Every call
// passes `activation` explicitly, so the shipped map is never read.

const assert = require('assert');
const { evaluateTrainActivation } = require('../../src/consensus/gates/train_gate.js');

const FLOOR = { '1.0.0': { mainnet: 0, testnet: 0, regtest: 0 } };
const TWO_ARM = {
    '1.0.0': { mainnet: 0, testnet: 0, regtest: 0 },
    '2.0.0': { mainnet: 970000, testnet: 150000, regtest: 0 }
};

function manifestRequiring(version, heights) {
    return { trainActivation: { ruleSetVersion: version, heights: heights === undefined ? {} : heights } };
}

const CARRIES = 'platform version 2.0.0 carries it; recover with the node update command, ' +
    'which installs the pinned component set from the signed manifest';

function verdictOn(manifest, height, activation) {
    return evaluateTrainActivation({ manifest, network: 'mainnet', height, activation: activation || FLOOR });
}

describe('train_gate verdict characterization: requirement read', function () {
    it('clears with every field spelled out when nothing is required', function () {
        assert.deepStrictEqual(verdictOn({}, 969999), {
            status: 'clear',
            activeRuleSet: '1.0.0',
            requiredRuleSet: null,
            requiredAtHeight: null,
            network: 'mainnet',
            height: 969999,
            classification: null,
            reason: null
        });
    });

    it('halts on a malformed block and carries no classification', function () {
        assert.deepStrictEqual(verdictOn({ trainActivation: { ruleSetVersion: 'two' } }, 969999), {
            status: 'halt',
            activeRuleSet: '1.0.0',
            requiredRuleSet: null,
            requiredAtHeight: null,
            network: 'mainnet',
            height: 969999,
            classification: null,
            reason: 'train_activation: the release manifest carries a trainActivation block this build ' +
                'cannot read (trainActivation.ruleSetVersion is not a bare X.Y.Z ("two")), so the ' +
                'required rule set cannot be determined; refusing to advance'
        });
    });

    it('clears and carries the manifest classification for a rule set the build implements', function () {
        const manifest = manifestRequiring('2.0.0', { mainnet: 970000 });
        manifest.trainActivation.classification = 'consensus';
        assert.deepStrictEqual(verdictOn(manifest, 969999, TWO_ARM), {
            status: 'clear',
            activeRuleSet: '1.0.0',
            requiredRuleSet: null,
            requiredAtHeight: null,
            network: 'mainnet',
            height: 969999,
            classification: 'consensus',
            reason: null
        });
    });
});

describe('train_gate verdict characterization: unprovable boundary', function () {
    it('halts when the manifest names no activation height for the network', function () {
        assert.deepStrictEqual(verdictOn(manifestRequiring('2.0.0', { testnet: 150000 }), 1), {
            status: 'halt',
            activeRuleSet: '1.0.0',
            requiredRuleSet: '2.0.0',
            requiredAtHeight: null,
            network: 'mainnet',
            height: 1,
            classification: null,
            reason: 'train_activation: the release manifest requires rule set 2.0.0, which this build ' +
                'does not implement, and names no activation height for network "mainnet", so the ' +
                'boundary cannot be proven to be ahead. ' + CARRIES
        });
    });

    it('halts naming no BTC height when the clock is null', function () {
        assert.deepStrictEqual(verdictOn(manifestRequiring('2.0.0', { mainnet: 970000 }), null), {
            status: 'halt',
            activeRuleSet: null,
            requiredRuleSet: '2.0.0',
            requiredAtHeight: 970000,
            network: 'mainnet',
            height: null,
            classification: null,
            reason: 'train_activation: the release manifest requires rule set 2.0.0 at BTC height 970000 ' +
                'on mainnet, which this build does not implement, and this service has no BTC height to ' +
                'compare against, so the boundary cannot be proven to be ahead. ' + CARRIES
        });
    });
});

describe('train_gate verdict characterization: boundary reached', function () {
    it('halts at the boundary and above it', function () {
        for (const height of [970000, 970001]) {
            assert.deepStrictEqual(verdictOn(manifestRequiring('2.0.0', { mainnet: 970000 }), height), {
                status: 'halt',
                activeRuleSet: '1.0.0',
                requiredRuleSet: '2.0.0',
                requiredAtHeight: 970000,
                network: 'mainnet',
                height,
                classification: null,
                reason: 'train_activation: block at BTC height ' + height + ' is at or above the 2.0.0 ' +
                    'activation height 970000 on mainnet, and this build does not implement rule set ' +
                    '2.0.0. Applying it under the old rules would fork. ' + CARRIES
            });
        }
    });
});

describe('train_gate verdict characterization', function () {
    describe('pending three blocks below the boundary', function () {
        const expected = {
            status: 'pending',
            activeRuleSet: '1.0.0',
            requiredRuleSet: '2.0.0',
            requiredAtHeight: 970000,
            network: 'mainnet',
            height: 969997,
            classification: null,
            reason: 'train_activation: the release manifest requires rule set 2.0.0 from BTC height 970000 ' +
                'on mainnet, which this build does not implement. This node will HALT at that height, ' +
                'in 3 block(s). ' + CARRIES
        };
        const manifest = manifestRequiring('2.0.0', { mainnet: 970000 });
        const base = { network: 'mainnet', height: 969997, activation: FLOOR };

        it('returns the full verdict from a manifest', function () {
            assert.deepStrictEqual(evaluateTrainActivation(Object.assign({ manifest }, base)), expected);
        });

        it('returns the same verdict from a raw manifest passed as required', function () {
            assert.deepStrictEqual(evaluateTrainActivation(Object.assign({ required: manifest }, base)), expected);
        });

        it('returns the same verdict from an already read block passed as required', function () {
            const block = {
                ruleSetVersion: '2.0.0',
                heights: { mainnet: 970000 },
                computedFromBtcTip: null,
                classification: null
            };
            assert.deepStrictEqual(evaluateTrainActivation(Object.assign({ required: block }, base)), expected);
        });
    });
});
