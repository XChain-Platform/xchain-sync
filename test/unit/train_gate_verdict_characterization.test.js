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

// Characterization of the whole verdict evaluateTrainActivation returns today,
// every field and the exact reason text, on monotone synthetic maps. Each call
// passes `activation` explicitly so the shipped map is never read; a later edit
// to the gate or its byte copies that moves any verdict on these maps fails here.

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

describe('train gate verdict characterization', function () {

    it('gives clear when nothing requires a rule set', function () {
        assert.deepStrictEqual(
            evaluateTrainActivation({ activation: FLOOR, network: 'mainnet', height: 100, required: null }),
            {
                status: 'clear',
                activeRuleSet: '1.0.0',
                requiredRuleSet: null,
                requiredAtHeight: null,
                network: 'mainnet',
                height: 100,
                classification: null,
                reason: null
            });
    });

    it('halts on a malformed block with a null classification', function () {
        assert.deepStrictEqual(
            evaluateTrainActivation({
                activation: FLOOR, network: 'mainnet', height: 100,
                manifest: { trainActivation: { ruleSetVersion: 'two' } }
            }),
            {
                status: 'halt',
                activeRuleSet: '1.0.0',
                requiredRuleSet: null,
                requiredAtHeight: null,
                network: 'mainnet',
                height: 100,
                classification: null,
                reason: 'train_activation: the release manifest carries a trainActivation block this build ' +
                        'cannot read (trainActivation.ruleSetVersion is not a bare X.Y.Z ("two")), so the ' +
                        'required rule set cannot be determined; refusing to advance'
            });
    });

    it('gives clear with the classification carried when the build implements the rule set', function () {
        const manifest = manifestRequiring('2.0.0', { mainnet: 970000 });
        manifest.trainActivation.classification = 'major';
        assert.deepStrictEqual(
            evaluateTrainActivation({ activation: TWO_ARM, network: 'mainnet', height: 100, manifest }),
            {
                status: 'clear',
                activeRuleSet: '1.0.0',
                requiredRuleSet: null,
                requiredAtHeight: null,
                network: 'mainnet',
                height: 100,
                classification: 'major',
                reason: null
            });
    });

});

describe('train gate verdict characterization, unproven boundary', function () {

    it('halts when the manifest names no activation height for the network', function () {
        assert.deepStrictEqual(
            evaluateTrainActivation({
                activation: FLOOR, network: 'mainnet', height: 100,
                manifest: manifestRequiring('2.0.0', { testnet: 150000 })
            }),
            {
                status: 'halt',
                activeRuleSet: '1.0.0',
                requiredRuleSet: '2.0.0',
                requiredAtHeight: null,
                network: 'mainnet',
                height: 100,
                classification: null,
                reason: 'train_activation: the release manifest requires rule set 2.0.0, which this build ' +
                        'does not implement, and names no activation height for network "mainnet", so the ' +
                        'boundary cannot be proven to be ahead. ' + CARRIES
            });
    });

    it('halts on a null height, naming no BTC height to compare against', function () {
        assert.deepStrictEqual(
            evaluateTrainActivation({
                activation: FLOOR, network: 'mainnet', height: null,
                manifest: manifestRequiring('2.0.0', { mainnet: 970000 })
            }),
            {
                status: 'halt',
                activeRuleSet: null,
                requiredRuleSet: '2.0.0',
                requiredAtHeight: 970000,
                network: 'mainnet',
                height: null,
                classification: null,
                reason: 'train_activation: the release manifest requires rule set 2.0.0 at BTC height ' +
                        '970000 on mainnet, which this build does not implement, and this service has no ' +
                        'BTC height to compare against, so the boundary cannot be proven to be ahead. ' + CARRIES
            });
    });

});

describe('train gate verdict characterization, reached boundary', function () {

    [[970000, 'at the boundary'], [970005, 'above the boundary']].forEach(function ([height, where]) {
        it('halts ' + where, function () {
            assert.deepStrictEqual(
                evaluateTrainActivation({
                    activation: FLOOR, network: 'mainnet', height,
                    manifest: manifestRequiring('2.0.0', { mainnet: 970000 })
                }),
                {
                    status: 'halt',
                    activeRuleSet: '1.0.0',
                    requiredRuleSet: '2.0.0',
                    requiredAtHeight: 970000,
                    network: 'mainnet',
                    height,
                    classification: null,
                    reason: 'train_activation: block at BTC height ' + height + ' is at or above the 2.0.0 ' +
                            'activation height 970000 on mainnet, and this build does not implement rule ' +
                            'set 2.0.0. Applying it under the old rules would fork. ' + CARRIES
                });
        });
    });

});

describe('train gate verdict characterization, pending', function () {
    describe('three blocks below the boundary', function () {
        const pending = {
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
        const base = { activation: FLOOR, network: 'mainnet', height: 969997 };
        const manifest = manifestRequiring('2.0.0', { mainnet: 970000 });

        it('gives pending from an already read block', function () {
            const required = {
                ruleSetVersion: '2.0.0', heights: { mainnet: 970000 }, computedFromBtcTip: null, classification: null
            };
            assert.deepStrictEqual(evaluateTrainActivation(Object.assign({ required }, base)), pending);
        });

        it('gives the same verdict from a raw manifest passed as required', function () {
            assert.deepStrictEqual(evaluateTrainActivation(Object.assign({ required: manifest }, base)), pending);
        });

        it('gives the same verdict from the manifest option', function () {
            assert.deepStrictEqual(evaluateTrainActivation(Object.assign({ manifest }, base)), pending);
        });
    });
});
