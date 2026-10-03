'use strict';

// Copyright © 2025-2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC - https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md.

const assert = require('assert');
const gateRegistry = require('../../../src/consensus/gate_registry.js');

describe('consensus-bound registry rows', function () {
    describe('anchor bundle order and rollcall activation', function () {
        it('resolves the anchor bundle order activation table', function () {
            assert.deepStrictEqual(
                gateRegistry.get('anchor_bundle_order_activation.ANCHOR_BUNDLE_ORDER_ACTIVATION'),
                { mainnet: 9999999999, 'BTC:testnet': 154939, 'LTC:testnet': 4905307, 'DOGE:testnet': 67960786, testnet: 9999999999, regtest: 0 }
            );
        });

        it('resolves the rollcall activation row exactly once', function () {
            const key = 'rollcall_activation.ROLLCALL_ACTIVATION';
            assert.strictEqual(gateRegistry.keys().filter((candidate) => candidate === key).length, 1);
            assert.doesNotThrow(() => gateRegistry.get(key));
        });
    });

    describe('price scale activation', function () {
        const NAMES = [
            'PRICE_SCALE_MAX_DECIMALS',
            'PRICE_SCALE_ACTIVATION',
            'PRICE_VALUE_RE_LEGACY',
            'PRICE_VALUE_RE_CANONICAL',
        ];

        for (const name of NAMES) {
            it('carries ' + name + ' exactly once', function () {
                const key = 'price_scale_activation.' + name;
                assert.strictEqual(gateRegistry.keys().filter((candidate) => candidate === key).length, 1);
            });
        }

        it('resolves the price scale activation table', function () {
            assert.deepStrictEqual(
                gateRegistry.get('price_scale_activation.PRICE_SCALE_ACTIVATION'),
                { mainnet: 0, testnet: 0, regtest: 0 }
            );
        });

        it('resolves the price scale max decimals bound', function () {
            assert.strictEqual(gateRegistry.get('price_scale_activation.PRICE_SCALE_MAX_DECIMALS'), 8);
        });

        it('accepts and refuses the canonical price value pattern', function () {
            const canonical = gateRegistry.get('price_scale_activation.PRICE_VALUE_RE_CANONICAL');
            assert.ok(canonical.test('1.5'));
            assert.ok(canonical.test('12345.12345678'));
            assert.ok(!canonical.test('01.5'));
            assert.ok(!canonical.test('1.123456789'));
        });
    });
});
