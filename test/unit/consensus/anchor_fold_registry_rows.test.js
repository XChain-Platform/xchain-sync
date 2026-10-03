'use strict';

// Copyright © 2025-2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC - https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md.

const assert = require('assert');

const registry = require('../../../src/consensus/gate_registry.js');

const ENV = 'XC_ANCHOR_FOLD_REGTEST_ACTIVATION';
const KEYS = [
    'anchor_fold_activation.ANCHOR_FOLD_ACTIVATION',
    'archive_section_verdict_activation.ARCHIVE_SECTION_VERDICT_STATE_HASH_ACTIVATION',
];

function withEnv(value, fn) {
    const saved = process.env[ENV];
    try {
        if (value === undefined) delete process.env[ENV];
        else process.env[ENV] = value;
        return fn();
    } finally {
        if (saved === undefined) delete process.env[ENV];
        else process.env[ENV] = saved;
    }
}

describe('consensus anchor fold registry rows', function () {
    it('carries both activation rows', function () {
        for (const key of KEYS) assert.strictEqual(registry.has(key), true);
    });

    it('keeps both activation maps inert by default', function () {
        withEnv(undefined, () => {
            for (const key of KEYS) {
                assert.deepStrictEqual(registry.get(key), {
                    mainnet: 9999999999,
                    'BTC:testnet': 154939,
                    'LTC:testnet': 4905307,
                    'DOGE:testnet': 67960786,
                    testnet: 9999999999,
                    regtest: null,
                });
            }
        });
    });

    it('arms both regtest entries from the shared venue variable', function () {
        withEnv('armed', () => {
            for (const key of KEYS) assert.strictEqual(registry.get(key).regtest, 0);
        });
    });

    it('keeps both rows inactive below the sentinel', function () {
        withEnv(undefined, () => {
            for (const key of KEYS) {
                assert.strictEqual(registry.activeAt(key, 'mainnet', null, 99999999, null), false);
                assert.strictEqual(registry.activeAt(key, 'testnet', null, 99999999, null), false);
            }
        });
    });
});
