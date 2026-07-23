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
 * Unit: coinTicker() normalization for per-chain activation lookups 
 *
 * The sync layer identifies a chain by `cfg.coin`, which arrives as the FULL
 * LOWERCASE NAME ('litecoin'). Every per-chain activation map is keyed
 * '<TICKER>:<network>' ('LTC:mainnet'), matching what the SOURCE indexer passes
 * (config['COIN']). Passing the full name silently missed the key, which:
 *   - made the follower's state-hash class gating disagree with the source, so the
 *     recompute diverged on every block at/after each chain's activation height and
 *     HALTED every production replica (reason 'state-hash-divergence'); and
 *   - made isStateCommitmentActive() resolve to "off" on every mainnet/testnet
 *     chain, so the follower never computed SMT roots and the light-client
 *     state-commitment check never ran at all (it failed OPEN, not closed).
 *
 * These tests pin the normalization AND the underlying trap, so a future refactor
 * that reverts to passing the full name fails here instead of in production.
 */

'use strict';

const assert = require('assert');
const { coinTicker } = require('../../src/consensus-constants');
const { isStateCommitmentActive } = require('../../src/state_commitment_activation');

describe('coinTicker() + per-chain activation lookup ', function () {

    describe('normalization', function () {
        it('maps full lowercase chain names to their ticker', function () {
            assert.strictEqual(coinTicker('bitcoin'),  'BTC');
            assert.strictEqual(coinTicker('litecoin'), 'LTC');
            assert.strictEqual(coinTicker('dogecoin'), 'DOGE');
        });

        it('is idempotent on tickers and case-insensitive', function () {
            assert.strictEqual(coinTicker('BTC'), 'BTC');
            assert.strictEqual(coinTicker('ltc'), 'LTC');
            assert.strictEqual(coinTicker('Dogecoin'), 'DOGE');
        });

        it('passes null/undefined through unchanged (legacy no-op callers)', function () {
            assert.strictEqual(coinTicker(null), null);
            assert.strictEqual(coinTicker(undefined), undefined);
        });

        it('returns an unrecognized coin UNCHANGED so no caller regresses', function () {
            assert.strictEqual(coinTicker('quatloo'), 'quatloo');
        });
    });

    describe('the trap this exists to prevent', function () {
        // Armed per-chain heights (state_commitment_activation.js). The bare
        // 'mainnet'/'testnet' keys deliberately do NOT exist, so a full-name lookup
        // falls through to undefined => inert.
        const ARMED = [
            { coin: 'BTC',  full: 'bitcoin',  network: 'mainnet', height: 958500  },
            { coin: 'LTC',  full: 'litecoin', network: 'mainnet', height: 3143000 },
            { coin: 'DOGE', full: 'dogecoin', network: 'mainnet', height: 6291000 },
            { coin: 'BTC',  full: 'bitcoin',  network: 'testnet', height: 145000  },
            { coin: 'LTC',  full: 'litecoin', network: 'testnet', height: 4805000 },
        ];

        it('the TICKER form activates at the armed height on every production chain', function () {
            for (const c of ARMED) {
                assert.strictEqual(
                    isStateCommitmentActive(c.height, c.network, c.coin), true,
                    `${c.coin}/${c.network} must be ACTIVE at its armed height ${c.height}`);
                assert.strictEqual(
                    isStateCommitmentActive(c.height - 1, c.network, c.coin), false,
                    `${c.coin}/${c.network} must be inactive one block below ${c.height}`);
            }
        });

        it('the FULL-NAME form silently resolves to OFF (the  bug)', function () {
            for (const c of ARMED) {
                assert.strictEqual(
                    isStateCommitmentActive(c.height, c.network, c.full), false,
                    `${c.full}/${c.network} misses the per-chain key and falls through to inert`);
            }
        });

        it('coinTicker() reconciles the two: normalized full name == ticker result', function () {
            for (const c of ARMED) {
                assert.strictEqual(
                    isStateCommitmentActive(c.height, c.network, coinTicker(c.full)),
                    isStateCommitmentActive(c.height, c.network, c.coin),
                    `${c.full}/${c.network} must agree with ${c.coin} once normalized`);
            }
        });

        it('regtest is unaffected either way (bare key), which is why regtest replicas never halted', function () {
            for (const form of ['BTC', 'bitcoin', 'LTC', 'litecoin', 'DOGE', 'dogecoin']) {
                assert.strictEqual(isStateCommitmentActive(0, 'regtest', form), true,
                    `regtest resolves via the bare key for ${form}`);
            }
        });
    });
});
