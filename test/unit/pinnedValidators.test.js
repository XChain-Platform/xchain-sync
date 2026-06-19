/*********************************************************************
 *
 * Copyright © 2025–2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC – https://dankest.llc
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This file is part of XChain Platform. Licensed under the GNU Affero
 * General Public License v3.0 or later; see LICENSE.md.
 *
 **********************************************************************
 * pinnedValidators: the out-of-band trust root for the SPV checkpoint-quorum
 * anchor. Must be INERT (null) for every real (chain, network) until launch
 * values land, and an env override must parse only when well-formed (fail
 * closed: a malformed override yields null, never a weak set).
 ********************************************************************/

const assert = require('assert');
const pinned = require('../../src/pinnedValidators');

const ENVKEY = 'CHECKPOINT_VALIDATORS_BTC_REGTEST';

describe('pinnedValidators @regression', function(){
    afterEach(function(){ delete process.env[ENVKEY]; });

    it('is INERT: every real (chain, network) pins null', function(){
        for(const chain of ['BTC', 'LTC', 'DOGE']){
            for(const net of ['mainnet', 'testnet', 'regtest']){
                assert.strictEqual(pinned.getPinnedValidators(chain, net), null, chain + ':' + net);
            }
        }
    });

    it('returns null for an unknown chain/network and for null args', function(){
        assert.strictEqual(pinned.getPinnedValidators('NOPE', 'mainnet'), null);
        assert.strictEqual(pinned.getPinnedValidators('BTC', 'nope'), null);
        assert.strictEqual(pinned.getPinnedValidators(null, null), null);
    });

    it('parses a well-formed env override (case-insensitive lookup)', function(){
        const set = [{ pubkey: 'aa'.repeat(32), weight: '100', source: 'S1' }];
        process.env[ENVKEY] = JSON.stringify(set);
        assert.deepStrictEqual(pinned.getPinnedValidators('btc', 'REGTEST'), set);
    });

    it('fails closed (null) on a malformed env override', function(){
        process.env[ENVKEY] = 'not json';
        assert.strictEqual(pinned.getPinnedValidators('BTC', 'regtest'), null);
        process.env[ENVKEY] = JSON.stringify([{ pubkey: 'aa', weight: 100 }]); // weight not string, no source
        assert.strictEqual(pinned.getPinnedValidators('BTC', 'regtest'), null);
        process.env[ENVKEY] = JSON.stringify([]);                              // empty
        assert.strictEqual(pinned.getPinnedValidators('BTC', 'regtest'), null);
    });
});
