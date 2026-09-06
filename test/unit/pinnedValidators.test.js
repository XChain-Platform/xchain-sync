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

describe('pinnedValidators: rotation seed checkpoint @regression', function(){
    const SEEDKEY = 'CHECKPOINT_SEED_BTC_REGTEST';
    afterEach(function(){ delete process.env[SEEDKEY]; });

    const goodSeed = {
        block_index: 1000, snapshot_block: 994, checkpoint_seq: 0,
        state_root: 'ab'.repeat(32), state_root_version: 1,
        block_merkle_root: 'cd'.repeat(32), block_merkle_version: 1
    };

    it('is INERT: every real (chain, network) seed pins null', function(){
        for(const chain of ['BTC', 'LTC', 'DOGE']){
            for(const net of ['mainnet', 'testnet', 'regtest']){
                assert.strictEqual(pinned.getPinnedCheckpoint(chain, net), null, chain + ':' + net);
            }
        }
    });

    it('returns null for an unknown chain/network and for null args', function(){
        assert.strictEqual(pinned.getPinnedCheckpoint('NOPE', 'mainnet'), null);
        assert.strictEqual(pinned.getPinnedCheckpoint('BTC', 'nope'), null);
        assert.strictEqual(pinned.getPinnedCheckpoint(null, null), null);
    });

    it('parses a well-formed env seed override (case-insensitive lookup)', function(){
        process.env[SEEDKEY] = JSON.stringify(goodSeed);
        assert.deepStrictEqual(pinned.getPinnedCheckpoint('btc', 'REGTEST'), goodSeed);
    });

    it('fails closed (null) on a malformed env seed override', function(){
        process.env[SEEDKEY] = 'not json';
        assert.strictEqual(pinned.getPinnedCheckpoint('BTC', 'regtest'), null);
        process.env[SEEDKEY] = JSON.stringify(Object.assign({}, goodSeed, { state_root: undefined })); // no state_root
        assert.strictEqual(pinned.getPinnedCheckpoint('BTC', 'regtest'), null);
        process.env[SEEDKEY] = JSON.stringify(Object.assign({}, goodSeed, { block_index: '1000' })); // not a number
        assert.strictEqual(pinned.getPinnedCheckpoint('BTC', 'regtest'), null);
        process.env[SEEDKEY] = JSON.stringify([goodSeed]);  // array, not an object
        assert.strictEqual(pinned.getPinnedCheckpoint('BTC', 'regtest'), null);
    });
});

// The getters answer null for an override nobody set AND for one the operator set and
// got wrong, and with every baked-in pin null those two states reach
// ClientSync._verifyCheckpointQuorum identically: `if(!validators||!validators.length)
// return;` skips checkpoint authentication on a replica whose operator armed
// VERIFY_CHECKPOINT_QUORUM. assertPinnedEnvOverrides is the startup refusal that keeps
// an explicit-but-invalid value from reading as "no pin configured".
describe('pinnedValidators: assertPinnedEnvOverrides @regression', function(){
    const VKEY = 'CHECKPOINT_VALIDATORS_BTC_REGTEST';
    const SKEY = 'CHECKPOINT_SEED_BTC_REGTEST';
    const goodSet  = [{ pubkey: 'aa'.repeat(32), weight: '100', source: 'S1' }];
    const goodSeed = {
        block_index: 1000, snapshot_block: 994, checkpoint_seq: 0,
        state_root: 'ab'.repeat(32), state_root_version: 1,
        block_merkle_root: 'cd'.repeat(32), block_merkle_version: 1
    };

    function assertRefuses(env, needle){
        assert.throws(
            () => pinned.assertPinnedEnvOverrides(env),
            (e) => e instanceof Error
                && /Refusing to start/.test(e.message)
                && e.message.indexOf(needle) !== -1,
            'expected a refusal naming ' + needle + ' for ' + JSON.stringify(env)
        );
    }

    it('does not throw when no override is present at all', function(){
        pinned.assertPinnedEnvOverrides({ SYNC_MODE: 'client' });
    });

    it('treats an unset or empty-string override as absent, not invalid', function(){
        pinned.assertPinnedEnvOverrides({ [VKEY]: '', [SKEY]: '' });
    });

    it('accepts well-formed validator and seed overrides', function(){
        pinned.assertPinnedEnvOverrides({ [VKEY]: JSON.stringify(goodSet), [SKEY]: JSON.stringify(goodSeed) });
    });

    it('refuses a validator override that is not JSON, not an array, empty, or wrongly shaped', function(){
        assertRefuses({ [VKEY]: 'not json' }, VKEY);
        assertRefuses({ [VKEY]: JSON.stringify({ pubkey: 'aa' }) }, 'is not a JSON array');
        assertRefuses({ [VKEY]: JSON.stringify([]) }, 'is an empty array');
        assertRefuses({ [VKEY]: JSON.stringify([{ pubkey: 'aa', weight: 100, source: 'S1' }]) }, 'string `weight`');
        assertRefuses({ [VKEY]: JSON.stringify([{ pubkey: 'aa', weight: '100' }]) }, 'string `source`');
    });

    it('refuses a seed override that is not JSON, not an object, or missing a required field', function(){
        assertRefuses({ [SKEY]: 'not json' }, SKEY);
        assertRefuses({ [SKEY]: JSON.stringify([goodSeed]) }, 'is not a JSON object');
        assertRefuses({ [SKEY]: JSON.stringify(Object.assign({}, goodSeed, { state_root: undefined })) }, '`state_root`');
        assertRefuses({ [SKEY]: JSON.stringify(Object.assign({}, goodSeed, { block_index: '1000' })) }, '`block_index`');
    });

    it('names every offender, not just the first', function(){
        try {
            pinned.assertPinnedEnvOverrides({ [VKEY]: 'not json', [SKEY]: 'not json' });
            assert.fail('expected a refusal');
        } catch(e){
            assert.ok(e.message.indexOf(VKEY) !== -1, 'names the validator override');
            assert.ok(e.message.indexOf(SKEY) !== -1, 'names the seed override');
        }
    });

    it('ignores a prefixed name with no CHAIN_NETWORK shape (the getters never read it)', function(){
        pinned.assertPinnedEnvOverrides({ CHECKPOINT_VALIDATORS_BADKEY: 'not json' });
    });

    it('does not reject a chain/network pair that is absent from the baked-in map', function(){
        pinned.assertPinnedEnvOverrides({ CHECKPOINT_VALIDATORS_BITCOIN_MAINNET: JSON.stringify(goodSet) });
    });

    it('defaults to process.env when called with no argument', function(){
        process.env[VKEY] = 'not json';
        try {
            assert.throws(() => pinned.assertPinnedEnvOverrides(), /Refusing to start/);
        } finally { delete process.env[VKEY]; }
    });
});
