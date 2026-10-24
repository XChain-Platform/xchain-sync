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
 * anchor. Testnet carries its live validator set while mainnet and regtest stay
 * INERT (null), and an env override must parse only when well-formed (fail
 * closed: a malformed override yields null, never a weak set).
 ********************************************************************/

const assert = require('assert');
const pinned = require('../../src/client/pinned_validators');

const ENVKEY = 'CHECKPOINT_VALIDATORS_BTC_REGTEST';
const TESTNET_VALIDATORS = [
    { pubkey: '18e4c1d54c8932d51fa54e4bc0164461a0605fb801bfca21cad57fa74495c174', weight: '50000.00000000', source: 'mverify2tE4RK7QYHRXbpmj6iLabcYtpWY' },
    { pubkey: '2661bd8f42b910bbb471387b7c28eaa440fc3c99077c160a69f22859f924aa7d', weight: '50000.00000000', source: 'mverify1Q6YPNqP4UzqLoa1MpDDZUu4ogC' },
    { pubkey: '72a25832d5f9d30baf704579fc440723f03cba55098cb2aac21063bf71029bad', weight: '50000.00000000', source: 'mverify4Hoe6uqa7ZJanX7GFXDpZ6dmtyX' },
    { pubkey: '8a1215431e8143b2ebdc9fed2fe7f655ecc327c4efd66798bc4e5504a5d5b0a8', weight: '50000.00000000', source: 'mverify3LaXvGitmYx3nHa3y2LMJ1odWVc' },
    { pubkey: 'c1de91459e8bd93de9e3263c26d886058593d3fdae57ad0c1bd48ca3dd22f32a', weight: '50000.00000000', source: 'mverify5szar1XwwAjqAnPUHbA814kfHh4' },
];

describe('pinnedValidators @regression', function(){
    afterEach(function(){ delete process.env[ENVKEY]; });

    it('pins the five live validators for every testnet chain', function(){
        for(const chain of ['BTC', 'LTC', 'DOGE']){
            assert.deepStrictEqual(pinned.getPinnedValidators(chain, 'testnet'), TESTNET_VALIDATORS, chain);
        }
    });

    it('keeps mainnet and regtest INERT', function(){
        for(const chain of ['BTC', 'LTC', 'DOGE']){
            for(const net of ['mainnet', 'regtest']){
                assert.strictEqual(pinned.getPinnedValidators(chain, net), null, chain + ':' + net);
            }
        }
    });

    it('freezes the shipped validator set and its entries', function(){
        const set = pinned.getPinnedValidators('BTC', 'testnet');
        assert.strictEqual(Object.isFrozen(set), true);
        assert.strictEqual(set.length, 5);
        set.forEach((validator) => assert.strictEqual(Object.isFrozen(validator), true));
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

// The service passes the hub's full name ('bitcoin'), while the documented env name
// and every baked-in key use the ticker, so a full-name lookup must still find them.
describe('pinnedValidators: the hub\'s full coin name @regression', function(){
    afterEach(function(){ delete process.env[ENVKEY]; });

    it('resolves the ticker-form env override under the hub\'s full coin name', function(){
        const set = [{ pubkey: 'aa'.repeat(32), weight: '100', source: 'S1' }];
        process.env[ENVKEY] = JSON.stringify(set);
        assert.deepStrictEqual(pinned.getPinnedValidators('bitcoin', 'regtest'), set);
        assert.deepStrictEqual(pinned.getPinnedValidators('BTC', 'regtest'), set);
    });

    it('still resolves a full-name env override, and the ticker form wins when both are set', function(){
        const FULLKEY = 'CHECKPOINT_VALIDATORS_BITCOIN_REGTEST';
        const full   = [{ pubkey: 'bb'.repeat(32), weight: '100', source: 'F1' }];
        const ticker = [{ pubkey: 'cc'.repeat(32), weight: '100', source: 'T1' }];
        try {
            process.env[FULLKEY] = JSON.stringify(full);
            assert.deepStrictEqual(pinned.getPinnedValidators('bitcoin', 'regtest'), full);
            process.env[ENVKEY] = JSON.stringify(ticker);
            assert.deepStrictEqual(pinned.getPinnedValidators('bitcoin', 'regtest'), ticker);
        } finally {
            delete process.env[FULLKEY];
        }
    });

    it('resolves testnet pins and inert networks under the full coin names too', function(){
        for(const chain of ['bitcoin', 'litecoin', 'dogecoin']){
            assert.deepStrictEqual(pinned.getPinnedValidators(chain, 'testnet'), TESTNET_VALIDATORS, chain);
            for(const net of ['mainnet', 'regtest']){
                assert.strictEqual(pinned.getPinnedValidators(chain, net), null, chain + ':' + net);
            }
        }
    });
});

describe('pinnedValidators: the hub\'s full coin name, seed checkpoint @regression', function(){
    const FULLKEY = 'CHECKPOINT_SEED_BITCOIN_REGTEST';
    afterEach(function(){ delete process.env[SKEY]; delete process.env[FULLKEY]; });

    it('resolves the ticker-form env seed under the hub\'s full coin name', function(){
        process.env[SKEY] = JSON.stringify(goodSeed);
        assert.deepStrictEqual(pinned.getPinnedCheckpoint('bitcoin', 'regtest'), goodSeed);
    });

    it('still resolves a full-name env seed, and the ticker form wins when both are set', function(){
        const fullSeed = Object.assign({}, goodSeed, { block_index: 2000 });
        process.env[FULLKEY] = JSON.stringify(fullSeed);
        assert.deepStrictEqual(pinned.getPinnedCheckpoint('bitcoin', 'regtest'), fullSeed);
        process.env[SKEY] = JSON.stringify(goodSeed);
        assert.deepStrictEqual(pinned.getPinnedCheckpoint('bitcoin', 'regtest'), goodSeed);
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

// These regtest getters answer null for an override nobody set AND for one the operator
// set and got wrong. Since their baked-in pins are null, those two states reach
// ClientSync.verifyCheckpointQuorum identically: `if(!validators||!validators.length)
// return;` skips checkpoint authentication on a replica whose operator armed
// VERIFY_CHECKPOINT_QUORUM. assertPinnedEnvOverrides is the startup refusal that keeps
// an explicit-but-invalid value from reading as "no pin configured".
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

describe('pinnedValidators: assertPinnedEnvOverrides @regression', function(){

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

    // Each of these is well typed but can never verify, so it must be named at startup
    // instead of starting and halting later on a quorum failure that blames the source.
    it('refuses a typed validator override the checkpoint verifier always rejects', function(){
        const entry = (o) => JSON.stringify([Object.assign({}, goodSet[0], o)]);
        assertRefuses({ [VKEY]: entry({ pubkey: 'aa' }) }, '64-hex');
        assertRefuses({ [VKEY]: entry({ pubkey: 'zz'.repeat(32) }) }, '64-hex');
        assertRefuses({ [VKEY]: entry({ source: '  ' }) }, 'unusable stake field');
        assertRefuses({ [VKEY]: entry({ weight: 'lots' }) }, 'unusable stake field');
        assertRefuses({ [VKEY]: entry({ weight: '-5' }) }, 'unusable stake field');
        assertRefuses({ [VKEY]: entry({ weight: '0x10' }) }, 'unusable stake field');
        assertRefuses({ [VKEY]: entry({ weight: '0' }) }, 'zero total stake');
        const mixed = goodSet.concat([{ pubkey: 'bb'.repeat(32), weight: '', source: 'S2' }]);
        assertRefuses({ [VKEY]: JSON.stringify(mixed) }, 'entry 1 has an unusable stake field');
        pinned.assertPinnedEnvOverrides({ [VKEY]: JSON.stringify(goodSet.concat(
            [{ pubkey: 'BB'.repeat(32), weight: '0', source: 'S2' }])) });
    });
});

describe('pinnedValidators: assertPinnedEnvOverrides @regression', function(){

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

    // A well-formed value under a name no getter reads leaves the anchor off exactly as an
    // invalid one does, so the name is refused; every name a getter reads still passes.
    it('refuses a well-formed override under a chain or network name the getters never read', function(){
        const set = JSON.stringify(goodSet), seed = JSON.stringify(goodSeed);
        for(const name of ['CHECKPOINT_VALIDATORS_BITCON_MAINNET', 'CHECKPOINT_VALIDATORS_BTC_MAINNET2',
            'CHECKPOINT_VALIDATORS_btc_mainnet', 'CHECKPOINT_VALIDATORS_BTC_TESTNET_NEXT'])
            assertRefuses({ [name]: set }, name + ' (pinned validator set) names no chain and network');
        assertRefuses({ CHECKPOINT_SEED_DOGE_SIGNET: seed }, 'CHECKPOINT_SEED_DOGE_SIGNET (pinned seed checkpoint) names no');
        for(const chain of ['BTC', 'LTC', 'DOGE', 'BITCOIN', 'LITECOIN', 'DOGECOIN']){
            for(const net of ['MAINNET', 'TESTNET', 'REGTEST']){
                pinned.assertPinnedEnvOverrides({ ['CHECKPOINT_VALIDATORS_' + chain + '_' + net]: set,
                    ['CHECKPOINT_SEED_' + chain + '_' + net]: seed });
            }
        }
    });

    it('defaults to process.env when called with no argument', function(){
        process.env[VKEY] = 'not json';
        try {
            assert.throws(() => pinned.assertPinnedEnvOverrides(), /Refusing to start/);
        } finally { delete process.env[VKEY]; }
    });
});
