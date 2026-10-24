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
 * Flag-day tripwire: VERIFY_CHECKPOINT_QUORUM's shipped default is decided per
 * (chain, network) (operator ruling 2026-10-12), in CHECKPOINT_QUORUM_DEFAULTS:
 *
 *   - testnet anchors by default, so the anchor is rehearsed before mainnet relies
 *     on it. Until its pins land the step skips (never bypasses).
 *   - mainnet stays OFF while its pinned set is null and must turn ON in the SAME
 *     change that fills its launch keys: a replica with a trust root available that
 *     does not use it is still trusting its source.
 *   - regtest stays OFF: a rail venue runs no signing federation.
 *
 * Nothing but this test enforces the mainnet coupling, so filling a mainnet key
 * without flipping its default fails here with the instruction inline.
 ********************************************************************/

const assert = require('assert');
const config = require('../../src/config');
const { PINNED, CHECKPOINT_QUORUM_DEFAULTS, checkpointQuorumEnabled } = require('../../src/client/pinned_validators');

// True once the SHIPPED file pins a launch validator set for `key`. Env overrides are
// deliberately ignored: this is about what the file pins, which decides the default.
function launchSetPinned(key){
    return Array.isArray(PINNED[key]) && PINNED[key].length > 0;
}

const ENVKEY = 'VERIFY_CHECKPOINT_QUORUM';

function withEnv(value, fn){
    const saved = process.env[ENVKEY];
    if(value === undefined) delete process.env[ENVKEY]; else process.env[ENVKEY] = value;
    try { return fn(); } finally {
        if(saved === undefined) delete process.env[ENVKEY]; else process.env[ENVKEY] = saved;
    }
}

// The anchor's on/off for (chain, network) as a client would resolve it with `envValue`.
function resolved(envValue, chain, network){
    return withEnv(envValue, () => checkpointQuorumEnabled(config.getConfig()[ENVKEY], chain, network));
}

function registerDefaultTests(){
    it('names a default for every pinned (chain, network), and nothing else', function(){
        assert.deepStrictEqual(Object.keys(CHECKPOINT_QUORUM_DEFAULTS).sort(), Object.keys(PINNED).sort());
    });

    it('default is OFF only while pinnedValidators.js is inert, ON once a launch set lands', function(){
        for(const key of Object.keys(PINNED).filter(k => k.endsWith(':mainnet'))){
            const armed = launchSetPinned(key);
            assert.strictEqual(CHECKPOINT_QUORUM_DEFAULTS[key], armed,
                armed
                    ? 'pinned_validators.js now carries a launch validator set for ' + key + ', so its ' +
                      'CHECKPOINT_QUORUM_DEFAULTS entry must flip ON in the SAME change: a replica that ' +
                      'has a trust root available and does not use it is trusting its source.'
                    : key + ' is still null (INERT), so its default must stay OFF until the launch key fill.');
        }
    });

    it('testnet defaults ON and regtest OFF', function(){
        for(const key of Object.keys(PINNED)){
            if(key.endsWith(':testnet')) assert.strictEqual(CHECKPOINT_QUORUM_DEFAULTS[key], true, key);
            if(key.endsWith(':regtest')) assert.strictEqual(CHECKPOINT_QUORUM_DEFAULTS[key], false, key);
        }
    });

    it('an unset env takes the per-(chain, network) default, for the full coin name too', function(){
        assert.strictEqual(withEnv(undefined, () => config.getConfig()[ENVKEY]), null);
        assert.strictEqual(resolved(undefined, 'BTC', 'testnet'), true);
        assert.strictEqual(resolved(undefined, 'dogecoin', 'testnet'), true);
        assert.strictEqual(resolved(undefined, 'bitcoin', 'mainnet'), launchSetPinned('BTC:mainnet'));
        assert.strictEqual(resolved(undefined, 'LTC', 'regtest'), false);
        assert.strictEqual(resolved(undefined, 'BTC', 'signet'), false, 'an unknown network is off');
    });
}

function registerOverrideTests(){
    it('an explicit env value wins over the shipped default, in both directions', function(){
        assert.strictEqual(resolved('true', 'BTC', 'mainnet'), true,
            'operators must be able to arm the anchor ahead of the flag day (e.g. with a ' +
            'CHECKPOINT_VALIDATORS_<CHAIN>_<NETWORK> override on a test venue)');
        assert.strictEqual(resolved('FALSE', 'BTC', 'testnet'), false,
            'a throwaway / non-consensus mirror must still be able to opt out of the testnet default');
        assert.strictEqual(resolved('yes', 'LTC', 'testnet'), false, 'anything but "true" is an explicit off');
    });

    it('each sync client resolves its own (chain, network), so one process can mix them', function(){
        const ClientSync = require('../../src/client/sync');
        const on = (cfgValue, chain, network) =>
            ClientSync.prototype.checkpointQuorumOn.call({ config: { [ENVKEY]: cfgValue }, chain, network });
        assert.strictEqual(on(null, 'bitcoin', 'testnet'), true);
        assert.strictEqual(on(null, 'bitcoin', 'mainnet'), launchSetPinned('BTC:mainnet'));
        assert.strictEqual(on(undefined, 'litecoin', 'regtest'), false, 'a config built without the key takes the default');
        assert.strictEqual(on(true, 'bitcoin', 'regtest'), true);
        assert.strictEqual(on(false, 'dogecoin', 'testnet'), false);
    });

    it('the interval that throttles the anchor stays usable at its default', function(){
        // A default-on anchor is only meaningful if it actually runs; guard the
        // companion knob against a 0/negative that would make the gate never fire.
        const cfg = config.getConfig();
        assert.strictEqual(cfg.CHECKPOINT_VERIFY_INTERVAL, 50);
        assert.ok(cfg.CHECKPOINT_VERIFY_INTERVAL >= 1);
    });
}

// One flat suite: these titles are pinned (bin/pins/at1-suite-titles.json), so the three
// that predate the per-(chain, network) defaults keep their names.
describe('checkpoint-quorum flag-day coupling @regression', function(){
    registerDefaultTests();
    registerOverrideTests();
});
