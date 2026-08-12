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
 * Flag-day tripwire: VERIFY_CHECKPOINT_QUORUM's default must track
 * whether pinnedValidators.js actually carries a launch validator set.
 *
 * The agreed posture is a COUPLED flip, in both directions:
 *
 *   - while every pinned key is null the anchor has no out-of-band trust root, so
 *     a default of true would be inert while reading as "protected". Default OFF.
 *   - the moment a real launch set lands, every consensus-relevant replica must
 *     anchor by default, not only the operators who happened to set the env var.
 *     Default ON.
 *
 * Nothing but this test enforces the second half, so populating PINNED without
 * flipping the default in the same change fails here with the instruction inline.
 ********************************************************************/

const assert = require('assert');
const config = require('../../src/config');
const { PINNED } = require('../../src/pinnedValidators');

// True once any baked-in (chain, network) carries a real launch validator set.
// Env overrides are deliberately ignored: this is about what the SHIPPED file
// pins, which is what decides the shipped default.
function anyLaunchSetPinned(){
    return Object.keys(PINNED).some(k => Array.isArray(PINNED[k]) && PINNED[k].length > 0);
}

describe('checkpoint-quorum flag-day coupling @regression', function(){

    const ENVKEY = 'VERIFY_CHECKPOINT_QUORUM';
    let saved;

    beforeEach(function(){ saved = process.env[ENVKEY]; delete process.env[ENVKEY]; });
    afterEach(function(){
        if(saved === undefined) delete process.env[ENVKEY];
        else process.env[ENVKEY] = saved;
    });

    it('default is OFF only while pinnedValidators.js is inert, ON once a launch set lands', function(){
        const armed = anyLaunchSetPinned();
        const cfg = config.getConfig();
        assert.strictEqual(cfg.VERIFY_CHECKPOINT_QUORUM, armed,
            armed
                ? 'pinnedValidators.js now carries a launch validator set, so the shipped default for ' +
                  'VERIFY_CHECKPOINT_QUORUM must flip ON in the SAME change (src/config.js): a replica ' +
                  'that has a trust root available and does not use it is trusting its source.'
                : 'pinnedValidators.js is still all-null (INERT), so VERIFY_CHECKPOINT_QUORUM must ' +
                  'default OFF: a true-but-inert flag reads as protected while nothing is verified.');
    });

    it('an explicit env value wins over the shipped default, in both directions', function(){
        process.env[ENVKEY] = 'true';
        assert.strictEqual(config.getConfig().VERIFY_CHECKPOINT_QUORUM, true,
            'operators must be able to arm the anchor ahead of the flag day (e.g. with a ' +
            'CHECKPOINT_VALIDATORS_<CHAIN>_<NETWORK> override on a test venue)');
        process.env[ENVKEY] = 'FALSE';
        assert.strictEqual(config.getConfig().VERIFY_CHECKPOINT_QUORUM, false,
            'a throwaway / non-consensus mirror must still be able to opt out after the flip');
    });

    it('the interval that throttles the anchor stays usable at its default', function(){
        // A flipped default is only meaningful if the anchor actually runs; guard the
        // companion knob against a 0/negative that would make the gate never fire.
        const cfg = config.getConfig();
        assert.strictEqual(cfg.CHECKPOINT_VERIFY_INTERVAL, 50);
        assert.ok(cfg.CHECKPOINT_VERIFY_INTERVAL >= 1);
    });
});
