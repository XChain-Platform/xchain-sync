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

// The sync half of the train-activation parity claim src/train_activation.js makes
// in its own header. Two independent guards:
//   1. VALUE parity to the canonical TRAIN_ACTIVATION map in xchain-documentation.
//   2. BYTE identity to the xchain-indexer twin, because the two copies are one
//      halt decision compiled into two services and a one-sided edit forks the
//      fleet at the train boundary with no CI failure anywhere else.
// Until now only the indexer carried these, so drift was caught on the indexer's
// clock, not on this repo's. THE ABSENT-SIBLING BRANCH IS ITSELF A TEST CASE: a
// guard whose only behaviour on a missing checkout is a bare skip reports a green
// run over nothing, so the skip decision is a pure function asserted below whatever
// the checkout state is, and a coverage floor case runs unconditionally.

const assert = require('assert');
const fs     = require('fs');
const path   = require('path');

const local = require('../../src/train_activation.js');

// CI sets these to wherever it checked the siblings out; fall back to the dev
// sibling layout one level above this repo.
const DOCS_DIR    = process.env.XCHAIN_DOCS_DIR    || path.join(__dirname, '..', '..', '..', 'xchain-documentation');
const INDEXER_DIR = process.env.XCHAIN_INDEXER_DIR || path.join(__dirname, '..', '..', '..', 'xchain-indexer');
const CONSTANTS_PATH = path.join(DOCS_DIR, 'protocol', 'constants.js');
const TWIN_PATH      = path.join(INDEXER_DIR, 'src', 'train_activation.js');
const HERE_PATH      = path.resolve(__dirname, '..', '..', 'src', 'train_activation.js');

const SIBLING_REQUIRED = process.env.XCHAIN_REQUIRE_SIBLINGS === '1';

// The skip-or-fail decision, as a function so it can be asserted in both states
// without deleting a checkout.
function resolveSibling(exists, requireSiblings, what) {
    if (exists) return { status: 'checked' };
    if (requireSiblings) throw new Error('XCHAIN_REQUIRE_SIBLINGS=1 but ' + what + ' is absent');
    return { status: 'skipped', reason: what + ' absent' };
}

describe('train_activation twin + canon parity', function () {

    it('reports skipped and names what it looked for when a sibling is absent', function () {
        const r = resolveSibling(false, false, CONSTANTS_PATH);
        assert.strictEqual(r.status, 'skipped');
        assert.strictEqual(r.reason, CONSTANTS_PATH + ' absent');
    });

    it('throws rather than skipping on an absent sibling when XCHAIN_REQUIRE_SIBLINGS=1', function () {
        assert.throws(() => resolveSibling(false, true, TWIN_PATH), /XCHAIN_REQUIRE_SIBLINGS=1/);
        assert.strictEqual(resolveSibling(true, true, TWIN_PATH).status, 'checked');
    });

    // The coverage floor, and the reason it is unconditional: both parity cases below
    // resolve the map by name, so a rename or a dropped export would otherwise leave
    // them comparing undefined to undefined on a standalone checkout and passing.
    it('still exports the gate the parity cases compare, whatever the checkout state', function () {
        assert.ok(local.TRAIN_ACTIVATION && typeof local.TRAIN_ACTIVATION === 'object'
            && !Array.isArray(local.TRAIN_ACTIVATION), 'TRAIN_ACTIVATION is no longer an object export');
        assert.ok(Object.keys(local.TRAIN_ACTIVATION).length >= 1, 'TRAIN_ACTIVATION has no rows left');
        for (const fn of ['parseRuleSetVersion', 'compareRuleSetVersions', 'implementedRuleSets',
            'activationHeightFor', 'asHeight', 'resolveRuleSet', 'readManifestTrainActivation',
            'evaluateTrainActivation'])
            assert.strictEqual(typeof local[fn], 'function', 'train_activation no longer exports ' + fn);
        assert.ok(fs.existsSync(HERE_PATH), 'the local gate is missing at ' + HERE_PATH);
    });

    it('holds TRAIN_ACTIVATION value-equal to the canonical map at ' + CONSTANTS_PATH, function () {
        if (!fs.existsSync(CONSTANTS_PATH)) {
            resolveSibling(false, SIBLING_REQUIRED, CONSTANTS_PATH);
            this.skip();
            return;
        }
        const canon = require(CONSTANTS_PATH).TRAIN_ACTIVATION;
        assert.ok(canon && typeof canon === 'object',
            'constants.js no longer exports TRAIN_ACTIVATION; this case would compare nothing');
        assert.deepStrictEqual(local.TRAIN_ACTIVATION, canon,
            'the sync copy of TRAIN_ACTIVATION has drifted from the canonical map; a height that ' +
            'differs between copies arms the train on different blocks per service.');
    });

    it('holds src/train_activation.js byte-identical to the twin at ' + TWIN_PATH, function () {
        if (!fs.existsSync(TWIN_PATH)) {
            resolveSibling(false, SIBLING_REQUIRED, TWIN_PATH);
            this.skip();
            return;
        }
        assert.strictEqual(fs.readFileSync(HERE_PATH, 'utf8'), fs.readFileSync(TWIN_PATH, 'utf8'),
            'xchain-sync/src/train_activation.js has drifted from the xchain-indexer copy; the two ' +
            'are vendored twins and a one-sided edit forks the fleet at the train boundary.');
    });
});
