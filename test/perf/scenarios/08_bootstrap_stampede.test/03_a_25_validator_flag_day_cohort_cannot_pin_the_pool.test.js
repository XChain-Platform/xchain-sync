'use strict';

// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

const assert = require('assert');
const { defineStampedeSuite } = require('./helpers/stampede_harness');

function registerTests(harness) {
    const { runStampede, assertLivenessHeld } = harness;

    it('a 25-validator flag-day cohort cannot pin the pool', async function () {
        // Cohort C is the scenario in the ledger item: more validators than the
        // pool has connections, arriving together, against a server that must
        // keep serving everyone already on the network.
        const result = await runStampede(25, '25-concurrent');
        assertLivenessHeld(result);

        assert.ok(result.stats.refused >= 25 - result.stats.cap,
            `expected at least ${25 - result.stats.cap} refusals at a cap of ${result.stats.cap}, got ${result.stats.refused}`);
        assert.ok(result.stats.accepted >= 1,
            'shedding must not degenerate into refusing everyone; some validators have to make progress');
        assert.strictEqual(result.stats.accepted + result.stats.refused, 25,
            'every request must be answered exactly once');
    });
}

defineStampedeSuite(registerTests);
