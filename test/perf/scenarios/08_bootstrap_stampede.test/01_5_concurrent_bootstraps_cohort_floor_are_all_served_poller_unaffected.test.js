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
const zlib   = require('zlib');
const { defineStampedeSuite } = require('./stampede_harness');

function registerTests(harness) {
    const { runStampede, assertLivenessHeld, STAMPEDE_BLOCKS } = harness;

    it('5 concurrent bootstraps (cohort floor) are all served, poller unaffected', async function () {
        const result = await runStampede(5, '05-concurrent');
        assertLivenessHeld(result);

        // Five is under the cap, so nothing should be shed: shedding when there
        // is capacity would turn a healthy cohort into a retry storm.
        assert.strictEqual(result.stats.refused, 0,
            `5 concurrent bootstraps are within the cap of ${result.stats.cap}; none should be refused`);
        assert.strictEqual(result.stats.accepted, 5, 'all five bootstraps should complete');

        for (const res of result.accepted) {
            const snapshot = JSON.parse(zlib.gunzipSync(res.data).toString());
            assert.ok(snapshot.block_height >= STAMPEDE_BLOCKS,
                `served snapshot is at height ${snapshot.block_height}, below the seeded tip ${STAMPEDE_BLOCKS}`);
            assert.ok(snapshot.tables && Object.keys(snapshot.tables).length > 0,
                'a snapshot served under load must still carry table data');
        }
    });
}

defineStampedeSuite(registerTests);
