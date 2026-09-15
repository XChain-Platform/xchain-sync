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
const { defineStampedeSuite } = require('./stampede_harness');

function registerTests(harness) {
    const { runStampede, assertLivenessHeld } = harness;

    it('10 concurrent bootstraps (cohort ceiling) shed the excess with a retryable 503', async function () {
        // Cap of 3 against 10 arrivals: the shed path is exercised deterministically
        // rather than depending on how fast this machine streams a snapshot.
        const result = await runStampede(10, '10-concurrent', { maxConcurrentSnapshots: 3 });
        assertLivenessHeld(result);

        assert.strictEqual(result.stats.cap, 3, 'MAX_CONCURRENT_SNAPSHOTS=3 should be honoured');
        assert.ok(result.stats.refused >= 10 - result.stats.cap,
            `expected at least ${10 - result.stats.cap} refusals at a cap of ${result.stats.cap}, got ${result.stats.refused}`);

        // A refusal has to be actionable, not just a failure: the client's retry
        // logic keys on the code, and the backoff on Retry-After.
        for (const res of result.refused) {
            const body = JSON.parse(Buffer.from(res.data).toString());
            assert.strictEqual(body.code, 'SNAPSHOT_BUSY',
                `a shed bootstrap must say why: got ${JSON.stringify(body)}`);
            assert.ok(res.headers['retry-after'],
                'a shed bootstrap must carry Retry-After so the validator backs off rather than hammering');
        }

        assert.strictEqual(result.stats.snapshotsRejectedCounter, result.stats.refused,
            'the snapshots_rejected counter must match the refusals, or operators cannot see a stampede');
    });
}

defineStampedeSuite(registerTests);
