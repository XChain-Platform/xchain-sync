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
const { defineStampedeSuite } = require('./08_bootstrap_stampede.test/helpers/stampede_harness');

function registerTests(harness) {
    const { SnapshotBuilder, poolSizing, POOL_LIMIT } = harness;

    it('derives a cap that always leaves the poller a connection', function () {
        // The clamp is the load-bearing half of the snapshot semaphore: no
        // operator setting may hand the poller's last connection to a snapshot
        // stampede.
        const builder = new SnapshotBuilder(null);
        const db = { dbType: 'indexer', connectionPoolParams: { connectionLimit: POOL_LIMIT } };

        delete process.env.MAX_CONCURRENT_SNAPSHOTS;
        assert.strictEqual(builder.snapshotCap(db), POOL_LIMIT - 2,
            'default cap should reserve one connection for the poller and one for short reads');

        process.env.MAX_CONCURRENT_SNAPSHOTS = '1000';
        assert.strictEqual(builder.snapshotCap(db), POOL_LIMIT - 1,
            'an over-large override must clamp to poolSize - 1, never the whole pool');

        process.env.MAX_CONCURRENT_SNAPSHOTS = '0';
        assert.strictEqual(builder.snapshotCap(db), 1,
            'a zero/negative override must clamp to 1, not deadlock every bootstrap');

        delete process.env.MAX_CONCURRENT_SNAPSHOTS;
        assert.strictEqual(poolSizing.resolvePoolSize('indexer'), 12,
            'indexer pool default changed; the cohort budget in this harness assumes 12');
    });
}

defineStampedeSuite(registerTests);

require('./08_bootstrap_stampede.test/01_5_concurrent_bootstraps_cohort_floor_are_all_served_poller_unaffected.test');
require('./08_bootstrap_stampede.test/02_10_concurrent_bootstraps_cohort_ceiling_shed_the_excess_with_a_retryable_503.test');
require('./08_bootstrap_stampede.test/03_a_25_validator_flag_day_cohort_cannot_pin_the_pool.test');
