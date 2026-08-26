// Copyright © 2025-2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC - https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md.
//
// Replica half of the stake-weight ordering-collation flag-day (see
// src/stake_weight_collation_activation.js, a byte-identical twin of the
// xchain-indexer copy). The follower rebuilds stakes_root from
// _cappedStakeWeightsSql, whose window caps truncate on an ORDER over
// index_addresses.address / index_pubkeys.pubkey. Those columns are declared
// utf8_general_ci (folding), so the collation decides WHICH sources and keys
// survive the cap - and pinning it on ONE side of the seam is itself the fork
// this gate exists to prevent, which is why both repos carry the same gate and
// the same emission sites.
//
// The gate module's own semantics are covered once, by the indexer's suite,
// against the same bytes (rollback-coverage.test.js locks the twin identical).
// What is sync-specific and covered HERE: that this repo's caller threads the
// gate, that a null coin/network stays inert as it does for the source cap, and
// that the fail-closed startup assertion halts on drift and passes on a correct
// schema under either the utf8 or the utf8mb3 spelling of the same collation.

const assert   = require('assert');
const sinon    = require('sinon');
const Database = require('../../src/db');
const swc      = require('../../src/stake_weight_collation_activation');

function makeUtil() { return { isNull: (x) => x == null, logError: () => {}, throwError: () => {} }; }

function dbWithCapturedQueries() {
    const db = new Database('localhost', 3306, 'idx', 'u', 'p', makeUtil(), 'indexer');
    const calls = [];
    sinon.stub(db, 'doQueryStrict').callsFake((q, a) => { calls.push({ q, a }); return Promise.resolve([]); });
    db._calls = calls;
    return db;
}

const INNER = { sql: 'SELECT 1 AS pubkey, 2 AS source, 3 AS weight', args: [] };

afterEach(function () { sinon.restore(); });

describe('sync: stake-weight ordering collation gate', function () {

    it('an unpinned chain emits no COLLATE', async function () {
        const db = dbWithCapturedQueries();
        await db._applyStakeWeightCap(INNER, 5000000, 100, 'BTC', 'mainnet', 'probe');
        const q = db._calls.map(c => c.q).join('\n');
        assert.ok(q.length > 0, 'no query was emitted');
        assert.doesNotMatch(q, /COLLATE/,
            'a COLLATE leaked onto an unpinned chain; below the gate the follower must order ' +
            'exactly as the source does today');
    });

    it('a null coin/network stays inert, as it does for the source cap', async function () {
        const db = dbWithCapturedQueries();
        await db._applyStakeWeightCap(INNER, 10, 100, null, null, 'probe');
        assert.doesNotMatch(db._calls.map(c => c.q).join('\n'), /COLLATE/);
    });

    it('regtest is armed and pins utf8_bin at every ordering site', async function () {
        const db = dbWithCapturedQueries();
        await db._applyStakeWeightCap(INNER, 10, 100, 'BTC', 'regtest', 'probe');
        const q = db._calls.map(c => c.q).join('\n').replace(/\s+/g, ' ');
        assert.match(q, /DENSE_RANK\(\) OVER \(ORDER BY b\.source COLLATE utf8_bin\)/);
        assert.match(q, /ROW_NUMBER\(\) OVER \(PARTITION BY b\.source COLLATE utf8_bin ORDER BY b\.pubkey COLLATE utf8_bin\)/);
        assert.match(q, /ORDER BY r\.source COLLATE utf8_bin, r\.pubkey COLLATE utf8_bin/);
    });

    describe('fail-closed startup assertion', function () {

        function dbAnswering(rows, dbType) {
            const db = new Database('localhost', 3306, 'idx', 'u', 'p', makeUtil(), dbType || 'indexer');
            sinon.stub(db, 'doQuery').resolves(rows);
            return db;
        }

        // The reader must be strict. doQuery's fail-soft default logs a driver fault and
        // returns [], which the absent-table branch reads as "no such column" and skips,
        // so a transient fault would turn this fail-closed gate into a pass on every
        // replica. Asserting the OPTIONS rather than the outcome, because a fail-soft
        // read and a genuinely absent column produce the same empty array.
        it('reads the column through a strict query, so a driver fault cannot read as absent', async function () {
            const db = new Database('localhost', 3306, 'idx', 'u', 'p', makeUtil(), 'indexer');
            const seen = [];
            sinon.stub(db, 'doQuery').callsFake((q, a, c, opts) => { seen.push(opts); return Promise.resolve([]); });
            await db.assertStakeWeightOrderingCollation();
            assert.ok(seen.length > 0, 'the gate consulted no column at all');
            for (const opts of seen) {
                assert.ok(opts && opts.rethrow === true,
                    'collation read used the fail-soft default; a driver fault would skip the gate');
            }
        });

        it('passes on a correct schema reported with the modern utf8mb3 spelling', async function () {
            // Verified on MariaDB 11.4.12: a column declared `CHARSET=utf8
            // COLLATE=utf8_general_ci` reports utf8mb3 / utf8mb3_general_ci. A literal
            // name comparison here would halt every replica in the fleet.
            const db = dbAnswering([{ CHARACTER_SET_NAME: 'utf8mb3', COLLATION_NAME: 'utf8mb3_general_ci' }]);
            await db.assertStakeWeightOrderingCollation();
        });

        it('passes when the table is not there yet', async function () {
            const db = dbAnswering([]);
            await db.assertStakeWeightOrderingCollation();
        });

        it('halts on a drifted charset, naming the column', async function () {
            // A utf8mb4 column makes the ARMED query fail outright (errno 1253), so the
            // replica must refuse at boot rather than mid-block.
            const db = dbAnswering([{ CHARACTER_SET_NAME: 'utf8mb4', COLLATION_NAME: 'utf8mb4_general_ci' }]);
            await assert.rejects(() => db.assertStakeWeightOrderingCollation(), /index_addresses\.address/);
        });

        it('is a no-op on a decoder replica, which holds no stakes', async function () {
            const db = dbAnswering([{ CHARACTER_SET_NAME: 'utf8mb4', COLLATION_NAME: 'utf8mb4_general_ci' }], 'decoder');
            await db.assertStakeWeightOrderingCollation();
        });
    });

    it('the gate map ships inert on every chain with history', function () {
        for (const [key, height] of Object.entries(swc.STAKE_WEIGHT_COLLATION_ACTIVATION)) {
            if (key === 'regtest') { assert.strictEqual(height, 0); continue; }
            assert.strictEqual(height, null,
                key + ' carries a pinned height; arming needs both fleets deployed first');
        }
    });
});
