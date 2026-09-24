// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// Covers scoped and full rebuild parity. One part of balance_rebuild_scoped.test.js.
const assert = require('assert');
const balanceHelpers = require('../../../src/db/balance_helpers');
const suite = require('./helpers/balance_rebuild_scoped_suite');

describe('Integration: scoped balance rebuilds', function() {
    suite.installHooks();
    describe('balances', function() {
        it('a scoped rebuild leaves the table exactly as a full rebuild would', async function() {
            await suite.insertLedgerRows(suite.db, 'credits', suite.history.credits);
            await suite.insertLedgerRows(suite.db, 'debits',  suite.history.debits);
            await balanceHelpers.rebuildBalances(suite.db);
            await suite.insertLedgerRows(suite.db, 'credits', suite.newCredits);
            await suite.insertLedgerRows(suite.db, 'debits',  suite.newDebits);
            await balanceHelpers.rebuildBalances(suite.db, suite.scope);
            let scoped = await suite.snapshotBalances(suite.db);
            await balanceHelpers.rebuildBalances(suite.db);
            let full = await suite.snapshotBalances(suite.db);
            assert.deepStrictEqual(scoped, full);

            // Spot-checks on the interesting rows so a both-paths-wrong
            // regression can't slip through the equivalence assertion.
            assert.ok(!scoped.some(r => r[0] === 2 && r[1] === 1), 'zeroed pair (2,1) pruned');
            assert.ok(scoped.some(r => r[0] === 1 && r[1] === 1 && r[2] === '600'), 'untouched pair (1,1) intact');
            assert.ok(scoped.some(r => r[0] === 2 && r[1] === 2 && r[2] === '99999999999999999999999999999999999999'),
                'big-number amount survives the scoped recompute byte-for-byte');
            assert.ok(scoped.some(r => r[0] === 3 && r[1] === 2 && r[2] === '20'), 'touched pair (3,2) recomputed');
        });
    });
});
