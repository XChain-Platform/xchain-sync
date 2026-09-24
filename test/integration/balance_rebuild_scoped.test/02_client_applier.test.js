// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// Covers the client applier rebuild path. One part of balance_rebuild_scoped.test.js.
const assert = require('assert');
const sinon  = require('sinon');
const balanceHelpers = require('../../../src/db/balance_helpers');
const ClientApplier  = require('../../../src/client/applier');
const suite = require('./helpers/balance_rebuild_scoped_suite');

describe('Integration: scoped balance rebuilds', function() {
    suite.installHooks();
    describe('balances', function() {
        it('ClientApplier.applyBlock drives the scoped rebuild end-to-end', async function() {
            await suite.insertLedgerRows(suite.db, 'credits', suite.history.credits);
            await suite.insertLedgerRows(suite.db, 'debits',  suite.history.debits);
            await balanceHelpers.rebuildBalances(suite.db);
            suite.db.dbType = 'indexer';
            let applier = new ClientApplier(suite.db, suite.testDb.util);
            let spy = sinon.spy(balanceHelpers, 'rebuildBalances');
            try {
                await applier.applyBlock({ block_index: 999, data: { credits: suite.newCredits, debits: suite.newDebits } });
                assert.deepStrictEqual(spy.firstCall.args[1], { addressIds: [3, 2], tickIds: [2, 1] },
                    'applier passed a scope (not a full rebuild)');
            } finally {
                spy.restore();
                delete suite.db.dbType;
            }
            let applied = await suite.snapshotBalances(suite.db);
            await balanceHelpers.rebuildBalances(suite.db);
            assert.deepStrictEqual(applied, await suite.snapshotBalances(suite.db));
        });
    });
});
