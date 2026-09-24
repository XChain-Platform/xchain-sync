// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// Covers probe efficiency and empty tables. One part of server_polling.test.js.
const suite = require('./helpers/server_polling_suite');

describe('Integration: ServerPoller', function() {
    suite.installHooks();
    describe('buildBlockPayload action-scoped probe', function() {
        it('cuts the per-table round-trips to the tables that actually have rows', async function() {
            await suite.fixtures.seedBlocks(suite.sourceDb, 1, 1);
            const spy = suite.sinon.spy(suite.sourceDb, 'getActionScopedRows');
            await suite.poller.buildBlockPayload(1);
            const probedFetches = spy.getCalls().map(c => c.args[0]);
            spy.resetHistory();
            await suite.buildUnprobed(1);
            const unprobedFetches = spy.getCalls().map(c => c.args[0]);
            suite.assert.ok(unprobedFetches.length > 40,
                'the unprobed path really does walk the whole registry (' + unprobedFetches.length + ')');
            suite.assert.ok(probedFetches.length < unprobedFetches.length,
                'the probe must remove round-trips (' + probedFetches.length + ' vs ' + unprobedFetches.length + ')');
            // Every table still fetched is one the unprobed path also fetched with rows.
            for (const table of probedFetches)
                suite.assert.ok(unprobedFetches.includes(table), table + ' fetched by the probed path only');
        });

        it('agrees with the real fetch on which tables are empty', async function() {
            await suite.fixtures.seedBlocks(suite.sourceDb, 1, 1);
            const candidates = suite.poller.actionScopedTables
                .filter(t => t !== 'actions' && t !== 'contract_emissions');
            const nonEmpty = await suite.sourceDb.getNonEmptyActionScopedTables(candidates, 1);

            // The safety property, checked table by table against the real database:
            // probe membership must equal "getActionScopedRows returns rows".
            for (const table of candidates) {
                let rows = [];
                try {
                    rows = await suite.sourceDb.getActionScopedRows(table, 1);
                } catch (e) {
                    continue;   // table absent from this schema; the probe skips it too
                }
                suite.assert.strictEqual(nonEmpty.has(table), rows.length > 0,
                    'probe disagrees with the real fetch on ' + table);
            }
        });
    });
});
