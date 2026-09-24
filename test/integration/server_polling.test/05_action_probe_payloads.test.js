// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// Covers probe payload parity. One part of server_polling.test.js.
const suite = require('./helpers/server_polling_suite');

describe('Integration: ServerPoller', function() {
    suite.installHooks();

    // The payload build asks getNonEmptyActionScopedTables once and fetches only
    // the tables that answer, rather than one getActionScopedRows per registry
    // table (86 today, growing with every replicated table added). payload.data
    // feeds a consensus hash followers recompute, so the only acceptable evidence is
    // byte-identity against a REAL database, not a mock: these run the same block
    // through both paths on the same rows and compare the serialized payloads.
    describe('buildBlockPayload action-scoped probe', function() {
        it('emits a byte-identical payload on a block that has rows', async function() {
            await suite.fixtures.seedBlocks(suite.sourceDb, 1, 1);
            let probed   = await suite.poller.buildBlockPayload(1);
            let unprobed = await suite.buildUnprobed(1);
            suite.assert.ok(probed.data.credits && probed.data.credits.length === 1,
                'the block must carry action-scoped rows, or this proves nothing');
            suite.assert.strictEqual(JSON.stringify(probed), JSON.stringify(unprobed));
        });

        it('emits a byte-identical payload on a block with no action-scoped rows', async function() {
            await suite.fixtures.seedBlocks(suite.sourceDb, 1, 2);
            await suite.sourceDb.doQuery("DELETE FROM credits");
            let probed   = await suite.poller.buildBlockPayload(2);
            let unprobed = await suite.buildUnprobed(2);
            suite.assert.ok(!probed.data.credits, 'no action-scoped rows in this block');
            suite.assert.strictEqual(JSON.stringify(probed), JSON.stringify(unprobed));
        });
    });
});
