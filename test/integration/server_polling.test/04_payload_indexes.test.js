// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// Covers indexed payload rows and misses. One part of server_polling.test.js.
const suite = require('./helpers/server_polling_suite');

describe('Integration: ServerPoller', function() {
    suite.installHooks();
    describe('buildBlockPayload', function() {
        it('includes index_transactions referenced by block', async function() {
            await suite.fixtures.seedBlocks(suite.sourceDb, 1, 1);
            let payload = await suite.poller.buildBlockPayload(1);
            suite.assert.ok(payload.data.index_transactions);
            suite.assert.ok(payload.data.index_transactions.length >= 3); // ledger, actions, contract hashes
        });

        it('includes index_addresses referenced by transactions', async function() {
            await suite.fixtures.seedBlocks(suite.sourceDb, 1, 1);
            let payload = await suite.poller.buildBlockPayload(1);
            suite.assert.ok(payload.data.index_addresses);
            suite.assert.ok(payload.data.index_addresses.length >= 1);
        });

        it('returns null for non-existent block', async function() {
            let payload = await suite.poller.buildBlockPayload(999);
            suite.assert.strictEqual(payload, null);
        });
    });
});
