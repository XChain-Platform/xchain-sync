// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// Covers primary payload rows. One part of server_polling.test.js.
const suite = require('./helpers/server_polling_suite');

describe('Integration: ServerPoller', function() {
    suite.installHooks();
    describe('buildBlockPayload', function() {
        it('builds payload with correct structure from real DB', async function() {
            await suite.fixtures.seedBlocks(suite.sourceDb, 1, 1);
            let payload = await suite.poller.buildBlockPayload(1);
            suite.assert.strictEqual(payload.type, 'block');
            suite.assert.strictEqual(payload.chain, 'bitcoin');
            suite.assert.strictEqual(payload.network, 'mainnet');
            suite.assert.strictEqual(payload.block_index, 1);
            suite.assert.ok(payload.block_time > 0);
            suite.assert.ok(payload.ledger_hash);
            suite.assert.ok(payload.data);
        });

        it('includes block-scoped table rows', async function() {
            await suite.fixtures.seedBlocks(suite.sourceDb, 1, 1);
            let payload = await suite.poller.buildBlockPayload(1);
            suite.assert.ok(payload.data.blocks);
            suite.assert.strictEqual(payload.data.blocks.length, 1);
        });

        it('includes transactions', async function() {
            await suite.fixtures.seedBlocks(suite.sourceDb, 1, 1);
            let payload = await suite.poller.buildBlockPayload(1);
            suite.assert.ok(payload.data.transactions);
            suite.assert.strictEqual(payload.data.transactions.length, 1);
        });

        it('includes action-scoped rows (credits)', async function() {
            await suite.fixtures.seedBlocks(suite.sourceDb, 1, 1);
            let payload = await suite.poller.buildBlockPayload(1);
            suite.assert.ok(payload.data.credits);
            suite.assert.strictEqual(payload.data.credits.length, 1);
            suite.assert.strictEqual(payload.data.credits[0].amount, '1000');
        });
    });
});
