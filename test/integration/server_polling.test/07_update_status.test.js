// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// Covers broadcaster status updates. One part of server_polling.test.js.
const suite = require('./helpers/server_polling_suite');

describe('Integration: ServerPoller', function() {
    suite.installHooks();
    describe('updateStatus', function() {
        it('updates broadcaster status with real block data', async function() {
            await suite.fixtures.seedBlocks(suite.sourceDb, 1, 5);
            suite.poller.lastPolledBlock = 5;
            await suite.poller.updateStatus();
            suite.assert.strictEqual(suite.broadcaster.updateStatus.calledOnce, true);
            let args = suite.broadcaster.updateStatus.firstCall.args;
            suite.assert.strictEqual(args[0], 'bitcoin');
            suite.assert.strictEqual(args[1], 'mainnet');
            suite.assert.strictEqual(args[2].block_height, 5);
            suite.assert.ok(args[2].ledger_hash);
            suite.assert.ok(args[2].block_time > 0);
        });
    });
});
