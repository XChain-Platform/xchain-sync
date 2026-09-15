// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// Covers initial polling outcomes. One part of server_polling.test.js.
const suite = require('./helpers/server_polling_suite');

describe('Integration: ServerPoller', function() {
    suite.installHooks();
    describe('poll', function() {
        it('returns early when no blocks in DB', async function() {
            await suite.poller.poll();
            suite.assert.strictEqual(suite.broadcaster.broadcast.called, false);
        });

        it('initializes lastPolledBlock on first poll', async function() {
            await suite.fixtures.seedBlocks(suite.sourceDb, 1, 3);
            suite.poller.lastPolledBlock = null;
            await suite.poller.poll();
            suite.assert.strictEqual(suite.poller.lastPolledBlock, 3);
            suite.assert.strictEqual(suite.broadcaster.broadcast.called, false); // initialization only
            suite.assert.strictEqual(suite.broadcaster.updateStatus.calledOnce, true);
        });

        it('detects and processes new blocks', async function() {
            await suite.fixtures.seedBlocks(suite.sourceDb, 1, 1);
            suite.poller.lastPolledBlock = 0;
            await suite.poller.poll();
            suite.assert.strictEqual(suite.broadcaster.broadcast.calledOnce, true);
            let event = suite.broadcaster.broadcast.firstCall.args[2];
            suite.assert.strictEqual(event.type, 'block');
            suite.assert.strictEqual(event.block_index, 1);
            suite.assert.strictEqual(event.chain, 'bitcoin');
            suite.assert.strictEqual(event.network, 'mainnet');
            suite.assert.ok(event.ledger_hash);
            suite.assert.ok(event.actions_hash);
            suite.assert.ok(event.contract_hash);
            suite.assert.ok(event.data);
        });
    });
});
