// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// Covers continued polling outcomes. One part of server_polling.test.js.
const suite = require('./helpers/server_polling_suite');

describe('Integration: ServerPoller', function() {
    suite.installHooks();
    describe('poll', function() {
        it('processes multiple sequential blocks', async function() {
            await suite.fixtures.seedBlocks(suite.sourceDb, 1, 5);
            suite.poller.lastPolledBlock = 0;
            await suite.poller.poll();
            suite.assert.strictEqual(suite.broadcaster.broadcast.callCount, 5);
            suite.assert.strictEqual(suite.poller.lastPolledBlock, 5);

            // Verify block order
            for (let i = 0; i < 5; i++) {
                let event = suite.broadcaster.broadcast.getCall(i).args[2];
                suite.assert.strictEqual(event.block_index, i + 1);
            }
        });

        it('records each block in transparency log', async function() {
            await suite.fixtures.seedBlocks(suite.sourceDb, 1, 3);
            suite.poller.lastPolledBlock = 0;
            await suite.poller.poll();
            suite.assert.strictEqual(suite.transparencyLog.recordBlock.callCount, 3);
        });

        it('detects reorg when block count decreases', async function() {
            await suite.fixtures.seedBlocks(suite.sourceDb, 1, 10);
            suite.poller.lastPolledBlock = 10;

            // Simulate reorg at source
            await suite.fixtures.deleteBlocksFrom(suite.sourceDb, 8);
            await suite.poller.poll();
            suite.assert.strictEqual(suite.broadcaster.broadcast.calledOnce, true);
            let event = suite.broadcaster.broadcast.firstCall.args[2];
            suite.assert.strictEqual(event.type, 'reorg');
            suite.assert.strictEqual(event.block_index, 8); // currentBlock(7) + 1
            suite.assert.strictEqual(suite.poller.lastPolledBlock, 7);
        });

        it('does nothing when no new blocks', async function() {
            await suite.fixtures.seedBlocks(suite.sourceDb, 1, 5);
            suite.poller.lastPolledBlock = 5;
            await suite.poller.poll();
            suite.assert.strictEqual(suite.broadcaster.broadcast.called, false);
            suite.assert.strictEqual(suite.transparencyLog.recordBlock.called, false);
        });
    });
});
