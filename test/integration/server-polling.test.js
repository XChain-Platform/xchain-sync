// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

const assert   = require('assert');
const sinon    = require('sinon');
const setup    = require('./helpers/setup');
const testDb   = require('./helpers/testDb');
const fixtures = require('./helpers/fixtures');
const ServerPoller = require('../../src/ServerPoller');

describe('Integration: ServerPoller', function() {

    let sourceDb, poller, broadcaster, transparencyLog, config;

    before(async function() {
        await setup.globalSetup();
    });

    after(async function() {
        await setup.globalTeardown();
    });

    beforeEach(async function() {
        sourceDb = setup.getSourceDb();
        await testDb.truncateAll(sourceDb);

        broadcaster = {
            broadcast: sinon.stub(),
            updateStatus: sinon.stub(),
            getSubscriberCount: sinon.stub().returns(0)
        };
        transparencyLog = {
            recordBlock: sinon.stub().resolves(),
            pruneFrom:   sinon.stub().resolves()
        };
        config = { BLOCK_POLL_INTERVAL: 100 };

        poller = new ServerPoller('bitcoin', 'mainnet', sourceDb, broadcaster, transparencyLog, config, testDb.util);
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });

    afterEach(function() {
        sinon.restore();
    });

    describe('_poll', function() {
        it('returns early when no blocks in DB', async function() {
            await poller._poll();
            assert.strictEqual(broadcaster.broadcast.called, false);
        });

        it('initializes lastPolledBlock on first poll', async function() {
            await fixtures.seedBlocks(sourceDb, 1, 3);
            poller.lastPolledBlock = null;

            await poller._poll();

            assert.strictEqual(poller.lastPolledBlock, 3);
            assert.strictEqual(broadcaster.broadcast.called, false); // initialization only
            assert.strictEqual(broadcaster.updateStatus.calledOnce, true);
        });

        it('detects and processes new blocks', async function() {
            await fixtures.seedBlocks(sourceDb, 1, 1);
            poller.lastPolledBlock = 0;

            await poller._poll();

            assert.strictEqual(broadcaster.broadcast.calledOnce, true);
            let event = broadcaster.broadcast.firstCall.args[2];
            assert.strictEqual(event.type, 'block');
            assert.strictEqual(event.block_index, 1);
            assert.strictEqual(event.chain, 'bitcoin');
            assert.strictEqual(event.network, 'mainnet');
            assert.ok(event.ledger_hash);
            assert.ok(event.actions_hash);
            assert.ok(event.contract_hash);
            assert.ok(event.data);
        });

        it('processes multiple sequential blocks', async function() {
            await fixtures.seedBlocks(sourceDb, 1, 5);
            poller.lastPolledBlock = 0;

            await poller._poll();

            assert.strictEqual(broadcaster.broadcast.callCount, 5);
            assert.strictEqual(poller.lastPolledBlock, 5);

            // Verify block order
            for (let i = 0; i < 5; i++) {
                let event = broadcaster.broadcast.getCall(i).args[2];
                assert.strictEqual(event.block_index, i + 1);
            }
        });

        it('records each block in transparency log', async function() {
            await fixtures.seedBlocks(sourceDb, 1, 3);
            poller.lastPolledBlock = 0;

            await poller._poll();

            assert.strictEqual(transparencyLog.recordBlock.callCount, 3);
        });

        it('detects reorg when block count decreases', async function() {
            await fixtures.seedBlocks(sourceDb, 1, 10);
            poller.lastPolledBlock = 10;

            // Simulate reorg at source
            await fixtures.deleteBlocksFrom(sourceDb, 8);

            await poller._poll();

            assert.strictEqual(broadcaster.broadcast.calledOnce, true);
            let event = broadcaster.broadcast.firstCall.args[2];
            assert.strictEqual(event.type, 'reorg');
            assert.strictEqual(event.block_index, 8); // currentBlock(7) + 1
            assert.strictEqual(poller.lastPolledBlock, 7);
        });

        it('does nothing when no new blocks', async function() {
            await fixtures.seedBlocks(sourceDb, 1, 5);
            poller.lastPolledBlock = 5;

            await poller._poll();

            assert.strictEqual(broadcaster.broadcast.called, false);
            assert.strictEqual(transparencyLog.recordBlock.called, false);
        });
    });

    describe('_buildBlockPayload', function() {
        it('builds payload with correct structure from real DB', async function() {
            await fixtures.seedBlocks(sourceDb, 1, 1);

            let payload = await poller._buildBlockPayload(1);

            assert.strictEqual(payload.type, 'block');
            assert.strictEqual(payload.chain, 'bitcoin');
            assert.strictEqual(payload.network, 'mainnet');
            assert.strictEqual(payload.block_index, 1);
            assert.ok(payload.block_time > 0);
            assert.ok(payload.ledger_hash);
            assert.ok(payload.data);
        });

        it('includes block-scoped table rows', async function() {
            await fixtures.seedBlocks(sourceDb, 1, 1);
            let payload = await poller._buildBlockPayload(1);

            assert.ok(payload.data.blocks);
            assert.strictEqual(payload.data.blocks.length, 1);
        });

        it('includes transactions', async function() {
            await fixtures.seedBlocks(sourceDb, 1, 1);
            let payload = await poller._buildBlockPayload(1);

            assert.ok(payload.data.transactions);
            assert.strictEqual(payload.data.transactions.length, 1);
        });

        it('includes action-scoped rows (credits)', async function() {
            await fixtures.seedBlocks(sourceDb, 1, 1);
            let payload = await poller._buildBlockPayload(1);

            assert.ok(payload.data.credits);
            assert.strictEqual(payload.data.credits.length, 1);
            assert.strictEqual(payload.data.credits[0].amount, '1000');
        });

        it('includes index_transactions referenced by block', async function() {
            await fixtures.seedBlocks(sourceDb, 1, 1);
            let payload = await poller._buildBlockPayload(1);

            assert.ok(payload.data.index_transactions);
            assert.ok(payload.data.index_transactions.length >= 3); // ledger, actions, contract hashes
        });

        it('includes index_addresses referenced by transactions', async function() {
            await fixtures.seedBlocks(sourceDb, 1, 1);
            let payload = await poller._buildBlockPayload(1);

            assert.ok(payload.data.index_addresses);
            assert.ok(payload.data.index_addresses.length >= 1);
        });

        it('returns null for non-existent block', async function() {
            let payload = await poller._buildBlockPayload(999);
            assert.strictEqual(payload, null);
        });
    });

    describe('_updateStatus', function() {
        it('updates broadcaster status with real block data', async function() {
            await fixtures.seedBlocks(sourceDb, 1, 5);
            poller.lastPolledBlock = 5;

            await poller._updateStatus();

            assert.strictEqual(broadcaster.updateStatus.calledOnce, true);
            let args = broadcaster.updateStatus.firstCall.args;
            assert.strictEqual(args[0], 'bitcoin');
            assert.strictEqual(args[1], 'mainnet');
            assert.strictEqual(args[2].block_height, 5);
            assert.ok(args[2].ledger_hash);
            assert.ok(args[2].block_time > 0);
        });
    });
});
