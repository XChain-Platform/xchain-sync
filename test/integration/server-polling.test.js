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

    // . The payload build used to issue one getActionScopedRows per registry
    // table (86 today, growing with every replicated table added); it now asks
    // getNonEmptyActionScopedTables once and fetches only what answers. payload.data
    // feeds a consensus hash followers recompute, so the only acceptable evidence is
    // byte-identity against a REAL database, not a mock: these run the same block
    // through both paths on the same rows and compare the serialized payloads.
    describe('_buildBlockPayload action-scoped probe ()', function() {
        // Same build with the probe removed from the db object, which is the pre-fix
        // query-every-table path verbatim.
        // getNonEmptyActionScopedTables lives on TestDatabase's PROTOTYPE, so it is
        // shadowed with an own `undefined` rather than deleted: a delete removes nothing
        // and the comparison would silently be probe-against-probe, which passes while
        // proving nothing.
        async function buildUnprobed(blockIndex) {
            sourceDb.getNonEmptyActionScopedTables = undefined;
            try {
                assert.strictEqual(typeof sourceDb.getNonEmptyActionScopedTables, 'undefined');
                return await poller._buildBlockPayload(blockIndex);
            } finally {
                delete sourceDb.getNonEmptyActionScopedTables;
            }
        }

        it('emits a byte-identical payload on a block that has rows', async function() {
            await fixtures.seedBlocks(sourceDb, 1, 1);

            let probed   = await poller._buildBlockPayload(1);
            let unprobed = await buildUnprobed(1);

            assert.ok(probed.data.credits && probed.data.credits.length === 1,
                'the block must carry action-scoped rows, or this proves nothing');
            assert.strictEqual(JSON.stringify(probed), JSON.stringify(unprobed));
        });

        it('emits a byte-identical payload on a block with no action-scoped rows', async function() {
            await fixtures.seedBlocks(sourceDb, 1, 2);
            await sourceDb.doQuery("DELETE FROM credits");

            let probed   = await poller._buildBlockPayload(2);
            let unprobed = await buildUnprobed(2);

            assert.ok(!probed.data.credits, 'no action-scoped rows in this block');
            assert.strictEqual(JSON.stringify(probed), JSON.stringify(unprobed));
        });

        it('cuts the per-table round-trips to the tables that actually have rows', async function() {
            await fixtures.seedBlocks(sourceDb, 1, 1);
            const spy = sinon.spy(sourceDb, 'getActionScopedRows');

            await poller._buildBlockPayload(1);
            const probedFetches = spy.getCalls().map(c => c.args[0]);
            spy.resetHistory();

            await buildUnprobed(1);
            const unprobedFetches = spy.getCalls().map(c => c.args[0]);

            assert.ok(unprobedFetches.length > 40,
                'the unprobed path really does walk the whole registry (' + unprobedFetches.length + ')');
            assert.ok(probedFetches.length < unprobedFetches.length,
                'the probe must remove round-trips (' + probedFetches.length + ' vs ' + unprobedFetches.length + ')');
            // Every table still fetched is one the unprobed path also fetched with rows.
            for (const table of probedFetches)
                assert.ok(unprobedFetches.includes(table), table + ' fetched by the probed path only');
        });

        it('agrees with the real fetch on which tables are empty', async function() {
            await fixtures.seedBlocks(sourceDb, 1, 1);
            const candidates = poller.actionScopedTables
                .filter(t => t !== 'actions' && t !== 'contract_emissions');

            const nonEmpty = await sourceDb.getNonEmptyActionScopedTables(candidates, 1);

            // The safety property, checked table by table against the real database:
            // probe membership must equal "getActionScopedRows returns rows".
            for (const table of candidates) {
                let rows = [];
                try {
                    rows = await sourceDb.getActionScopedRows(table, 1);
                } catch (e) {
                    continue;   // table absent from this schema; the probe skips it too
                }
                assert.strictEqual(nonEmpty.has(table), rows.length > 0,
                    'probe disagrees with the real fetch on ' + table);
            }
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
