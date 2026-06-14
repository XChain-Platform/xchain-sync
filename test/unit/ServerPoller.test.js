// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available —
// contact legal@dankest.llc.

const assert = require('assert');
const sinon  = require('sinon');
const ServerPoller = require('../../src/ServerPoller');
const Utility = require('../../src/utility');

function createMockDb(){
    return {
        getLastBlock: sinon.stub().resolves(null),
        getBlockHashRow: sinon.stub().resolves(null),
        getBlockScopedRows: sinon.stub().resolves([]),
        getActionScopedRows: sinon.stub().resolves([]),
        getEmissionRowsForBlock: sinon.stub().resolves([]),
        getTransactions: sinon.stub().resolves([]),
        getActions: sinon.stub().resolves([]),
        doQuery: sinon.stub().resolves([])
    };
}

function createMockBroadcaster(){
    return {
        broadcast: sinon.stub(),
        updateStatus: sinon.stub(),
        getSubscribers: sinon.stub().returns([]),
        getSubscriberCount: sinon.stub().returns(0)
    };
}

function createMockLog(){
    return {
        recordBlock: sinon.stub().resolves(),
        pruneFrom:   sinon.stub().resolves()
    };
}

describe('ServerPoller', function(){

    let poller, db, broadcaster, log, config, util;

    beforeEach(function(){
        db = createMockDb();
        broadcaster = createMockBroadcaster();
        log = createMockLog();
        config = { BLOCK_POLL_INTERVAL: 3000 };
        util = new Utility();
        poller = new ServerPoller('bitcoin', 'mainnet', db, broadcaster, log, config, util);
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });

    afterEach(function(){
        sinon.restore();
    });

    describe('table lists', function(){
        it('has block-scoped tables', function(){
            assert.ok(poller.blockScopedTables.includes('blocks'));
            assert.ok(poller.blockScopedTables.includes('transactions'));
            assert.ok(poller.blockScopedTables.includes('slash_events'));
            assert.strictEqual(poller.blockScopedTables.length, 5);
        });

        it('has action-scoped tables', function(){
            assert.ok(poller.actionScopedTables.length > 40);
            assert.ok(poller.actionScopedTables.includes('actions'));
            assert.ok(poller.actionScopedTables.includes('attests'));
            assert.ok(poller.actionScopedTables.includes('gated_files'));
            assert.ok(poller.actionScopedTables.includes('contract_stakes'));
            assert.ok(poller.actionScopedTables.includes('contract_unstakes'));
            assert.ok(poller.actionScopedTables.includes('contract_delegations'));
        });

        it('has index tables', function(){
            assert.strictEqual(poller.indexTables.length, 10);
            assert.ok(poller.indexTables.includes('index_transactions'));
            assert.ok(poller.indexTables.includes('index_addresses'));
        });
    });

    describe('_poll', function(){
        it('returns early when no blocks in DB', async function(){
            db.getLastBlock.resolves(null);
            await poller._poll();
            assert.strictEqual(broadcaster.broadcast.called, false);
        });

        it('initializes lastPolledBlock on first poll', async function(){
            db.getLastBlock.resolves(100);
            poller.lastPolledBlock = null;
            await poller._poll();
            assert.strictEqual(poller.lastPolledBlock, 100);
            assert.strictEqual(broadcaster.updateStatus.calledOnce, true);
            assert.strictEqual(broadcaster.broadcast.called, false);
        });

        it('processes new blocks when currentBlock > lastPolledBlock', async function(){
            poller.lastPolledBlock = 99;
            db.getLastBlock.resolves(100);
            db.getBlockHashRow.resolves({
                block_index: 100, block_time: 1700000000,
                ledger_hash: 'lh', actions_hash: 'ah', contract_hash: 'ch'
            });

            await poller._poll();

            assert.strictEqual(log.recordBlock.calledOnce, true);
            assert.strictEqual(broadcaster.broadcast.calledOnce, true);
            let event = broadcaster.broadcast.firstCall.args[2];
            assert.strictEqual(event.type, 'block');
            assert.strictEqual(event.block_index, 100);
            assert.strictEqual(event.chain, 'bitcoin');
            assert.strictEqual(event.network, 'mainnet');
            assert.strictEqual(poller.lastPolledBlock, 100);
        });

        it('does nothing when currentBlock equals lastPolledBlock', async function(){
            poller.lastPolledBlock = 100;
            db.getLastBlock.resolves(100);
            await poller._poll();
            assert.strictEqual(broadcaster.broadcast.called, false);
            assert.strictEqual(log.recordBlock.called, false);
        });

        it('limits to 100 blocks per poll', async function(){
            poller.lastPolledBlock = 0;
            db.getLastBlock.resolves(200);
            db.getBlockHashRow.resolves({
                block_index: 1, block_time: 100,
                ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c'
            });

            await poller._poll();

            assert.strictEqual(broadcaster.broadcast.callCount, 100);
            assert.strictEqual(poller.lastPolledBlock, 100);
        });

        it('detects reorg and broadcasts reorg event', async function(){
            poller.lastPolledBlock = 100;
            db.getLastBlock.resolves(95);
            db.getBlockHashRow.resolves({
                block_index: 95, block_time: 100,
                ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c'
            });

            await poller._poll();

            assert.strictEqual(broadcaster.broadcast.calledOnce, true);
            let event = broadcaster.broadcast.firstCall.args[2];
            assert.strictEqual(event.type, 'reorg');
            assert.strictEqual(event.block_index, 96); // currentBlock + 1
            assert.strictEqual(poller.lastPolledBlock, 95);
        });

        it('prunes the source transparency log on reorg (to currentBlock + 1)', async function(){
            poller.lastPolledBlock = 100;
            db.getLastBlock.resolves(95);
            db.getBlockHashRow.resolves({
                block_index: 95, block_time: 100,
                ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c'
            });

            await poller._poll();

            assert.strictEqual(log.pruneFrom.calledOnce, true);
            assert.strictEqual(log.pruneFrom.firstCall.args[0], 96); // orphaned suffix starts here
        });

        it('does not attempt a transparency prune on reorg for the decoder (no log)', async function(){
            let decoderDb = createMockDb();
            decoderDb.dbType = 'decoder';
            decoderDb.getLastBlock.resolves(95);
            decoderDb.getBlockHashRow.resolves({ block_index: 95, block_time: 100, block_hash: 'bh' });
            let decoderPoller = new ServerPoller('bitcoin', 'mainnet', decoderDb, broadcaster, null, config, util);
            decoderPoller.lastPolledBlock = 100;

            await decoderPoller._poll();  // must not throw despite transparencyLog === null

            // The reorg is still broadcast to subscribers.
            assert.strictEqual(broadcaster.broadcast.calledOnce, true);
            assert.strictEqual(broadcaster.broadcast.firstCall.args[2].type, 'reorg');
        });

        it('processes multiple sequential blocks', async function(){
            poller.lastPolledBlock = 97;
            db.getLastBlock.resolves(100);
            db.getBlockHashRow.callsFake(async (idx) => ({
                block_index: idx, block_time: idx * 10,
                ledger_hash: 'l' + idx, actions_hash: 'a' + idx, contract_hash: 'c' + idx
            }));

            await poller._poll();

            assert.strictEqual(broadcaster.broadcast.callCount, 3); // blocks 98, 99, 100
            assert.strictEqual(poller.lastPolledBlock, 100);
        });
    });

    describe('_buildBlockPayload', function(){
        it('returns null when block hash row is missing', async function(){
            db.getBlockHashRow.resolves(null);
            let result = await poller._buildBlockPayload(100);
            assert.strictEqual(result, null);
        });

        it('builds complete payload with correct structure', async function(){
            db.getBlockHashRow.resolves({
                block_index: 100, block_time: 1700000000,
                ledger_hash: 'lh', actions_hash: 'ah', contract_hash: 'ch'
            });
            db.getBlockScopedRows.resolves([{ block_index: 100 }]);
            db.getTransactions.resolves([{ tx_index: 1, block_index: 100, source_id: 10, tx_hash_id: 20 }]);
            db.getActions.resolves([{ action_index: 50, tx_index: 1 }]);

            let payload = await poller._buildBlockPayload(100);

            assert.strictEqual(payload.type, 'block');
            assert.strictEqual(payload.chain, 'bitcoin');
            assert.strictEqual(payload.network, 'mainnet');
            assert.strictEqual(payload.block_index, 100);
            assert.strictEqual(payload.block_time, 1700000000);
            assert.strictEqual(payload.ledger_hash, 'lh');
            assert.ok(payload.data);
        });

        it('streams contract_emissions via getEmissionRowsForBlock, not getActionScopedRows', async function(){
            db.getBlockHashRow.resolves({
                block_index: 100, block_time: 1700000000,
                ledger_hash: 'lh', actions_hash: 'ah', contract_hash: 'ch'
            });
            // Internal SLASH emission: action_index is NULL — getActionScopedRows would drop it.
            db.getEmissionRowsForBlock.resolves([
                { execution_index: 10, emitted_action: 'SLASH', action_index: null, position: 0 }
            ]);

            let payload = await poller._buildBlockPayload(100);

            assert.ok(db.getEmissionRowsForBlock.calledOnceWith(100),
                'contract_emissions must be sourced via getEmissionRowsForBlock');
            assert.ok(!db.getActionScopedRows.getCalls().some(c => c.args[0] === 'contract_emissions'),
                'contract_emissions must NOT go through getActionScopedRows');
            assert.ok(payload.data['contract_emissions'], 'emission rows present in payload');
            assert.strictEqual(payload.data['contract_emissions'][0].action_index, null);
        });

        it('includes a sync_meta transparency row in the indexer payload', async function(){
            db.getBlockHashRow.resolves({
                block_index: 100, block_time: 1700000000,
                ledger_hash: 'lh', actions_hash: 'ah', contract_hash: 'ch'
            });
            let payload = await poller._buildBlockPayload(100);

            assert.ok(payload.data['sync_meta'], 'sync_meta present in indexer payload');
            assert.strictEqual(payload.data['sync_meta'].length, 1);
            let row = payload.data['sync_meta'][0];
            assert.strictEqual(row.block_index, 100);
            assert.strictEqual(row.block_time, 1700000000);
            assert.strictEqual(row.ledger_hash, 'lh');
            assert.strictEqual(row.actions_hash, 'ah');
            assert.strictEqual(row.contract_hash, 'ch');
        });

        it('omits sync_meta from the decoder payload', async function(){
            let decoderDb = createMockDb();
            decoderDb.dbType = 'decoder';
            decoderDb.getBlockHashRow.resolves({
                block_index: 100, block_time: 1700000000, block_hash: 'bh'
            });
            // Decoder has no transparency log.
            let decoderPoller = new ServerPoller('bitcoin', 'mainnet', decoderDb, broadcaster, null, config, util);

            let payload = await decoderPoller._buildBlockPayload(100);
            assert.strictEqual(payload.data['sync_meta'], undefined, 'decoder payload has no sync_meta');
            assert.strictEqual(payload.block_hash, 'bh');
        });

        it('includes block-scoped table rows in data', async function(){
            db.getBlockHashRow.resolves({
                block_index: 1, block_time: 100,
                ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c'
            });
            let blockRows = [{ block_index: 1, block_time: 100 }];
            db.getBlockScopedRows.resolves(blockRows);

            let payload = await poller._buildBlockPayload(1);
            assert.ok(payload.data['blocks']);
        });

        it('skips tables that throw errors', async function(){
            db.getBlockHashRow.resolves({
                block_index: 1, block_time: 100,
                ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c'
            });
            db.getBlockScopedRows.rejects(new Error('table missing'));
            db.getTransactions.resolves([]);
            db.getActions.resolves([]);

            let payload = await poller._buildBlockPayload(1);
            assert.ok(payload); // Should not be null
            assert.strictEqual(payload.type, 'block');
        });

        it('fetches index_transactions by referenced IDs', async function(){
            db.getBlockHashRow.resolves({
                block_index: 1, block_time: 100,
                ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c'
            });
            let blockRow = { block_index: 1, ledger_hash_id: 1, actions_hash_id: 2, contract_hash_id: 3 };
            db.getBlockScopedRows.callsFake(async (table) => {
                if(table === 'blocks') return [blockRow];
                return [];
            });
            db.getTransactions.resolves([{ tx_index: 1, source_id: 10, tx_hash_id: 5 }]);
            db.getActions.resolves([]);
            db.doQuery.resolves([{ id: 1, hash: 'abc' }]);

            let payload = await poller._buildBlockPayload(1);

            // Should have called doQuery for index_transactions with IDs 1,2,3,5
            let idxCall = db.doQuery.getCalls().find(c => c.args[0].includes('index_transactions'));
            assert.ok(idxCall);
        });

        it('fetches index_addresses by source_id from transactions', async function(){
            db.getBlockHashRow.resolves({
                block_index: 1, block_time: 100,
                ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c'
            });
            db.getBlockScopedRows.resolves([]);
            db.getTransactions.resolves([
                { tx_index: 1, source_id: 10 },
                { tx_index: 2, source_id: 10 }, // duplicate source_id
                { tx_index: 3, source_id: 20 }
            ]);
            db.getActions.resolves([]);
            db.doQuery.resolves([{ id: 10 }]);

            let payload = await poller._buildBlockPayload(1);

            let addrCall = db.doQuery.getCalls().find(c => c.args[0].includes('index_addresses'));
            assert.ok(addrCall);
            // Should deduplicate source_ids: [10, 20]
            assert.strictEqual(addrCall.args[1].length, 2);
        });

        it('extracts the remaining indexer index tables from referenced _id columns', async function(){
            db.getBlockHashRow.resolves({
                block_index: 1, block_time: 100,
                ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c'
            });
            db.getBlockScopedRows.resolves([]);
            db.getTransactions.resolves([{ tx_index: 1, source_id: 10 }]);
            // An action interning a brand-new action name (action_id) + status (status_id),
            // and a send interning a new ticker (tick_id) — the mid-stream "new value" case.
            db.getActions.resolves([{ action_index: 1, action_id: 7, status_id: 3 }]);
            db.getActionScopedRows.callsFake(async (table) => {
                if(table === 'sends') return [{ action_index: 1, tick_id: 42, get_coin_id: 5 }];
                return [];
            });
            // Return a row only for the index tables actually queried by the generic pass.
            db.doQuery.callsFake(async (sql, params) => {
                if(sql.includes('index_actions'))   return [{ id: 7, action: 'NEWACTION' }];
                if(sql.includes('index_statuses'))  return [{ id: 3, status: 'valid' }];
                if(sql.includes('index_tickers'))   return [{ id: 42, tick: 'NEWTICK' }];
                if(sql.includes('index_coins'))     return [{ id: 5, coin: 'litecoin' }];
                return [];
            });

            let payload = await poller._buildBlockPayload(1);

            // The new interned rows ride the live block payload (previously snapshot-only).
            assert.deepStrictEqual(payload.data['index_actions'],  [{ id: 7, action: 'NEWACTION' }]);
            assert.deepStrictEqual(payload.data['index_statuses'], [{ id: 3, status: 'valid' }]);
            assert.deepStrictEqual(payload.data['index_tickers'],  [{ id: 42, tick: 'NEWTICK' }]);
            assert.deepStrictEqual(payload.data['index_coins'],    [{ id: 5, coin: 'litecoin' }]);

            // The generic pass must skip the two explicitly-handled tables (no double extraction).
            let genericTables = db.doQuery.getCalls()
                .map(c => c.args[0])
                .filter(sql => /WHERE id IN/.test(sql));
            assert.ok(!genericTables.some(sql => sql.includes('`index_transactions`')),
                'generic pass should not re-query index_transactions');
            assert.ok(!genericTables.some(sql => sql.includes('`index_addresses`')),
                'generic pass should not re-query index_addresses');
        });

        it('does not run the generic index pass for the decoder', async function(){
            let decoderDb = createMockDb();
            decoderDb.dbType = 'decoder';
            decoderDb.getBlockHashRow.resolves({ block_index: 1, block_time: 100, block_hash: 'bh' });
            decoderDb.getTransactions.resolves([{ tx_index: 1, source_id: 10 }]);
            let decoderPoller = new ServerPoller('bitcoin', 'mainnet', decoderDb, broadcaster, null, config, util);

            await decoderPoller._buildBlockPayload(1);

            // Decoder must never query the indexer-only interning tables.
            let touchedIndexerOnly = decoderDb.doQuery.getCalls().some(c =>
                /index_(actions|statuses|tickers|fiats|coins|memos|mime_types|pubkeys)/.test(c.args[0]));
            assert.ok(!touchedIndexerOnly, 'decoder payload must not touch indexer-only index tables');
        });
    });

    describe('_updateStatus', function(){
        it('calls broadcaster.updateStatus with correct shape', async function(){
            poller.lastPolledBlock = 50;
            db.getBlockHashRow.resolves({
                block_index: 50, block_time: 1700000000,
                ledger_hash: 'lh', actions_hash: 'ah', contract_hash: 'ch'
            });

            await poller._updateStatus();

            assert.strictEqual(broadcaster.updateStatus.calledOnce, true);
            let args = broadcaster.updateStatus.firstCall.args;
            assert.strictEqual(args[0], 'bitcoin');
            assert.strictEqual(args[1], 'mainnet');
            assert.strictEqual(args[2].block_height, 50);
            assert.strictEqual(args[2].ledger_hash, 'lh');
        });

        it('handles null lastPolledBlock', async function(){
            poller.lastPolledBlock = null;
            await poller._updateStatus();
            let status = broadcaster.updateStatus.firstCall.args[2];
            assert.strictEqual(status.block_height, null);
            assert.strictEqual(status.block_time, null);
        });
    });

    describe('stop', function(){
        it('sets running to false', function(){
            poller.running = true;
            poller.stop();
            assert.strictEqual(poller.running, false);
        });
    });
});
