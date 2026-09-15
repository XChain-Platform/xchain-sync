// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

const assert = require('assert');
const sinon  = require('sinon');
const ServerPoller = require('../../../src/server/poller');
const Utility = require('../../../src/util');
const { withDbMixins } = require('../../helpers/db_mixins.js');

function createMockDb(){
    // Queries read through named Database methods. The real ones are installed for
    // any this fake does not stub, so they still reach doQuery below and every
    // doQuery call count these suites assert keeps counting them.
    return withDbMixins({
        getLastBlock: sinon.stub().resolves(null),
        getBlockHashRow: sinon.stub().resolves(null),
        getBlockScopedRows: sinon.stub().resolves([]),
        getTxScopedRows: sinon.stub().resolves([]),
        getActionScopedRows: sinon.stub().resolves([]),
        getEmissionRowsForBlock: sinon.stub().resolves([]),
        getStateRootsRow: sinon.stub().resolves({
            balances_root: 'br', block_merkle_root: 'bmr', state_root: 'sr'
        }),
        getTransactions: sinon.stub().resolves([]),
        getActions: sinon.stub().resolves([]),
        // Used by collectMaturedCooldownCredits; null short-circuits it to no credits.
        getStatusId: sinon.stub().resolves(null),
        doQuery: sinon.stub().resolves([]),
        // The forward batch is pinned to a REPEATABLE READ snapshot (H-P2).
        beginReadSnapshot: sinon.stub().resolves({ mockSnapshotConn: true }),
        commitReadSnapshot: sinon.stub().resolves(),
        rollbackReadSnapshot: sinon.stub().resolves()
    });
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
        epochSize:        100,
        recordBlock:      sinon.stub().resolves(),
        pruneFrom:        sinon.stub().resolves(),
        getHighWaterMark: sinon.stub().resolves(null),
        getRecordedHash:  sinon.stub().resolves(null),
        findGaps:         sinon.stub().resolves([]),
        recommitEpoch:    sinon.stub().resolves()
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

    describe('buildBlockPayload', function(){
        it('fails closed on a TRANSIENT updated_rows collection error (deadlock 1213) so the block is retried, not broadcast without updated_rows @regression', async function(){
            db.getBlockHashRow.resolves({
                block_index: 1, block_time: 100,
                ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c'
            });
            db.getBlockScopedRows.resolves([]);
            db.getTransactions.resolves([]);
            db.getActions.resolves([]);
            // collectUpdatedRows runs its reads via db.doQuery and re-throws transients;
            // ServerPoller must NOT swallow that (a dropped in-place mutation forks followers).
            let transient = new Error('Deadlock found when trying to get lock'); transient.errno = 1213;
            db.doQuery.rejects(transient);

            let threw = false;
            try {
                await poller.buildBlockPayload(1);
            } catch(e){
                threw = true;
                assert.strictEqual(e.errno, 1213);
            }
            assert.ok(threw, 'a transient updated_rows fault must propagate out of buildBlockPayload');
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

            let payload = await poller.buildBlockPayload(1);

            // Assert the ID SET, not merely that a call happened: a missing hash id rides
            // green against the weaker assertion (see the state_hash_id regression below).
            let idxCall = db.doQuery.getCalls().find(c => c.args[0].includes('index_transactions'));
            assert.ok(idxCall);
            assert.deepStrictEqual([...idxCall.args[1]].sort((a, b) => a - b), [1, 2, 3, 5]);
        });

    });

    describe('buildBlockPayload', function(){
        it('streams the blocks.state_hash_id index_transactions row with the live block @regression', async function(){
            // The generic `*_id` pass skips index_transactions, so this explicit
            // collection is the only path the state hash row takes to a follower: omit it
            // and every live block leaves a dangling blocks.state_hash_id behind.
            db.getBlockHashRow.resolves({
                block_index: 1, block_time: 100,
                ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c', state_hash: 's'
            });
            db.getBlockScopedRows.callsFake(async (table) => {
                if(table === 'blocks') return [{
                    block_index: 1, ledger_hash_id: 1, actions_hash_id: 2,
                    contract_hash_id: 3, state_hash_id: 4
                }];
                return [];
            });
            db.getTransactions.resolves([]);
            db.getActions.resolves([]);
            db.doQuery.resolves([]);

            await poller.buildBlockPayload(1);

            let idxCall = db.doQuery.getCalls().find(c => c.args[0].includes('index_transactions'));
            assert.ok(idxCall, 'index_transactions must be queried for the block hash ids');
            assert.ok(idxCall.args[1].includes(4),
                'the state_hash_id row must ride the live block payload, or the replica ' +
                'holds a dangling reference and serves state_hash NULL forever');
        });

        it('collects both decoder blocks hash ids under the shared *_hash_id rule @regression', async function(){
            let decoderDb = createMockDb();
            decoderDb.dbType = 'decoder';
            decoderDb.getBlockHashRow.resolves({ block_index: 1, block_time: 100, block_hash: 'bh' });
            decoderDb.getBlockScopedRows.callsFake(async (table) => {
                if(table === 'blocks') return [{
                    block_index: 1, block_hash_id: 11, previous_block_hash_id: 12
                }];
                return [];
            });
            decoderDb.getTransactions.resolves([{ tx_index: 1, tx_hash_id: 13 }]);
            decoderDb.doQuery.resolves([]);
            let decoderPoller = new ServerPoller('bitcoin', 'mainnet', decoderDb, broadcaster, null, config, util);

            await decoderPoller.buildBlockPayload(1);

            let idxCall = decoderDb.doQuery.getCalls().find(c => c.args[0].includes('index_transactions'));
            assert.ok(idxCall);
            assert.deepStrictEqual([...idxCall.args[1]].sort((a, b) => a - b), [11, 12, 13]);
        });

    });

    describe('buildBlockPayload', function(){
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

            let payload = await poller.buildBlockPayload(1);

            let addrCall = db.doQuery.getCalls().find(c => c.args[0].includes('index_addresses'));
            assert.ok(addrCall);
            // Should deduplicate source_ids: [10, 20]
            assert.strictEqual(addrCall.args[1].length, 2);
        });

    });

    describe('buildBlockPayload', function(){
        it('extracts the remaining indexer index tables from referenced _id columns', async function(){
            db.getBlockHashRow.resolves({
                block_index: 1, block_time: 100,
                ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c'
            });
            db.getBlockScopedRows.resolves([]);
            db.getTransactions.resolves([{ tx_index: 1, source_id: 10 }]);
            // An action interning a brand-new action name (action_id) + status (status_id),
            // and a send interning a new ticker (tick_id): the mid-stream "new value" case.
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

            let payload = await poller.buildBlockPayload(1);

            // Interned rows ride the live block payload as well as snapshots.
            assert.deepStrictEqual(payload.data['index_actions'],  [{ id: 7, action: 'NEWACTION' }]);
            assert.deepStrictEqual(payload.data['index_statuses'], [{ id: 3, status: 'valid' }]);
            assert.deepStrictEqual(payload.data['index_tickers'],  [{ id: 42, tick: 'NEWTICK' }]);
            assert.deepStrictEqual(payload.data['index_coins'],    [{ id: 5, coin: 'litecoin' }]);

            // The generic pass must skip index_transactions (it carries block-hash/
            // tx-hash IDs the generic _id scan can't see, so it keeps its explicit
            // join). index_addresses, by contrast, IS intentionally re-fetched by the
            // generic pass over the full ref set, so a non-tx-interned address is
            // streamed at its intern block (without it the follower index map forks).
            let genericTables = db.doQuery.getCalls()
                .map(c => c.args[0])
                .filter(sql => /WHERE id IN/.test(sql));
            assert.ok(!genericTables.some(sql => sql.includes('`index_transactions`')),
                'generic pass should not re-query index_transactions');
            assert.ok(genericTables.some(sql => sql.includes('`index_addresses`')),
                'generic pass should re-fetch index_addresses over the full ref set');
        });

        it('does not run the generic index pass for the decoder', async function(){
            let decoderDb = createMockDb();
            decoderDb.dbType = 'decoder';
            decoderDb.getBlockHashRow.resolves({ block_index: 1, block_time: 100, block_hash: 'bh' });
            decoderDb.getTransactions.resolves([{ tx_index: 1, source_id: 10 }]);
            let decoderPoller = new ServerPoller('bitcoin', 'mainnet', decoderDb, broadcaster, null, config, util);

            await decoderPoller.buildBlockPayload(1);

            // Decoder must never query the indexer-only interning tables.
            let touchedIndexerOnly = decoderDb.doQuery.getCalls().some(c =>
                /index_(actions|statuses|tickers|fiats|coins|memos|mime_types|pubkeys)/.test(c.args[0]));
            assert.ok(!touchedIndexerOnly, 'decoder payload must not touch indexer-only index tables');
        });
    });
});
