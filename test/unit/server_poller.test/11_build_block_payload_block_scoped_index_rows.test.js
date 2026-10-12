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
const fs = require('fs');
const path = require('path');
const sinon = require('sinon');
const lifecycle = require('../../../src/table_lifecycle');
const ServerPoller = require('../../../src/server/poller');
const Utility = require('../../../src/util');
const { withDbMixins } = require('../../helpers/db_mixins.js');

const BLOCK_INDEX = 67951729;
const ADDRESS_ROW = { id: 49, address: 'C:DOGE:3919', block_index: BLOCK_INDEX };
const TICKER_ROW = { id: 71, tick: 'DEPLOYED', block_index: BLOCK_INDEX };
const DEPLOY_ACTION = { id: 6, action: 'DEPLOY' };

function createDb(referenced){
    const db = withDbMixins({
        dbType: 'indexer',
        getBlockHashRow: sinon.stub().resolves({
            block_index: BLOCK_INDEX,
            block_time: 1700000000,
            ledger_hash: 'ledger',
            actions_hash: 'actions',
            contract_hash: 'contract',
            state_hash: 'state'
        }),
        getTransactions: sinon.stub().resolves(referenced
            ? [{ tx_index: 1, block_index: BLOCK_INDEX, source_id: ADDRESS_ROW.id }]
            : []),
        getActions: sinon.stub().resolves([
            { action_index: 1, block_index: BLOCK_INDEX, action_id: DEPLOY_ACTION.id }
        ]),
        getActionScopedRows: sinon.stub().callsFake(async (table) => {
            if(referenced && table === 'credits')
                return [{ action_index: 1, address_id: ADDRESS_ROW.id, tick_id: TICKER_ROW.id, amount: 1 }];
            return [];
        }),
        getEmissionRowsForBlock: sinon.stub().resolves([]),
        getNonEmptyActionScopedTables: sinon.stub().resolves(null),
        getStateRootsRow: sinon.stub().resolves(null),
        getStatusId: sinon.stub().resolves(null),
        doQuery: sinon.stub().callsFake(async (sql, params) => {
            if(/^SELECT \* FROM `index_addresses` WHERE block_index = \?/.test(sql))
                return [ADDRESS_ROW];
            if(/^SELECT \* FROM `index_tickers` WHERE block_index = \?/.test(sql))
                return [TICKER_ROW];
            if(/FROM `?index_actions`? WHERE id IN/.test(sql) && params.includes(DEPLOY_ACTION.id))
                return [DEPLOY_ACTION];
            if(/FROM `?index_addresses`? WHERE id IN/.test(sql) && params.includes(ADDRESS_ROW.id))
                return [ADDRESS_ROW];
            if(/FROM `?index_tickers`? WHERE id IN/.test(sql) && params.includes(TICKER_ROW.id))
                return [TICKER_ROW];
            return [];
        })
    });
    return db;
}

function createPoller(db){
    const broadcaster = {
        broadcast: sinon.stub(),
        updateStatus: sinon.stub(),
        getSubscribers: sinon.stub().returns([]),
        getSubscriberCount: sinon.stub().returns(0)
    };
    return new ServerPoller(
        'dogecoin', 'testnet', db, broadcaster, null,
        { BLOCK_POLL_INTERVAL: 3000 }, new Utility()
    );
}

describe('ServerPoller buildBlockPayload block-scoped index rows', function(){
    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });

    afterEach(function(){
        sinon.restore();
    });

    it('includes address and ticker rows interned by a DEPLOY with no same-block references', async function(){
        const db = createDb(false);

        const payload = await createPoller(db).buildBlockPayload(BLOCK_INDEX);

        assert.deepStrictEqual(payload.data.index_addresses, [ADDRESS_ROW]);
        assert.deepStrictEqual(payload.data.index_tickers, [TICKER_ROW]);
        assert.deepStrictEqual(payload.data.index_actions, [DEPLOY_ACTION]);
        assert.ok(db.doQuery.calledWithMatch(
            /^SELECT \* FROM `index_addresses` WHERE block_index = \?/, [BLOCK_INDEX]));
        assert.ok(db.doQuery.calledWithMatch(
            /^SELECT \* FROM `index_tickers` WHERE block_index = \?/, [BLOCK_INDEX]));
    });

    it('de-duplicates the block-scoped rows when existing routes also fetch them', async function(){
        const db = createDb(true);

        const payload = await createPoller(db).buildBlockPayload(BLOCK_INDEX);

        assert.deepStrictEqual(payload.data.index_addresses, [ADDRESS_ROW]);
        assert.deepStrictEqual(payload.data.index_tickers, [TICKER_ROW]);
        assert.strictEqual(payload.data.index_addresses.filter(row => row.id === ADDRESS_ROW.id).length, 1);
        assert.strictEqual(payload.data.index_tickers.filter(row => row.id === TICKER_ROW.id).length, 1);
    });

    // The forward pass must stream exactly the lookups the registry rolls back by block,
    // so a lookup later moved to rollback 'index' is streamed with no edit here.
    it('reads the block-scoped lookups the lifecycle registry declares, and only those', async function(){
        const registrySet = lifecycle.tablesWhere(t => t.rollback === 'index' && t.replication === 'stream:index');
        assert.ok(registrySet.length > 0, 'registry declares at least one block-scoped lookup');
        const db = createDb(false);
        const read = sinon.spy(db, 'getBlockScopedRows');
        const poller = createPoller(db);
        sinon.stub(poller, 'addUpdatedPayloadRows').resolves();

        await poller.addBlockScopedIndexRows({ data: {} }, BLOCK_INDEX, null);

        assert.deepStrictEqual(read.getCalls().map(c => c.args[0]).sort(), registrySet.slice().sort());
    });

    it('derives the block-scoped lookup list instead of hand-listing it', function(){
        const source = fs.readFileSync(path.join(__dirname, '../../../src/server/poller.js'), 'utf8');
        assert.ok(!/\[\s*'index_addresses'\s*,\s*'index_tickers'\s*\]/.test(source),
            'ServerPoller.addBlockScopedIndexRows hand-lists its lookups; derive them from ' +
            "lifecycle.tablesWhere(t => t.rollback === 'index' && t.replication === 'stream:index')");
    });
});
