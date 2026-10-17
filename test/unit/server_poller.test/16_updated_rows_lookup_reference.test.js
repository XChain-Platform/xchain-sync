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
const sinon = require('sinon');
const ServerPoller = require('../../../src/server/poller');
const Utility = require('../../../src/util');
const { withDbMixins } = require('../../helpers/db_mixins.js');

const BLOCK_INDEX = 67951800;
const STAMPED_STATUS = { id: 9001, status: 'quorum failed (stamped on batch completion)' };
const HEAD_ROW = { action_index: 40, block_index: 67951700, version: 5, batch_chunk_index: 0, status_id: STAMPED_STATUS.id };
const VALID_STATUS = { id: 3, status: 'valid' };

function createDb(opts){
    return withDbMixins({
        dbType: 'indexer',
        getBlockHashRow: sinon.stub().resolves({
            block_index: BLOCK_INDEX,
            block_time: 1700000000,
            ledger_hash: 'ledger',
            actions_hash: 'actions',
            contract_hash: 'contract',
            state_hash: 'state'
        }),
        getTransactions: sinon.stub().resolves([]),
        getActions: sinon.stub().resolves([]),
        getActionScopedRows: sinon.stub().callsFake(async (table) => {
            if(opts.validChunk && table === 'attests')
                return [{ action_index: 41, block_index: BLOCK_INDEX, version: 6, status_id: VALID_STATUS.id }];
            return [];
        }),
        getEmissionRowsForBlock: sinon.stub().resolves([]),
        getNonEmptyActionScopedTables: sinon.stub().resolves(null),
        getStateRootsRow: sinon.stub().resolves(null),
        getStatusId: sinon.stub().resolves(null),
        findFailedAttestBatchHeads: sinon.stub().resolves(opts.stampedHead ? [HEAD_ROW] : []),
        doQuery: sinon.stub().callsFake(async (sql, params) => {
            if(!/FROM `?index_statuses`? WHERE id IN/.test(sql)) return [];
            return [STAMPED_STATUS, VALID_STATUS].filter(row => params.includes(row.id));
        })
    });
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

describe('ServerPoller buildBlockPayload lookups referenced only by updated rows', function(){
    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });

    afterEach(function(){
        sinon.restore();
    });

    it('ships a status interned by an in-place head stamp that no payload.data row references @regression', async function(){
        const db = createDb({ stampedHead: true, validChunk: true });

        const payload = await createPoller(db).buildBlockPayload(BLOCK_INDEX);

        assert.deepStrictEqual(payload.updated_rows.attests, [HEAD_ROW]);
        const statuses = payload.data.index_statuses || [];
        assert.ok(statuses.some(row => row.id === STAMPED_STATUS.id),
            'the stamped head references status 9001, so its index_statuses row must ride payload.data');
        assert.ok(statuses.some(row => row.id === VALID_STATUS.id),
            'the completing chunk status found by the payload.data scan must still ride');
        assert.strictEqual(statuses.filter(row => row.id === VALID_STATUS.id).length, 1,
            'an id already shipped by the payload.data scan is not fetched twice');
    });

    it('adds no lookup rows when the block has no updated rows', async function(){
        const db = createDb({ stampedHead: false, validChunk: false });

        const payload = await createPoller(db).buildBlockPayload(BLOCK_INDEX);

        assert.strictEqual(payload.updated_rows, undefined);
        assert.strictEqual(payload.data.index_statuses, undefined);
    });

    it('pools *_id values from any table map', function(){
        const poller = createPoller(createDb({}));
        const ids = poller.collectReferenceIds({
            attests: [{ status_id: 9001, action_index: 40 }],
            stakes: [{ address_id: 12, tick_id: null }]
        });
        assert.deepStrictEqual([...ids].sort((a, b) => a - b), [12, 9001]);
    });
});
