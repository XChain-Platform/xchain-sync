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

// A decoder source whose block 1 holds one tx (source 10) and one DISPENSER create.
function makeDecoderDb(dispenserRows){
    let db = withDbMixins({
        getLastBlock: sinon.stub().resolves(null),
        getBlockHashRow: sinon.stub().resolves({ block_index: 1, block_time: 100, block_hash: 'bh' }),
        getBlockScopedRows: sinon.stub().resolves([]),
        getTxScopedRows: sinon.stub().callsFake(async (table) => (table === 'dispensers' ? dispenserRows() : [])),
        getTransactions: sinon.stub().resolves([{ tx_index: 7, source_id: 10 }]),
        getStatusId: sinon.stub().resolves(null),
        doQuery: sinon.stub().resolves([]),
        beginReadSnapshot: sinon.stub().resolves({ mockSnapshotConn: true }),
        commitReadSnapshot: sinon.stub().resolves(),
        rollbackReadSnapshot: sinon.stub().resolves()
    });
    db.dbType = 'decoder';
    db.findIndexAddressesByIds = sinon.stub().callsFake(async (ids) => ids.map(id => ({ id, address: 'a' + id })));
    db.findPubkeysByAddressIds = sinon.stub().resolves([]);
    return db;
}

function makePoller(db){
    let broadcaster = { broadcast: sinon.stub(), updateStatus: sinon.stub(),
        getSubscribers: sinon.stub().returns([]), getSubscriberCount: sinon.stub().returns(0) };
    return new ServerPoller('bitcoin', 'mainnet', db, broadcaster, null, { BLOCK_POLL_INTERVAL: 3000 }, new Utility());
}

// A dispenser's GET_ADDRESS and oracle address are interned in the create's block
// but referenced only by the never-streamed dispensers row, so without this the
// replica's MAX(id) cursor passes them and leaves a permanent hole.
describe('ServerPoller decoder payload ships dispenser-interned address ids @regression', function(){
    beforeEach(function(){ sinon.stub(console, 'log'); sinon.stub(console, 'error'); });
    afterEach(function(){ sinon.restore(); });

    it('adds the dispenser address and oracle ids to index_addresses', async function(){
        let db = makeDecoderDb(() => [{ tx_index: 7, address_id: 40, oracle_address_id: 41, source_address_id: 10 }]);
        let payload = await makePoller(db).buildBlockPayload(1);
        assert.deepStrictEqual(db.findIndexAddressesByIds.firstCall.args[0].slice().sort(), [10, 40, 41]);
        assert.deepStrictEqual(payload.data.index_addresses.map(r => r.id).sort(), [10, 40, 41]);
        assert.deepStrictEqual(db.findPubkeysByAddressIds.firstCall.args[0].slice().sort(), [10, 40, 41],
            'pubkeys follow the payload index_addresses, so they ship for these ids too');
    });

    it('never streams the dispensers rows themselves', async function(){
        let db = makeDecoderDb(() => [{ tx_index: 7, address_id: 40, oracle_address_id: null, source_address_id: null }]);
        let payload = await makePoller(db).buildBlockPayload(1);
        assert.strictEqual(payload.data.dispensers, undefined);
        assert.deepStrictEqual(db.findIndexAddressesByIds.firstCall.args[0].slice().sort(), [10, 40]);
    });

    it('keeps the tx address ids when the source has no dispensers table', async function(){
        let gap = Object.assign(new Error("Table 'dispensers' doesn't exist"), { errno: 1146, code: 'ER_NO_SUCH_TABLE' });
        let db = makeDecoderDb(() => { throw gap; });
        let payload = await makePoller(db).buildBlockPayload(1);
        assert.deepStrictEqual(payload.data.index_addresses.map(r => r.id), [10]);
    });

    it('fails the block on a transient dispensers read error so it is retried', async function(){
        let db = makeDecoderDb(() => { throw Object.assign(new Error('deadlock'), { errno: 1213 }); });
        await assert.rejects(makePoller(db).buildBlockPayload(1), /deadlock/);
    });
});
