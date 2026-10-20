// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// The poller reads each id-bounded lookup's MAX(id) inside the forward batch's pinned
// snapshot once the batch reaches the snapshot tip, so /status can publish a content
// parity id ceiling that describes exactly the height it broadcast.

const assert = require('assert');
const sinon  = require('sinon');
const ServerPoller = require('../../../src/server/poller');
const Utility = require('../../../src/util');
const replicatedTables = require('../../../src/schema/replicated_tables');
const { withDbMixins } = require('../../helpers/db_mixins.js');

const HASH_ROW = {
    block_index: 100, block_time: 1700000000,
    ledger_hash: 'lh', actions_hash: 'ah', contract_hash: 'ch'
};

function createMockDb(){
    return withDbMixins({
        getLastBlock: sinon.stub().resolves(null),
        getBlockHashRow: sinon.stub().resolves(HASH_ROW),
        getBlockScopedRows: sinon.stub().resolves([]),
        getTxScopedRows: sinon.stub().resolves([]),
        getActionScopedRows: sinon.stub().resolves([]),
        getEmissionRowsForBlock: sinon.stub().resolves([]),
        getStateRootsRow: sinon.stub().resolves({ balances_root: 'br', block_merkle_root: 'bmr', state_root: 'sr' }),
        getTransactions: sinon.stub().resolves([]),
        getActions: sinon.stub().resolves([]),
        getStatusId: sinon.stub().resolves(null),
        doQuery: sinon.stub().resolves([]),
        getMaxRowId: sinon.stub().callsFake(async (table, conn) => (conn ? 40 : 55)),
        beginReadSnapshot: sinon.stub().resolves({ mockSnapshotConn: true }),
        commitReadSnapshot: sinon.stub().resolves(),
        rollbackReadSnapshot: sinon.stub().resolves()
    });
}

function createPoller(config){
    let db = createMockDb();
    let broadcaster = {
        broadcast: sinon.stub(), updateStatus: sinon.stub(),
        getSubscribers: sinon.stub().returns([]), getSubscriberCount: sinon.stub().returns(0)
    };
    let log = {
        epochSize: 100, recordBlock: sinon.stub().resolves(), pruneFrom: sinon.stub().resolves(),
        getHighWaterMark: sinon.stub().resolves(null), getRecordedHash: sinon.stub().resolves(null),
        findGaps: sinon.stub().resolves([]), recommitEpoch: sinon.stub().resolves()
    };
    let poller = new ServerPoller('bitcoin', 'mainnet', db, broadcaster, log,
        Object.assign({ BLOCK_POLL_INTERVAL: 3000 }, config || {}), new Utility());
    return { poller, db };
}

const ID_TABLES = replicatedTables.contentParityPlan('indexer').filter(s => s.bound === 'id').map(s => s.table);

describe('ServerPoller content-parity id ceiling', function(){

    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });
    afterEach(function(){ sinon.restore(); });

    it('reads every id-bounded lookup on the snapshot connection at the broadcast tip', async function(){
        let { poller, db } = createPoller({ TABLE_CONTENT_PARITY_CHECK: true });
        poller.lastPolledBlock = 99;
        db.getLastBlock.resolves(100);

        await poller.poll();

        let ceiling = poller.getContentParityIdCeiling();
        assert.ok(ID_TABLES.length > 0, 'the indexer plan must carry id-bounded lookups');
        assert.strictEqual(ceiling.block, 100);
        assert.deepStrictEqual(Object.keys(ceiling.ids).sort(), ID_TABLES.slice().sort());
        // 40 is the snapshot view; 55 is what a live read would have returned.
        for(let t of ID_TABLES) assert.strictEqual(ceiling.ids[t], 40);
        assert.ok(db.getMaxRowId.getCalls().filter(c => ID_TABLES.includes(c.args[0]))
            .every(c => c.args[1] && c.args[1].mockSnapshotConn), 'every ceiling read is pinned');
    });

    it('records nothing while the advisory check is off', async function(){
        let { poller, db } = createPoller({});
        poller.lastPolledBlock = 99;
        db.getLastBlock.resolves(100);

        await poller.poll();

        assert.strictEqual(poller.getContentParityIdCeiling(), null);
        assert.ok(!db.getMaxRowId.getCalls().some(c => ID_TABLES.includes(c.args[0])));
    });

    it('records nothing for a batch capped below its snapshot tip', async function(){
        // The pinned view is at 250 while only 100 blocks were broadcast, so its MAX(id)
        // would describe blocks no follower has yet.
        let { poller, db } = createPoller({ TABLE_CONTENT_PARITY_CHECK: true });
        poller.lastPolledBlock = 150;
        poller.contentParityIdCeiling = { block: 150, ids: { [ID_TABLES[0]]: 1 } };
        db.getLastBlock.resolves(350);

        await poller.poll();

        assert.strictEqual(poller.lastPolledBlock, 250);
        let ceiling = poller.getContentParityIdCeiling();
        assert.ok(ceiling === null || ceiling.block !== poller.lastPolledBlock,
            'a ceiling labelled with the broadcast height must come from a view at that height');
        assert.ok(!db.getMaxRowId.getCalls().some(c => ID_TABLES.includes(c.args[0])),
            'no ceiling is read from a view ahead of the broadcast height');
    });

    it('drops the ceiling when a reorg is broadcast', function(){
        let { poller } = createPoller({ TABLE_CONTENT_PARITY_CHECK: true });
        poller.contentParityIdCeiling = { block: 100, ids: { [ID_TABLES[0]]: 40 } };
        poller.broadcastReorg(98);
        assert.strictEqual(poller.getContentParityIdCeiling(), null);
    });

    it('leaves out a lookup whose read fails, keeping the others', async function(){
        let { poller, db } = createPoller({ TABLE_CONTENT_PARITY_CHECK: true });
        db.getMaxRowId = sinon.stub().callsFake(async table => {
            if(table === ID_TABLES[0]) throw Object.assign(new Error('no such table'), { errno: 1146 });
            return 7;
        });
        await poller.recordContentParityIdCeiling({ mockSnapshotConn: true }, 100);
        let ceiling = poller.getContentParityIdCeiling();
        assert.ok(!(ID_TABLES[0] in ceiling.ids));
        if(ID_TABLES.length > 1) assert.strictEqual(ceiling.ids[ID_TABLES[1]], 7);
    });
});
