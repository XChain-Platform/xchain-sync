// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.
//
// A legacy-era cooldown maturity writes a refund credit AND a negative escrow
// release under the unstake's earlier action_index, and mints no actions row, so a
// catch-up window can hold nothing else. Both rows must ride that window.

const { assert, sinon, PassThrough, zlib, SnapshotBuilder, Utility, createMockDb } = require('./helpers/support');

// A real writable response (gzip pipes into it) that collects what it is sent.
function streamRes(){
    let res = new PassThrough();
    let chunks = [];
    res.setHeader = () => {};
    res.status = sinon.stub().returnsThis();
    res.json = sinon.stub();
    res.on('data', c => chunks.push(c));
    res.getCollectedData = () => Buffer.concat(chunks);
    return res;
}

// A quiet indexer window whose only content is one matured capability unstake.
function quietWindowDb(){
    let db = createMockDb();
    db.dbType = 'indexer';
    db.getLastBlock.resolves(100);
    db.getBlockHashRow.resolves(null);
    db.getFirstActionIndex.resolves(null);
    db.getStatusId = sinon.stub().resolves(3);
    db.doQuery.callsFake(async (sql) => {
        if(/information_schema/.test(sql)) return [{ table_name: 'credits' }, { table_name: 'escrows' }];
        if(/FROM escrows e JOIN unstakes u/.test(sql)) return [{ action_index: 42, address_id: 7, tick_id: 1, amount: '-100' }];
        if(/FROM credits c JOIN unstakes u/.test(sql)) return [{ action_index: 42, address_id: 7, tick_id: 1, amount: '100' }];
        return [];
    });
    return db;
}

describe('SnapshotBuilder', function(){
    let builder;
    beforeEach(function(){ builder = new SnapshotBuilder(new Utility()); sinon.stub(console, 'error'); });
    afterEach(function(){ sinon.restore(); });

    it('ships the matured escrow release beside the refund credit in a quiet window @regression', async function(){
        let res = streamRes();
        await new Promise(r => { res.on('finish', r); builder.streamIncrementalSnapshot(quietWindowDb(), 3, res); });
        let out = JSON.parse(zlib.gunzipSync(res.getCollectedData()).toString());
        assert.deepStrictEqual(out.tables.credits.map(c => [c.action_index, String(c.amount)]), [[42, '100']]);
        assert.ok('escrows' in out.tables, 'escrows present even with null firstActionIndex');
        assert.deepStrictEqual(out.tables.escrows.map(e => [e.action_index, String(e.amount)]), [[42, '-100']],
            'the release ships as an escrows row, not as a credit');
    });
});
