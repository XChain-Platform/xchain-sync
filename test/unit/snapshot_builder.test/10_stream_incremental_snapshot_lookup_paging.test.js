// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

const {
    assert,
    sinon,
    PassThrough,
    Readable,
    EventEmitter,
    zlib,
    SnapshotBuilder,
    SnapshotStreamWriter,
    Utility,
    poolSizing,
    createMockDb,
    createMockRes
} = require('./helpers/support');

let builder;

function prepareSnapshotBuilder(){
    const util = new Utility();
    builder = new SnapshotBuilder(util);
    sinon.stub(console, 'error');
}

function restoreSnapshotBuilder(){
    sinon.restore();
}

// Fix #2 (MED): the incremental full-dump of append-only lookup tables ran an
// unbounded `SELECT *`, materializing a whole multi-million-row table into the
// driver array. They stream by id cursor in pageSize batches.
function streamIncrementalSnapshotLookupPagingTests(){
    it('pages a full-dump lookup table by id cursor instead of one unbounded SELECT *', async function(){
        let db = createMockDb();
        db.dbType = 'indexer';
        builder.pageSize = 2;              // force multiple pages over a small set
        db.getLastBlock.resolves(100);
        db.getBlockHashRow.resolves({ ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c' });
        db.getFirstActionIndex.resolves(null);
        let pageCalls = [];
        db.doQuery.callsFake(async (query, args) => {
            if(query.includes('information_schema')) return [{ table_name: 'index_addresses' }];
            let m = /SELECT \* FROM `index_addresses` WHERE `id` > \? ORDER BY `id` ASC LIMIT \?/.exec(query);
            if(m){
                pageCalls.push(args);      // [after, limit]
                let after = args[0];
                // 3 rows total (ids 1,2,3), served 2 per page by id cursor.
                let all = [{ id: 1, address: 'a1' }, { id: 2, address: 'a2' }, { id: 3, address: 'a3' }];
                return all.filter(r => r.id > after).slice(0, args[1]);
            }
            return [];
        });

        let res = new PassThrough();
        let chunks = [];
        res.on('data', c => chunks.push(c));
        res.setHeader = sinon.stub();
        await new Promise((resolve) => { res.on('finish', resolve); builder.streamIncrementalSnapshot(db, 80, res); });

        let parsed = JSON.parse(zlib.gunzipSync(Buffer.concat(chunks)).toString());
        assert.deepStrictEqual(parsed.tables.index_addresses.map(r => r.id), [1, 2, 3], 'all rows streamed across pages');
        // Never an unbounded SELECT *: every read of the table carried the id-cursor + LIMIT.
        assert.ok(pageCalls.length >= 2, 'read in more than one bounded page');
        assert.ok(pageCalls.every(a => a[1] === 2), 'each page bounded by pageSize');
        assert.deepStrictEqual(pageCalls[0], [0, 2], 'first page starts at cursor 0');
    });
}

function snapshotBuilderTests(){
    beforeEach(prepareSnapshotBuilder);
    afterEach(restoreSnapshotBuilder);
    describe('streamIncrementalSnapshot lookup paging', streamIncrementalSnapshotLookupPagingTests);
}

describe('SnapshotBuilder', snapshotBuilderTests);
