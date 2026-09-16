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

function streamTableRowsByIdPagingTests(){
    it('rejects a non-pageable table with 400', async function(){
        let db = createMockDb();
        let res = createMockRes();
        await builder.streamTableRowsById(db, 'balances', 0, 100, res);
        assert.ok(res.status.calledWith(400), 'only allowlisted .index tables are pageable');
    });

    it('streams an id-ordered page with max_id and has_more=false when short', async function(){
        let db = createMockDb();
        db.doQuery.resolves([{ id: 11, x: 'a' }, { id: 12, x: 'b' }]);
        let res = new PassThrough();
        let chunks = [];
        res.on('data', c => chunks.push(c));
        res.setHeader = sinon.stub();
        await new Promise((resolve) => {
            res.on('finish', resolve);
            builder.streamTableRowsById(db, 'index_transactions', 10, 50000, res);
        });
        let parsed = JSON.parse(zlib.gunzipSync(Buffer.concat(chunks)).toString());
        assert.strictEqual(parsed.table, 'index_transactions');
        assert.strictEqual(parsed.rows.length, 2);
        assert.strictEqual(parsed.max_id, 12, 'max_id is the last returned id');
        assert.strictEqual(parsed.has_more, false, 'short page -> no more');
    });

    it('queries id > after_id ORDER BY id LIMIT, and sets has_more when a full page returns', async function(){
        let db = createMockDb();
        db.doQuery.resolves([{ id: 1 }, { id: 2 }, { id: 3 }]);
        let res = new PassThrough();
        let chunks = [];
        res.on('data', c => chunks.push(c));
        res.setHeader = sinon.stub();
        await new Promise((resolve) => {
            res.on('finish', resolve);
            builder.streamTableRowsById(db, 'index_transactions', 0, 3, res);
        });
        let parsed = JSON.parse(zlib.gunzipSync(Buffer.concat(chunks)).toString());
        assert.strictEqual(parsed.has_more, true, 'rows.length === limit -> more pages remain');
        let q = db.doQuery.firstCall.args[0];
        assert.ok(/WHERE `id` > \? ORDER BY `id` ASC LIMIT \?/.test(q), 'paged id-cursor query');
        assert.deepStrictEqual(db.doQuery.firstCall.args[1], [0, 3], 'after_id and limit bound as params');
    });
}

function streamTableRowsByIdLimitTests(){

    it('clamps an oversized limit to the ceiling', async function(){
        let db = createMockDb();
        db.doQuery.resolves([]);
        let res = new PassThrough();
        res.on('data', () => {});
        res.setHeader = sinon.stub();
        await new Promise((resolve) => {
            res.on('finish', resolve);
            builder.streamTableRowsById(db, 'index_transactions', 0, 9999999, res);
        });
        assert.strictEqual(db.doQuery.firstCall.args[1][1], SnapshotBuilder.ROWS_PAGE_MAX, 'limit clamped to ceiling');
    });

    // The decoder pubkeys table's PK (address_id) is NOT monotonic w.r.t. insert
    // order because insertion and address assignment happen at different events, so
    // an address_id cursor would permanently skip late-inserted rows. pubkeys carries
    // a surrogate monotonic AUTO_INCREMENT `id` and must page by it.
    it('decoder: pages the pubkeys table by its surrogate monotonic id cursor', async function(){
        let db = createMockDb('decoder_db');
        db.dbType = 'decoder';
        db.doQuery.resolves([
            { id: 5, address_id: 90, pubkey: 'p1' },
            { id: 9, address_id: 12, pubkey: 'p2' }
        ]);
        let res = new PassThrough();
        let chunks = [];
        res.on('data', c => chunks.push(c));
        res.setHeader = sinon.stub();
        await new Promise((resolve) => {
            res.on('finish', resolve);
            builder.streamTableRowsById(db, 'pubkeys', 0, 50000, res);
        });
        let parsed = JSON.parse(zlib.gunzipSync(Buffer.concat(chunks)).toString());
        assert.strictEqual(parsed.table, 'pubkeys');
        assert.strictEqual(parsed.max_id, 9, 'cursor high-water is the max surrogate id, not address_id');
        let q = db.doQuery.firstCall.args[0];
        assert.ok(/WHERE `id` > \? ORDER BY `id` ASC LIMIT \?/.test(q), 'pages by surrogate id, not address_id');
    });
}

function snapshotBuilderTests(){
    beforeEach(prepareSnapshotBuilder);
    afterEach(restoreSnapshotBuilder);
    describe('streamTableRowsById', streamTableRowsByIdPagingTests);
    describe('streamTableRowsById', streamTableRowsByIdLimitTests);
}

describe('SnapshotBuilder', snapshotBuilderTests);
