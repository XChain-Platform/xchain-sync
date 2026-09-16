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

function streamFullSnapshotStatusTests(){
    it('returns 404 when no blocks in database', async function(){
        let db = createMockDb();
        db.getLastBlock.resolves(null);
        let res = createMockRes();
        await builder.streamFullSnapshot(db, res);
        assert.strictEqual(res.status.calledWith(404), true);
        assert.strictEqual(res.json.calledOnce, true);
    });

    it('sets correct response headers', async function(){
        let db = createMockDb();
        db.getLastBlock.resolves(100);
        db.getBlockHashRow.resolves({ ledger_hash: 'lh', actions_hash: 'ah', contract_hash: 'ch' });
        db.doQuery.resolves([]); // no tables

        let res = new PassThrough();
        let setHeaderStub = sinon.stub();
        res.setHeader = setHeaderStub;

        await new Promise((resolve) => {
            res.on('finish', resolve);
            builder.streamFullSnapshot(db, res);
        });

        assert.ok(setHeaderStub.calledWith('Content-Type', 'application/json'));
        assert.ok(setHeaderStub.calledWith('Content-Encoding', 'gzip'));
        assert.ok(setHeaderStub.calledWith('X-Block-Height', 100));
        assert.ok(setHeaderStub.calledWith('X-Ledger-Hash', 'lh'));
        assert.ok(setHeaderStub.calledWith('X-Actions-Hash', 'ah'));
        assert.ok(setHeaderStub.calledWith('X-Contract-Hash', 'ch'));
    });
}

function streamFullSnapshotPayloadTests(){
    it('streams valid gzip JSON for tables with data', async function(){
        let db = createMockDb();
        db.getLastBlock.resolves(50);
        db.getBlockHashRow.resolves({ ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c' });
        db.doQuery.resolves([{ table_name: 'blocks' }]);
        db.getTableCount.resolves(1);
        db.streamTableRows.callsFake(() => Readable.from([{ block_index: 50, block_time: 100 }]));

        // Collect the gzipped output
        let res = new PassThrough();
        let chunks = [];
        res.on('data', c => chunks.push(c));
        res.setHeader = sinon.stub();

        await new Promise((resolve) => {
            res.on('finish', resolve);
            builder.streamFullSnapshot(db, res);
        });

        let compressed = Buffer.concat(chunks);
        let json = zlib.gunzipSync(compressed).toString();
        let parsed = JSON.parse(json);

        assert.strictEqual(parsed.block_height, 50);
        assert.ok(parsed.tables);
        assert.ok(parsed.tables.blocks);
        assert.strictEqual(parsed.tables.blocks.length, 1);
        assert.strictEqual(parsed.tables.blocks[0].block_index, 50);
    });

    it('skips tables with 0 rows', async function(){
        let db = createMockDb();
        db.getLastBlock.resolves(10);
        db.getBlockHashRow.resolves({ ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c' });
        db.doQuery.resolves([{ table_name: 'blocks' }, { table_name: 'empty_table' }]);
        db.getTableCount.withArgs('blocks').resolves(1);
        db.getTableCount.withArgs('empty_table').resolves(0);
        db.streamTableRows.callsFake(() => Readable.from([{ block_index: 10 }]));

        let res = new PassThrough();
        let chunks = [];
        res.on('data', c => chunks.push(c));
        res.setHeader = sinon.stub();

        await new Promise((resolve) => {
            res.on('finish', resolve);
            builder.streamFullSnapshot(db, res);
        });

        let json = zlib.gunzipSync(Buffer.concat(chunks)).toString();
        let parsed = JSON.parse(json);
        assert.ok(parsed.tables.blocks);
        assert.strictEqual(parsed.tables.empty_table, undefined);
    });
}

function streamFullSnapshotOrderingTests(){
    it('emits every row of a keyless table exactly once in a single ordered pass (no offset re-paging)', async function(){
        // Regression for the `ORDER BY 1 LIMIT ? OFFSET ?` pagination: keyless
        // ledger tables (credits/debits/...) have a non-unique first column, so
        // offset re-paging had no total order and a page-boundary tie could be
        // emitted twice or skipped. The table here far exceeds pageSize; the
        // builder must read it in ONE streaming pass.
        let db = createMockDb();
        builder.pageSize = 2; // the old scheme would have re-queried 3 offset pages
        db.getLastBlock.resolves(50);
        db.getBlockHashRow.resolves({ ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c' });
        db.doQuery.resolves([{ table_name: 'credits' }]);
        // Five rows sharing one action_index (a match credits buyer+seller+fee...).
        let rows = [0, 1, 2, 3, 4].map(i => ({ action_index: 7, address_id: i, amount: String(i) }));
        db.getTableCount.resolves(rows.length);
        db.streamTableRows.callsFake(() => Readable.from(rows));

        let res = new PassThrough();
        let chunks = [];
        res.on('data', c => chunks.push(c));
        res.setHeader = sinon.stub();

        await new Promise((resolve) => {
            res.on('finish', resolve);
            builder.streamFullSnapshot(db, res);
        });

        let parsed = JSON.parse(zlib.gunzipSync(Buffer.concat(chunks)).toString());
        assert.deepStrictEqual(parsed.tables.credits.map(r => r.address_id), [0, 1, 2, 3, 4],
            'every row exactly once, in stream order (no duplicate, no skip)');
        assert.strictEqual(db.streamTableRows.callCount, 1,
            'a table is read in one single-pass stream, not per-offset pages');
    });
}

function snapshotBuilderTests(){
    beforeEach(prepareSnapshotBuilder);
    afterEach(restoreSnapshotBuilder);
    describe('streamFullSnapshot', streamFullSnapshotStatusTests);
    describe('streamFullSnapshot', streamFullSnapshotPayloadTests);
    describe('streamFullSnapshot', streamFullSnapshotOrderingTests);
}

describe('SnapshotBuilder', snapshotBuilderTests);
