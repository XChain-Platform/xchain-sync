// Copyright © 2025-2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC - https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

'use strict';

const {
    assert,
    sinon,
    PassThrough,
    Readable,
    zlib,
    SnapshotBuilder,
    Utility,
    createMockDb
} = require('./helpers/support');

function syncMetaRows(from, to){
    return Array.from({ length: to - from + 1 }, (_, offset) => {
        const block = from + offset;
        return {
            id: block,
            block_index: block,
            block_time: 1700000000 + block,
            ledger_hash: 'l' + block,
            actions_hash: 'a' + block,
            contract_hash: 'c' + block,
            logged_at: '2026-11-21 12:00:00'
        };
    });
}

function snapshotResponse(){
    const res = new PassThrough();
    const chunks = [];
    res.on('data', chunk => chunks.push(chunk));
    res.setHeader = sinon.stub();
    return { res, chunks };
}

async function collectSnapshot(run){
    const { res, chunks } = snapshotResponse();
    await new Promise((resolve, reject) => {
        res.on('finish', resolve);
        run(res).catch(reject);
    });
    return JSON.parse(zlib.gunzipSync(Buffer.concat(chunks)).toString());
}

function prepareDb(){
    const db = createMockDb();
    db.dbType = 'indexer';
    db.getLastBlock.resolves(110);
    db.getBlockHashRow.resolves({ ledger_hash: 'l110', actions_hash: 'a110', contract_hash: 'c110' });
    sinon.stub(db, 'getSyncMetaTip').resolves([{ tip: 104 }]);
    db.getBlockHashRowsInRange = sinon.stub().resolves(syncMetaRows(105, 110));
    return db;
}

let builder;

// Give each describe a fresh builder and a silenced console.
function useBuilder(){
    beforeEach(function () {
        builder = new SnapshotBuilder(new Utility());
        sinon.stub(console, 'error');
    });

    afterEach(function () {
        sinon.restore();
    });
}

describe('SnapshotBuilder sync_meta tail parity', function () {
    useBuilder();

    it('reads the high-water mark through the pinned snapshot connection', async function () {
        const db = createMockDb();
        const conn = { pinned: true };
        db.doQuery.resolves([{ tip: 104 }]);

        const rows = await db.getSyncMetaTip(conn);

        assert.deepStrictEqual(rows, [{ tip: 104 }]);
        assert.strictEqual(db.doQuery.calledWith(
            'SELECT MAX(block_index) AS tip FROM sync_meta', null, conn), true);
    });

    it('builds synthetic rows with the same local-column shape as SELECT star', async function () {
        const db = createMockDb();
        const conn = { pinned: true };
        db.doQuery.resolves(syncMetaRows(105, 110));

        await db.getBlockHashRowsInRange(105, 110, conn);

        const [sql, args, usedConn] = db.doQuery.firstCall.args;
        assert.match(sql, /NULL as id/);
        assert.match(sql, /CURRENT_TIMESTAMP as logged_at/);
        assert.deepStrictEqual(args, [105, 110]);
        assert.strictEqual(usedConn, conn);
    });

    it('fills a full snapshot through its advertised block height', async function () {
        const db = prepareDb();
        db.doQuery.resolves([{ table_name: 'sync_meta' }]);
        db.getTableCount.resolves(5);
        db.streamTableRows.returns(Readable.from(syncMetaRows(100, 104)));

        const body = await collectSnapshot(res => builder.streamFullSnapshot(db, res));

        assert.deepStrictEqual(body.tables.sync_meta.map(row => row.block_index),
            [100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110]);
        assert.strictEqual(body.tables.sync_meta.at(-1).ledger_hash, 'l110');
    });

});

describe('SnapshotBuilder sync_meta tail parity on incremental and edge snapshots', function () {
    useBuilder();

    it('fills an incremental snapshot through its advertised block height', async function () {
        const db = prepareDb();
        db.getFirstActionIndex.resolves(null);
        db.doQuery.resolves([{ table_name: 'sync_meta' }]);
        sinon.stub(db, 'streamRowsFromBlockKey').returns(Readable.from(syncMetaRows(100, 104)));

        const body = await collectSnapshot(res => builder.streamIncrementalSnapshot(db, 100, res, 'BTC'));

        assert.deepStrictEqual(body.tables.sync_meta.map(row => row.block_index),
            [100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110]);
        assert.strictEqual(body.tables.sync_meta.at(-1).contract_hash, 'c110');
    });

    it('does not invent rows when the transparency log has no high-water mark', async function () {
        const db = prepareDb();
        db.getSyncMetaTip.resolves([{ tip: null }]);
        db.getFirstActionIndex.resolves(null);
        db.doQuery.resolves([{ table_name: 'sync_meta' }]);
        sinon.stub(db, 'streamRowsFromBlockKey').returns(Readable.from([]));

        const body = await collectSnapshot(res => builder.streamIncrementalSnapshot(db, 100, res, 'BTC'));

        assert.strictEqual(body.tables.sync_meta, undefined);
        assert.strictEqual(db.getBlockHashRowsInRange.called, false);
    });

    it('does not query a synthetic tail when sync_meta already reaches the tip', async function () {
        const db = prepareDb();
        db.getSyncMetaTip.resolves([{ tip: 110 }]);
        db.getFirstActionIndex.resolves(null);
        db.doQuery.resolves([{ table_name: 'sync_meta' }]);
        sinon.stub(db, 'streamRowsFromBlockKey').returns(Readable.from(syncMetaRows(100, 110)));

        const body = await collectSnapshot(res => builder.streamIncrementalSnapshot(db, 100, res, 'BTC'));

        assert.strictEqual(body.tables.sync_meta.length, 11);
        assert.strictEqual(db.getBlockHashRowsInRange.called, false);
    });

    it('leaves decoder snapshots unchanged', async function () {
        const db = prepareDb();
        db.dbType = 'decoder';
        db.getBlockHashRow.resolves({ block_hash: 'b110' });
        db.doQuery.resolves([]);

        const body = await collectSnapshot(res => builder.streamFullSnapshot(db, res));

        assert.deepStrictEqual(body.tables, {});
        assert.strictEqual(db.getSyncMetaTip.called, false);
    });
});
