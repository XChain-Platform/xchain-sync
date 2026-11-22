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
    assert, sinon, PassThrough, Readable, zlib,
    SnapshotBuilder, Utility, createMockDb, createMockRes
} = require('./helpers/support');

function streamBody(builder, db, table){
    let chunks = [];
    let res = new PassThrough();
    res.on('data', chunk => chunks.push(chunk));
    res.setHeader = sinon.stub();
    return new Promise((resolve, reject) => {
        res.on('finish', () => resolve(JSON.parse(zlib.gunzipSync(Buffer.concat(chunks)).toString())));
        builder.streamRepairTable(db, table, res).catch(reject);
    });
}

let builder;

function registerStreamRepairTableTests(){
    it('rejects non-repairable, unsafe, and decoder table requests before SQL @regression', async function(){
        for(const table of ['blocks', 'index_transactions', 'mappings_actions;DROP TABLE blocks']){
            let db = createMockDb();
            let res = createMockRes();
            await builder.streamRepairTable(db, table, res);
            assert.ok(res.status.calledWith(400), table);
            assert.strictEqual(res.json.firstCall.args[0].code, 'REPAIR_TABLE_NOT_ALLOWED');
            assert.strictEqual(db.beginReadSnapshot.called, false);
        }
        let decoder = createMockDb();
        decoder.dbType = 'decoder';
        await builder.streamRepairTable(decoder, 'mappings_actions', createMockRes());
        assert.strictEqual(decoder.beginReadSnapshot.called, false);
    });

    it('streams one allowlisted table from the same read snapshot as its height @regression', async function(){
        let db = createMockDb();
        db.dbType = 'indexer';
        db.getLastBlock.resolves(100);
        db.streamTableRows.callsFake(() => Readable.from([
            { action_index: 7, payload: Buffer.from('binary') }
        ]));

        let body = await streamBody(builder, db, 'mappings_actions');
        let conn = await db.beginReadSnapshot.firstCall.returnValue;
        assert.strictEqual(body.block_height, 100);
        assert.strictEqual(body.table, 'mappings_actions');
        assert.deepStrictEqual(body.rows[0].payload, { __xbin__: Buffer.from('binary').toString('base64') });
        assert.strictEqual(db.getLastBlock.firstCall.args[0], conn);
        assert.strictEqual(db.streamTableRows.firstCall.args[1], conn);
        assert.strictEqual(db.commitReadSnapshot.calledOnceWith(conn), true);
        assert.strictEqual(db.rollbackReadSnapshot.called, false);
        assert.strictEqual(builder._inflightSnapshots.has(db), false, 'snapshot slot released');
    });

    it('rolls back and releases the snapshot slot when the authoritative read fails', async function(){
        let db = createMockDb();
        db.dbType = 'indexer';
        db.getLastBlock.resolves(100);
        db.streamTableRows.callsFake(() => Readable.from((async function*(){
            throw new Error('read failed');
        })()));
        let res = new PassThrough();
        res.on('data', () => {});
        res.setHeader = sinon.stub();

        await assert.rejects(builder.streamRepairTable(db, 'mappings_actions', res), /read failed/);
        assert.strictEqual(db.rollbackReadSnapshot.calledOnce, true);
        assert.strictEqual(db.commitReadSnapshot.called, false);
        assert.strictEqual(builder._inflightSnapshots.has(db), false, 'snapshot slot released');
    });
}

describe('SnapshotBuilder streamRepairTable', function(){
    beforeEach(function(){
        builder = new SnapshotBuilder(new Utility());
    });

    afterEach(function(){
        sinon.restore();
    });

    registerStreamRepairTableTests();
});
