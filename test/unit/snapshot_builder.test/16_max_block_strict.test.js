// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

const { assert, sinon, zlib, SnapshotBuilder, Utility, createMockDb, createMockRes } = require('./helpers/support');
const { PassThrough } = require('stream');

describe('streamTableRowsById max_block is strict', function(){
    let builder;
    beforeEach(function(){ builder = new SnapshotBuilder(new Utility()); sinon.stub(console, 'error'); });
    afterEach(function(){ sinon.restore(); });

    const rowsAbove = [{ id: 1, block_index: 470 }, { id: 2, block_index: 471 }, { id: 3, block_index: 472 }];

    for(const bad of ['-1', '', '12abc', '1e3', ' 5', '0x10', ['1', '2'], {}]){
        it('rejects max_block ' + JSON.stringify(bad) + ' with 400 and runs no SQL', async function(){
            let db = createMockDb();
            db.doQuery.resolves(rowsAbove);
            let res = createMockRes();
            res.req = { query: { max_block: bad } };
            await builder.streamTableRowsById(db, 'index_addresses', 0, 3, res);
            assert.ok(res.status.calledWith(400));
            assert.strictEqual(db.doQuery.callCount, 0);
            assert.strictEqual(res._chunks.length, 0);
        });
    }

    async function page(maxBlock){
        let db = createMockDb();
        db.doQuery.resolves(rowsAbove);
        let res = new PassThrough();
        let chunks = [];
        res.on('data', c => chunks.push(c));
        res.setHeader = sinon.stub();
        res.req = { query: maxBlock === undefined ? {} : { max_block: maxBlock } };
        await new Promise((resolve) => {
            res.on('finish', resolve);
            builder.streamTableRowsById(db, 'index_addresses', 0, 3, res);
        });
        return JSON.parse(zlib.gunzipSync(Buffer.concat(chunks)).toString());
    }

    it('applies the cut for 0 and 470', async function(){
        assert.strictEqual((await page('0')).rows.length, 0);
        assert.strictEqual((await page('470')).rows.length, 1);
    });

    it('serves the whole page when max_block is absent', async function(){
        assert.strictEqual((await page(undefined)).rows.length, 3);
    });
});
