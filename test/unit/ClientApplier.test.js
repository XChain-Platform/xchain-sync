// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available —
// contact legal@dankest.llc.

const assert = require('assert');
const sinon  = require('sinon');
const ClientApplier = require('../../src/ClientApplier');
const Utility = require('../../src/utility');
const { SCHEMA_VERSION } = require('../../src/schema-version');

function createMockDb(){
    return {
        doQuery: sinon.stub().resolves([]),
        getBlockHashRow: sinon.stub().resolves(null),
        beginTransaction: sinon.stub().resolves(),
        commitTransaction: sinon.stub().resolves(),
        rollbackTransaction: sinon.stub().resolves(),
        truncateTable: sinon.stub().resolves()
    };
}

describe('ClientApplier', function(){

    let applier, db, util;

    beforeEach(function(){
        db = createMockDb();
        util = new Utility();
        applier = new ClientApplier(db, util);
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });

    afterEach(function(){
        sinon.restore();
    });

    describe('applyBlock', function(){
        it('skips null payload', async function(){
            await applier.applyBlock(null);
            assert.strictEqual(db.beginTransaction.called, false);
        });

        it('skips payload without data', async function(){
            await applier.applyBlock({ block_index: 1 });
            assert.strictEqual(db.beginTransaction.called, false);
        });

        it('skips payload without block_index', async function(){
            await applier.applyBlock({ data: {} });
            assert.strictEqual(db.beginTransaction.called, false);
        });

        it('skips existing block (duplicate detection)', async function(){
            db.getBlockHashRow.resolves({ block_index: 1, ledger_hash: 'abc' });
            await applier.applyBlock({ block_index: 1, data: { blocks: [{ block_index: 1 }] } });
            assert.strictEqual(db.beginTransaction.called, false);
        });

        it('applies block in a transaction', async function(){
            let payload = {
                block_index: 5,
                data: {
                    blocks: [{ block_index: 5, block_time: 100 }],
                    transactions: [{ tx_index: 1, block_index: 5 }]
                }
            };
            await applier.applyBlock(payload);
            assert.strictEqual(db.beginTransaction.calledOnce, true);
            assert.strictEqual(db.commitTransaction.calledOnce, true);
            assert.ok(db.doQuery.called);
        });

        it('rolls back on error', async function(){
            db.doQuery.rejects(new Error('insert fail'));
            let payload = {
                block_index: 5,
                data: { blocks: [{ block_index: 5 }] }
            };
            await assert.rejects(() => applier.applyBlock(payload), { message: 'insert fail' });
            assert.strictEqual(db.rollbackTransaction.calledOnce, true);
        });

        it('skips empty table arrays', async function(){
            let payload = {
                block_index: 5,
                data: { blocks: [], transactions: [{ tx_index: 1 }] }
            };
            await applier.applyBlock(payload);
            // Only 1 INSERT for transactions (blocks is empty)
            assert.strictEqual(db.doQuery.callCount, 1);
        });
    });

    describe('applyFullSnapshot', function(){
        it('skips null snapshot', async function(){
            await applier.applyFullSnapshot(null);
            assert.strictEqual(db.beginTransaction.called, false);
        });

        it('skips snapshot without tables', async function(){
            await applier.applyFullSnapshot({ block_height: 10 });
            assert.strictEqual(db.beginTransaction.called, false);
        });

        it('clears tables in reverse order and inserts in forward order', async function(){
            let snapshot = {
                schema_version: SCHEMA_VERSION.indexer,
                block_height: 10,
                tables: {
                    tableA: [{ id: 1 }],
                    tableB: [{ id: 2 }]
                }
            };
            await applier.applyFullSnapshot(snapshot);
            assert.strictEqual(db.beginTransaction.calledOnce, true);
            // Tables are cleared child-before-parent (reverse of declared order)
            // via DELETE, not TRUNCATE: MariaDB rejects TRUNCATE on FK-referenced
            // tables, so the bootstrap uses FK-safe row-by-row DELETEs.
            let deletes = db.doQuery.getCalls()
                .map(c => c.args[0])
                .filter(q => /^DELETE FROM/.test(q));
            assert.deepStrictEqual(deletes, ['DELETE FROM `tableB`', 'DELETE FROM `tableA`']);
            assert.strictEqual(db.commitTransaction.calledOnce, true);
        });

        it('rolls back on error', async function(){
            db.doQuery.rejects(new Error('truncate fail'));
            let snapshot = { schema_version: SCHEMA_VERSION.indexer, block_height: 10, tables: { t: [{ id: 1 }] } };
            await assert.rejects(() => applier.applyFullSnapshot(snapshot));
            assert.strictEqual(db.rollbackTransaction.calledOnce, true);
        });
    });

    describe('applyIncrementalSnapshot', function(){
        it('inserts rows without truncation', async function(){
            let snapshot = {
                schema_version: SCHEMA_VERSION.indexer,
                block_height: 20,
                since_block: 10,
                tables: { blocks: [{ block_index: 11 }] }
            };
            await applier.applyIncrementalSnapshot(snapshot);
            assert.strictEqual(db.truncateTable.called, false);
            assert.strictEqual(db.beginTransaction.calledOnce, true);
            assert.strictEqual(db.commitTransaction.calledOnce, true);
        });

        it('skips null snapshot', async function(){
            await applier.applyIncrementalSnapshot(null);
            assert.strictEqual(db.beginTransaction.called, false);
        });
    });

    describe('_insertRows', function(){
        it('does nothing for empty rows', async function(){
            await applier._insertRows('blocks', []);
            assert.strictEqual(db.doQuery.called, false);
        });

        it('does nothing for null rows', async function(){
            await applier._insertRows('blocks', null);
            assert.strictEqual(db.doQuery.called, false);
        });

        it('uses INSERT IGNORE for index tables', async function(){
            await applier._insertRows('index_actions', [{ id: 1, name: 'test' }]);
            let query = db.doQuery.firstCall.args[0];
            assert.ok(query.startsWith('INSERT IGNORE'));
        });

        it('uses INSERT for non-index tables', async function(){
            await applier._insertRows('blocks', [{ block_index: 1 }]);
            let query = db.doQuery.firstCall.args[0];
            assert.ok(query.startsWith('INSERT INTO'));
            assert.ok(!query.includes('IGNORE'));
        });

        it('batches inserts in groups of 100', async function(){
            let rows = [];
            for(let i = 0; i < 250; i++) rows.push({ id: i });
            await applier._insertRows('blocks', rows);
            assert.strictEqual(db.doQuery.callCount, 3); // 100 + 100 + 50
        });

        it('handles null column values', async function(){
            await applier._insertRows('blocks', [{ id: 1, name: null }]);
            let args = db.doQuery.firstCall.args[1];
            assert.strictEqual(args[1], null);
        });

        it('handles undefined column values as null', async function(){
            await applier._insertRows('blocks', [{ id: 1, name: undefined }]);
            let args = db.doQuery.firstCall.args[1];
            assert.strictEqual(args[1], null);
        });

        it('backtick-wraps column names', async function(){
            await applier._insertRows('blocks', [{ 'block_index': 1 }]);
            let query = db.doQuery.firstCall.args[0];
            assert.ok(query.includes('`block_index`'));
        });
    });
});
