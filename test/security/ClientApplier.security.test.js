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
const proxyquire = require('proxyquire').noCallThru();

// Use proxyquire to inject validation into ClientApplier
const ClientApplier = proxyquire('../../src/ClientApplier', {
    './validation': require('../../src/validation')
});
const { SCHEMA_VERSION } = require('../../src/schema-version');

function createMockDb(){
    return {
        dbName: 'test_db',
        doQuery: sinon.stub().resolves([]),
        truncateTable: sinon.stub().resolves(),
        getBlockHashRow: sinon.stub().resolves(null),
        beginTransaction: sinon.stub().resolves(),
        commitTransaction: sinon.stub().resolves(true),
        rollbackTransaction: sinon.stub().resolves()
    };
}

function createMockUtil(){
    return {
        startTimer: sinon.stub().returns(Date.now()),
        getTimer: sinon.stub().returns('0ms')
    };
}

// Malformed identifiers FAIL CLOSED: _insertRows/_upsertRows throw so the apply
// transaction rolls back and the block/snapshot is retried or the client halts.
// (A silent skip would drop the table's rows while the transaction still commits,
// leaving the replica permanently short with no divergence signal.)
describe('ClientApplier security', function(){

    let db, util, applier;

    beforeEach(function(){
        db = createMockDb();
        util = createMockUtil();
        applier = new ClientApplier(db, util);
        sinon.stub(console, 'error');
        sinon.stub(console, 'log');
    });

    afterEach(function(){
        sinon.restore();
    });

    // ── _insertRows: table name validation ──

    describe('_insertRows: table name validation', function(){

        it('allows valid table name', async function(){
            await applier._insertRows('blocks', [{ block_index: 1, block_time: 100 }]);
            assert.strictEqual(db.doQuery.called, true);
        });

        async function assertInsertRejectsTable(table){
            await assert.rejects(
                () => applier._insertRows(table, [{ id: 1 }]),
                /Rejected table name/
            );
            assert.strictEqual(db.doQuery.called, false);
        }

        it('rejects table name with semicolon', async function(){
            await assertInsertRejectsTable('blocks;DROP TABLE blocks');
        });

        it('rejects table name with backtick', async function(){
            await assertInsertRejectsTable('blo`cks');
        });

        it('rejects empty table name', async function(){
            await assertInsertRejectsTable('');
        });

        it('rejects table name with SQL comment', async function(){
            await assertInsertRejectsTable('blocks--');
        });

        it('rejects table name with space', async function(){
            await assertInsertRejectsTable('blo cks');
        });

        it('rejects table name with dot notation', async function(){
            await assertInsertRejectsTable('mysql.user');
        });

        it('rejects table name with path traversal', async function(){
            await assertInsertRejectsTable('../etc');
        });
    });

    // ── _insertRows: column name validation ──

    describe('_insertRows: column name validation', function(){

        it('allows valid column names', async function(){
            await applier._insertRows('blocks', [{ block_index: 1, block_time: 100 }]);
            assert.strictEqual(db.doQuery.called, true);
        });

        async function assertInsertRejectsColumn(col){
            let row = {};
            row[col] = 1;
            await assert.rejects(
                () => applier._insertRows('blocks', [row]),
                /Rejected column name/
            );
            assert.strictEqual(db.doQuery.called, false);
        }

        it('rejects column name with backtick', async function(){
            await assertInsertRejectsColumn('col`');
        });

        it('rejects column name with semicolon', async function(){
            await assertInsertRejectsColumn('col;drop');
        });

        it('rejects empty column name', async function(){
            await assertInsertRejectsColumn('');
        });

        it('rejects column name with space', async function(){
            await assertInsertRejectsColumn('col name');
        });
    });

    // ── applyFullSnapshot: table name validation on truncate ──

    describe('applyFullSnapshot: table name validation', function(){

        it('truncates valid table names', async function(){
            let snapshotData = {
                schema_version: SCHEMA_VERSION.indexer,
                block_height: 100,
                tables: {
                    blocks: [{ block_index: 1 }],
                    transactions: [{ tx_index: 1 }]
                }
            };
            await applier.applyFullSnapshot(snapshotData);
            assert.strictEqual(db.doQuery.calledWith('DELETE FROM `blocks`'), true);
            assert.strictEqual(db.doQuery.calledWith('DELETE FROM `transactions`'), true);
        });

        it('rejects a snapshot carrying an invalid table name (fail closed, transaction rolled back)', async function(){
            let snapshotData = {
                schema_version: SCHEMA_VERSION.indexer,
                block_height: 100,
                tables: {
                    'evil;DROP TABLE': [{ id: 1 }],
                    blocks: [{ block_index: 1 }]
                }
            };
            await assert.rejects(() => applier.applyFullSnapshot(snapshotData), /Rejected table name/);
            // the malicious name must never reach a query
            for(let call of db.doQuery.getCalls()){
                assert.strictEqual(call.args[0].includes('evil'), false);
            }
            assert.strictEqual(db.rollbackTransaction.called, true);
            assert.strictEqual(db.commitTransaction.called, false);
        });

        it('rejects a snapshot carrying a path traversal table name (fail closed)', async function(){
            let snapshotData = {
                schema_version: SCHEMA_VERSION.indexer,
                block_height: 100,
                tables: {
                    '../etc': [{ id: 1 }]
                }
            };
            await assert.rejects(() => applier.applyFullSnapshot(snapshotData), /Rejected table name/);
            // path-traversal name must never reach a query
            for(let call of db.doQuery.getCalls()){
                assert.strictEqual(call.args[0].includes('../etc'), false);
            }
            assert.strictEqual(db.rollbackTransaction.called, true);
        });

        it('one invalid table poisons the whole snapshot: nothing commits', async function(){
            let snapshotData = {
                schema_version: SCHEMA_VERSION.indexer,
                block_height: 100,
                tables: {
                    blocks: [{ block_index: 1 }],
                    'evil;DROP': [{ id: 1 }],
                    transactions: [{ tx_index: 1 }]
                }
            };
            await assert.rejects(() => applier.applyFullSnapshot(snapshotData), /Rejected table name/);
            assert.strictEqual(db.rollbackTransaction.called, true);
            assert.strictEqual(db.commitTransaction.called, false);
        });
    });

    // ── applyBlock: table name validation in data object ──

    describe('applyBlock: data key validation', function(){

        it('applies data with valid table name', async function(){
            db.getBlockHashRow.resolves(null);
            let payload = {
                block_index: 100,
                data: {
                    blocks: [{ block_index: 100, block_time: 1000 }]
                }
            };
            await applier.applyBlock(payload);
            assert.strictEqual(db.doQuery.called, true);
        });

        it('rejects a block carrying an invalid table name key (fail closed, transaction rolled back)', async function(){
            db.getBlockHashRow.resolves(null);
            let payload = {
                block_index: 100,
                data: {
                    'evil;DROP TABLE blocks': [{ id: 1 }]
                }
            };
            await assert.rejects(() => applier.applyBlock(payload), /Rejected table name/);
            // no INSERT may be issued for the malicious key
            let insertCalls = db.doQuery.getCalls().filter(c => {
                let q = c.args[0];
                return typeof q === 'string' && q.includes('INSERT');
            });
            assert.strictEqual(insertCalls.length, 0);
            assert.strictEqual(db.rollbackTransaction.called, true);
            assert.strictEqual(db.commitTransaction.called, false);
        });
    });
});
