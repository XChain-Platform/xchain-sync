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

    // ── _insertRows — table name validation ──

    describe('_insertRows — table name validation', function(){

        it('allows valid table name', async function(){
            await applier._insertRows('blocks', [{ block_index: 1, block_time: 100 }]);
            assert.strictEqual(db.doQuery.called, true);
        });

        it('rejects table name with semicolon', async function(){
            await applier._insertRows('blocks;DROP TABLE blocks', [{ id: 1 }]);
            assert.strictEqual(db.doQuery.called, false);
            assert.strictEqual(console.error.calledOnce, true);
            assert.ok(console.error.firstCall.args[0].includes('Rejected table name'));
        });

        it('rejects table name with backtick', async function(){
            await applier._insertRows('blo`cks', [{ id: 1 }]);
            assert.strictEqual(db.doQuery.called, false);
        });

        it('rejects empty table name', async function(){
            await applier._insertRows('', [{ id: 1 }]);
            assert.strictEqual(db.doQuery.called, false);
        });

        it('rejects table name with SQL comment', async function(){
            await applier._insertRows('blocks--', [{ id: 1 }]);
            assert.strictEqual(db.doQuery.called, false);
        });

        it('rejects table name with space', async function(){
            await applier._insertRows('blo cks', [{ id: 1 }]);
            assert.strictEqual(db.doQuery.called, false);
        });

        it('rejects table name with dot notation', async function(){
            await applier._insertRows('mysql.user', [{ id: 1 }]);
            assert.strictEqual(db.doQuery.called, false);
        });

        it('rejects table name with path traversal', async function(){
            await applier._insertRows('../etc', [{ id: 1 }]);
            assert.strictEqual(db.doQuery.called, false);
        });
    });

    // ── _insertRows — column name validation ──

    describe('_insertRows — column name validation', function(){

        it('allows valid column names', async function(){
            await applier._insertRows('blocks', [{ block_index: 1, block_time: 100 }]);
            assert.strictEqual(db.doQuery.called, true);
        });

        it('rejects column name with backtick', async function(){
            let row = {};
            row['col`'] = 1;
            await applier._insertRows('blocks', [row]);
            assert.strictEqual(db.doQuery.called, false);
            assert.strictEqual(console.error.calledOnce, true);
            assert.ok(console.error.firstCall.args[0].includes('Rejected column name'));
        });

        it('rejects column name with semicolon', async function(){
            let row = {};
            row['col;drop'] = 1;
            await applier._insertRows('blocks', [row]);
            assert.strictEqual(db.doQuery.called, false);
        });

        it('rejects empty column name', async function(){
            let row = {};
            row[''] = 1;
            await applier._insertRows('blocks', [row]);
            assert.strictEqual(db.doQuery.called, false);
        });

        it('rejects column name with space', async function(){
            let row = {};
            row['col name'] = 1;
            await applier._insertRows('blocks', [row]);
            assert.strictEqual(db.doQuery.called, false);
        });
    });

    // ── applyFullSnapshot — table name validation on truncate ──

    describe('applyFullSnapshot — table name validation', function(){

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

        it('skips truncate for invalid table name', async function(){
            let snapshotData = {
                schema_version: SCHEMA_VERSION.indexer,
                block_height: 100,
                tables: {
                    'evil;DROP TABLE': [{ id: 1 }],
                    blocks: [{ block_index: 1 }]
                }
            };
            await applier.applyFullSnapshot(snapshotData);
            // evil table should NOT be cleared — no DELETE issued referencing it
            for(let call of db.doQuery.getCalls()){
                assert.strictEqual(call.args[0].includes('evil'), false);
            }
            assert.strictEqual(console.error.called, true);
        });

        it('skips truncate for path traversal table name', async function(){
            let snapshotData = {
                schema_version: SCHEMA_VERSION.indexer,
                block_height: 100,
                tables: {
                    '../etc': [{ id: 1 }]
                }
            };
            await applier.applyFullSnapshot(snapshotData);
            // path-traversal name must never reach a DELETE
            for(let call of db.doQuery.getCalls()){
                assert.strictEqual(call.args[0].includes('../etc'), false);
            }
        });

        it('valid tables not affected by one invalid table in same snapshot', async function(){
            let snapshotData = {
                schema_version: SCHEMA_VERSION.indexer,
                block_height: 100,
                tables: {
                    blocks: [{ block_index: 1 }],
                    'evil;DROP': [{ id: 1 }],
                    transactions: [{ tx_index: 1 }]
                }
            };
            await applier.applyFullSnapshot(snapshotData);
            assert.strictEqual(db.doQuery.calledWith('DELETE FROM `blocks`'), true);
            assert.strictEqual(db.doQuery.calledWith('DELETE FROM `transactions`'), true);
        });
    });

    // ── applyBlock — table name validation in data object ──

    describe('applyBlock — data key validation', function(){

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

        it('skips data with invalid table name key', async function(){
            db.getBlockHashRow.resolves(null);
            let payload = {
                block_index: 100,
                data: {
                    'evil;DROP TABLE blocks': [{ id: 1 }]
                }
            };
            await applier.applyBlock(payload);
            // doQuery should only be called for beginTransaction/commitTransaction, not for insert
            // The invalid table _insertRows should return early without calling doQuery
            let insertCalls = db.doQuery.getCalls().filter(c => {
                let q = c.args[0];
                return typeof q === 'string' && q.includes('INSERT');
            });
            assert.strictEqual(insertCalls.length, 0);
        });
    });
});
