/*********************************************************************
 *
 * Copyright © 2025–2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC – https://dankest.llc
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This file is part of XChain Platform. Licensed under the GNU Affero
 * General Public License v3.0 or later; see LICENSE.md. A commercial
 * license (without AGPL source-disclosure terms) is available -
 * contact legal@dankest.llc.
 *
 **********************************************************************
 * Tier 1 Fuzz: ClientApplier
 *
 * Tests that applyBlock, applyFullSnapshot, applyIncrementalSnapshot, and
 * _insertRows never crash for any generated input. Also verifies transaction
 * safety (commit XOR rollback), INSERT IGNORE correctness, and batching.
 */

const assert = require('assert');
const fc = require('fast-check');
const sinon = require('sinon');
const { NUM_RUNS, createMockDb, createMockUtil } = require('../setup/harness');
const {
    blockPayload, partialBlockPayload,
    fullSnapshotPayload, partialSnapshotPayload,
} = require('../generators/payloads');
const { genericDataRow, rowArray, mixedRows } = require('../generators/rows');
const { SCHEMA_VERSION } = require('../../../src/schema-version');

let crashLog = [];

describe('Tier 1 - ClientApplier @tier1', function () {
    this.timeout(0);
    let ClientApplier, applier, db, util;

    before(function () {
        ClientApplier = require('../../../src/ClientApplier');
    });

    beforeEach(function () {
        db = createMockDb();
        util = createMockUtil();
        applier = new ClientApplier(db, util);
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });

    afterEach(function () {
        sinon.restore();
    });

    async function runAndLog(fn, input) {
        try {
            await fn();
        } catch (err) {
            crashLog.push({ input: JSON.stringify(input).substring(0, 200), error: err.message });
            throw err;
        }
    }

    describe('applyBlock', function () {

        it('never throws for any payload shape', function () {
            return fc.assert(fc.asyncProperty(
                partialBlockPayload(),
                async (payload) => {
                    db.doQuery.resolves([]);
                    db.getBlockHashRow.resolves(null);
                    db.beginTransaction.resolves();
                    db.commitTransaction.resolves();
                    db.rollbackTransaction.resolves();
                    await applier.applyBlock(payload);
                }
            ), { numRuns: NUM_RUNS });
        });

        it('when block already exists, no transaction is opened', function () {
            return fc.assert(fc.asyncProperty(
                blockPayload(),
                async (payload) => {
                    db.getBlockHashRow.resolves({ block_index: payload.block_index, ledger_hash: 'x' });
                    db.beginTransaction.resetHistory();
                    await applier.applyBlock(payload);
                    assert.strictEqual(db.beginTransaction.callCount, 0);
                }
            ), { numRuns: NUM_RUNS });
        });

        it('transaction is always committed or rolled back, never leaked', function () {
            return fc.assert(fc.asyncProperty(
                blockPayload(),
                fc.boolean(),
                async (payload, shouldFail) => {
                    db.getBlockHashRow.resolves(null);
                    db.beginTransaction.resetHistory();
                    db.commitTransaction.resetHistory();
                    db.rollbackTransaction.resetHistory();

                    if (shouldFail) {
                        db.doQuery.rejects(new Error('fuzz-inject'));
                    } else {
                        db.doQuery.resolves([]);
                    }

                    try {
                        await applier.applyBlock(payload);
                    } catch (e) {
                        // expected when shouldFail
                    }

                    let began = db.beginTransaction.callCount;
                    let committed = db.commitTransaction.callCount;
                    let rolledBack = db.rollbackTransaction.callCount;

                    if (began > 0) {
                        assert.strictEqual(committed + rolledBack, began,
                            'Transaction leaked: began=' + began + ' committed=' + committed + ' rolledBack=' + rolledBack);
                    }
                }
            ), { numRuns: NUM_RUNS });
        });
    });

    describe('applyFullSnapshot', function () {

        it('never throws for any snapshot shape', function () {
            return fc.assert(fc.asyncProperty(
                partialSnapshotPayload(),
                async (snapshot) => {
                    await applier.applyFullSnapshot(snapshot);
                }
            ), { numRuns: NUM_RUNS });
        });

        it('clears tables in reverse order via DELETE (FK-safe)', function () {
            // applyFullSnapshot clears via `DELETE FROM` (not TRUNCATE) for FK
            // compatibility, iterating the table keys in reverse so child tables are
            // cleared before their parents.
            return fc.assert(fc.asyncProperty(
                fc.array(
                    fc.string({ unit: fc.constantFrom(...'abcdefghijklmnopqrstuvwxyz'.split('')), minLength: 1, maxLength: 15 }),
                    { minLength: 2, maxLength: 8 }
                ).filter(names => new Set(names).size === names.length),
                async (tableNames) => {
                    let snapshot = {
                        schema_version: SCHEMA_VERSION.indexer,
                        block_height: 100,
                        tables: Object.fromEntries(tableNames.map(n => [n, [{ id: 1 }]])),
                    };

                    db.doQuery.resetHistory();
                    await applier.applyFullSnapshot(snapshot);

                    let deleteOrder = [];
                    for (let i = 0; i < db.doQuery.callCount; i++) {
                        let sql = db.doQuery.getCall(i).args[0];
                        let m = typeof sql === 'string' && sql.match(/^DELETE FROM `(.+)`$/);
                        if (m) deleteOrder.push(m[1]);
                    }

                    let expectedOrder = [...tableNames].reverse();
                    assert.deepStrictEqual(deleteOrder, expectedOrder);
                }
            ), { numRuns: Math.min(NUM_RUNS, 500) });
        });
    });

    describe('applyIncrementalSnapshot', function () {

        it('never throws for any snapshot shape', function () {
            return fc.assert(fc.asyncProperty(
                partialSnapshotPayload(),
                async (snapshot) => {
                    await applier.applyIncrementalSnapshot(snapshot);
                }
            ), { numRuns: NUM_RUNS });
        });

        it('never calls truncateTable', function () {
            return fc.assert(fc.asyncProperty(
                fullSnapshotPayload(),
                async (snapshot) => {
                    snapshot.since_block = 1;
                    db.truncateTable.resetHistory();
                    await applier.applyIncrementalSnapshot(snapshot);
                    assert.strictEqual(db.truncateTable.callCount, 0);
                }
            ), { numRuns: NUM_RUNS });
        });
    });

    describe('_insertRows', function () {

        it('never throws for any table name and row array', function () {
            return fc.assert(fc.asyncProperty(
                fc.string({ unit: fc.constantFrom(...'abcdefghijklmnopqrstuvwxyz_'.split('')), minLength: 1, maxLength: 30 }),
                mixedRows(),
                async (table, rows) => {
                    await applier._insertRows(table, rows);
                }
            ), { numRuns: NUM_RUNS });
        });

        it('uses INSERT IGNORE for all index tables', function () {
            let indexTables = [
                'index_actions', 'index_addresses', 'index_coins', 'index_fiats',
                'index_memos', 'index_mime_types', 'index_pubkeys', 'index_statuses',
                'index_tickers', 'index_transactions',
            ];
            return fc.assert(fc.asyncProperty(
                fc.constantFrom(...indexTables),
                rowArray(genericDataRow(), 1, 3),
                async (table, rows) => {
                    db.doQuery.resetHistory();
                    await applier._insertRows(table, rows);
                    if (db.doQuery.callCount > 0) {
                        let query = db.doQuery.firstCall.args[0];
                        assert.ok(query.startsWith('INSERT IGNORE'),
                            'Expected INSERT IGNORE for ' + table + ', got: ' + query.substring(0, 30));
                    }
                }
            ), { numRuns: Math.min(NUM_RUNS, 200) });
        });

        it('uses plain INSERT for non-index tables', function () {
            return fc.assert(fc.asyncProperty(
                fc.constantFrom('blocks', 'transactions', 'actions', 'credits', 'sends', 'orders'),
                rowArray(genericDataRow(), 1, 3),
                async (table, rows) => {
                    db.doQuery.resetHistory();
                    await applier._insertRows(table, rows);
                    if (db.doQuery.callCount > 0) {
                        let query = db.doQuery.firstCall.args[0];
                        assert.ok(query.startsWith('INSERT INTO'),
                            'Expected INSERT INTO for ' + table + ', got: ' + query.substring(0, 30));
                        assert.ok(!query.includes('IGNORE'),
                            'Unexpected IGNORE for non-index table ' + table);
                    }
                }
            ), { numRuns: Math.min(NUM_RUNS, 200) });
        });

        it('batches inserts in groups of 100', function () {
            return fc.assert(fc.asyncProperty(
                fc.integer({ min: 101, max: 350 }),
                async (count) => {
                    let rows = [];
                    for (let i = 0; i < count; i++) rows.push({ id: i });
                    db.doQuery.resetHistory();
                    await applier._insertRows('test_table', rows);
                    assert.strictEqual(db.doQuery.callCount, Math.ceil(count / 100));
                }
            ), { numRuns: 50 });
        });

        it('null and undefined column values become null in query args', function () {
            return fc.assert(fc.asyncProperty(
                rowArray(
                    fc.record({ id: fc.integer(), val: fc.constantFrom(null, undefined) }),
                    1, 5
                ),
                async (rows) => {
                    db.doQuery.resetHistory();
                    await applier._insertRows('test_table', rows);
                    if (db.doQuery.callCount > 0) {
                        let args = db.doQuery.firstCall.args[1];
                        // Every second arg (the 'val' column) should be null
                        for (let i = 1; i < args.length; i += 2) {
                            assert.strictEqual(args[i], null, 'Expected null for val column, got: ' + args[i]);
                        }
                    }
                }
            ), { numRuns: NUM_RUNS });
        });

        it('does nothing for empty or null rows', function () {
            return fc.assert(fc.asyncProperty(
                fc.constantFrom([], null, undefined),
                async (rows) => {
                    db.doQuery.resetHistory();
                    await applier._insertRows('blocks', rows);
                    assert.strictEqual(db.doQuery.callCount, 0);
                }
            ), { numRuns: 30 });
        });
    });

    after(function () {
        if (crashLog.length > 0) {
            let unique = [...new Set(crashLog.map(c => c.error))];
            console.log('\n    [FUZZ CRASH REPORT] ' + crashLog.length + ' crashes across ' + unique.length + ' unique error types:');
            for (let err of unique) {
                let count = crashLog.filter(c => c.error === err).length;
                console.log('      - (' + count + 'x) ' + err.substring(0, 120));
            }
        }
    });
});
