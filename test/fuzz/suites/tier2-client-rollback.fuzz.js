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
 * Tier 2 Fuzz: ClientRollback
 *
 * Tests that rollback() never crashes regardless of block_index value,
 * DB error patterns, or firstActionIndex state. Verifies transaction safety
 * and correct ordering of DELETE/INSERT operations.
 */

const assert = require('assert');
const fc = require('fast-check');
const sinon = require('sinon');
const { NUM_RUNS, createMockDb, createMockUtil } = require('../setup/harness');
const { blockIndex } = require('../generators/values');

let crashLog = [];

describe('Tier 2 - ClientRollback @tier2', function () {
    this.timeout(0);
    let ClientRollback, rollback, db, util;

    before(function () {
        ClientRollback = require('../../../src/ClientRollback');
    });

    beforeEach(function () {
        db = createMockDb();
        util = createMockUtil();
        rollback = new ClientRollback(db, util);
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });

    afterEach(function () {
        sinon.restore();
    });

    describe('rollback', function () {

        it('never throws for any valid block_index', function () {
            return fc.assert(fc.asyncProperty(
                blockIndex(),
                async (bi) => {
                    db.getFirstActionIndex.resolves(bi * 100);
                    await rollback.rollback(bi);
                }
            ), { numRuns: NUM_RUNS });
        });

        it('transaction is always committed or rolled back, never leaked', function () {
            return fc.assert(fc.asyncProperty(
                blockIndex(),
                fc.boolean(),
                async (bi, commitFails) => {
                    db.getFirstActionIndex.resolves(500);
                    db.beginTransaction.resetHistory();
                    db.commitTransaction.resetHistory();
                    db.rollbackTransaction.resetHistory();

                    if (commitFails) {
                        db.commitTransaction.rejects(new Error('commit-fail'));
                    } else {
                        db.commitTransaction.resolves();
                    }

                    try {
                        await rollback.rollback(bi);
                    } catch (e) {
                        // expected when commitFails
                    }

                    let began = db.beginTransaction.callCount;
                    let committed = db.commitTransaction.callCount;
                    let rolledBack = db.rollbackTransaction.callCount;

                    // A transaction was opened; verify it was resolved
                    if (began > 0) {
                        // Commit is always attempted. If it fails, rollback is also called.
                        // Valid states: committed=1,rolledBack=0 (success) or committed=1,rolledBack=1 (commit failed)
                        assert.ok(committed >= 1,
                            'Commit should always be attempted: committed=' + committed);
                        if (commitFails) {
                            assert.strictEqual(rolledBack, 1,
                                'Rollback should be called when commit fails');
                        } else {
                            assert.strictEqual(rolledBack, 0,
                                'Rollback should not be called when commit succeeds');
                        }
                    }
                }
            ), { numRuns: NUM_RUNS });
        });

        it('individual table DELETE errors do not abort the operation', function () {
            return fc.assert(fc.asyncProperty(
                blockIndex(),
                fc.array(fc.integer({ min: 1, max: 60 }), { minLength: 1, maxLength: 5 }),
                async (bi, failIndices) => {
                    db.getFirstActionIndex.resolves(bi * 100);
                    db.commitTransaction.resetHistory();

                    let callCount = 0;
                    let failSet = new Set(failIndices);
                    db.doQuery.callsFake(async (query) => {
                        callCount++;
                        if (failSet.has(callCount)) throw new Error('fuzz-table-fail');
                        return [];
                    });

                    await rollback.rollback(bi);
                    assert.strictEqual(db.commitTransaction.callCount, 1,
                        'Expected commit after individual table errors');
                }
            ), { numRuns: NUM_RUNS });
        });

        it('when firstActionIndex is null, action-scoped DELETEs use no action_index filter', function () {
            return fc.assert(fc.asyncProperty(
                blockIndex(),
                async (bi) => {
                    db.getFirstActionIndex.resolves(null);
                    db.doQuery.resetHistory();

                    await rollback.rollback(bi);

                    // Verify no query contains 'action_index >='
                    for (let i = 0; i < db.doQuery.callCount; i++) {
                        let query = db.doQuery.getCall(i).args[0];
                        if (query.includes('action_index >=')) {
                            assert.fail('Expected no action_index queries when firstActionIndex is null, got: ' + query.substring(0, 80));
                        }
                    }
                }
            ), { numRuns: NUM_RUNS });
        });

        it('balance DELETE comes before balance INSERT', function () {
            return fc.assert(fc.asyncProperty(
                blockIndex(),
                async (bi) => {
                    db.getFirstActionIndex.resolves(bi * 100);
                    db.doQuery.resetHistory();

                    await rollback.rollback(bi);

                    let deleteBalanceIdx = -1;
                    let insertBalanceIdx = -1;
                    for (let i = 0; i < db.doQuery.callCount; i++) {
                        let query = db.doQuery.getCall(i).args[0];
                        if (query.includes('DELETE FROM balances') && deleteBalanceIdx === -1) {
                            deleteBalanceIdx = i;
                        }
                        if (query.includes('INSERT INTO balances') && insertBalanceIdx === -1) {
                            insertBalanceIdx = i;
                        }
                    }

                    if (deleteBalanceIdx >= 0 && insertBalanceIdx >= 0) {
                        assert.ok(deleteBalanceIdx < insertBalanceIdx,
                            'DELETE FROM balances (call ' + deleteBalanceIdx + ') must come before INSERT INTO balances (call ' + insertBalanceIdx + ')');
                    }
                }
            ), { numRuns: NUM_RUNS });
        });

        it('sync_meta DELETE uses correct block_index argument', function () {
            return fc.assert(fc.asyncProperty(
                blockIndex(),
                async (bi) => {
                    db.getFirstActionIndex.resolves(bi * 100);
                    db.doQuery.resetHistory();

                    await rollback.rollback(bi);

                    let found = false;
                    for (let i = 0; i < db.doQuery.callCount; i++) {
                        let call = db.doQuery.getCall(i);
                        let query = call.args[0];
                        if (query.includes('sync_meta')) {
                            found = true;
                            assert.deepStrictEqual(call.args[1], [bi],
                                'sync_meta DELETE should use block_index=' + bi);
                        }
                    }
                    assert.ok(found, 'Expected a sync_meta DELETE query');
                }
            ), { numRuns: NUM_RUNS });
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
