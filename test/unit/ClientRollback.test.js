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
const ClientRollback = require('../../src/ClientRollback');
const Utility = require('../../src/utility');
const balanceHelpers = require('../../src/balance-helpers');

function createMockDb(){
    return {
        doQuery: sinon.stub().resolves([]),
        getFirstActionIndex: sinon.stub().resolves(500),
        beginTransaction: sinon.stub().resolves(),
        commitTransaction: sinon.stub().resolves(),
        rollbackTransaction: sinon.stub().resolves()
    };
}

describe('ClientRollback', function(){

    let rollback, db, util;

    beforeEach(function(){
        db = createMockDb();
        util = new Utility();
        rollback = new ClientRollback(db, util);
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });

    afterEach(function(){
        sinon.restore();
    });

    describe('table lists', function(){
        it('has 5 block-scoped tables', function(){
            assert.strictEqual(rollback.blockTables.length, 5);
            assert.ok(rollback.blockTables.includes('blocks'));
            assert.ok(rollback.blockTables.includes('transactions'));
            assert.ok(rollback.blockTables.includes('slash_events'));
        });

        it('has action-scoped data tables', function(){
            assert.ok(rollback.dataTables.length > 40);
            assert.ok(rollback.dataTables.includes('actions'));
            assert.ok(rollback.dataTables.includes('credits'));
            assert.ok(rollback.dataTables.includes('debits'));
            assert.ok(rollback.dataTables.includes('attests'));
            assert.ok(rollback.dataTables.includes('balances') === false); // balances are recalculated, not in dataTables
        });
    });

    describe('rollback', function(){
        it('wraps everything in a transaction', async function(){
            await rollback.rollback(100);
            assert.strictEqual(db.beginTransaction.calledOnce, true);
            assert.strictEqual(db.commitTransaction.calledOnce, true);
            assert.ok(db.beginTransaction.calledBefore(db.commitTransaction));
        });

        it('gets first action index for the block', async function(){
            await rollback.rollback(100);
            assert.strictEqual(db.getFirstActionIndex.calledOnce, true);
            assert.strictEqual(db.getFirstActionIndex.firstCall.args[0], 100);
        });

        it('deletes contract_emissions first', async function(){
            await rollback.rollback(100);
            let firstDelete = db.doQuery.getCalls().find(c => c.args[0].includes('DELETE'));
            assert.ok(firstDelete.args[0].includes('contract_emissions'));
        });

        it('deletes from action-scoped tables with action_index', async function(){
            await rollback.rollback(100);
            let actionDeletes = db.doQuery.getCalls().filter(c =>
                c.args[0].includes('DELETE FROM') && c.args[0].includes('action_index >=') && !c.args[0].includes('contract_emissions')
            );
            // Should have one delete per dataTable
            assert.strictEqual(actionDeletes.length, rollback.dataTables.length);
            // Each should use firstActionIndex = 500
            for(let call of actionDeletes){
                assert.deepStrictEqual(call.args[1], [500]);
            }
        });

        it('deletes from block-scoped tables with block_index', async function(){
            await rollback.rollback(100);
            let blockDeletes = db.doQuery.getCalls().filter(c =>
                c.args[0].includes('block_index >=') && !c.args[0].includes('sync_meta')
            );
            assert.strictEqual(blockDeletes.length, rollback.blockTables.length);
            for(let call of blockDeletes){
                assert.deepStrictEqual(call.args[1], [100]);
            }
        });

        it('deletes from sync_meta', async function(){
            await rollback.rollback(100);
            let syncMetaDelete = db.doQuery.getCalls().find(c =>
                c.args[0].includes('sync_meta') && c.args[0].includes('DELETE')
            );
            assert.ok(syncMetaDelete);
            assert.deepStrictEqual(syncMetaDelete.args[1], [100]);
        });

        it('recalculates balances from credits/debits', async function(){
            await rollback.rollback(100);
            let balanceDelete = db.doQuery.getCalls().find(c =>
                c.args[0] === 'DELETE FROM balances'
            );
            assert.ok(balanceDelete);
            let balanceInsert = db.doQuery.getCalls().find(c =>
                c.args[0].includes('INSERT INTO balances') && c.args[0].includes('credits')
            );
            assert.ok(balanceInsert);
        });

        it('skips action-scoped deletes when firstActionIndex is null', async function(){
            db.getFirstActionIndex.resolves(null);
            await rollback.rollback(100);
            let actionDeletes = db.doQuery.getCalls().filter(c =>
                c.args[0].includes('action_index >=')
            );
            assert.strictEqual(actionDeletes.length, 0);
        });

        it('replays the escrow_action_index re-NULL on surviving tokens (firstActionIndex-keyed)', async function(){
            await rollback.rollback(100);
            let escrowReset = db.doQuery.getCalls().find(c =>
                c.args[0].includes('UPDATE tokens') && c.args[0].includes('escrow_action_index = NULL')
            );
            assert.ok(escrowReset, 'expected an escrow_action_index re-NULL update');
            assert.deepStrictEqual(escrowReset.args[1], [500]); // firstActionIndex
        });

        it('replays the attests and xcalls request_status resets (block_index-keyed)', async function(){
            await rollback.rollback(100);
            let attestReset = db.doQuery.getCalls().find(c =>
                c.args[0].includes('UPDATE attests') && c.args[0].includes("request_status = 'pending'")
            );
            assert.ok(attestReset, 'expected an attests request_status reset');
            assert.deepStrictEqual(attestReset.args[1], [100]); // block_index
            let xcallReset = db.doQuery.getCalls().find(c =>
                c.args[0].includes('UPDATE xcalls') && c.args[0].includes("request_status = 'pending'")
            );
            assert.ok(xcallReset, 'expected an xcalls request_status reset');
            assert.deepStrictEqual(xcallReset.args[1], [100]); // block_index
        });

        it('runs the in-place resets before the action-scoped deletes', async function(){
            await rollback.rollback(100);
            let calls = db.doQuery.getCalls();
            let escrowIdx = calls.findIndex(c => c.args[0].includes('UPDATE tokens') && c.args[0].includes('escrow_action_index = NULL'));
            let tokenDeleteIdx = calls.findIndex(c => c.args[0].includes('DELETE FROM `tokens`'));
            assert.ok(escrowIdx >= 0 && tokenDeleteIdx >= 0);
            assert.ok(escrowIdx < tokenDeleteIdx, 'escrow reset must precede the tokens delete');
        });

        it('skips the in-place resets when firstActionIndex is null', async function(){
            db.getFirstActionIndex.resolves(null);
            await rollback.rollback(100);
            let resets = db.doQuery.getCalls().filter(c =>
                c.args[0].includes('escrow_action_index = NULL') ||
                (c.args[0].includes("request_status = 'pending'"))
            );
            assert.strictEqual(resets.length, 0);
        });

        it('rolls back transaction on error and rethrows', async function(){
            db.commitTransaction.rejects(new Error('commit fail'));
            await assert.rejects(() => rollback.rollback(100), { message: 'commit fail' });
            assert.strictEqual(db.rollbackTransaction.calledOnce, true);
        });

        it('handles per-table errors gracefully (table may not exist)', async function(){
            // Make some doQuery calls throw (simulating missing tables)
            let callCount = 0;
            db.doQuery.callsFake(async (query) => {
                callCount++;
                if(callCount === 3) throw new Error('Table does not exist');
                return [];
            });
            // Should not throw — individual table errors are caught
            await rollback.rollback(100);
            assert.strictEqual(db.commitTransaction.calledOnce, true);
        });

        it('swallows missing-table errors on every bespoke optional delete', async function(){
            // Make each individually-guarded optional delete throw; rollback must
            // still complete (each has its own try/catch).
            db.doQuery.callsFake(async (query) => {
                if(/contract_emissions|price_snapshots|sync_meta|attest_validator_stats/.test(query))
                    throw new Error('Table does not exist');
                if(/DELETE FROM `blocks`/.test(query))
                    throw new Error('Table does not exist'); // a blockTables-loop catch
                return [];
            });
            await rollback.rollback(100);
            assert.strictEqual(db.commitTransaction.calledOnce, true);
        });
    });

    describe('balance-rebuild error handling', function(){
        it('logs (does not rethrow) a 1146 error from rebuildBalances', async function(){
            sinon.stub(balanceHelpers, 'rebuildBalances')
                .rejects(Object.assign(new Error('no table'), { errno: 1146 }));
            await rollback.rollback(100); // must not throw
            assert.strictEqual(db.commitTransaction.calledOnce, true);
        });

        it('rethrows a non-1146 error from rebuildBalances', async function(){
            sinon.stub(balanceHelpers, 'rebuildBalances')
                .rejects(Object.assign(new Error('real db error'), { errno: 2002 }));
            await assert.rejects(() => rollback.rollback(100), { message: 'real db error' });
            assert.strictEqual(db.rollbackTransaction.calledOnce, true);
        });

        it('logs (does not rethrow) a 1146 error from rebuildContractBalances', async function(){
            sinon.stub(balanceHelpers, 'rebuildBalances').resolves();
            sinon.stub(balanceHelpers, 'rebuildContractBalances')
                .rejects(Object.assign(new Error('no table'), { errno: 1146 }));
            await rollback.rollback(100);
            assert.strictEqual(db.commitTransaction.calledOnce, true);
        });

        it('rethrows a non-1146 error from rebuildContractBalances', async function(){
            sinon.stub(balanceHelpers, 'rebuildBalances').resolves();
            sinon.stub(balanceHelpers, 'rebuildContractBalances')
                .rejects(Object.assign(new Error('real cb error'), { errno: 2002 }));
            await assert.rejects(() => rollback.rollback(100), { message: 'real cb error' });
            assert.strictEqual(db.rollbackTransaction.calledOnce, true);
        });
    });

    describe('_rollbackDecoder', function(){
        let decoderDb, decoderRollback;

        beforeEach(function(){
            decoderDb = createMockDb();
            decoderDb.dbType = 'decoder';
            decoderRollback = new ClientRollback(decoderDb, util);
        });

        it('routes a decoder DB through _rollbackDecoder', async function(){
            let spy = sinon.spy(decoderRollback, '_rollbackDecoder');
            await decoderRollback.rollback(50);
            assert.strictEqual(spy.calledOnceWith(50), true);
        });

        it('deletes tx-scoped tables by tx_index, then block-scoped tables by block_index', async function(){
            decoderDb.doQuery.withArgs(sinon.match(/SELECT tx_index/))
                .resolves([{ tx_index: 7 }, { tx_index: 9 }]);
            await decoderRollback.rollback(50);

            let txDelete = decoderDb.doQuery.getCalls().find(c => /DELETE FROM `transaction_outputs`/.test(c.args[0]));
            assert.ok(txDelete, 'deletes the tx-scoped table');
            assert.ok(/tx_index IN \(\?,\?\)/.test(txDelete.args[0]));
            assert.deepStrictEqual(txDelete.args[1], [7, 9]);

            assert.ok(decoderDb.doQuery.getCalls().some(c =>
                /DELETE FROM `transactions` WHERE block_index >= /.test(c.args[0]) && c.args[1][0] === 50));
            assert.ok(decoderDb.doQuery.getCalls().some(c =>
                /DELETE FROM `blocks` WHERE block_index >= /.test(c.args[0]) && c.args[1][0] === 50));
            assert.strictEqual(decoderDb.commitTransaction.calledOnce, true);
        });

        it('skips tx-scoped deletes when no transactions are in range', async function(){
            decoderDb.doQuery.withArgs(sinon.match(/SELECT tx_index/)).resolves([]);
            await decoderRollback.rollback(50);
            assert.ok(!decoderDb.doQuery.getCalls().some(c => /transaction_outputs/.test(c.args[0])),
                'no tx-scoped delete when nothing is in range');
            assert.strictEqual(decoderDb.commitTransaction.calledOnce, true);
        });

        it('swallows a missing tx-scoped table error and still completes', async function(){
            decoderDb.doQuery.withArgs(sinon.match(/SELECT tx_index/)).resolves([{ tx_index: 1 }]);
            decoderDb.doQuery.withArgs(sinon.match(/transaction_outputs/)).rejects(new Error('no table'));
            await decoderRollback.rollback(50);
            assert.strictEqual(decoderDb.commitTransaction.calledOnce, true);
        });

        it('rolls back and rethrows when a block-scoped delete fails', async function(){
            decoderDb.doQuery.withArgs(sinon.match(/SELECT tx_index/)).resolves([]);
            decoderDb.doQuery.withArgs(sinon.match(/DELETE FROM `transactions`/)).rejects(new Error('decoder boom'));
            await assert.rejects(() => decoderRollback.rollback(50), { message: 'decoder boom' });
            assert.strictEqual(decoderDb.rollbackTransaction.calledOnce, true);
        });
    });
});
