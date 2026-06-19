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
const ClientRollback = require('../../src/ClientRollback');
const Utility = require('../../src/utility');
const balanceHelpers = require('../../src/balance-helpers');

function createMockDb(){
    return {
        doQuery: sinon.stub().resolves([]),
        getFirstActionIndex: sinon.stub().resolves(500),
        getStatusId: sinon.stub().resolves(null),
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
        it('has 9 block-scoped tables', function(){
            assert.strictEqual(rollback.blockTables.length, 9);
            assert.ok(rollback.blockTables.includes('blocks'));
            assert.ok(rollback.blockTables.includes('transactions'));
            assert.ok(rollback.blockTables.includes('slash_events'));
            assert.ok(rollback.blockTables.includes('contract_slash_debits'));
            // WI-2 bump 2 capability-stake equivocation slashing (committed 8e95482).
            assert.ok(rollback.blockTables.includes('capability_slash_events'));
            assert.ok(rollback.blockTables.includes('capability_slash_debits'));
            // Light-client per-block SMT roots (SPV spec sec.4), block-scoped so
            // orphaned-fork roots drop on reorg; state_tree_nodes stays (COW/immutable).
            assert.ok(rollback.blockTables.includes('state_tree_roots'));
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
                c.args[0].includes('DELETE FROM') && c.args[0].includes('action_index >=') &&
                !c.args[0].includes('contract_emissions') && !c.args[0].includes('oracle_prices')
            );
            // Should have one delete per dataTable (oracle_prices is a bespoke delete excluded above)
            assert.strictEqual(actionDeletes.length, rollback.dataTables.length);
            // Each should use firstActionIndex = 500
            for(let call of actionDeletes){
                assert.deepStrictEqual(call.args[1], [500]);
            }
        });

        it('deletes from block-scoped tables with block_index', async function(){
            await rollback.rollback(100);
            let blockDeletes = db.doQuery.getCalls().filter(c =>
                c.args[0].includes('DELETE FROM') && c.args[0].includes('block_index >=') && !c.args[0].includes('sync_meta')
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

        // tokens.escrow_action_index is RE-DERIVED after the dataTables delete (mirror of
        // xchain-indexer rollback.js, TP-03 #4017): set to the surviving open GIVE_OWNERSHIP
        // offer's action_index, else NULL. Collapses both directions and matches the source
        // byte-for-byte.
        const AFFECTED_RE  = /escrow_action_index IS NOT NULL/i;
        const OPENOFFER_RE = /SELECT o\.action_index FROM orders/i;
        const REDERIVE_RE  = /UPDATE tokens SET escrow_action_index=\?\s+WHERE tick_id=\(SELECT id FROM index_tickers/i;

        it('re-stamps escrow to a surviving open offer (orphaned release / CLEAR direction)', async function(){
            db.doQuery.withArgs(sinon.match(AFFECTED_RE)).resolves([{ tick: 'FOO' }]);
            db.doQuery.withArgs(sinon.match(OPENOFFER_RE)).resolves([{ action_index: 30 }]);
            await rollback.rollback(100);
            let rederive = db.doQuery.getCalls().find(c => REDERIVE_RE.test(c.args[0]));
            assert.ok(rederive, 'expected a re-derive UPDATE on tokens.escrow_action_index');
            assert.deepStrictEqual(rederive.args[1], [30, 'FOO']);
            // old SET-only reset must be gone
            assert.ok(!db.doQuery.getCalls().some(c => /escrow_action_index\s*>=\s*\?/i.test(c.args[0])),
                'the old SET-only `escrow_action_index >= ?` reset must no longer be issued');
        });

        it('clears escrow when no offer survives (orphaned offer / SET direction)', async function(){
            db.doQuery.withArgs(sinon.match(AFFECTED_RE)).resolves([{ tick: 'BAR' }]);
            db.doQuery.withArgs(sinon.match(OPENOFFER_RE)).resolves([]);
            await rollback.rollback(100);
            let rederive = db.doQuery.getCalls().find(c => REDERIVE_RE.test(c.args[0]));
            assert.ok(rederive, 'expected a re-derive UPDATE');
            assert.deepStrictEqual(rederive.args[1], [null, 'BAR']);
        });

        it('re-derives escrow AFTER the action-scoped deletes', async function(){
            db.doQuery.withArgs(sinon.match(AFFECTED_RE)).resolves([{ tick: 'FOO' }]);
            await rollback.rollback(100);
            let calls = db.doQuery.getCalls();
            let tokenDeleteIdx = calls.findIndex(c => c.args[0].includes('DELETE FROM `tokens`'));
            let affectedIdx    = calls.findIndex(c => AFFECTED_RE.test(c.args[0]));
            assert.ok(tokenDeleteIdx >= 0 && affectedIdx >= 0);
            assert.ok(affectedIdx > tokenDeleteIdx, 'escrow re-derive must run AFTER the tokens delete');
        });

        it('does not throw if the escrow re-derive tables are missing (older replica schema)', async function(){
            // Simulate a MariaDB "table not found" error with errno 1146 (the errno the
            // schema-gap catch checks for). A plain Error without errno would be rethrown.
            db.doQuery.withArgs(sinon.match(AFFECTED_RE))
                .rejects(Object.assign(new Error('Table does not exist'), { errno: 1146 }));
            await rollback.rollback(100);
            assert.strictEqual(db.commitTransaction.calledOnce, true, 'rollback still commits');
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
            // attests/xcalls request_status resets are in-place on surviving rows and must
            // precede the deletes (escrow is the exception: it is re-derived AFTER, tested above).
            let attestIdx = calls.findIndex(c => c.args[0].includes('UPDATE attests') && c.args[0].includes("request_status = 'pending'"));
            let tokenDeleteIdx = calls.findIndex(c => c.args[0].includes('DELETE FROM `tokens`'));
            assert.ok(attestIdx >= 0 && tokenDeleteIdx >= 0);
            assert.ok(attestIdx < tokenDeleteIdx, 'attests reset must precede the deletes');
        });

        it('skips the in-place resets and the escrow re-derive when firstActionIndex is null', async function(){
            db.getFirstActionIndex.resolves(null);
            await rollback.rollback(100);
            let touched = db.doQuery.getCalls().filter(c =>
                AFFECTED_RE.test(c.args[0]) ||
                REDERIVE_RE.test(c.args[0]) ||
                (c.args[0].includes("request_status = 'pending'"))
            );
            assert.strictEqual(touched.length, 0);
        });

        it('replays the slash-debit restore for both stake tables (block_index-keyed, before deletes)', async function(){
            await rollback.rollback(100);
            let calls = db.doQuery.getCalls();
            let stakeRestore = calls.find(c => c.args[0].includes('UPDATE contract_stakes') && c.args[0].includes('contract_slash_debits') && c.args[0].includes('SET t.amount = d.prev_amount'));
            let unstakeRestore = calls.find(c => c.args[0].includes('UPDATE contract_unstakes') && c.args[0].includes('contract_slash_debits'));
            assert.ok(stakeRestore, 'expected a contract_stakes slash restore');
            assert.ok(unstakeRestore, 'expected a contract_unstakes slash restore');
            assert.deepStrictEqual(stakeRestore.args[1], ['contract_stakes', 100, 100]);
            assert.deepStrictEqual(unstakeRestore.args[1], ['contract_unstakes', 100, 100]);
            let restoreIdx = calls.indexOf(stakeRestore);
            let deleteIdx = calls.findIndex(c => c.args[0].includes('DELETE FROM `contract_stakes`'));
            assert.ok(restoreIdx >= 0 && deleteIdx >= 0 && restoreIdx < deleteIdx, 'slash restore must precede the contract_stakes delete');
        });

        it('contract slash-restore tiebreaks on (execution_index, slash_position), byte-matching the source (not AUTO_INCREMENT id)', async function(){
            // Must mirror xchain-indexer rollback.js exactly. If the replica tiebreaks on the
            // AUTO_INCREMENT `id` (assigned in physical insert order, differs source-vs-replica),
            // a reorg retracting a block with >=2 contract slashes on one stake_action_index
            // restores a divergent prev_amount on the replica -> stake-weight / quorum fork.
            await rollback.rollback(100);
            let calls = db.doQuery.getCalls();
            let restores = calls.filter(c =>
                /UPDATE contract_(?:un)?stakes/.test(c.args[0]) &&
                c.args[0].includes('contract_slash_debits') &&
                c.args[0].includes('d.prev_amount'));
            assert.strictEqual(restores.length, 2, 'expected contract_stakes + contract_unstakes slash restores');
            for(let r of restores){
                let sql = r.args[0];
                assert.ok(/e\.execution_index\s*<\s*d\.execution_index/.test(sql),
                    'restore must order by execution_index (deterministic, replay-stable)');
                assert.ok(/e\.slash_position\s*<\s*d\.slash_position/.test(sql),
                    'restore must use slash_position as the within-EXECUTE secondary tiebreak');
                assert.ok(!/e\.id\s*<\s*d\.id/.test(sql),
                    'restore must NOT tiebreak on the non-deterministic AUTO_INCREMENT id');
            }
        });

        it('rolls back transaction on error and rethrows', async function(){
            db.commitTransaction.rejects(new Error('commit fail'));
            await assert.rejects(() => rollback.rollback(100), { message: 'commit fail' });
            assert.strictEqual(db.rollbackTransaction.calledOnce, true);
        });

        it('handles per-table errors gracefully (table may not exist)', async function(){
            // Simulate a MariaDB "table not found" error (errno 1146) on the 3rd query.
            // All bespoke optional-delete paths check e.errno 1146/1054 and swallow it.
            let callCount = 0;
            db.doQuery.callsFake(async (query) => {
                callCount++;
                if(callCount === 3) throw Object.assign(new Error('Table does not exist'), { errno: 1146 });
                return [];
            });
            // Should not throw; individual table errors are caught
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
