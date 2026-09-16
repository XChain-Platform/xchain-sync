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
const ClientRollback = require('../../../src/client/rollback');
const Utility = require('../../../src/util');

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

let rollback, db, util;

function registerHooks(){
    beforeEach(function(){
        db = createMockDb();
        util = new Utility();
        rollback = new ClientRollback(db, util, undefined, 'regtest');
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });

    afterEach(function(){
        sinon.restore();
    });
}

describe('ClientRollback', function(){

    describe('rollback', function(){
        registerHooks();
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

        // The pre-transaction reads gate every action-scoped delete while the
        // blockTables/indexTables sweeps run unconditionally and commit, so a
        // swallowed fault here commits a PARTIAL ledger rollback.
        it('reads the first action index fail-CLOSED (opts.rethrow)', async function(){
            await rollback.rollback(100);
            assert.deepStrictEqual(db.getFirstActionIndex.firstCall.args[2], { rethrow: true });
        });

        it('aborts before the transaction when the first-action read faults', async function(){
            let err = new Error('lock wait timeout'); err.errno = 1205;
            db.getFirstActionIndex.rejects(err);
            await assert.rejects(() => rollback.rollback(100), /lock wait timeout/);
            assert.strictEqual(db.beginTransaction.called, false);
            assert.strictEqual(db.commitTransaction.called, false);
        });

        it('reads the affected market pairs fail-CLOSED (opts.rethrow)', async function(){
            await rollback.rollback(100);
            let pairRead = db.doQuery.getCalls().find(c =>
                typeof c.args[0] === 'string' && c.args[0].includes('FROM order_matches'));
            assert.ok(pairRead, 'the pre-transaction market-pair read was issued');
            assert.deepStrictEqual(pairRead.args[3], { rethrow: true });
        });

        it('aborts before the transaction when the market-pair read faults', async function(){
            // The errno guard on that read claims to escalate everything but a schema
            // gap; with a fail-soft read it never saw one.
            let err = new Error('deadlock found'); err.errno = 1213;
            db.doQuery.withArgs(sinon.match(/FROM order_matches/)).rejects(err);
            await assert.rejects(() => rollback.rollback(100), /deadlock found/);
            assert.strictEqual(db.beginTransaction.called, false);
        });

    });
});

describe('ClientRollback', function(){

    describe('rollback', function(){
        registerHooks();
        it('still skips the market sweep on a schema gap (errno 1146)', async function(){
            let err = new Error('table missing'); err.errno = 1146;
            db.doQuery.withArgs(sinon.match(/FROM order_matches/)).rejects(err);
            await rollback.rollback(100);
            assert.strictEqual(db.commitTransaction.calledOnce, true);
            let pairScopedDeletes = db.doQuery.getCalls().filter(c =>
                typeof c.args[0] === 'string' &&
                c.args[0].includes('DELETE FROM markets') && c.args[0].includes('tick1_id=?'));
            assert.strictEqual(pairScopedDeletes.length, 0);
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
                !c.args[0].includes('contract_emissions') && !c.args[0].includes('oracle_prices') &&
                !c.args[0].includes('cross_chain_calls') && !c.args[0].includes('cross_chain_matches') &&
                !c.args[0].includes('bridge_transfers')
            );
            // Should have one delete per dataTable. oracle_prices and the three hub-mirror
            // deletes (cross_chain_calls / cross_chain_matches / bridge_transfers) are bespoke
            // source_chain-qualified deletes, excluded above (their source_action_index /
            // a_action_index / src_action_index predicates also contain the 'action_index >='
            // substring this filter keys on). They are not dropped from coverage: the
            // dedicated per-chain case below drives them with a real coin and pins each
            // statement's binds exactly.
            assert.strictEqual(actionDeletes.length, rollback.dataTables.length);
            // Each should use firstActionIndex = 500
            for(let call of actionDeletes){
                assert.deepStrictEqual(call.args[1], [500]);
            }
        });

    });
});

describe('ClientRollback', function(){

    describe('rollback', function(){
        registerHooks();
        // The hub-mirrored per-action tables are pruned locally on reorg so the orphaned
        // range cannot be served while hub-driven convergence (row:deleted) catches up.
        // Each delete MUST be qualified by THIS replica's chain: action_index is only
        // unique within a chain, so an unqualified cut would delete another chain's live
        // mirror rows. cross_chain_matches is two-sided (a match dies when either leg was
        // rolled back); cross_chain_calls and bridge_transfers are one-sided (the XCALL
        // source leg, and the XBRIDGE lock or burn named by src_chain/src_action_index).
        it('prunes the hub-mirrored per-action tables scoped to this chain and firstActionIndex', async function(){
            let chainDb = createMockDb();
            let chainRollback = new ClientRollback(chainDb, new Utility(), 'BTC', 'regtest');
            await chainRollback.rollback(100);

            let expected = {
                cross_chain_calls:   ['BTC', 500],
                cross_chain_matches: ['BTC', 500, 'BTC', 500],
                bridge_transfers:    ['BTC', 500]
            };
            for(let table of Object.keys(expected)){
                let deletes = chainDb.doQuery.getCalls().filter(c =>
                    typeof c.args[0] === 'string' &&
                    c.args[0].includes('DELETE FROM ' + table + ' WHERE')
                );
                assert.strictEqual(deletes.length, 1,
                    'expected exactly one reorg delete against ' + table);
                assert.deepStrictEqual(deletes[0].args[1], expected[table],
                    table + ' reorg delete must bind this chain and firstActionIndex');
            }
        });

        // The three run inside ONE try block, so a replica predating the bridge tables
        // raises errno 1146 on the bridge_transfers statement. It is issued LAST precisely
        // so that skip cannot cost the two cross_chain deletes above it, and the rollback
        // must still commit rather than abort the whole reorg reset.
        it('still deletes the cross_chain mirrors when bridge_transfers is missing (errno 1146)', async function(){
            let chainDb = createMockDb();
            let missing = new Error('Table \'bridge_transfers\' doesn\'t exist'); missing.errno = 1146;
            chainDb.doQuery.withArgs(sinon.match(/DELETE FROM bridge_transfers/)).rejects(missing);
            let chainRollback = new ClientRollback(chainDb, new Utility(), 'BTC', 'regtest');
            await chainRollback.rollback(100);

            for(let table of ['cross_chain_calls', 'cross_chain_matches']){
                let deletes = chainDb.doQuery.getCalls().filter(c =>
                    typeof c.args[0] === 'string' &&
                    c.args[0].includes('DELETE FROM ' + table + ' WHERE')
                );
                assert.strictEqual(deletes.length, 1,
                    table + ' must still be pruned when the bridge table is absent');
            }
            assert.strictEqual(chainDb.commitTransaction.calledOnce, true);
            assert.strictEqual(chainDb.rollbackTransaction.called, false);
        });

    });
});

describe('ClientRollback', function(){

    describe('rollback', function(){
        registerHooks();
        it('deletes from block-scoped tables with block_index', async function(){
            await rollback.rollback(100);
            let blockDeletes = db.doQuery.getCalls().filter(c =>
                c.args[0].includes('DELETE FROM') && c.args[0].includes('block_index >=') && !c.args[0].includes('sync_meta')
            );
            // Block-scoped deletes cover both blockTables and the index id tables
            // (index_addresses / index_tickers), which are also pruned by block_index >=, plus
            // the one validator_rewards delete keyed on derive_block_index, whose
            // column name ends in the same substring this filter matches on.
            assert.strictEqual(blockDeletes.length,
                rollback.blockTables.length + rollback.indexTables.length + 1);
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

        it('deletes from merkle_epochs by end_block, mirroring the server pruneFrom (item 4770)', async function(){
            await rollback.rollback(100);
            // merkle_epochs is sync-owned and snapshot-ride-along only (applied
            // INSERT IGNORE), so without this delete a reorg that re-roots a closed
            // epoch leaves the follower serving the stale root forever. It is keyed
            // by end_block, not block_index.
            let merkleDelete = db.doQuery.getCalls().find(c =>
                c.args[0].includes('merkle_epochs') && c.args[0].includes('DELETE')
            );
            assert.ok(merkleDelete, 'expected a DELETE FROM merkle_epochs on reorg');
            assert.ok(/end_block\s*>=\s*\?/.test(merkleDelete.args[0]),
                'merkle_epochs delete must be scoped by end_block >= ?');
            assert.deepStrictEqual(merkleDelete.args[1], [100]);
        });

    });
});

describe('ClientRollback', function(){

    describe('rollback', function(){
        registerHooks();
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

    });
});
