const assert = require('assert');
const sinon  = require('sinon');
const ClientRollback = require('../../src/ClientRollback');
const Utility = require('../../src/utility');

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
                c.args[0].includes('action_index >=') && !c.args[0].includes('contract_emissions')
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
    });
});
