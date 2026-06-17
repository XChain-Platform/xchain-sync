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
const ClientRollback = require('../../../src/ClientRollback');
const Utility = require('../../../src/utility');

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

describe('Boundary: Rollback Scope', function(){

    let rollback, db;

    beforeEach(function(){
        db = createMockDb();
        rollback = new ClientRollback(db, new Utility());
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });

    afterEach(function(){ sinon.restore(); });

    it('rollback at highest block: deletes only that block', async function(){
        db.getFirstActionIndex.resolves(1000);
        await rollback.rollback(10);
        let blockDeletes = db.doQuery.getCalls().filter(c =>
            c.args[0].includes('DELETE') && c.args[0].includes('block_index >=') && !c.args[0].includes('sync_meta')
        );
        for(let call of blockDeletes){
            assert.deepStrictEqual(call.args[1], [10]);
        }
    });

    it('rollback at block 1: deletes everything', async function(){
        db.getFirstActionIndex.resolves(100);
        await rollback.rollback(1);
        let blockDeletes = db.doQuery.getCalls().filter(c =>
            c.args[0].includes('DELETE') && c.args[0].includes('block_index >=') && !c.args[0].includes('sync_meta')
        );
        for(let call of blockDeletes){
            assert.deepStrictEqual(call.args[1], [1]);
        }
        // Only the dataTables DELETE loop binds a bare [firstActionIndex]; the anchor
        // invalid_archive reset is an in-place UPDATE (not a DELETE) that also references
        // `action_index >=` but binds [firstActionIndex, firstActionIndex] so exclude it.
        // oracle_prices is a bespoke delete that binds [coin, firstActionIndex] so also exclude it.
        let actionDeletes = db.doQuery.getCalls().filter(c =>
            c.args[0].includes('DELETE') && c.args[0].includes('action_index >=') &&
            !c.args[0].includes('contract_emissions') && !c.args[0].includes('oracle_prices')
        );
        for(let call of actionDeletes){
            assert.deepStrictEqual(call.args[1], [100]);
        }
    });

    it('rollback at block 0: deletes all data', async function(){
        db.getFirstActionIndex.resolves(0);
        await rollback.rollback(0);
        let blockDeletes = db.doQuery.getCalls().filter(c =>
            c.args[0].includes('DELETE') && c.args[0].includes('block_index >=') && !c.args[0].includes('sync_meta')
        );
        for(let call of blockDeletes){
            assert.deepStrictEqual(call.args[1], [0]);
        }
    });

    it('rollback with no actions at block: skips action-scoped deletes', async function(){
        db.getFirstActionIndex.resolves(null);
        await rollback.rollback(5);
        let actionDeletes = db.doQuery.getCalls().filter(c =>
            c.args[0].includes('action_index >=')
        );
        assert.strictEqual(actionDeletes.length, 0);
        // Block-scoped deletes still happen
        let blockDeletes = db.doQuery.getCalls().filter(c =>
            c.args[0].includes('DELETE') && c.args[0].includes('block_index >=') && !c.args[0].includes('sync_meta')
        );
        assert.strictEqual(blockDeletes.length, rollback.blockTables.length);
    });

    it('rollback at non-existent block: still runs block deletes', async function(){
        db.getFirstActionIndex.resolves(null);
        await rollback.rollback(9999);
        // Block-scoped deletes execute (but DELETE WHERE block_index >= 9999 will match nothing)
        let blockDeletes = db.doQuery.getCalls().filter(c =>
            c.args[0].includes('DELETE') && c.args[0].includes('block_index >=') && !c.args[0].includes('sync_meta')
        );
        assert.strictEqual(blockDeletes.length, rollback.blockTables.length);
        assert.strictEqual(db.commitTransaction.calledOnce, true);
    });

    it('always recalculates balances (DELETE + INSERT)', async function(){
        await rollback.rollback(50);
        let balanceDelete = db.doQuery.getCalls().find(c =>
            c.args[0] === 'DELETE FROM balances'
        );
        assert.ok(balanceDelete, 'Should DELETE FROM balances');
        let balanceInsert = db.doQuery.getCalls().find(c =>
            c.args[0].includes('INSERT INTO balances')
        );
        assert.ok(balanceInsert, 'Should INSERT INTO balances');
    });

    it('deletes sync_meta entries at rollback block', async function(){
        await rollback.rollback(50);
        let syncMetaDelete = db.doQuery.getCalls().find(c =>
            c.args[0].includes('sync_meta') && c.args[0].includes('DELETE')
        );
        assert.ok(syncMetaDelete);
        assert.deepStrictEqual(syncMetaDelete.args[1], [50]);
    });

    it('transaction rolls back on error', async function(){
        db.commitTransaction.rejects(new Error('commit fail'));
        await assert.rejects(() => rollback.rollback(10), { message: 'commit fail' });
        assert.strictEqual(db.rollbackTransaction.calledOnce, true);
    });
});
