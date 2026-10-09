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
const sinon = require('sinon');
const ClientRollback = require('../../../src/client/rollback');
const Utility = require('../../../src/util');

function createMockDb(firstActionIndex = 500){
    const db = {
        doQuery: sinon.stub().resolves([]),
        getFirstActionIndex: sinon.stub().resolves(firstActionIndex),
        getStatusId: sinon.stub(),
        beginTransaction: sinon.stub().resolves(),
        commitTransaction: sinon.stub().resolves(),
        rollbackTransaction: sinon.stub().resolves()
    };
    db.getStatusId.withArgs('completed').resolves(8);
    db.getStatusId.withArgs('valid').resolves(3);
    return db;
}

function maturityReleaseDeletes(db){
    return db.doQuery.getCalls().filter(c => /^DELETE e FROM escrows e/.test(c.args[0]));
}

describe('ClientRollback cooldown-maturity escrow-release reversal', function(){
    afterEach(function(){ sinon.restore(); });

    it('deletes both backdated releases with the same keys as their refund credits', async function(){
        const db = createMockDb();
        const rollback = new ClientRollback(db, new Utility(), undefined, 'regtest');

        await rollback.rollback(100);

        const deletes = maturityReleaseDeletes(db);
        assert.strictEqual(deletes.length, 2, 'expected capability and contract escrow-release deletes');

        assert.match(deletes[0].args[0], /JOIN unstakes u ON u\.action_index = e\.action_index AND u\.source_id = e\.address_id/);
        assert.match(deletes[0].args[0], /JOIN index_tickers g ON g\.id = e\.tick_id AND g\.tick = \?/);
        assert.match(deletes[0].args[0], /WHERE u\.status_id = \? AND u\.cooldown_end_block >= \? AND u\.block_index < \?/);
        assert.deepStrictEqual(deletes[0].args[1], ['XCHAIN', 8, 100, 100]);

        assert.match(deletes[1].args[0], /JOIN contract_unstakes cu ON cu\.action_index = e\.action_index/);
        assert.match(deletes[1].args[0], /AND cu\.source_id = e\.address_id AND cu\.tick_id = e\.tick_id/);
        assert.match(deletes[1].args[0], /WHERE cu\.status_id = \? AND cu\.cooldown_end_block >= \? AND cu\.block_index < \?/);
        assert.deepStrictEqual(deletes[1].args[1], [8, 100, 100]);
    });

    it('deletes the releases before resetting status and before generic escrow deletion', async function(){
        const db = createMockDb();
        const rollback = new ClientRollback(db, new Utility(), undefined, 'regtest');

        await rollback.rollback(100);

        const calls = db.doQuery.getCalls();
        const deletes = maturityReleaseDeletes(db);
        const statusReset = calls.find(c => /^UPDATE unstakes SET status_id/.test(c.args[0]));
        const genericDelete = calls.find(c => /DELETE FROM `escrows` WHERE action_index >= \?/.test(c.args[0]));
        assert.strictEqual(deletes.length, 2, 'expected capability and contract escrow-release deletes');
        assert.ok(statusReset, 'expected the completed-to-valid status reset');
        assert.ok(genericDelete, 'expected the generic action-range escrow delete');
        assert.ok(calls.indexOf(deletes[1]) < calls.indexOf(statusReset), 'release deletes must precede the status reset');
        assert.ok(calls.indexOf(deletes[1]) < calls.indexOf(genericDelete), 'release deletes must precede the generic delete');
    });

    it('deletes releases for an action-empty maturity range', async function(){
        const db = createMockDb(null);
        const rollback = new ClientRollback(db, new Utility(), undefined, 'regtest');

        await rollback.rollback(100);

        assert.strictEqual(maturityReleaseDeletes(db).length, 2,
            'legacy maturities have no action in their maturity block, so reversal must be unconditional');
    });

    it('rolls back the transaction when a release delete faults', async function(){
        const db = createMockDb();
        const deadlock = Object.assign(new Error('deadlock deleting escrow release'), { errno: 1213 });
        db.doQuery.withArgs(sinon.match(/^DELETE e FROM escrows e/)).rejects(deadlock);
        const rollback = new ClientRollback(db, new Utility(), undefined, 'regtest');

        await assert.rejects(() => rollback.rollback(100), /deadlock deleting escrow release/);
        assert.strictEqual(db.commitTransaction.called, false);
        assert.strictEqual(db.rollbackTransaction.calledOnce, true);
    });
});
