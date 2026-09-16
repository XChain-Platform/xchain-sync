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
const balanceHelpers = require('../../../src/db/balance_helpers');

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

    describe('balance-rebuild error handling', function(){
        registerHooks();
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

        // Fix #5 (MED): a reorg must recompute tokens.supply, not just balances -
        // an orphaned MINT mutates supply in place, so the deleted credit alone leaves
        // it inflated. recomputeTokenSupplies runs after rebuildBalances, before commit.
        it('recomputes token supplies after rebuilding balances (before commit)', async function(){
            let rebuild = sinon.stub(balanceHelpers, 'rebuildBalances').resolves();
            let supplies = sinon.stub(balanceHelpers, 'recomputeTokenSupplies').resolves();
            await rollback.rollback(100);
            assert.ok(supplies.calledOnce, 'tokens.supply is recomputed on reorg');
            assert.ok(rebuild.calledBefore(supplies), 'balances first, then supply (supply reads the rebuilt ledger)');
            assert.ok(supplies.calledBefore(db.commitTransaction), 'supply recompute lands inside the rollback txn');
        });

        it('logs (does not rethrow) a 1146 error from recomputeTokenSupplies', async function(){
            sinon.stub(balanceHelpers, 'recomputeTokenSupplies')
                .rejects(Object.assign(new Error('no tokens table'), { errno: 1146 }));
            await rollback.rollback(100); // must not throw (older/decoder-shaped schema)
            assert.strictEqual(db.commitTransaction.calledOnce, true);
        });

        it('rethrows a non-1146 error from recomputeTokenSupplies', async function(){
            sinon.stub(balanceHelpers, 'recomputeTokenSupplies')
                .rejects(Object.assign(new Error('real supply db error'), { errno: 2002 }));
            await assert.rejects(() => rollback.rollback(100), { message: 'real supply db error' });
            assert.strictEqual(db.rollbackTransaction.calledOnce, true);
        });
    });
});
