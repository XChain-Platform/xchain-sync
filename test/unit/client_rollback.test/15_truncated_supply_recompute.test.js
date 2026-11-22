// Copyright © 2025-2026 Dankest, LLC
// SPDX-License-Identifier: AGPL-3.0-or-later

'use strict';

const assert = require('assert');
const sinon = require('sinon');
const ClientRollback = require('../../../src/client/rollback');
const Utility = require('../../../src/util');
const balanceHelpers = require('../../../src/db/balance_helpers');

function createMockDb(truncated){
    const db = {
        dbType: 'indexer',
        doQuery: sinon.stub().resolves([]),
        getFirstActionIndex: sinon.stub(),
        getStatusId: sinon.stub().resolves(null),
        beginTransaction: sinon.stub().resolves(),
        commitTransaction: sinon.stub().resolves(),
        rollbackTransaction: sinon.stub().resolves()
    };
    db.getFirstActionIndex.onFirstCall().resolves(500);
    if(truncated){
        db.getFirstActionIndex.onSecondCall().resolves(9000);
        db.getSyncState = sinon.stub().resolves('900');
    }
    return db;
}

describe('ClientRollback truncated token supply recompute', function(){
    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'warn');
        sinon.stub(console, 'error');
    });

    afterEach(function(){ sinon.restore(); });

    it('passes the held floor action index on a truncated replica', async function(){
        const db = createMockDb(true);
        const supplies = sinon.stub(balanceHelpers, 'recomputeTokenSupplies').resolves();
        await new ClientRollback(db, new Utility(), undefined, 'regtest').rollback(100);
        assert.strictEqual(supplies.calledOnceWithExactly(db,
            { minTokenActionIndex: 9000 }), true);
        assert.strictEqual(db.commitTransaction.calledOnce, true);
    });

    it('keeps the unscoped call on a full-history replica', async function(){
        const db = createMockDb(false);
        const supplies = sinon.stub(balanceHelpers, 'recomputeTokenSupplies').resolves();
        await new ClientRollback(db, new Utility(), undefined, 'regtest').rollback(100);
        assert.strictEqual(supplies.calledOnceWithExactly(db), true);
        assert.strictEqual(db.commitTransaction.calledOnce, true);
    });
});
