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
const ClientApplier = require('../../../src/client/applier');
const Utility = require('../../../src/util');
const { SCHEMA_VERSION } = require('../../../src/schema/version');
const balanceHelpers = require('../../../src/db/balance_helpers');
const { withDbMixins } = require('../../helpers/db_mixins.js');

function createMockDb(){
    return withDbMixins({
        doQuery: sinon.stub().resolves([]),
        getBlockHashRow: sinon.stub().resolves(null),
        beginTransaction: sinon.stub().resolves(),
        commitTransaction: sinon.stub().resolves(),
        rollbackTransaction: sinon.stub().resolves(),
        truncateTable: sinon.stub().resolves()
    });
}

describe('ClientApplier', function(){

    let applier, db, util;

    beforeEach(function(){
        db = createMockDb();
        util = new Utility();
        applier = new ClientApplier(db, util);
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });

    afterEach(function(){
        sinon.restore();
    });

    describe('applyDispensersReplace', function(){
        it('is a no-op on a non-decoder DB', async function(){
            // createMockDb has no dbType (indexer-shaped); the guard short-circuits.
            await applier.applyDispensersReplace([{ tx_index: 1, address_id: 2 }]);
            assert.strictEqual(db.beginTransaction.called, false);
            assert.strictEqual(db.doQuery.called, false);
        });

        it('replaces atomically: DELETE then INSERT inside one transaction (decoder)', async function(){
            db.dbType = 'decoder';
            await applier.applyDispensersReplace([
                { tx_index: 5, address_id: 9, expiration: 1700000000, expired_block_index: null },
                { tx_index: 7, address_id: 2, expiration: 1700000001, expired_block_index: 42 }
            ]);
            assert.ok(db.beginTransaction.calledOnce, 'opened a transaction');
            assert.ok(db.commitTransaction.calledOnce, 'committed');
            assert.strictEqual(db.rollbackTransaction.called, false);
            let calls = db.doQuery.getCalls().map(c => c.args[0]);
            assert.ok(/^DELETE FROM `dispensers`/.test(calls[0]), 'DELETE runs first');
            assert.ok(calls.some(q => /^INSERT INTO `dispensers`/.test(q)), 'rows re-inserted with plain INSERT (not IGNORE)');
        });

        it('clears the table even when the new set is empty (decoder)', async function(){
            db.dbType = 'decoder';
            await applier.applyDispensersReplace([]);
            assert.ok(db.beginTransaction.calledOnce);
            assert.ok(db.commitTransaction.calledOnce);
            let calls = db.doQuery.getCalls().map(c => c.args[0]);
            assert.ok(/^DELETE FROM `dispensers`/.test(calls[0]));
            assert.ok(!calls.some(q => /^INSERT/.test(q)), 'no INSERT for an empty set');
        });

        it('rolls back and rethrows if a write fails (decoder, table left intact)', async function(){
            db.dbType = 'decoder';
            db.doQuery.rejects(new Error('boom'));
            let threw = false;
            try { await applier.applyDispensersReplace([{ tx_index: 1, address_id: 2 }]); }
            catch(e){ threw = true; }
            assert.ok(threw, 'error propagates so the caller can leave the table intact');
            assert.ok(db.rollbackTransaction.calledOnce, 'transaction rolled back');
            assert.strictEqual(db.commitTransaction.called, false);
        });
    });
});
