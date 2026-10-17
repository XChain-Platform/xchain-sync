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

const BRIDGED_CLEAR = /^\s*UPDATE tokens SET bridged=0\s+WHERE bridged=1 AND NOT EXISTS/;

// tokens.bridged is set IN PLACE on a surviving token row by the first applied XBRIDGE
// v3 lock, and the source's reorg clears it again when no valid v3 lock survives
// (xchain-indexer rederiveTokenBridged). Nothing re-sends the token row after a reorg,
// so the replica rollback has to run the same clear or the explorer keeps a BRIDGED
// badge the source does not hold.
let rollback, db;

function registerHooks(){
    beforeEach(function(){
        db = createMockDb();
        rollback = new ClientRollback(db, new Utility(), undefined, 'regtest');
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });

    afterEach(function(){
        sinon.restore();
    });
}

describe('ClientRollback tokens.bridged re-derive @regression', function(){
    registerHooks();

    it('clears bridged on tokens with no surviving valid v3 lock after the xbridges delete', async function(){
        await rollback.rollback(100);
        const calls = db.doQuery.getCalls();
        const clearIdx = calls.findIndex(c => BRIDGED_CLEAR.test(c.args[0]));
        const xbridgesDelIdx = calls.findIndex(c => /DELETE FROM `xbridges` WHERE action_index >= \?/.test(c.args[0]));
        assert.ok(clearIdx >= 0, 'the replica rollback must re-derive tokens.bridged');
        assert.ok(xbridgesDelIdx >= 0, 'xbridges must be in the action-scoped delete');
        assert.ok(xbridgesDelIdx < clearIdx,
            'the re-derive must read xbridges AFTER the orphaned locks are deleted');
        const sql = calls[clearIdx].args[0];
        assert.ok(/x\.tick_id=tokens\.tick_id/.test(sql) && /x\.version=3/.test(sql) && /s\.status='valid'/.test(sql),
            'only an applied (valid) v3 lock of the same tick keeps the bit');
        assert.strictEqual(db.commitTransaction.calledOnce, true);
    });

    it('runs between the escrow and COINPay re-derives, as the source does', async function(){
        await rollback.rollback(100);
        const calls = db.doQuery.getCalls();
        const escrowIdx = calls.findIndex(c => /t\.escrow_action_index IS NOT NULL/.test(c.args[0]));
        const clearIdx = calls.findIndex(c => BRIDGED_CLEAR.test(c.args[0]));
        const coinpayIdx = calls.findIndex(c => /UPDATE order_matches SET status_id=/.test(c.args[0]));
        assert.ok(escrowIdx >= 0 && clearIdx >= 0 && coinpayIdx >= 0);
        assert.ok(escrowIdx < clearIdx && clearIdx < coinpayIdx);
    });

    it('does not run when the range holds no actions', async function(){
        db.getFirstActionIndex = sinon.stub().resolves(null);
        await rollback.rollback(100);
        assert.ok(!db.doQuery.getCalls().some(c => BRIDGED_CLEAR.test(c.args[0])));
    });

});

describe('ClientRollback tokens.bridged re-derive guards @regression', function(){
    registerHooks();

    // A truncated replica holds only [base..tip] of xbridges, so a token whose first lock
    // sits below the floor has no local lock row and would be cleared wrongly.
    it('skips the re-derive on a truncated replica', async function(){
        db.getSyncState = sinon.stub().resolves('900');
        await rollback.rollback(100);
        assert.ok(!db.doQuery.getCalls().some(c => BRIDGED_CLEAR.test(c.args[0])),
            'no bridged clear on a truncated replica');
        assert.strictEqual(db.commitTransaction.calledOnce, true);
    });

    it('tolerates a schema gap (no xbridges table or bridged column)', async function(){
        db.doQuery.callsFake(async (query) => {
            if(BRIDGED_CLEAR.test(query)) throw Object.assign(new Error("Table 'xbridges' doesn't exist"), { errno: 1146 });
            return [];
        });
        await rollback.rollback(100);
        assert.strictEqual(db.commitTransaction.calledOnce, true);
    });

    it('aborts (fail-closed) on a transient error in the re-derive', async function(){
        db.doQuery.callsFake(async (query) => {
            if(BRIDGED_CLEAR.test(query)) throw Object.assign(new Error('Deadlock found'), { errno: 1213 });
            return [];
        });
        await assert.rejects(() => rollback.rollback(100), { errno: 1213 });
        assert.strictEqual(db.commitTransaction.called, false, 'a partial rollback must never commit');
        assert.strictEqual(db.rollbackTransaction.calledOnce, true);
    });

    it('is exported on the class for the shared call shape', function(){
        assert.strictEqual(typeof ClientRollback.rederiveTokenBridged, 'function');
    });
});
