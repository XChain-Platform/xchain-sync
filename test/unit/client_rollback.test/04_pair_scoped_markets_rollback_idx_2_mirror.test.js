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

// The dangling-tick markets sweep is the only markets-rollback path: a market
// whose pair keeps both ticks but loses its only order/trade to the reorg is
// deleted on the source and retained here. markets rides the snapshot as an
// UPSERT-only full dump, so no later replication removes the stale zeroed
// OHLCV row that xchain-explorer then serves.
// Answer the affected-pair collection with one pair, everything else empty, so
// the survival probes find no surviving orders/order_matches for it.
function orphanOnePair(pair){
    db.doQuery.callsFake(async (query) => {
        if(/FROM orders\b[\s\S]*action_index >= \?/.test(query) && /UNION/.test(query))
            return [{ tick1_id: pair[0], tick2_id: pair[1] }];
        return [];
    });
}

describe('ClientRollback', function(){

    describe('pair-scoped markets rollback (IDX-2 mirror)', function(){
        registerHooks();
        it('deletes a market whose pair kept no surviving order or match', async function(){
            orphanOnePair([7, 9]);
            await rollback.rollback(100);
            let del = db.doQuery.getCalls().find(c =>
                /DELETE FROM markets WHERE \(tick1_id=\? AND tick2_id=\?\)/.test(c.args[0]));
            assert.ok(del, 'pair-scoped markets delete must run for an orphaned pair');
            assert.deepStrictEqual(del.args[1], [7, 9, 9, 7], 'both pair orientations must be deleted');
            assert.strictEqual(db.commitTransaction.calledOnce, true);
        });

        it('collects the affected pairs BEFORE the action-scoped delete removes them', async function(){
            orphanOnePair([7, 9]);
            await rollback.rollback(100);
            let calls = db.doQuery.getCalls();
            let collectIdx = calls.findIndex(c => /UNION/.test(c.args[0]) && /order_matches/.test(c.args[0]));
            let ordersDelIdx = calls.findIndex(c => /DELETE FROM `orders` WHERE action_index >= \?/.test(c.args[0]));
            assert.ok(collectIdx >= 0 && ordersDelIdx >= 0);
            assert.ok(collectIdx < ordersDelIdx,
                'the pair collection must read orders before the dataTables delete empties the orphaned range');
        });

        it('keeps the market when a surviving order still references the pair', async function(){
            db.doQuery.callsFake(async (query) => {
                if(/FROM orders\b[\s\S]*action_index >= \?/.test(query) && /UNION/.test(query))
                    return [{ tick1_id: 7, tick2_id: 9 }];
                if(/SELECT 1 FROM orders WHERE \(COALESCE\(give_tick_id,0\)=\?/.test(query)) return [{ 1: 1 }];
                return [];
            });
            await rollback.rollback(100);
            assert.ok(!db.doQuery.getCalls().some(c =>
                /DELETE FROM markets WHERE \(tick1_id=\?/.test(c.args[0])),
                'a pair with a surviving order must keep its market row');
        });

        it('keeps the market when only an order_match survives', async function(){
            db.doQuery.callsFake(async (query) => {
                if(/FROM orders\b[\s\S]*action_index >= \?/.test(query) && /UNION/.test(query))
                    return [{ tick1_id: 7, tick2_id: 9 }];
                if(/SELECT 1 FROM order_matches WHERE \(COALESCE\(give_tick_id,0\)=\?/.test(query)) return [{ 1: 1 }];
                return [];
            });
            await rollback.rollback(100);
            assert.ok(!db.doQuery.getCalls().some(c =>
                /DELETE FROM markets WHERE \(tick1_id=\?/.test(c.args[0])),
                'a pair with a surviving match must keep its market row');
        });

    });
});

describe('ClientRollback', function(){

    describe('pair-scoped markets rollback (IDX-2 mirror)', function(){
        registerHooks();
        // A truncated replica holds only [base..tip] of orders/order_matches, so "no
        // surviving reference" there can simply mean "traded below my join floor". The
        // sweep is skipped rather than deleting a market the source still keeps.
        it('skips the sweep on a truncated replica', async function(){
            db.getSyncState = sinon.stub().resolves('900');
            orphanOnePair([7, 9]);
            await rollback.rollback(100);
            assert.ok(!db.doQuery.getCalls().some(c => /UNION/.test(c.args[0]) && /order_matches/.test(c.args[0])),
                'no pair collection on a truncated replica');
            assert.ok(!db.doQuery.getCalls().some(c => /DELETE FROM markets WHERE \(tick1_id=\?/.test(c.args[0])),
                'no pair-scoped markets delete on a truncated replica');
        });

        it('aborts (fail-closed) on a transient error in the pair-scoped sweep', async function(){
            db.doQuery.callsFake(async (query) => {
                if(/FROM orders\b[\s\S]*action_index >= \?/.test(query) && /UNION/.test(query))
                    return [{ tick1_id: 7, tick2_id: 9 }];
                if(/SELECT 1 FROM orders WHERE \(COALESCE\(give_tick_id,0\)=\?/.test(query))
                    throw Object.assign(new Error('Lock wait timeout'), { errno: 1205 });
                return [];
            });
            await assert.rejects(() => rollback.rollback(100), { errno: 1205 });
            assert.strictEqual(db.commitTransaction.called, false, 'a partial rollback must never commit');
        });

        it('swallows a schema gap in the pair-scoped sweep', async function(){
            db.doQuery.callsFake(async (query) => {
                if(/FROM orders\b[\s\S]*action_index >= \?/.test(query) && /UNION/.test(query))
                    throw Object.assign(new Error('Table does not exist'), { errno: 1146 });
                return [];
            });
            await rollback.rollback(100);
            assert.strictEqual(db.commitTransaction.calledOnce, true);
        });
    });
});
