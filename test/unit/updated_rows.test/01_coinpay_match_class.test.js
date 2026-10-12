// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.
//
// The COINPay match-promotion class of the in-place updated-rows channel.

'use strict';

const assert = require('assert');
const sinon  = require('sinon');
const { collectUpdatedRows } = require('../../../src/server/updated_rows');
const { fakeDb } = require('./helpers/fake_db.js');

describe('updatedRows.collectUpdatedRows', function(){

    afterEach(() => sinon.restore());

    // A later COINPAY promotes an earlier block's match row in place; the class carries
    // the source's current row, keyed by the 'fulfilled' status row's own block window.
    const COINPAY_MATCH_SQL = 'FROM `order_matches` m';

    it('carries COINPay-settled order_matches rows keyed by the window of the fulfilled status', async function(){
        let db = fakeDb([
            { match: COINPAY_MATCH_SQL,
              rows: [{ action_index: 185, settlement_type: 'coinpay', status_id: 4 }] }
        ]);
        let out = await collectUpdatedRows(db, 782, 790, 6);
        assert.deepStrictEqual(out.order_matches, [{ action_index: 185, settlement_type: 'coinpay', status_id: 4 }]);
        let q = db.calls.find(c => c.sql.indexOf(COINPAY_MATCH_SQL) !== -1);
        assert.deepStrictEqual(q.args, [782, 790]);
        assert.ok(/si\.status = 'fulfilled' AND a\.block_index BETWEEN \? AND \?/.test(q.sql),
            'the window must key on the fulfilled status row\'s own action block');
    });

    it('skips the COINPay match class on a pre-COINPay schema instead of throwing', async function(){
        let db = fakeDb([]);
        db.doQuery = sinon.stub().callsFake(async (sql) => {
            if(sql.indexOf(COINPAY_MATCH_SQL) !== -1){
                let e = new Error("Table 'coinpay_statuses' doesn't exist");
                e.errno = 1146;
                throw e;
            }
            return [];
        });
        let out = await collectUpdatedRows(db, 300, 300, 6);
        assert.strictEqual(out.order_matches, undefined);
    });

    it('rethrows a non-schema error from the COINPay match class (never a silent drop)', async function(){
        let db = fakeDb([]);
        db.doQuery = sinon.stub().callsFake(async (sql) => {
            if(sql.indexOf(COINPAY_MATCH_SQL) !== -1){
                let e = new Error('Deadlock found when trying to get lock');
                e.errno = 1213;
                throw e;
            }
            return [];
        });
        await assert.rejects(() => collectUpdatedRows(db, 300, 300, 6), /Deadlock found/);
    });
});
