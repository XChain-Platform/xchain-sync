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
// The in-place updated-rows classes skip only a schema gap (errno 1146/1054). An
// error with no errno is a fault, so it must reach the caller's isSchemaGapError gate,
// which retries the block rather than shipping it without the class's rows.

'use strict';

const assert = require('assert');
const sinon  = require('sinon');
const { collectUpdatedRows } = require('../../../src/server/updated_rows');
const { fakeDb } = require('./helpers/fake_db.js');

// One SQL fragment per class, unique to the query that class runs.
const CLASS_SQL = [
    ['deactivation_block stamp', 'deactivation_block IS NOT NULL'],
    ['SLASH debit join', 'capability_slash_debits'],
    ['DELEGATE key rotation', 'JOIN `contract_delegation_rotations`'],
    ['request_status flip', 'WHERE version = 0 AND resolved_block'],
    ['poll finalization', 'callback_due_block BETWEEN'],
    ['cooldown-maturity status flip', 'cooldown_end_block BETWEEN'],
    ['BET status flip', 'FROM `bets` WHERE'],
    ['invalid_archive stamp', 'FROM anchor_actions p'],
    ['attest batch-head verdict', 'JOIN attests ac ON ac.request_id = ah.request_id'],
    ['COINPay match promotion', 'FROM `order_matches` m'],
];

// A database whose query for `fragment` throws `err` and whose other queries answer [].
function dbThrowingOn(fragment, err){
    let db = fakeDb([]);
    db.doQuery = sinon.stub().callsFake(async (sql) => {
        if(sql.indexOf(fragment) !== -1) throw err;
        return [];
    });
    return db;
}

describe('updatedRows.collectUpdatedRows errno-less errors @regression', function(){

    afterEach(() => sinon.restore());

    for(const [name, fragment] of CLASS_SQL){
        it('re-throws an errno-less error from the ' + name + ' class', async function(){
            let err = new Error('socket hang up');
            await assert.rejects(() => collectUpdatedRows(dbThrowingOn(fragment, err), 300, 300, 6),
                (thrown) => thrown === err);
        });
    }

    it('re-throws a TypeError raised inside the cooldown-maturity class', async function(){
        let err = new TypeError("Cannot read properties of undefined (reading 'query')");
        await assert.rejects(() => collectUpdatedRows(dbThrowingOn('cooldown_end_block BETWEEN', err), 300, 300, 6),
            (thrown) => thrown === err);
    });

    it('still skips a schema gap (errno 1146 or 1054) in the cooldown-maturity class', async function(){
        for(const errno of [1146, 1054]){
            let gap = Object.assign(new Error('schema gap ' + errno), { errno });
            let out = await collectUpdatedRows(dbThrowingOn('cooldown_end_block BETWEEN', gap), 300, 300, 6);
            assert.strictEqual(out.unstakes, undefined);
            assert.strictEqual(out.contract_unstakes, undefined);
        }
    });
});
