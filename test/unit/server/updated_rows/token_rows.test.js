'use strict';

// Copyright © 2025-2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC - https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

const assert = require('assert');
const {
    collectTokenSupplyRows,
    collectTokenEditRows
} = require('../../../../src/server/updated_rows/token_rows.js');

const CONN = { id: 'conn' };

function makeDb(result){
    const calls = { ledger: [], edited: [] };
    const settle = () => {
        if(result instanceof Error) throw result;
        return result;
    };
    return {
        calls,
        async findLedgerTouchedTokens(...args){
            calls.ledger.push(args);
            return settle();
        },
        async findEditedTokens(...args){
            calls.edited.push(args);
            return settle();
        }
    };
}

function errnoError(errno){
    const err = new Error('db failure');
    if(errno !== undefined) err.errno = errno;
    return err;
}

const COLLECTORS = [
    ['collectTokenSupplyRows', collectTokenSupplyRows],
    ['collectTokenEditRows', collectTokenEditRows]
];

describe('updated rows token classes', function(){

    it('supply class passes from, to and conn through and keys rows by action_index', async function(){
        const rows = [{ action_index: 5, tick: 'A' }, { action_index: 9, tick: 'B' }];
        const db = makeDb(rows);
        const acc = {};
        await collectTokenSupplyRows(db, 100, 200, CONN, acc);
        assert.deepStrictEqual(db.calls.ledger, [[100, 200, CONN]]);
        assert.strictEqual(db.calls.edited.length, 0);
        assert.ok(acc.tokens instanceof Map);
        assert.deepStrictEqual([...acc.tokens.keys()], ['5', '9']);
        assert.strictEqual(acc.tokens.get('5').tick, 'A');
    });

    it('edit class reads through findEditedTokens with from, to and conn', async function(){
        const rows = [{ action_index: 3, tick: 'C' }];
        const db = makeDb(rows);
        const acc = {};
        await collectTokenEditRows(db, 10, 20, CONN, acc);
        assert.deepStrictEqual(db.calls.edited, [[10, 20, CONN]]);
        assert.strictEqual(db.calls.ledger.length, 0);
        assert.ok(acc.tokens instanceof Map);
        assert.deepStrictEqual([...acc.tokens.keys()], ['3']);
    });
});

for(const [name, collect] of COLLECTORS){
    describe(name, function(){
        it('skips rows without an action_index', async function(){
            const rows = [{ tick: 'X' }, { action_index: 4, tick: 'Y' }, { action_index: null }];
            const acc = {};
            await collect(makeDb(rows), 1, 2, CONN, acc);
            assert.deepStrictEqual([...acc.tokens.keys()], ['4']);
        });

        it('leaves acc.tokens undefined for an empty result', async function(){
            const acc = {};
            await collect(makeDb([]), 1, 2, CONN, acc);
            assert.strictEqual(acc.tokens, undefined);
        });

        it('swallows missing table and column errnos', async function(){
            for(const errno of [1146, 1054]){
                const acc = {};
                await collect(makeDb(errnoError(errno)), 1, 2, CONN, acc);
                assert.strictEqual(acc.tokens, undefined);
            }
        });

        it('rethrows other numeric errnos', async function(){
            const err = errnoError(1213);
            await assert.rejects(collect(makeDb(err), 1, 2, CONN, {}), (thrown) => thrown === err);
        });

        it('swallows an error that carries no errno', async function(){
            const acc = {};
            await collect(makeDb(errnoError()), 1, 2, CONN, acc);
            assert.strictEqual(acc.tokens, undefined);
        });
    });
}

describe('findEditedTokens mixin', function(){
    it('runs the valid-issues tokens query with [from, to] and conn third', async function(){
        const calls = [];
        const fake = Object.assign({
            async doQuery(...args){ calls.push(args); return []; }
        }, require('../../../../src/db/token_edit_rows.js'));
        await fake.findEditedTokens(10, 20, CONN);
        assert.strictEqual(calls.length, 1);
        const [sql, params, conn] = calls[0];
        assert.ok(/FROM `tokens`/.test(sql));
        assert.deepStrictEqual(params, [10, 20]);
        assert.strictEqual(conn, CONN);
    });
});
