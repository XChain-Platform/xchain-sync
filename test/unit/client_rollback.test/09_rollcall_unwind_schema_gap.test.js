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

function schemaError(errno){
    const e = new Error('schema gap ' + errno);
    e.errno = errno;
    return e;
}

// A mock db whose doQuery rejects any statement on a table listed in `missing`.
function createMockDb(missing){
    return {
        doQuery: sinon.stub().callsFake(async (sql) => {
            for(const table of missing)
                if(typeof sql === 'string' && sql.startsWith('DELETE FROM ' + table + ' WHERE close_block'))
                    throw schemaError(1146);
            return [];
        }),
        getFirstActionIndex: sinon.stub().resolves(500),
        getStatusId: sinon.stub().resolves(null),
        beginTransaction: sinon.stub().resolves(),
        commitTransaction: sinon.stub().resolves(),
        rollbackTransaction: sinon.stub().resolves()
    };
}

function closeBlockDeletes(db){
    return db.doQuery.getCalls()
        .map(c => c.args[0])
        .filter(sql => typeof sql === 'string' && /^DELETE FROM rollcall\w* WHERE close_block >= \?$/.test(sql));
}

// The replica mirrors the source indexer's roll-call unwind. rollcall_gates arrives
// in a later migration than rollcalls and rollcall_absences, so a replica missing
// only that table must still unwind the older two.
describe('ClientRollback roll-call unwind schema-gap guard @regression', function(){
    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'warn');
        sinon.stub(console, 'error');
    });
    afterEach(function(){ sinon.restore(); });

    it('deletes gates, absences and verdicts in that order on a migrated replica', async function(){
        const db = createMockDb([]);
        await new ClientRollback(db, new Utility(), 'BTC', 'regtest').rollback(100);
        assert.deepStrictEqual(closeBlockDeletes(db), [
            'DELETE FROM rollcall_gates WHERE close_block >= ?',
            'DELETE FROM rollcall_absences WHERE close_block >= ?',
            'DELETE FROM rollcalls WHERE close_block >= ?'
        ]);
    });

    it('still unwinds absences and verdicts when rollcall_gates is missing', async function(){
        const db = createMockDb(['rollcall_gates']);
        await new ClientRollback(db, new Utility(), 'BTC', 'regtest').rollback(100);
        assert.deepStrictEqual(closeBlockDeletes(db), [
            'DELETE FROM rollcall_gates WHERE close_block >= ?',
            'DELETE FROM rollcall_absences WHERE close_block >= ?',
            'DELETE FROM rollcalls WHERE close_block >= ?'
        ]);
        assert.strictEqual(db.rollbackTransaction.called, false);
    });
});
