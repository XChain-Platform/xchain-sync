// Copyright © 2025–2026 Dankest, LLC
// SPDX-License-Identifier: AGPL-3.0-or-later

'use strict';

const {
    assert,
    sinon,
    makeDb,
    fakeConn,
    silenceConsole,
} = require('./support/helpers');

// getSyncState is fail-soft by default (a fault reads as "unset"), and a caller
// whose decision must not run on a swallowed fault opts into rethrow. The reorg
// rollback's truncation-floor read is that caller: a fault read as "unset" makes a
// truncated replica run full-history re-derives against history it does not hold.
describe('Database.getSyncState() strict read', function () {
    let db;
    beforeEach(function () { silenceConsole(); db = makeDb(); });
    afterEach(async function () { sinon.restore(); await db.close(); });

    // Make the CREATE TABLE succeed and the SELECT fail, the shape of a lock-wait
    // timeout landing on the read itself.
    function failingSelect(){
        let conn = fakeConn();
        let err = new Error('Lock wait timeout exceeded'); err.errno = 1205;
        conn.query.callsFake((sql) => /^CREATE TABLE/.test(sql) ? Promise.resolve([]) : Promise.reject(err));
        sinon.stub(db, 'getConnection').resolves(conn);
        return err;
    }

    it('keeps the fail-soft default: a failing SELECT resolves null', async function () {
        failingSelect();
        assert.strictEqual(await db.getSyncState('bootstrap_base:indexer'), null);
    });

    it('rejects with the query error when opts.rethrow is set and the SELECT fails', async function () {
        let err = failingSelect();
        await assert.rejects(() => db.getSyncState('bootstrap_base:indexer', { rethrow: true }), (e) => e === err);
    });

    it('rejects when opts.rethrow is set and the on-demand CREATE fails, leaving the table unmarked', async function () {
        let conn = fakeConn();
        let err = new Error('connection dropped'); err.errno = 2013;
        conn.query.callsFake((sql) => /^CREATE TABLE/.test(sql) ? Promise.reject(err) : Promise.resolve([]));
        sinon.stub(db, 'getConnection').resolves(conn);
        await assert.rejects(() => db.getSyncState('bootstrap_base:indexer', { rethrow: true }), (e) => e === err);
        assert.ok(!db._syncStateReady, 'a failed CREATE must not mark the table ready');
    });

    it('returns the stored value under opts.rethrow when both statements succeed', async function () {
        let conn = fakeConn();
        conn.query.callsFake((sql) => /^CREATE TABLE/.test(sql)
            ? Promise.resolve([]) : Promise.resolve([{ state_value: '900' }]));
        sinon.stub(db, 'getConnection').resolves(conn);
        assert.strictEqual(await db.getSyncState('bootstrap_base:indexer', { rethrow: true }), '900');
        assert.strictEqual(db._syncStateReady, true);
    });
});
