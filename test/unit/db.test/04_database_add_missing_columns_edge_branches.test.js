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

describe('Database.addMissingColumns(): edge branches', function () {
    let db;
    beforeEach(function () { silenceConsole(); db = makeDb(); });
    afterEach(async function () { sinon.restore(); await db.close(); });

    // Line 210: r.COLUMN_NAME fallback (uppercase key)
    it('reads COLUMN_NAME (uppercase) from information_schema rows', async function () {
        let ddl = [
            'CREATE TABLE `t` (',
            '  `myfield` int(11) NOT NULL,',
            '  PRIMARY KEY (`id`)',
            ') ENGINE=InnoDB'
        ].join('\n');
        sinon.stub(db, 'doQuery').callsFake(async (sql) => {
            if (/information_schema\.columns/.test(sql))
                return [{ COLUMN_NAME: 'myfield' }];  // uppercase key
            return [];
        });
        // myfield is already present (via COLUMN_NAME) so nothing added
        let added = await db.addMissingColumns('t', ddl);
        assert.strictEqual(added, 0);
    });

    // A refused ALTER leaves the column missing, so it must reach the caller as a
    // failure: the old swallow let the replica report a healed schema and
    // then stall on errno 1054 forever.
    it('throws and logs when ALTER TABLE fails', async function () {
        let ddl = [
            'CREATE TABLE `t` (',
            '  `newcol` int(11) DEFAULT NULL,',
            '  PRIMARY KEY (`id`)',
            ') ENGINE=InnoDB'
        ].join('\n');
        let alterCalled = false;
        sinon.stub(db, 'doQuery').callsFake(async (sql) => {
            if (/information_schema\.columns/.test(sql)) return [];  // column missing
            if (/ALTER TABLE/.test(sql)) {
                alterCalled = true;
                let e = new Error('alter fail');
                e.errno = 1075;
                throw e;
            }
            return [];
        });
        let thrown = null;
        try { await db.addMissingColumns('t', ddl); } catch (e) { thrown = e; }
        assert.ok(thrown, 'addMissingColumns must throw when an ALTER is refused');
        assert.strictEqual(thrown.errno, 1075);
        assert.deepStrictEqual(thrown.failedColumns.map(f => f.column), ['newcol']);
        assert.ok(alterCalled);
        assert.ok(console.error.called);
    });
});


describe('Database.getBlockHashRow()', function () {
    afterEach(async function () { sinon.restore(); });

    it('indexer: returns row with ledger/actions/contract hashes', async function () {
        silenceConsole();
        let db = makeDb('indexer');
        let row = { block_index: 100, ledger_hash: 'abc', actions_hash: 'def', contract_hash: 'ghi' };
        sinon.stub(db, 'doQuery').resolves([row]);
        let result = await db.getBlockHashRow(100);
        assert.deepStrictEqual(result, row);
        let sql = db.doQuery.firstCall.args[0];
        assert.ok(sql.includes('ledger_hash'));
        await db.close();
    });

    it('indexer: returns null when no rows', async function () {
        silenceConsole();
        let db = makeDb('indexer');
        sinon.stub(db, 'doQuery').resolves([]);
        let result = await db.getBlockHashRow(999);
        assert.strictEqual(result, null);
        await db.close();
    });

    it('swallows a query error and returns null by default (fail-soft preserved)', async function () {
        silenceConsole();
        let db = makeDb('indexer');
        let conn = fakeConn();
        conn.query.rejects(new Error('hash row blip'));
        sinon.stub(db, 'getConnection').resolves(conn);
        assert.strictEqual(await db.getBlockHashRow(100), null);
        await db.close();
    });

    it('PROPAGATES a query error with opts.rethrow (fail-closed duplicate guard)', async function () {
        // ClientApplier's already-applied guard reads null as "never applied" and
        // re-INSERTs the block's ledger rows, so opts must reach doQuery.
        silenceConsole();
        let db = makeDb('indexer');
        let conn = fakeConn();
        conn.query.rejects(new Error('hash row blip'));
        sinon.stub(db, 'getConnection').resolves(conn);
        await assert.rejects(
            () => db.getBlockHashRow(100, null, { rethrow: true }),
            /hash row blip/
        );
        await db.close();
    });

});

describe('Database.getBlockHashRow()', function () {
    afterEach(async function () { sinon.restore(); });

    it('decoder: returns row with block_hash only', async function () {
        silenceConsole();
        let db = makeDb('decoder');
        let row = { block_index: 100, block_hash: 'xyz' };
        sinon.stub(db, 'doQuery').resolves([row]);
        let result = await db.getBlockHashRow(100);
        assert.deepStrictEqual(result, row);
        let sql = db.doQuery.firstCall.args[0];
        assert.ok(sql.includes('block_hash'));
        assert.ok(!sql.includes('ledger_hash'));
        await db.close();
    });

    it('decoder: returns null when no rows', async function () {
        silenceConsole();
        let db = makeDb('decoder');
        sinon.stub(db, 'doQuery').resolves([]);
        let result = await db.getBlockHashRow(999);
        assert.strictEqual(result, null);
        await db.close();
    });
});


describe('Database.getBlockRows()', function () {
    afterEach(async function () { sinon.restore(); });

    it('indexer: uses ledger/actions/contract hash columns', async function () {
        silenceConsole();
        let db = makeDb('indexer');
        let rows = [{ block_index: 1 }];
        sinon.stub(db, 'doQuery').resolves(rows);
        let result = await db.getBlockRows(1, 10);
        assert.strictEqual(result, rows);
        let sql = db.doQuery.firstCall.args[0];
        assert.ok(sql.includes('ledger_hash'));
        await db.close();
    });

    it('decoder: uses block_hash column', async function () {
        silenceConsole();
        let db = makeDb('decoder');
        let rows = [{ block_index: 1 }];
        sinon.stub(db, 'doQuery').resolves(rows);
        let result = await db.getBlockRows(1, 10);
        assert.strictEqual(result, rows);
        let sql = db.doQuery.firstCall.args[0];
        assert.ok(sql.includes('block_hash'));
        assert.ok(!sql.includes('ledger_hash'));
        await db.close();
    });
});


describe('Database.getFirstActionIndex()', function () {
    let db;
    beforeEach(function () { silenceConsole(); db = makeDb(); });
    afterEach(async function () { sinon.restore(); await db.close(); });

    it('returns Number when found', async function () {
        sinon.stub(db, 'doQuery').resolves([{ action_index: 42 }]);
        let result = await db.getFirstActionIndex(100);
        assert.strictEqual(result, 42);
    });

    it('returns null when no rows', async function () {
        sinon.stub(db, 'doQuery').resolves([]);
        let result = await db.getFirstActionIndex(100);
        assert.strictEqual(result, null);
    });

    it('swallows a query error and returns null by default (fail-soft preserved)', async function () {
        let conn = fakeConn();
        conn.query.rejects(new Error('actions read blip'));
        sinon.stub(db, 'getConnection').resolves(conn);
        assert.strictEqual(await db.getFirstActionIndex(100), null);
    });

    it('PROPAGATES a query error with opts.rethrow (fail-closed rollback gate)', async function () {
        // ClientRollback gates every action-scoped delete on this value while the
        // block/index sweeps run unconditionally, so a swallowed fault would commit a
        // PARTIAL ledger rollback. opts must reach doQuery for that to fail closed.
        let conn = fakeConn();
        conn.query.rejects(new Error('actions read blip'));
        sinon.stub(db, 'getConnection').resolves(conn);
        await assert.rejects(
            () => db.getFirstActionIndex(100, null, { rethrow: true }),
            /actions read blip/
        );
    });
});


describe('Database.getBlockScopedRows()', function () {
    let db;
    beforeEach(function () { silenceConsole(); db = makeDb(); });
    afterEach(async function () { sinon.restore(); await db.close(); });

    it('queries the given table by block_index', async function () {
        let rows = [{ id: 1 }];
        sinon.stub(db, 'doQuery').resolves(rows);
        let result = await db.getBlockScopedRows('blocks', 5);
        assert.strictEqual(result, rows);
        let sql = db.doQuery.firstCall.args[0];
        assert.ok(sql.includes('blocks'));
        assert.ok(sql.includes('block_index'));
    });

    it('scopes a close_block-keyed table by close_block, not the class default', async function () {
        // rollcalls and rollcall_absences are declared stream:block and have no
        // block_index column, so the old literal raised errno 1054, which
        // ServerPoller classifies as an older source schema and drops without a log
        // line: the tables were streamed by membership and delivered by nothing.
        // ServerPoller.test.js asserts the membership; this asserts the read.
        sinon.stub(db, 'doQuery').resolves([]);
        await db.getBlockScopedRows('rollcalls', 900);
        await db.getBlockScopedRows('rollcall_absences', 900);
        assert.strictEqual(db.doQuery.firstCall.args[0],
            'SELECT * FROM `rollcalls` WHERE close_block = ? ORDER BY close_block ASC, 1 ASC');
        assert.strictEqual(db.doQuery.secondCall.args[0],
            'SELECT * FROM `rollcall_absences` WHERE close_block = ? ORDER BY close_block ASC, 1 ASC');
    });

    it('leaves every other block-scoped table on block_index', async function () {
        // The registry field defaults, so the eleven tables that really are
        // block_index-scoped must produce byte-identical SQL to before the change.
        sinon.stub(db, 'doQuery').resolves([]);
        for(const table of ['blocks', 'transactions', 'slash_events', 'escrow_leaf_journal'])
            await db.getBlockScopedRows(table, 5);
        for(const call of db.doQuery.getCalls())
            assert.match(call.args[0], /WHERE block_index = \? ORDER BY block_index ASC, 1 ASC$/);
    });
});


describe('Database.getActionScopedRows()', function () {
    let db;
    beforeEach(function () { silenceConsole(); db = makeDb(); });
    afterEach(async function () { sinon.restore(); await db.close(); });

    it('queries with action/transaction join', async function () {
        let rows = [{ action_index: 1 }];
        sinon.stub(db, 'doQuery').resolves(rows);
        let result = await db.getActionScopedRows('orders', 10);
        assert.strictEqual(result, rows);
        let sql = db.doQuery.firstCall.args[0];
        assert.ok(sql.includes('actions'));
        assert.ok(sql.includes('orders'));
    });
});


describe('Database.getNonEmptyActionScopedTables()', function () {
    let db;
    beforeEach(function () { silenceConsole(); db = makeDb(); });
    afterEach(async function () { sinon.restore(); await db.close(); });

    it('probes every existing candidate in ONE round-trip, with getActionScopedRows\' predicate', async function () {
        sinon.stub(db, 'listExistingTables').resolves(new Set(['sends', 'credits', 'orders']));
        sinon.stub(db, 'doQueryStrict').resolves([{ tbl: 'sends' }, { tbl: 'orders' }]);

        let result = await db.getNonEmptyActionScopedTables(['sends', 'credits', 'orders'], 10);

        assert.strictEqual(db.doQueryStrict.callCount, 1, 'one query, not one per table');
        let [sql, args] = db.doQueryStrict.firstCall.args;
        assert.strictEqual((sql.match(/UNION ALL/g) || []).length, 2, 'three branches, two joiners');
        // The equivalence that makes skipping a table safe: same join, same block scope
        // as getActionScopedRows, so "probe says empty" IS "that fetch returns []".
        assert.strictEqual((sql.match(/INNER JOIN actions a ON \(a\.action_index = t\.action_index\)/g) || []).length, 3);
        assert.strictEqual((sql.match(/WHERE a\.block_index = \? LIMIT 1/g) || []).length, 3);
        assert.deepStrictEqual(args, [10, 10, 10]);
        assert.deepStrictEqual([...result].sort(), ['orders', 'sends']);
    });

    it('drops candidates the source schema does not have (one missing table fails the whole UNION)', async function () {
        sinon.stub(db, 'listExistingTables').resolves(new Set(['sends']));
        sinon.stub(db, 'doQueryStrict').resolves([]);

        await db.getNonEmptyActionScopedTables(['sends', 'bet_resolves'], 10);

        let sql = db.doQueryStrict.firstCall.args[0];
        assert.ok(sql.includes('`sends`'));
        assert.ok(!sql.includes('bet_resolves'), 'a table the source lacks must not enter the UNION');
    });

    it('issues no query at all when no candidate exists', async function () {
        sinon.stub(db, 'listExistingTables').resolves(new Set());
        sinon.stub(db, 'doQueryStrict').resolves([]);

        let result = await db.getNonEmptyActionScopedTables(['sends'], 10);

        assert.strictEqual(db.doQueryStrict.callCount, 0);
        assert.strictEqual(result.size, 0);
    });

    it('refuses an unsafe table identifier before it reaches the query string', async function () {
        sinon.stub(db, 'listExistingTables').resolves(new Set(['sends']));
        sinon.stub(db, 'doQueryStrict').resolves([]);

        await assert.rejects(
            () => db.getNonEmptyActionScopedTables(['sends`, (SELECT 1)'], 10),
            /unsafe table identifier/);
    });
});
