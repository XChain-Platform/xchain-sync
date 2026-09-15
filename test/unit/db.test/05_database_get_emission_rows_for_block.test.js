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

describe('Database.getEmissionRowsForBlock()', function () {
    let db;
    beforeEach(function () { silenceConsole(); db = makeDb(); });
    afterEach(async function () { sinon.restore(); await db.close(); });

    it('joins through contract_executions on execution_index (includes NULL action_index rows)', async function () {
        let rows = [{ execution_index: 1, emitted_action: 'SLASH', action_index: null, position: 0 }];
        sinon.stub(db, 'doQuery').resolves(rows);
        let result = await db.getEmissionRowsForBlock(10);
        assert.strictEqual(result, rows);
        let sql = db.doQuery.firstCall.args[0];
        assert.ok(sql.includes('contract_emissions'));
        assert.ok(sql.includes('contract_executions'));
        assert.ok(sql.includes('ce.action_index = em.execution_index'));
        // Must NOT use the action_index-scoped join (which drops NULL-action_index emissions).
        assert.ok(!sql.includes('a.action_index = em.action_index'));
    });

    it('selects only the four protocol columns, never em.* (would carry the id PK)', async function () {
        sinon.stub(db, 'doQuery').resolves([]);
        await db.getEmissionRowsForBlock(5);
        let sql = db.doQuery.firstCall.args[0];
        assert.ok(sql.includes('em.execution_index'));
        assert.ok(sql.includes('em.emitted_action'));
        assert.ok(sql.includes('em.action_index'));
        assert.ok(sql.includes('em.position'));
        assert.ok(!sql.includes('em.*'));
    });
});


describe('Database.getTxScopedRows()', function () {
    let db;
    beforeEach(function () { silenceConsole(); db = makeDb(); });
    afterEach(async function () { sinon.restore(); await db.close(); });

    it('queries with tx join', async function () {
        let rows = [{ tx_index: 1 }];
        sinon.stub(db, 'doQuery').resolves(rows);
        let result = await db.getTxScopedRows('transaction_outputs', 10);
        assert.strictEqual(result, rows);
        let sql = db.doQuery.firstCall.args[0];
        assert.ok(sql.includes('transactions'));
        assert.ok(sql.includes('transaction_outputs'));
    });
});


describe('Database.getTransactions()', function () {
    let db;
    beforeEach(function () { silenceConsole(); db = makeDb(); });
    afterEach(async function () { sinon.restore(); await db.close(); });

    it('returns rows from transactions table', async function () {
        let rows = [{ tx_index: 7 }];
        sinon.stub(db, 'doQuery').resolves(rows);
        let result = await db.getTransactions(10);
        assert.strictEqual(result, rows);
        let sql = db.doQuery.firstCall.args[0];
        assert.ok(sql.includes('transactions'));
    });
});


describe('Database.getActions()', function () {
    let db;
    beforeEach(function () { silenceConsole(); db = makeDb(); });
    afterEach(async function () { sinon.restore(); await db.close(); });

    it('returns rows from actions table', async function () {
        let rows = [{ action_index: 3 }];
        sinon.stub(db, 'doQuery').resolves(rows);
        let result = await db.getActions(10);
        assert.strictEqual(result, rows);
        let sql = db.doQuery.firstCall.args[0];
        assert.ok(sql.includes('actions'));
    });
});


describe('Database.streamTableRows()', function () {
    let db;
    beforeEach(function () { silenceConsole(); db = makeDb(); });
    afterEach(async function () { sinon.restore(); await db.close(); });

    it('runs ONE un-paged ordered query on the given snapshot connection', function () {
        // Regression: the snapshot path must never re-page with LIMIT/OFFSET.
        // Keyless tables have no total order, so separate offset executions could
        // duplicate or skip a page-boundary tie. A single execution is the fix.
        let conn = { queryStream: sinon.stub().returns('ROW_STREAM') };
        let stream = db.streamTableRows('blocks', conn);
        assert.strictEqual(stream, 'ROW_STREAM');
        assert.ok(conn.queryStream.calledOnce);
        let sql = conn.queryStream.firstCall.args[0];
        assert.ok(/ORDER BY 1/.test(sql), 'keeps the historical snapshot row order');
        assert.ok(!/LIMIT|OFFSET/i.test(sql), 'must not re-page (no total order on keyless tables)');
    });
});


describe('Database.getTableCount()', function () {
    let db;
    beforeEach(function () { silenceConsole(); db = makeDb(); });
    afterEach(async function () { sinon.restore(); await db.close(); });

    it('returns count as Number', async function () {
        sinon.stub(db, 'doQuery').resolves([{ cnt: '42' }]);
        let result = await db.getTableCount('blocks');
        assert.strictEqual(result, 42);
    });

    // getTableCount must not read through the fail-soft doQuery, which outside a
    // transaction logs the SqlError and returns []: `rows[0].cnt` then throws a
    // TypeError with no errno, so every errno-based caller (notably
    // ClientSync.verifyTableCounts, whose catch routes errno 1146 into a schema
    // heal that CREATEs the missing table) never fires, and a missing table stays
    // missing however many times the error repeats. This asserts the DATABASE's
    // error survives the call.
    it('propagates the database error with errno intact when the table is absent', async function () {
        const err = new Error("Table 'db.bet_resolves' doesn't exist");
        err.errno = 1146; err.sqlState = '42S02'; err.code = 'ER_NO_SUCH_TABLE';
        const conn = fakeConn();
        conn.query = sinon.stub().rejects(err);
        sinon.stub(db, 'getConnection').resolves(conn);

        const caught = await db.getTableCount('bet_resolves').then(
            () => null, e => e);
        assert.ok(caught, 'getTableCount must reject when the table is absent');
        assert.strictEqual(caught.errno, 1146,
            'the caller classifies by errno; a TypeError from a fail-soft [] carries none');
        assert.strictEqual(caught.code, 'ER_NO_SUCH_TABLE');
        assert.ok(!(caught instanceof TypeError), 'must not surface as a TypeError');
    });
});


describe('Database.listExistingTables()', function () {
    let db;
    beforeEach(function () { silenceConsole(); db = makeDb(); });
    afterEach(async function () { sinon.restore(); await db.close(); });

    it('returns the table names as a Set, tolerating either column case', async function () {
        sinon.stub(db, 'doQueryStrict').resolves([{ table_name: 'blocks' }, { TABLE_NAME: 'credits' }]);
        const set = await db.listExistingTables();
        assert.ok(set instanceof Set);
        assert.deepStrictEqual([...set].sort(), ['blocks', 'credits']);
    });

    // The point of the helper: a caller can skip an absent table instead of
    // discovering it by failing a query, which is what logged 11,542 stacks for
    // tables neither the replica nor its SOURCE has.
    it('reports a table the caller should skip', async function () {
        sinon.stub(db, 'doQueryStrict').resolves([{ table_name: 'blocks' }]);
        const set = await db.listExistingTables();
        assert.ok(!set.has('bet_resolves'));
    });

    // Strict, so a failure to LIST is never read as "nothing exists": an empty set
    // would empty table_counts and make an incomplete replica look complete to
    // verifyTableCounts, which is the M-17 fail-soft trap one layer up.
    it('THROWS rather than reporting an empty schema when the listing fails', async function () {
        const err = new Error('connection lost'); err.errno = 2013;
        const conn = fakeConn();
        conn.query = sinon.stub().rejects(err);
        sinon.stub(db, 'getConnection').resolves(conn);
        await assert.rejects(() => db.listExistingTables(), /connection lost/);
    });
});


describe('Database.getDatabaseStats()', function () {
    let db;
    beforeEach(function () { silenceConsole(); db = makeDb(); });
    afterEach(async function () { sinon.restore(); await db.close(); });

    it('returns rows from information_schema query', async function () {
        let rows = [{ db_name: 'XChain_BTC', tables: 12 }];
        sinon.stub(db, 'doQuery').resolves(rows);
        let result = await db.getDatabaseStats();
        assert.strictEqual(result, rows);
        let sql = db.doQuery.firstCall.args[0];
        assert.ok(sql.includes('information_schema'));
    });
});


describe('Database.truncateTable()', function () {
    let db;
    beforeEach(function () { silenceConsole(); db = makeDb(); });
    afterEach(async function () { sinon.restore(); await db.close(); });

    it('calls doQuery with TRUNCATE TABLE', async function () {
        sinon.stub(db, 'doQuery').resolves([]);
        await db.truncateTable('blocks');
        let sql = db.doQuery.firstCall.args[0];
        assert.ok(sql.includes('TRUNCATE TABLE'));
        assert.ok(sql.includes('blocks'));
    });
});


describe('Database halt methods', function () {
    let db;
    beforeEach(function () { silenceConsole(); db = makeDb(); });
    afterEach(async function () { sinon.restore(); await db.close(); });

    it('getActiveHalt: returns the first row when rows present', async function () {
        let row = { id: 1, block_index: 500 };
        sinon.stub(db, 'doQuery').resolves([row]);
        let result = await db.getActiveHalt('indexer');
        assert.deepStrictEqual(result, row);
    });

    it('getActiveHalt: returns null when no rows', async function () {
        sinon.stub(db, 'doQuery').resolves([]);
        let result = await db.getActiveHalt('indexer');
        assert.strictEqual(result, null);
    });

    it('getActiveHalt: PROPAGATES a transient query error (fail-closed, not a silent [])', async function () {
        // The halt read must fail closed: a lock-wait/timeout/"server gone away" is
        // NOT the same as "no active halt", so the error surfaces (via rethrow) rather
        // than resolving to [] and letting a durably-halted replica silently resume.
        let conn = fakeConn();
        conn.query.rejects(new Error('sync_halt read blip'));
        sinon.stub(db, 'getConnection').resolves(conn);
        await assert.rejects(() => db.getActiveHalt('indexer'), /sync_halt read blip/);
    });

    it('clearHalt: returns affectedRows', async function () {
        sinon.stub(db, 'doQuery').resolves({ affectedRows: 1 });
        let result = await db.clearHalt('indexer');
        assert.strictEqual(result, 1);
    });

    it('clearHalt: returns 0 when result is falsy', async function () {
        sinon.stub(db, 'doQuery').resolves(null);
        let result = await db.clearHalt('indexer');
        assert.strictEqual(result, 0);
    });

    it('recordHalt: idempotent (returns existing when block_index matches)', async function () {
        let existing = { id: 1, block_index: 100 };
        sinon.stub(db, 'getActiveHalt').resolves(existing);
        let doQ = sinon.stub(db, 'doQuery').resolves([]);
        let result = await db.recordHalt('indexer', 100, 'divergence', [], []);
        assert.deepStrictEqual(result, existing);
        assert.ok(doQ.notCalled);
    });

    it('recordHalt: inserts new halt when none active', async function () {
        let newHalt = { id: 2, block_index: 200 };
        let getHalt = sinon.stub(db, 'getActiveHalt');
        getHalt.onFirstCall().resolves(null);           // no existing
        getHalt.onSecondCall().resolves(newHalt);       // after insert
        sinon.stub(db, 'doQuery').resolves([]);
        let result = await db.recordHalt('indexer', 200, 'divergence', ['m1'], ['s1']);
        assert.deepStrictEqual(result, newHalt);
    });

});
