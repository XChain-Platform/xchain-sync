// Copyright © 2025–2026 Dankest, LLC
// SPDX-License-Identifier: AGPL-3.0-or-later

'use strict';

const {
    assert,
    sinon,
    fs,
    makeDb,
    fakeConn,
    silenceConsole,
} = require('./support/helpers');

describe('Database.verifySyncTables()', function () {
    let db;
    beforeEach(function () { silenceConsole(); db = makeDb(); });
    afterEach(async function () { sinon.restore(); await db.close(); });

    it('indexer dbType: applies the full sync-owned set including sync_halt', async function () {
        sinon.stub(fs, 'readdirSync').returns(['merkle_epochs.sql', 'sync_halt.sql', 'sync_meta.sql']);
        let conn = fakeConn([{ TABLE_NAME: 'x' }]);
        sinon.stub(db, 'getConnection').resolves(conn);
        let result = await db.verifySyncTables();
        assert.strictEqual(result, true);
        assert.strictEqual(conn.query.callCount, 3, 'all three sync-owned tables probed');
    });

    it('throws (via util.throwError) when query fails', async function () {
        sinon.stub(fs, 'readdirSync').returns(['sync_meta.sql']);
        let conn = {
            query:   sinon.stub().rejects(new Error('query fail')),
            release: sinon.stub().resolves()
        };
        sinon.stub(db, 'getConnection').resolves(conn);
        // util.throwError throws, propagating out of verifySyncTables
        await assert.rejects(
            () => db.verifySyncTables(),
            /Error verifying sync_meta table/
        );
    });

    it('createTableFromFile: executes all statements from file', async function () {
        sinon.stub(fs, 'readFileSync').returns('CREATE TABLE foo (id INT); CREATE INDEX idx ON foo(id);');
        let doQueryStub = sinon.stub(db, 'doQuery').resolves([]);
        await db.createTableFromFile('foo.sql');
        assert.strictEqual(doQueryStub.callCount, 2);
    });
});


describe('Database.ensureReplicatedColumns()', function () {
    afterEach(async function () { sinon.restore(); });

    it('returns immediately for decoder dbType (no-op)', async function () {
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
        sinon.stub(console, 'warn');
        let db = makeDb('decoder');
        let spy = sinon.stub(db, 'doQuery').resolves([]);
        await db.ensureReplicatedColumns();
        assert.ok(spy.notCalled);
        await db.close();
    });

    it('adds missing columns for indexer dbType', async function () {
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
        sinon.stub(console, 'warn');
        let db = makeDb('indexer');
        let calls = [];
        sinon.stub(db, 'doQuery').callsFake(async (sql, args) => {
            calls.push(sql);
            // Table exists check → return a row (table present)
            if (/information_schema\.tables/.test(sql)) return [{ table_name: args[1] }];
            // Column check → return empty (column absent) so ALTER fires
            if (/information_schema\.columns/.test(sql)) return [];
            return [];
        });
        await db.ensureReplicatedColumns();
        let alters = calls.filter(s => /ALTER TABLE/.test(s));
        // Assert the exact statements rather than a count: a count check passes
        // when an entry is silently replaced, and each of these is the only thing
        // standing between an aged replica and an "Unknown column" stall.
        assert.deepStrictEqual(alters, [
            'ALTER TABLE `orders` ADD COLUMN `give_ownership` TINYINT(1) NOT NULL DEFAULT 0',
            'ALTER TABLE `orders` ADD COLUMN `get_ownership` TINYINT(1) NOT NULL DEFAULT 0',
            'ALTER TABLE `swaps` ADD COLUMN `give_ownership` TINYINT(1) NOT NULL DEFAULT 0',
            'ALTER TABLE `swaps` ADD COLUMN `get_ownership` TINYINT(1) NOT NULL DEFAULT 0',
            'ALTER TABLE `state_tree_roots` ADD COLUMN `contract_state_root` CHAR(64) NULL AFTER `block_merkle_root`',
            'ALTER TABLE `state_tree_roots` ADD COLUMN `contract_state_root_shadow` CHAR(64) NULL AFTER `contract_state_root`',
            'ALTER TABLE `state_tree_roots` ADD COLUMN `balances_root_escrow_shadow` CHAR(64) NULL AFTER `contract_state_root_shadow`',
            // The three key-rebuild preconditions: the rebuilds in
            // ensureReplicaSecondaryIndexes name these columns, so they must land in this
            // step, ahead of it, in the same startup.
            'ALTER TABLE `anchor_actions` ADD COLUMN `section_index` TINYINT UNSIGNED NOT NULL DEFAULT 0 AFTER `action_index`',
            'ALTER TABLE `validator_rewards` ADD COLUMN `round_qualifier` BIGINT UNSIGNED NOT NULL DEFAULT 0 AFTER `round_reference`',
            'ALTER TABLE `anchor_reward_reconcile_log` ADD COLUMN `round_qualifier` BIGINT UNSIGNED NOT NULL DEFAULT 0 AFTER `round_reference`'
        ]);
        await db.close();
    });

});

describe('Database.ensureReplicatedColumns()', function () {
    afterEach(async function () { sinon.restore(); });

    it('skips column when table does not exist on replica', async function () {
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
        sinon.stub(console, 'warn');
        let db = makeDb('indexer');
        let calls = [];
        sinon.stub(db, 'doQuery').callsFake(async (sql) => {
            calls.push(sql);
            // Table does not exist
            if (/information_schema\.tables/.test(sql)) return [];
            return [];
        });
        await db.ensureReplicatedColumns();
        let alters = calls.filter(s => /ALTER TABLE/.test(s));
        assert.strictEqual(alters.length, 0);
        await db.close();
    });

    it('skips column when column already exists on replica', async function () {
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
        sinon.stub(console, 'warn');
        let db = makeDb('indexer');
        let calls = [];
        sinon.stub(db, 'doQuery').callsFake(async (sql, args) => {
            calls.push(sql);
            if (/information_schema\.tables/.test(sql)) return [{ table_name: args[1] }];
            // AUTO_INCREMENT-check reads (SELECT EXTRA ...): return 'auto_increment' so the
            // repair is correctly skipped when the column is already in the right state.
            // These reads are intentional new behavior added by the #3713 AUTO_INCREMENT
            // self-heal; the real intent of this test is that no ADD COLUMN or MODIFY is
            // issued when all columns are already present.
            if (/SELECT EXTRA FROM information_schema\.columns/.test(sql)) return [{ EXTRA: 'auto_increment' }];
            // ENUM-widen check (5244): return a COLUMN_TYPE that already includes 'rejected'
            // so the heal is correctly skipped when the ENUM is already up to date.
            if (/SELECT COLUMN_TYPE FROM information_schema\.columns/.test(sql))
                return [{ COLUMN_TYPE: "enum('pending','fulfilled','expired','errored','rejected')" }];
            // Ownership/nullability column-presence checks: column exists on replica
            if (/information_schema\.columns/.test(sql)) return [{ COLUMN_NAME: args[2] }];
            return [];
        });
        await db.ensureReplicatedColumns();
        let alters = calls.filter(s => /ALTER TABLE/.test(s));
        assert.strictEqual(alters.length, 0);
        await db.close();
    });
});


describe('Database.getLastBlock()', function () {
    let db;
    beforeEach(function () { silenceConsole(); db = makeDb(); });
    afterEach(async function () { sinon.restore(); await db.close(); });

    it('returns Number when rows contain a block_index', async function () {
        sinon.stub(db, 'doQuery').resolves([{ block_index: 500 }]);
        let result = await db.getLastBlock();
        assert.strictEqual(result, 500);
    });

    it('returns null when row.block_index is null', async function () {
        sinon.stub(db, 'doQuery').resolves([{ block_index: null }]);
        let result = await db.getLastBlock();
        assert.strictEqual(result, null);
    });

    it('returns null when rows is empty', async function () {
        sinon.stub(db, 'doQuery').resolves([]);
        let result = await db.getLastBlock();
        assert.strictEqual(result, null);
    });

    it('swallows a query error and returns null by default (fail-soft preserved)', async function () {
        let conn = fakeConn();
        conn.query.rejects(new Error('tip read blip'));
        sinon.stub(db, 'getConnection').resolves(conn);
        assert.strictEqual(await db.getLastBlock(), null);
    });

    it('PROPAGATES a query error with opts.rethrow (fail-closed resume cursor)', async function () {
        // A null tip is also what a genuinely empty replica returns, and the catch-up
        // resume cursor turns that into sinceBlock=1 (re-request the whole history).
        // opts must reach doQuery so an unreadable tip aborts instead.
        let conn = fakeConn();
        conn.query.rejects(new Error('tip read blip'));
        sinon.stub(db, 'getConnection').resolves(conn);
        await assert.rejects(
            () => db.getLastBlock(null, { rethrow: true }),
            /tip read blip/
        );
    });
});


// getLastBlock above reads the SERVED database, so on a node fronting a native
// SQL replica the source and served heights share one failure domain: when
// replication stops applying, both freeze at the same number and the derived
// lag_blocks publishes 0 for an hours-behind node. Only the replication engine
// can tell those apart, and it must fail CLOSED on every ambiguous answer.
describe('Database.getReplicaStatus()', function () {
    let db;
    beforeEach(function () { silenceConsole(); db = makeDb(); });
    afterEach(async function () { sinon.restore(); await db.close(); });

    it('reports not-a-replica on an empty result set (primary / co-located source)', async function () {
        sinon.stub(db, 'doQueryStrict').resolves([]);
        assert.deepStrictEqual(await db.getReplicaStatus(),
            { isReplica: false, running: null, secondsBehind: null });
    });

    it('reads a healthy replica row', async function () {
        sinon.stub(db, 'doQueryStrict').resolves([{
            Replica_IO_Running: 'Yes', Replica_SQL_Running: 'Yes', Seconds_Behind_Source: 3
        }]);
        assert.deepStrictEqual(await db.getReplicaStatus(),
            { isReplica: true, running: true, secondsBehind: 3 });
    });

    it('reports a stopped SQL thread as running:false with NULL, never 0 behind', async function () {
        sinon.stub(db, 'doQueryStrict').resolves([{
            Replica_IO_Running: 'Yes', Replica_SQL_Running: 'No', Seconds_Behind_Source: null
        }]);
        let res = await db.getReplicaStatus();
        assert.strictEqual(res.running, false);
        assert.strictEqual(res.secondsBehind, null, 'NULL is unbounded lag, not zero');
    });

    it('falls back to the pre-10.5 SLAVE spelling', async function () {
        // A server knowing only the oldest spelling: no `SHOW ALL SLAVES STATUS`
        // (MariaDB-only) and no `SHOW REPLICA STATUS` (pre-8.0.22), so both are
        // rejected to exercise the tail of the fallback chain.
        let stub = sinon.stub(db, 'doQueryStrict');
        stub.withArgs('SHOW ALL SLAVES STATUS').rejects(new Error('You have an error in your SQL syntax'));
        stub.withArgs('SHOW REPLICA STATUS').rejects(new Error('You have an error in your SQL syntax'));
        stub.withArgs('SHOW SLAVE STATUS').resolves([{
            Slave_IO_Running: 'Yes', Slave_SQL_Running: 'Yes', Seconds_Behind_Master: 7
        }]);
        assert.deepStrictEqual(await db.getReplicaStatus(),
            { isReplica: true, running: true, secondsBehind: 7 });
    });

    it('returns an unknown result when the grant is missing, never a healthy one', async function () {
        sinon.stub(db, 'doQueryStrict').rejects(new Error('Access denied; you need REPLICATION CLIENT'));
        assert.deepStrictEqual(await db.getReplicaStatus(),
            { isReplica: null, running: null, secondsBehind: null });
    });
});


// ═══════════════════════════════════════════════════════════════════════════
// 14b. addMissingColumns(): remaining branch coverage
// (The basic happy-path cases live in db-schema-evolution.test.js)
// ═══════════════════════════════════════════════════════════════════════════
describe('Database.addMissingColumns(): edge branches', function () {
    let db;
    beforeEach(function () { silenceConsole(); db = makeDb(); });
    afterEach(async function () { sinon.restore(); await db.close(); });

    // Lines 218-220: column name that fails validateIdentifier
    it('skips (logs error) a column with an invalid identifier name', async function () {
        // Build a DDL with a column line that has a name containing a dash (invalid)
        // extractColumnNames returns names from backtick lines; validateIdentifier
        // rejects non-[A-Za-z0-9_] chars.  We can't directly forge extractColumnNames
        // output, so we construct a DDL where the column name passes extraction but
        // fails the identifier check.  A column named "bad-col" passes the backtick
        // extraction (extractColumnNames just slices between backticks) but fails
        // validateIdentifier (contains '-').
        let ddl = [
            'CREATE TABLE `t` (',
            '  `bad-col` int(11) NOT NULL,',
            '  PRIMARY KEY (`id`)',
            ') ENGINE=InnoDB'
        ].join('\n');
        sinon.stub(db, 'doQuery').callsFake(async (sql) => {
            // Replica has no columns for this table
            if (/information_schema\.columns/.test(sql)) return [];
            return [];
        });
        let added = await db.addMissingColumns('t', ddl);
        // bad-col extracted but rejected; nothing added
        assert.strictEqual(added, 0);
        // console.error should have been called for the invalid column
        assert.ok(console.error.called);
    });

    // Lines 224-226: column whose definition can't be extracted (null def)
    it('warns and skips a column when extractColumnDefinition returns null', async function () {
        // DDL has column `new_col` in the name list but its definition line contains
        // a semicolon, which causes extractColumnDefinition to return null.
        let ddl = [
            'CREATE TABLE `t` (',
            '  `new_col` int(11) DEFAULT NULL; /* injected */',
            '  PRIMARY KEY (`id`)',
            ') ENGINE=InnoDB'
        ].join('\n');
        sinon.stub(db, 'doQuery').callsFake(async (sql) => {
            if (/information_schema\.columns/.test(sql)) return [];  // replica has nothing
            return [];
        });
        let added = await db.addMissingColumns('t', ddl);
        assert.strictEqual(added, 0);
        assert.ok(console.warn.called);
    });

});
