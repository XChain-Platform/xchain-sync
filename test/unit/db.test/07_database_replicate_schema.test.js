// Copyright © 2025–2026 Dankest, LLC
// SPDX-License-Identifier: AGPL-3.0-or-later

'use strict';

const {
    assert,
    sinon,
    makeDb,
    silenceConsole,
} = require('./support/helpers');

describe('Database.replicateSchema()', function () {
    let db, sourceDb;
    beforeEach(function () {
        silenceConsole();
        db       = makeDb('indexer');
        sourceDb = makeDb('indexer', 'source_db');
    });
    afterEach(async function () {
        sinon.restore();
        await db.close();
        await sourceDb.close();
    });

    it('retry block: skips table when SHOW CREATE TABLE returns empty (retry ddlRows empty)', async function () {
        // Trigger retry block: child fails CREATE first pass (created=0 < 1-0=1).
        // In retry, SHOW CREATE TABLE returns [] → ddlRows.length===0 → continue.
        let validDdl = 'CREATE TABLE child (id INT NOT NULL, PRIMARY KEY (id))';
        sinon.stub(sourceDb, 'doQuery').callsFake(async (sql) => {
            if (/information_schema\.tables/.test(sql))
                return [{ table_name: 'child' }];
            if (/SHOW CREATE TABLE/.test(sql)) return [{ 'Create Table': validDdl }];
            return [];
        });
        let targetInfoCalls = 0;
        sinon.stub(db, 'doQuery').callsFake(async (sql) => {
            if (/information_schema\.tables/.test(sql)) {
                targetInfoCalls++;
                return [];  // child always missing → retry fires
            }
            if (/CREATE TABLE/.test(sql)) throw new Error('FK fail');  // always fails
            return [];
        });
        // Override sourceDb.doQuery to return empty ddlRows in retry path
        let sourceQueryCount = 0;
        sourceDb.doQuery.restore();
        sinon.stub(sourceDb, 'doQuery').callsFake(async (sql) => {
            if (/information_schema\.tables/.test(sql))
                return [{ table_name: 'child' }];
            if (/SHOW CREATE TABLE/.test(sql)) {
                sourceQueryCount++;
                if (sourceQueryCount === 1) return [{ 'Create Table': validDdl }]; // first pass: has DDL
                return [];  // retry pass: empty rows → continue
            }
            return [];
        });
        sinon.stub(db, 'ensureReplicatedColumns').resolves();
        await db.replicateSchema(sourceDb);
    });

});

describe('Database.replicateSchema()', function () {
    let db, sourceDb;
    beforeEach(function () {
        silenceConsole();
        db       = makeDb('indexer');
        sourceDb = makeDb('indexer', 'source_db');
    });
    afterEach(async function () {
        sinon.restore();
        await db.close();
        await sourceDb.close();
    });

    it('retry block: skips table when Create Table key is missing (retry createSql null)', async function () {
        let validDdl = 'CREATE TABLE child (id INT NOT NULL, PRIMARY KEY (id))';
        let targetInfoCalls = 0;
        sinon.stub(db, 'doQuery').callsFake(async (sql) => {
            if (/information_schema\.tables/.test(sql)) {
                targetInfoCalls++;
                return [];
            }
            if (/CREATE TABLE/.test(sql)) throw new Error('FK fail');
            return [];
        });
        let sourceQueryCount = 0;
        sinon.stub(sourceDb, 'doQuery').callsFake(async (sql) => {
            if (/information_schema\.tables/.test(sql))
                return [{ table_name: 'child' }];
            if (/SHOW CREATE TABLE/.test(sql)) {
                sourceQueryCount++;
                if (sourceQueryCount === 1) return [{ 'Create Table': validDdl }];
                return [{}];  // retry: row without 'Create Table' key → createSql=undefined → continue
            }
            return [];
        });
        sinon.stub(db, 'ensureReplicatedColumns').resolves();
        await db.replicateSchema(sourceDb);
    });

});

describe('Database.replicateSchema()', function () {
    let db, sourceDb;
    beforeEach(function () {
        silenceConsole();
        db       = makeDb('indexer');
        sourceDb = makeDb('indexer', 'source_db');
    });
    afterEach(async function () {
        sinon.restore();
        await db.close();
        await sourceDb.close();
    });

    it('retry block: handles TABLE_NAME uppercase keys in retrySet and source rows', async function () {
        // Force retry block; retryTables and sourceTables use uppercase TABLE_NAME key.
        let validDdl = 'CREATE TABLE child (id INT NOT NULL, PRIMARY KEY (id))';
        let sourceQueryCount = 0;
        sinon.stub(sourceDb, 'doQuery').callsFake(async (sql) => {
            if (/information_schema\.tables/.test(sql))
                return [{ TABLE_NAME: 'child' }];  // ← uppercase key in source table list
            if (/SHOW CREATE TABLE/.test(sql)) {
                sourceQueryCount++;
                if (sourceQueryCount === 1) return [{ 'Create Table': validDdl }];
                return [];  // retry: empty → skip
            }
            return [];
        });
        let targetInfoCalls = 0;
        sinon.stub(db, 'doQuery').callsFake(async (sql) => {
            if (/information_schema\.tables/.test(sql)) {
                targetInfoCalls++;
                if (targetInfoCalls === 1) return [];
                // retry pass: return uppercase TABLE_NAME so retrySet uses the TABLE_NAME fallback
                return [{ TABLE_NAME: 'child' }];
            }
            if (/CREATE TABLE/.test(sql)) throw new Error('FK fail');
            return [];
        });
        sinon.stub(db, 'ensureReplicatedColumns').resolves();
        await db.replicateSchema(sourceDb);
    });

});

describe('Database.replicateSchema()', function () {
    let db, sourceDb;
    beforeEach(function () {
        silenceConsole();
        db       = makeDb('indexer');
        sourceDb = makeDb('indexer', 'source_db');
    });
    afterEach(async function () {
        sinon.restore();
        await db.close();
        await sourceDb.close();
    });

    it('retry block: successfully creates a deferred table (covers the success log)', async function () {
        // One source table 'child' that fails on first pass but succeeds on retry.
        // Setup: created=0, sourceTables.length=1, existingSet.size=0 → 0 < 1 → retry fires.
        let validDdl = 'CREATE TABLE child (id INT NOT NULL, PRIMARY KEY (id))';
        sinon.stub(sourceDb, 'doQuery').callsFake(async (sql) => {
            if (/information_schema\.tables/.test(sql))
                return [{ table_name: 'child' }];
            if (/SHOW CREATE TABLE `child`/.test(sql)) return [{ 'Create Table': validDdl }];
            return [];
        });

        let firstPassDone = false;
        let targetInfoCalls = 0;
        sinon.stub(db, 'doQuery').callsFake(async (sql) => {
            if (/information_schema\.tables/.test(sql)) {
                targetInfoCalls++;
                if (targetInfoCalls === 1) return [];   // first pass: target has no tables
                return [];                               // retry pass: child still missing → retry creates it
            }
            if (/CREATE TABLE child/.test(sql)) {
                if (!firstPassDone) {
                    firstPassDone = true;
                    throw new Error('FK not ready');    // first attempt fails
                }
                return [];                              // retry succeeds
            }
            return [];
        });
        sinon.stub(db, 'ensureReplicatedColumns').resolves();
        await db.replicateSchema(sourceDb);
        // console.log should have been called for 'Created table child (retry)'
        let logCalls = console.log.args.map(a => a[0]);
        assert.ok(logCalls.some(s => typeof s === 'string' && s.includes('retry')));
    });
});


describe('Database: table identifier guard', function () {
    let db;
    beforeEach(function () { silenceConsole(); db = makeDb(); });
    afterEach(async function () { sinon.restore(); await db.close(); });

    const EVIL = 'blocks` WHERE 1=1 UNION SELECT 1 -- ';

    it('getTableCount rejects an unsafe identifier before querying', async function () {
        let spy = sinon.stub(db, 'doQuery').resolves([{ cnt: 0 }]);
        await assert.rejects(() => db.getTableCount(EVIL), /unsafe table identifier/);
        assert.ok(spy.notCalled, 'doQuery must not run for an unsafe identifier');
    });

    it('streamTableRows rejects an unsafe identifier before querying', function () {
        let conn = { queryStream: sinon.stub() };
        assert.throws(() => db.streamTableRows(EVIL, conn), /unsafe table identifier/);
        assert.ok(conn.queryStream.notCalled, 'queryStream must not run for an unsafe identifier');
    });

    it('truncateTable rejects an unsafe identifier before querying', async function () {
        let spy = sinon.stub(db, 'doQuery').resolves([]);
        await assert.rejects(() => db.truncateTable(EVIL), /unsafe table identifier/);
        assert.ok(spy.notCalled);
    });

    it('allows a normal table name through to the query', async function () {
        let spy = sinon.stub(db, 'doQuery').resolves([{ cnt: 42 }]);
        let n = await db.getTableCount('contract_stakes');
        assert.strictEqual(n, 42);
        assert.ok(spy.calledOnce);
        assert.ok(spy.firstCall.args[0].includes('`contract_stakes`'));
    });
});


// ═══════════════════════════════════════════════════════════════════════════
// ensureReplicaSecondaryIndexes(): votes append-only unique-key migration (M-20)
// A replica that bootstrapped before indexer 219da33 carries the stale
// UNIQUE(poll_voter_choice) key; append-only re-ballot rows then wedge it on
// ER_DUP_ENTRY (unhealable, since the applier's last-write-wins pre-delete was
// removed). The self-heal must drop the stale key and add the widened one.
// ═══════════════════════════════════════════════════════════════════════════
describe('Database.ensureReplicaSecondaryIndexes(): votes append-only migration', function () {
    // Fake doQuery that treats only `votes` as present and all other tables
    // (index_tickers/index_addresses/attests) as absent, so the pre-existing
    // ensure/relax steps no-op and the test isolates the votes migration. The
    // votes index checks resolve from votesIndexes: keys are index names.
    function stubDoQuery(db, votesIndexes) {
        let calls = [];
        sinon.stub(db, 'doQuery').callsFake(async (sql, params) => {
            calls.push({ sql, params });
            if (/information_schema\.tables/.test(sql)) {
                // The votes/attests existence checks inline the table name; the
                // index-ensure checks pass it as a bound param. Only `votes` is present.
                if (/table_name = 'votes'/.test(sql)) return [{ table_name: 'votes' }];
                return [];
            }
            if (/information_schema\.statistics/.test(sql)) {
                if (/poll_voter_action_choice'/.test(sql))
                    return votesIndexes.poll_voter_action_choice ? [{ index_name: 'poll_voter_action_choice' }] : [];
                if (/poll_voter_choice'/.test(sql))
                    return votesIndexes.poll_voter_choice ? [{ index_name: 'poll_voter_choice' }] : [];
                return [];
            }
            return [];
        });
        return calls;
    }

    let db;
    beforeEach(function () { silenceConsole(); db = makeDb('indexer'); });
    afterEach(async function () { sinon.restore(); await db.close(); });

    it('drops the stale poll_voter_choice and creates the widened unique key', async function () {
        // Pre-219da33 replica: stale key present, widened key absent.
        let calls = stubDoQuery(db, { poll_voter_choice: true, poll_voter_action_choice: false });
        await db.ensureReplicaSecondaryIndexes();
        let ddl = calls.map(c => c.sql);
        assert.ok(ddl.some(s => /ALTER TABLE `votes` DROP INDEX `poll_voter_choice`/.test(s)),
            'must drop the stale poll_voter_choice unique key');
        assert.ok(ddl.some(s => /CREATE UNIQUE INDEX `poll_voter_action_choice` ON `votes`/.test(s)),
            'must create the append-only widened unique key');
    });

    it('is a no-op when the widened key already exists (idempotent)', async function () {
        // Post-migration / fresh replica: stale absent, widened present.
        let calls = stubDoQuery(db, { poll_voter_choice: false, poll_voter_action_choice: true });
        await db.ensureReplicaSecondaryIndexes();
        let ddl = calls.map(c => c.sql);
        assert.ok(!ddl.some(s => /DROP INDEX `poll_voter_choice`/.test(s)), 'no drop when stale key absent');
        assert.ok(!ddl.some(s => /CREATE UNIQUE INDEX `poll_voter_action_choice`/.test(s)), 'no create when already present');
    });

});
