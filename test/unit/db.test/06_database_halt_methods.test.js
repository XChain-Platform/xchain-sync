// Copyright © 2025–2026 Dankest, LLC
// SPDX-License-Identifier: AGPL-3.0-or-later

'use strict';

const {
    assert,
    sinon,
    makeDb,
    silenceConsole,
} = require('./support/helpers');

describe('Database halt methods', function () {
    let db;
    beforeEach(function () { silenceConsole(); db = makeDb(); });
    afterEach(async function () { sinon.restore(); await db.close(); });

    it('recordHalt: inserts new halt when existing block_index differs', async function () {
        let existingOther = { id: 1, block_index: 50 };
        let newHalt = { id: 2, block_index: 200 };
        let getHalt = sinon.stub(db, 'getActiveHalt');
        getHalt.onFirstCall().resolves(existingOther);  // different block
        getHalt.onSecondCall().resolves(newHalt);
        let doQ = sinon.stub(db, 'doQuery').resolves([]);
        let result = await db.recordHalt('indexer', 200, 'divergence', [], []);
        assert.deepStrictEqual(result, newHalt);
        assert.ok(doQ.calledOnce);
    });

    it('recordHalt: still INSERTs when the idempotency pre-check read throws (durability first)', async function () {
        // getActiveHalt now fails closed (throws) on a transient error; recordHalt must
        // fall through to the INSERT rather than skip recording the halt on a read blip.
        let getHalt = sinon.stub(db, 'getActiveHalt');
        getHalt.onFirstCall().rejects(new Error('pre-check blip'));
        getHalt.onSecondCall().resolves({ id: 9, block_index: 400 });
        let doQ = sinon.stub(db, 'doQuery').resolves([]);
        let result = await db.recordHalt('indexer', 400, 'divergence', [], []);
        assert.ok(doQ.calledOnce, 'the halt is recorded despite the pre-check read error');
        assert.deepStrictEqual(result, { id: 9, block_index: 400 });
    });

    it('recordHalt: returns null (not a throw) when the post-insert read throws', async function () {
        let getHalt = sinon.stub(db, 'getActiveHalt');
        getHalt.onFirstCall().resolves(null);
        getHalt.onSecondCall().rejects(new Error('post-insert blip'));
        let doQ = sinon.stub(db, 'doQuery').resolves([]);
        let result = await db.recordHalt('indexer', 500, 'divergence', [], []);
        assert.ok(doQ.calledOnce, 'INSERT ran');
        assert.strictEqual(result, null);
    });

    it('recordHalt: uses default "divergence" when reason is null, defaults mismatches/sources to []', async function () {
        let newHalt = { id: 3, block_index: 300 };
        let getHalt = sinon.stub(db, 'getActiveHalt');
        getHalt.onFirstCall().resolves(null);
        getHalt.onSecondCall().resolves(newHalt);
        let insertArgs;
        sinon.stub(db, 'doQuery').callsFake(async (sql, args) => {
            insertArgs = args;
            return [];
        });
        await db.recordHalt('indexer', 300, null, null, null);
        // Defaults should have been applied: reason→'divergence', mismatches→[], sources→[]
        assert.strictEqual(insertArgs[2], 'divergence');
        assert.strictEqual(insertArgs[3], '[]');
        assert.strictEqual(insertArgs[4], '[]');
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

    it('creates a missing table and calls ensureReplicatedColumns', async function () {
        let validDdl = 'CREATE TABLE blocks (id INT NOT NULL, PRIMARY KEY (id))';
        let sourceCalls = [];
        sinon.stub(sourceDb, 'doQuery').callsFake(async (sql) => {
            sourceCalls.push(sql);
            if (/information_schema\.tables/.test(sql))
                return [{ table_name: 'blocks' }];
            if (/SHOW CREATE TABLE/.test(sql))
                return [{ 'Create Table': validDdl }];
            return [];
        });
        let targetCalls = [];
        sinon.stub(db, 'doQuery').callsFake(async (sql) => {
            targetCalls.push(sql);
            // Target has no tables initially
            if (/information_schema\.tables/.test(sql)) return [];
            return [];
        });
        sinon.stub(db, 'ensureReplicatedColumns').resolves();
        await db.replicateSchema(sourceDb);
        assert.ok(db.ensureReplicatedColumns.calledOnce);
        let creates = targetCalls.filter(s => s.includes('CREATE TABLE'));
        assert.ok(creates.length >= 1);
    });

    it('skips invalid table name (validateIdentifier fail)', async function () {
        sinon.stub(sourceDb, 'doQuery').callsFake(async (sql) => {
            if (/information_schema\.tables/.test(sql))
                return [{ table_name: 'bad-table-name' }];
            return [];
        });
        sinon.stub(db, 'doQuery').resolves([]);
        sinon.stub(db, 'ensureReplicatedColumns').resolves();
        // Should not throw
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

    it('calls addMissingColumns for already-existing tables', async function () {
        let validDdl = 'CREATE TABLE blocks (id INT NOT NULL, PRIMARY KEY (id))';
        sinon.stub(sourceDb, 'doQuery').callsFake(async (sql) => {
            if (/information_schema\.tables/.test(sql)) return [{ table_name: 'blocks' }];
            if (/SHOW CREATE TABLE/.test(sql)) return [{ 'Create Table': validDdl }];
            return [];
        });
        sinon.stub(db, 'doQuery').callsFake(async (sql) => {
            // Target already has 'blocks'
            if (/information_schema\.tables/.test(sql)) return [{ table_name: 'blocks' }];
            return [];
        });
        sinon.stub(db, 'addMissingColumns').resolves(0);
        sinon.stub(db, 'ensureReplicatedColumns').resolves();
        await db.replicateSchema(sourceDb);
        assert.ok(db.addMissingColumns.calledOnce);
    });

    it('skips table when SHOW CREATE TABLE returns empty rows', async function () {
        sinon.stub(sourceDb, 'doQuery').callsFake(async (sql) => {
            if (/information_schema\.tables/.test(sql))
                return [{ table_name: 'blocks' }];
            if (/SHOW CREATE TABLE/.test(sql))
                return [];   // ← empty rows, triggers the continue
            return [];
        });
        sinon.stub(db, 'doQuery').resolves([]);
        sinon.stub(db, 'ensureReplicatedColumns').resolves();
        await db.replicateSchema(sourceDb);
    });

    it('skips table when SHOW CREATE TABLE row has no Create Table key', async function () {
        sinon.stub(sourceDb, 'doQuery').callsFake(async (sql) => {
            if (/information_schema\.tables/.test(sql))
                return [{ table_name: 'blocks' }];
            if (/SHOW CREATE TABLE/.test(sql))
                return [{}];   // ← row with no 'Create Table' field, triggers continue
            return [];
        });
        sinon.stub(db, 'doQuery').resolves([]);
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

    it('handles TABLE_NAME (uppercase) keys from information_schema rows', async function () {
        let validDdl = 'CREATE TABLE blocks (id INT NOT NULL, PRIMARY KEY (id))';
        sinon.stub(sourceDb, 'doQuery').callsFake(async (sql) => {
            if (/information_schema\.tables/.test(sql))
                return [{ TABLE_NAME: 'blocks' }];   // uppercase key
            if (/SHOW CREATE TABLE/.test(sql))
                return [{ 'Create Table': validDdl }];
            return [];
        });
        let targetCallCount = 0;
        sinon.stub(db, 'doQuery').callsFake(async (sql) => {
            if (/information_schema\.tables/.test(sql)) {
                targetCallCount++;
                if (targetCallCount === 1)
                    return [{ TABLE_NAME: 'blocks' }];  // target has 'blocks' with uppercase key
                return [{ TABLE_NAME: 'blocks' }];
            }
            return [];
        });
        sinon.stub(db, 'addMissingColumns').resolves(0);
        sinon.stub(db, 'ensureReplicatedColumns').resolves();
        await db.replicateSchema(sourceDb);
        assert.ok(db.addMissingColumns.calledOnce);
    });

    it('skips DDL that fails validateDdl (invalid DDL)', async function () {
        sinon.stub(sourceDb, 'doQuery').callsFake(async (sql) => {
            if (/information_schema\.tables/.test(sql)) return [{ table_name: 'blocks' }];
            if (/SHOW CREATE TABLE/.test(sql)) return [{ 'Create Table': 'DROP TABLE blocks' }];
            return [];
        });
        sinon.stub(db, 'doQuery').resolves([]);
        sinon.stub(db, 'ensureReplicatedColumns').resolves();
        // Should not throw
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

    it('handles CREATE TABLE throw (deferred) and executes retry block', async function () {
        // Two source tables: 'parent' and 'child'.
        // First pass: parent succeeds (created=1), child throws (deferred, created stays 1).
        // created(1) < sourceTables.length(2) - existingSet.size(0) = 2 → retry fires.
        // In retry: parent is now in retrySet (skip), child is not → retry child CREATE.
        let validDdlParent = 'CREATE TABLE parent (id INT NOT NULL, PRIMARY KEY (id))';
        let validDdlChild  = 'CREATE TABLE child (id INT NOT NULL, PRIMARY KEY (id))';

        sinon.stub(sourceDb, 'doQuery').callsFake(async (sql) => {
            if (/information_schema\.tables/.test(sql))
                return [{ table_name: 'parent' }, { table_name: 'child' }];
            if (/SHOW CREATE TABLE `parent`/.test(sql)) return [{ 'Create Table': validDdlParent }];
            if (/SHOW CREATE TABLE `child`/.test(sql))  return [{ 'Create Table': validDdlChild }];
            return [];
        });

        let callCount = 0;
        sinon.stub(db, 'doQuery').callsFake(async (sql) => {
            // info_schema calls: first = existing tables (none), subsequent = retry set
            if (/information_schema\.tables/.test(sql)) {
                callCount++;
                if (callCount === 1) return [];                                  // first pass: target has no tables
                return [{ table_name: 'parent' }];                               // retry pass: parent was created
            }
            // CREATE TABLE parent → success
            if (/CREATE TABLE parent/.test(sql)) return [];
            // CREATE TABLE child → throw on first attempt
            if (/CREATE TABLE child/.test(sql)) throw new Error('FK not ready');
            return [];
        });
        sinon.stub(db, 'ensureReplicatedColumns').resolves();

        // Should not throw; the deferred+retry block should execute without error
        await db.replicateSchema(sourceDb);
        assert.ok(db.ensureReplicatedColumns.calledOnce);
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

    it('retry block: skips invalid table name', async function () {
        // Force the retry path: one table that fails CREATE, one with a bad name in source
        let validDdl = 'CREATE TABLE child (id INT NOT NULL, PRIMARY KEY (id))';
        sinon.stub(sourceDb, 'doQuery').callsFake(async (sql) => {
            if (/information_schema\.tables/.test(sql))
                return [{ table_name: 'child' }];
            if (/SHOW CREATE TABLE/.test(sql)) return [{ 'Create Table': validDdl }];
            return [];
        });
        let targetCallCount = 0;
        sinon.stub(db, 'doQuery').callsFake(async (sql) => {
            if (/information_schema\.tables/.test(sql)) {
                targetCallCount++;
                if (targetCallCount === 1) return [];
                return [];  // child still missing → retry fires
            }
            // child CREATE fails
            if (/CREATE TABLE/.test(sql)) throw new Error('FK fail');
            return [];
        });
        sinon.stub(db, 'ensureReplicatedColumns').resolves();
        await db.replicateSchema(sourceDb);
    });

});
