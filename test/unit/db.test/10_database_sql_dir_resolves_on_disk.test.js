// Copyright © 2025–2026 Dankest, LLC
// SPDX-License-Identifier: AGPL-3.0-or-later

'use strict';

const path = require('path');
const {
    assert,
    sinon,
    fs,
    makeDb,
    fakeConn,
    silenceConsole,
} = require('./support/helpers');
const Database = require('../../../src/db');

// Touch the REAL src/sql/ directory with fs left unstubbed: the sibling tests
// stub readdirSync, so only these red when the resolved path drifts off disk.

// List the sync-owned DDL files by name, never from a glob, so a missing file
// fails instead of silently shrinking the expected set.
const SYNC_OWNED_DDL = [
    'escrow_leaf_journal.sql',
    'merkle_epochs.sql',
    'merkle_reorgs.sql',
    'state_tree_nodes.sql',
    'state_tree_roots.sql',
    'sync_halt.sql',
    'sync_meta.sql',
];

describe('Database.sqlDir(): the sync-owned DDL directory exists on disk', function () {

    it('resolves src/sql/ relative to the repo, not src/db/sql/', function () {
        let dir = Database.sqlDir();
        let expected = path.resolve(__dirname, '..', '..', '..', 'src', 'sql');
        assert.strictEqual(path.resolve(dir), expected);
    });

    it('the resolved directory exists and is a directory', function () {
        let dir = Database.sqlDir();
        assert.ok(fs.existsSync(dir), 'missing on disk: ' + dir);
        assert.ok(fs.statSync(dir).isDirectory(), 'not a directory: ' + dir);
    });

    it('contains every sync-owned .sql file the boot path expects', function () {
        let dir = Database.sqlDir();
        let present = fs.readdirSync(dir).filter(f => f.endsWith('.sql')).sort();
        for (const file of SYNC_OWNED_DDL)
            assert.ok(present.includes(file), 'missing DDL file: ' + path.join(dir, file));
        assert.deepStrictEqual(present, SYNC_OWNED_DDL.slice().sort(),
            'the on-disk set differs from the expected sync-owned set');
    });
});

describe('Database.verifySyncTables() reads the real DDL directory', function () {
    let db;
    beforeEach(function () { silenceConsole(); db = makeDb('indexer'); });
    afterEach(async function () { sinon.restore(); await db.close(); });

    it('probes one table per on-disk DDL file with fs left unstubbed', async function () {
        // Only the connection is faked; readdirSync hits the filesystem, which
        // is the call that threw ENOENT in production.
        let conn = fakeConn([{ TABLE_NAME: 'present' }]);
        sinon.stub(db, 'getConnection').resolves(conn);
        let result = await db.verifySyncTables();
        assert.strictEqual(result, true);
        assert.strictEqual(conn.query.callCount, SYNC_OWNED_DDL.length,
            'one information_schema probe per sync-owned DDL file');
        let probed = conn.query.getCalls().map(c => c.args[1][1]).sort();
        assert.deepStrictEqual(probed, SYNC_OWNED_DDL.map(f => f.replace(/\.sql$/, '')).sort());
    });

    it('createTableFromFile() reads a real DDL file and runs its statements', async function () {
        let doQueryStub = sinon.stub(db, 'doQuery').resolves([]);
        await db.createTableFromFile('sync_halt.sql');
        assert.ok(doQueryStub.callCount >= 1, 'at least one statement executed from sync_halt.sql');
        let statements = doQueryStub.getCalls().map(c => c.args[0]);
        assert.ok(statements.some(s => /CREATE TABLE/i.test(s)),
            'a CREATE TABLE statement was executed from sync_halt.sql, got: ' + JSON.stringify(statements));
    });
});
