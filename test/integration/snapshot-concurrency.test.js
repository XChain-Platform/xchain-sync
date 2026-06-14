// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md.

// Integration proof for #3732 against a REAL MariaDB.
//
// Pre-refactor, src/db.js beginReadSnapshot pinned the single shared
// this.transactionConnection. A concurrent writer's beginTransaction() then
// called releaseConnection() on that very connection, yanking it out from under
// an in-flight snapshot read (and two concurrent snapshots clobbered each
// other). The fix gives each read snapshot a DEDICATED pool connection.
//
// These tests exercise the production src/db.js Db (not the lightweight
// TestDatabase) so they validate the shipped code path. They would fail on the
// pre-#3732 code: `during` would observe the concurrent write (snapshot
// released), and db.commitReadSnapshot would not exist.

const assert   = require('assert');
const Database = require('../../src/db');
const Utility  = require('../../src/utility');
const {
    TEST_DB_HOST, TEST_DB_PORT, TEST_DB_USER, TEST_DB_PASS,
    createDatabase, dropDatabase
} = require('./helpers/testDb');

const DB = 'xchain_sync_test_concurrency';

describe('read-snapshot connection isolation (#3732) @integration', function () {
    this.timeout(0);
    let db, helperDb;

    before(async function () {
        helperDb = await createDatabase(DB);
        await helperDb.doQuery('DROP TABLE IF EXISTS blocks');
        await helperDb.doQuery('CREATE TABLE blocks (block_index BIGINT PRIMARY KEY, block_time BIGINT)');
        for (let i = 1; i <= 5; i++)
            await helperDb.doQuery('INSERT INTO blocks (block_index, block_time) VALUES (?, ?)', [i, 1000 + i]);

        db = new Database(TEST_DB_HOST, TEST_DB_PORT, DB, TEST_DB_USER, TEST_DB_PASS, new Utility(), 'indexer');
    });

    after(async function () {
        try { await db.close(); } catch (e) {}
        try { await helperDb.close(); } catch (e) {}
        await dropDatabase(DB);
    });

    it('a held read snapshot survives a concurrent writer transaction', async function () {
        const snap = await db.beginReadSnapshot();          // dedicated connection
        assert.strictEqual(await db.getTableCount('blocks', snap), 5);

        // Concurrent writer on the SAME Db instance via the shared transaction
        // path. Pre-#3732 this released the snapshot's pinned connection.
        await db.beginTransaction();
        await db.doQuery('INSERT INTO blocks (block_index, block_time) VALUES (?, ?)', [6, 1006]);
        await db.commitTransaction();

        // REPEATABLE READ + CONSISTENT SNAPSHOT → the snapshot still sees 5.
        assert.strictEqual(await db.getTableCount('blocks', snap), 5,
            'snapshot must stay isolated from the concurrent committed write');

        await db.commitReadSnapshot(snap);

        // A fresh (non-snapshot) read observes the committed write.
        assert.strictEqual(await db.getTableCount('blocks'), 6);
    });

    it('two concurrent read snapshots use independent connections', async function () {
        const a = await db.beginReadSnapshot();
        const b = await db.beginReadSnapshot();             // pre-#3732 this released `a`
        assert.strictEqual(await db.getTableCount('blocks', a), 6);
        assert.strictEqual(await db.getTableCount('blocks', b), 6);

        await db.beginTransaction();
        await db.doQuery('INSERT INTO blocks (block_index, block_time) VALUES (?, ?)', [7, 1007]);
        await db.commitTransaction();

        // Each snapshot keeps its own fixed read view.
        assert.strictEqual(await db.getTableCount('blocks', a), 6);
        assert.strictEqual(await db.getTableCount('blocks', b), 6);

        await db.commitReadSnapshot(a);
        await db.commitReadSnapshot(b);
        assert.strictEqual(await db.getTableCount('blocks'), 7);
    });
});
