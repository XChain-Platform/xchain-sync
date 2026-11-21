// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// A client that disconnects while a windowed table is mid-stream leaves the
// driver holding unread rows on the snapshot connection. The rollback queued
// behind them must still run, the connection must go back to the pool, and
// the builder's snapshot slot must free for the next request.

const assert = require('assert');
const testDb = require('../helpers/testDb');
const SnapshotBuilder = require('../../../src/server/snapshot_builder');
const { SnapshotStreamWriter } = require('../../../src/server/snapshot_builder/stream_writer');
const { SINCE, EXPECTED, seedWindow } = require('./helpers/window_seed');
const { makeSink, within, runIncremental } = require('./helpers/snapshot_run');

const DB_NAME = 'xchain_sync_test_incr_abort';

// Disconnect the response once `rowsIn` rows of `table` have been written.
function disconnectMidTable(table, rowsIn) {
    const write = SnapshotStreamWriter.prototype.write;
    let seen = -1;
    SnapshotStreamWriter.prototype.write = function (chunk) {
        if (chunk === '"' + table + '":[') seen = 0;
        else if (seen >= 0 && ++seen === rowsIn * 2) this._res.emit('close');
        return write.call(this, chunk);
    };
    return () => { SnapshotStreamWriter.prototype.write = write; };
}

describe('Integration: incremental snapshot client abort mid-table', function () {
    this.timeout(60000);
    let db;
    let builder;

    before(async function () {
        await testDb.dropDatabase(DB_NAME);
        db = await testDb.createDatabase(DB_NAME);
        await testDb.seedSchema(db);
        await seedWindow(db);
        builder = new SnapshotBuilder(testDb.util);
    });

    after(async function () {
        if (db) await db.close();
        await testDb.dropDatabase(DB_NAME);
    });

    it('rolls back the read view and frees the slot when the reader leaves mid-table', async function () {
        const rollbacks = [];
        const rollback = db.rollbackReadSnapshot;
        db.rollbackReadSnapshot = async function (conn) {
            const done = rollback.call(this, conn);
            rollbacks.push(done);
            return await done;
        };
        const restore = disconnectMidTable('credits', 3);
        try {
            const sink = makeSink();
            await within(builder.streamIncrementalSnapshot(db, SINCE, sink), 10000, 'aborted snapshot');
            assert.strictEqual(rollbacks.length, 1, 'the read snapshot was not rolled back');
            await within(rollbacks[0], 10000, 'rollback behind the abandoned stream');
        } finally {
            restore();
            delete db.rollbackReadSnapshot;
        }
        assert.strictEqual(db.pool.activeConnections(), 0, 'the snapshot connection was not released');
        assert.strictEqual(builder._inflightSnapshots.get(db), undefined, 'the snapshot slot was not freed');

        const next = await runIncremental(builder, db, SINCE);
        assert.strictEqual(JSON.parse(next.text).tables.credits.length, EXPECTED.credits);
    });
});
