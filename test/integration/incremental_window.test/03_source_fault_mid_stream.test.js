// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// A streamed table can lose its source connection after the response has
// started, as MariaDB does when net_write_timeout expires behind a stalled
// reader. The snapshot must fail closed: reject, destroy the partial
// response so the follower sees the cut at once, and free the slot.

const assert = require('assert');
const zlib = require('zlib');
const testDb = require('../helpers/testDb');
const SnapshotBuilder = require('../../../src/server/snapshot_builder');
const { SnapshotStreamWriter } = require('../../../src/server/snapshot_builder/stream_writer');
const { SINCE, EXPECTED, seedWindow } = require('./helpers/window_seed');
const { makeSink, within, runIncremental } = require('./helpers/snapshot_run');

const DB_NAME = 'xchain_sync_test_incr_fault';
const pause = (ms) => new Promise(r => setTimeout(r, ms));

// Once `rowsIn` rows of `table` are written, stall the reader, kill the
// snapshot connection from another session, then let the reader resume.
function killMidTable(db, sink, conns, table, rowsIn) {
    const write = SnapshotStreamWriter.prototype.write;
    let seen = -1;
    const fired = [];
    SnapshotStreamWriter.prototype.write = function (chunk) {
        if (chunk === '"' + table + '":[') seen = 0;
        else if (seen >= 0 && ++seen === rowsIn * 2) {
            let release;
            sink.hold = new Promise(r => { release = r; });
            fired.push((async () => {
                await pause(200);
                await db.doQuery('KILL CONNECTION ' + Number(conns[0].threadId));
                await pause(300);
                sink.hold = null;
                release();
            })());
        }
        return write.call(this, chunk);
    };
    return { fired, restore: () => { SnapshotStreamWriter.prototype.write = write; } };
}

describe('Integration: incremental snapshot source fault mid-stream', function () {
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

    it('rejects, destroys the started response and frees the slot when the source connection drops', async function () {
        const conns = [];
        const begin = db.beginReadSnapshot;
        db.beginReadSnapshot = async function () { const c = await begin.call(this); conns.push(c); return c; };
        const sink = makeSink();
        const fault = killMidTable(db, sink, conns, 'credits', 3);
        let err = null;
        try {
            await within(builder.streamIncrementalSnapshot(db, SINCE, sink), 15000, 'faulted snapshot')
                .catch((e) => { err = e; });
            await Promise.all(fault.fired);
        } finally {
            fault.restore();
            delete db.beginReadSnapshot;
        }
        assert.strictEqual(fault.fired.length, 1, 'the fault was never injected');
        assert.ok(err && !err.aborted && !/did not settle/.test(err.message), 'expected a source error, got ' + err);
        assert.strictEqual(sink.destroyed, true, 'the started response was left open');
        assert.strictEqual(sink.writableFinished, false);
        assert.throws(() => JSON.parse(zlib.gunzipSync(sink.bytes(), { finishFlush: zlib.constants.Z_SYNC_FLUSH }).toString()));
        assert.strictEqual(builder._inflightSnapshots.get(db), undefined, 'the snapshot slot was not freed');

        const next = await runIncremental(builder, db, SINCE);
        assert.strictEqual(JSON.parse(next.text).tables.credits.length, EXPECTED.credits);
    });
});
