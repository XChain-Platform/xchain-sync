// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// The incremental snapshot over a window larger than the driver's row buffer,
// on a real MariaDB: every windowed table streams, every side set reads only
// the rows below the window, and the payload matches the array path's bytes.

const assert = require('assert');
const testDb = require('../helpers/testDb');
const SnapshotBuilder = require('../../../src/server/snapshot_builder');
const { encodeRow, bigIntReplacer } = require('../../../src/util/wire_codec');
const { SINCE, LAST, QUIET_FROM, EXPECTED, seedWindow } = require('./helpers/window_seed');
const { runIncremental } = require('./helpers/snapshot_run');

const DB_NAME = 'xchain_sync_test_incr_window';
const SIDE_TABLES = /\b(credits|escrows|validator_rewards)\b/;

const wire = (rows) => rows.map(r => JSON.stringify(encodeRow(r), bigIntReplacer));
const sorted = (strings) => [...strings].sort();

// The array path over one read view: each table's base window as the unbounded
// SELECT returns it, and the rows the side sets merge after it.
async function arrayPathTables(builder, db) {
    const conn = await db.beginReadSnapshot();
    try {
        const first = await db.getFirstActionIndex(SINCE, conn);
        const byAction = (t) => db.findRowsFromActionIndex(t, first, conn);
        const byBlock = (t) => db.findRowsFromBlockKey(t, 'block_index', SINCE, conn);
        const out = {};
        for (const t of ['credits', 'escrows']) {
            const base = await byAction(t);
            out[t] = { base, merged: await builder.mergeMaturedCooldownRows(db, t, [...base], SINCE, LAST, conn), key: 'action_index' };
        }
        const vr = await byBlock('validator_rewards');
        out.validator_rewards = { base: vr, merged: await builder.mergeValidatorRewardRows(db, [...vr], SINCE, LAST, conn), key: 'block_index' };
        for (const [t, key, read] of [['actions', 'action_index', byAction], ['blocks', 'block_index', byBlock]]) {
            const base = await read(t);
            out[t] = { base, merged: base, key };
        }
        return out;
    } finally {
        await db.rollbackReadSnapshot(conn);
    }
}

// Same rows, the same ORDER BY key sequence across the base window, and the
// same side rows after it. Rows tied on a non-unique key, and side rows read
// without an ORDER BY, have no fixed order in either path.
function assertSameAsArrayPath(table, got, want) {
    const n = want.base.length;
    const gotWire = got.map(r => JSON.stringify(r));
    const wantWire = wire(want.merged).map(s => JSON.stringify(JSON.parse(s)));
    assert.ok(n > 16, table + ' window too small to exercise the driver buffer');
    assert.deepStrictEqual(sorted(gotWire), sorted(wantWire), table + ' rows differ from the array path');
    assert.deepStrictEqual(got.slice(0, n).map(r => r[want.key]), want.base.map(r => Number(r[want.key])), table + ' base order differs');
    assert.deepStrictEqual(sorted(gotWire.slice(n)), sorted(wantWire.slice(n)), table + ' side rows differ');
}

function assertBoundedReads(out) {
    const sideReads = out.reads.filter(r => SIDE_TABLES.test(r.sql));
    // Two cooldown legs per ledger table plus two reward channels.
    assert.strictEqual(sideReads.length, 6, JSON.stringify(sideReads.map(r => r.sql.slice(0, 60))));
    for (const r of sideReads)
        assert.ok(r.n <= EXPECTED.maxSideSetRead, r.n + ' rows from a side set: ' + r.sql.slice(0, 120));
    for (const table of ['credits', 'escrows', 'validator_rewards', 'actions', 'blocks'])
        assert.ok(out.streamed.some(sql => sql.includes('`' + table + '`')), table + ' was not streamed');
}

describe('Integration: incremental snapshot over a deep window', function () {
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

    it('streams every windowed table and reads each side set only below the window', async function () {
        const out = await runIncremental(builder, db, SINCE);
        const payload = JSON.parse(out.text);
        assert.strictEqual(payload.block_height, LAST);
        assert.strictEqual(payload.tables.credits.length, EXPECTED.credits);
        assert.strictEqual(payload.tables.escrows.length, EXPECTED.escrows);
        assert.strictEqual(payload.tables.validator_rewards.length, EXPECTED.validator_rewards);
        assertBoundedReads(out);
    });

    it('emits the rows the unbounded array path builds for the same view', async function () {
        const payload = JSON.parse((await runIncremental(builder, db, SINCE)).text);
        const expected = await arrayPathTables(builder, db);
        for (const [table, want] of Object.entries(expected))
            assertSameAsArrayPath(table, payload.tables[table], want);
    });

    it('ships a backdated maturity over a window that has no first action', async function () {
        const out = await runIncremental(builder, db, QUIET_FROM);
        const payload = JSON.parse(out.text);
        assert.strictEqual(payload.since_block, QUIET_FROM);
        // 210 is backdated below the window; 2950 and 3050 were unstaked in
        // blocks 29 and 30 and mature in the quiet tail.
        const matured = (rows) => rows.map(r => r.action_index).sort((a, b) => a - b);
        assert.deepStrictEqual(matured(payload.tables.credits), [210, 2950, 3050]);
        assert.deepStrictEqual(matured(payload.tables.escrows), [210, 2950, 3050]);
        assert.strictEqual(payload.tables.actions, undefined);
    });
});
