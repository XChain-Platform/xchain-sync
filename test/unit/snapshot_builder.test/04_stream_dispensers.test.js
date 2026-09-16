// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

const {
    assert,
    sinon,
    PassThrough,
    Readable,
    EventEmitter,
    zlib,
    SnapshotBuilder,
    SnapshotStreamWriter,
    Utility,
    poolSizing,
    createMockDb,
    createMockRes
} = require('./helpers/support');

let builder;

function prepareSnapshotBuilder(){
    const util = new Utility();
    builder = new SnapshotBuilder(util);
    sinon.stub(console, 'error');
}

function restoreSnapshotBuilder(){
    sinon.restore();
}


// The snapshot must be read inside a single REPEATABLE READ transaction so
// the block-height anchor, the hash headers, and every table read observe
// one consistent point in time. These tests pin that boundary: the snapshot
// opens before the anchor read, and the connection is always released
// (commit on success/empty, rollback on error) so it can't leak.
function streamDispensersPagingTests(){
    it('rejects a non-decoder dbType with 400', async function(){
        let db = createMockDb(); // dbType defaults to indexer-shaped (undefined)
        let res = createMockRes();
        await builder.streamDispensers(db, NaN, NaN, 50000, res);
        assert.ok(res.status.calledWith(400), 'dispensers reconcile is decoder-only');
    });

    it('first page (no cursor) selects the full table ordered by the composite PK', async function(){
        let db = createMockDb();
        db.dbType = 'decoder';
        db.doQueryStrict.resolves([{ tx_index: 5, address_id: 9 }, { tx_index: 7, address_id: 2 }]);
        let res = new PassThrough();
        let chunks = [];
        res.on('data', c => chunks.push(c));
        res.setHeader = sinon.stub();
        await new Promise((resolve) => {
            res.on('finish', resolve);
            builder.streamDispensers(db, NaN, NaN, 50000, res);
        });
        let parsed = JSON.parse(zlib.gunzipSync(Buffer.concat(chunks)).toString());
        assert.strictEqual(parsed.rows.length, 2);
        assert.strictEqual(parsed.max_tx, 7, 'max_tx is the last row tx_index');
        assert.strictEqual(parsed.max_addr, 2, 'max_addr is the last row address_id');
        assert.strictEqual(parsed.has_more, false, 'single-response contract: never more');
        let q = db.doQueryStrict.firstCall.args[0];
        assert.ok(/ORDER BY tx_index ASC, address_id ASC/.test(q));
        assert.ok(!/LIMIT/.test(q),
            'no LIMIT: the full table must ship in ONE statement-consistent response ' +
            '(cross-request pages tear under in-place soft-expires)');
        assert.ok(!/WHERE/.test(q), 'no cursor -> no predicate');
    });

    it('honours a legacy cursor within the same single response and never reports more', async function(){
        let db = createMockDb();
        db.dbType = 'decoder';
        db.doQueryStrict.resolves([{ tx_index: 8, address_id: 1 }, { tx_index: 8, address_id: 4 }, { tx_index: 9, address_id: 0 }]);
        let res = new PassThrough();
        let chunks = [];
        res.on('data', c => chunks.push(c));
        res.setHeader = sinon.stub();
        await new Promise((resolve) => {
            res.on('finish', resolve);
            builder.streamDispensers(db, 8, 1, 3, res);
        });
        let parsed = JSON.parse(zlib.gunzipSync(Buffer.concat(chunks)).toString());
        assert.strictEqual(parsed.has_more, false,
            'has_more is always false so an old paging client completes in one round trip');
        let q = db.doQueryStrict.firstCall.args[0];
        assert.ok(/WHERE \(tx_index > \? OR \(tx_index = \? AND address_id > \?\)\)/.test(q), 'composite keyset predicate');
        assert.ok(!/LIMIT/.test(q), 'legacy limit arg is ignored: no paging');
        assert.deepStrictEqual(db.doQueryStrict.firstCall.args[1], [8, 8, 1]);
    });
}

function streamDispensersFailureTests(){

    // Fail CLOSED on the dump read. doQuery is fail-soft outside a transaction, so
    // a source-DB fault read through it becomes a 200 body of zero rows, which the
    // follower applies as DELETE-without-insert over its whole dispensers table.
    it('rejects instead of shipping an empty dump when the source read fails @regression', async function(){
        let db = createMockDb();
        db.dbType = 'decoder';
        db.doQueryStrict.rejects(new Error('ER_LOCK_WAIT_TIMEOUT: errno 1205'));
        // doQuery must not be the escape hatch: a fail-soft [] here would ship a dump.
        db.doQuery.resolves([]);
        let chunks = [];
        let res = new PassThrough();
        res.on('data', c => chunks.push(c));
        res.setHeader = sinon.stub();

        await assert.rejects(
            () => builder.streamDispensers(db, NaN, NaN, 50000, res),
            /errno 1205/,
            'the read error must reach the route handler, which answers 500');
        assert.strictEqual(res.setHeader.called, false, 'no response headers before the read succeeds');
        assert.strictEqual(Buffer.concat(chunks).length, 0, 'no dump body written');
    });

    it('rejects on a failed read in the cursor branch too @regression', async function(){
        let db = createMockDb();
        db.dbType = 'decoder';
        db.doQueryStrict.rejects(new Error('ER_LOCK_WAIT_TIMEOUT: errno 1205'));
        let res = new PassThrough();
        res.on('data', () => {});
        res.setHeader = sinon.stub();

        await assert.rejects(
            () => builder.streamDispensers(db, 8, 1, 3, res),
            /errno 1205/);
    });
}

function snapshotBuilderTests(){
    beforeEach(prepareSnapshotBuilder);
    afterEach(restoreSnapshotBuilder);
    describe('streamDispensers', streamDispensersPagingTests);
    describe('streamDispensers', streamDispensersFailureTests);
}

describe('SnapshotBuilder', snapshotBuilderTests);
