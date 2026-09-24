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

// Backpressure + client-abort also cover the two bounded paging streams
// (streamTableRowsById / streamDispensers), which hold no read view but could still
// buffer their output in RAM / write to a dead socket on a slow or vanished reader.
// Force gzip backpressure by stubbing createGzip with a fake whose write() always
// returns false, so the first writer.write parks on 'drain'; then disconnect.
function fakeBackpressuredGzip(){
    let ee = new (require('events').EventEmitter)();
    ee.destroyed = false;
    ee.write   = sinon.stub().returns(false);
    ee.end     = sinon.stub();
    ee.destroy = sinon.stub().callsFake(() => { ee.destroyed = true; });
    ee.pipe    = sinon.stub();
    return ee;
}

function pagingStreamAbortTests(){
    it('streamTableRowsById: swallows the abort and destroys gzip on client disconnect', async function(){
        let db = createMockDb();
        db.doQuery.resolves([{ id: 1 }, { id: 2 }]);
        let fake = fakeBackpressuredGzip();
        sinon.stub(zlib, 'createGzip').returns(fake);
        let res = new PassThrough(); res.setHeader = sinon.stub(); res.on('data', () => {});

        let p = builder.streamTableRowsById(db, 'index_transactions', 0, 50000, res);
        await new Promise(r => setImmediate(r));   // reach the parked first write
        res.emit('close');                          // client vanished
        await p;                                    // resolves (abort swallowed, not a throw)
        assert.ok(fake.destroy.called, 'gzip destroyed to free buffered output on abort');
    });

    it('streamDispensers: swallows the abort and destroys gzip on client disconnect', async function(){
        let db = createMockDb(); db.dbType = 'decoder';
        db.doQueryStrict.resolves([{ tx_index: 1, address_id: 2 }]);
        let fake = fakeBackpressuredGzip();
        sinon.stub(zlib, 'createGzip').returns(fake);
        let res = new PassThrough(); res.setHeader = sinon.stub(); res.on('data', () => {});

        let p = builder.streamDispensers(db, NaN, NaN, res);
        await new Promise(r => setImmediate(r));
        res.emit('close');
        await p;
        assert.ok(fake.destroy.called, 'gzip destroyed on abort');
    });
}

function snapshotBuilderTests(){
    beforeEach(prepareSnapshotBuilder);
    afterEach(restoreSnapshotBuilder);
    describe('paging-stream client-abort', pagingStreamAbortTests);
}

describe('SnapshotBuilder', snapshotBuilderTests);
