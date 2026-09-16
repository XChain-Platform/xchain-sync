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

// Fix #1 (HIGH): the gzip write loop ignored backpressure and had no client-abort
// handler, so a slow/half-open reader pinned the REPEATABLE READ connection and
// buffered the whole snapshot in RAM (a pool-exhaustion / OOM path on the
// unauthenticated snapshot routes). SnapshotStreamWriter applies backpressure and
// tears the stream down the moment the client disconnects.
// Minimal gzip stand-in: an EventEmitter whose write() returns a preset value
// so we can drive the drain/backpressure paths deterministically.
function fakeGzip(writeReturns){
    let ee = new EventEmitter();
    ee.destroyed = false;
    ee.write = sinon.stub().returns(writeReturns);
    ee.end = sinon.stub();
    ee.destroy = sinon.stub().callsFake(() => { ee.destroyed = true; });
    return ee;
}

const tick = () => new Promise(r => setImmediate(r));

function snapshotStreamWriterTests(){
    it('resolves immediately when the buffer has room (write() returns true)', async function(){
        let gzip = fakeGzip(true);
        let w = new SnapshotStreamWriter(gzip, new EventEmitter());
        await w.write('x');
        assert.ok(gzip.write.calledOnceWith('x'));
    });

    it('blocks until drain when the buffer is full (write() returns false)', async function(){
        let gzip = fakeGzip(false);
        let w = new SnapshotStreamWriter(gzip, new EventEmitter());
        let settled = false;
        let p = w.write('x').then(() => { settled = true; });
        await tick();
        assert.strictEqual(settled, false, 'write awaits drain when backpressured');
        gzip.emit('drain');
        await p;
        assert.strictEqual(settled, true, 'resolves once drain fires');
    });

    it('rejects (aborted) and destroys gzip when the client disconnects mid-write', async function(){
        let gzip = fakeGzip(false);
        let res = new EventEmitter();
        let w = new SnapshotStreamWriter(gzip, res);
        let p = w.write('x');            // parks on drain
        res.emit('close');               // client vanished
        await assert.rejects(p, e => e.aborted === true);
        assert.ok(gzip.destroy.called, 'gzip destroyed to free buffered chunks');
        // Every subsequent write also fails fast rather than writing to a dead stream.
        await assert.rejects(w.write('y'), e => e.aborted === true);
    });

    it('finish() detaches the disconnect handler and ends the stream', async function(){
        let gzip = fakeGzip(true);
        let res = new EventEmitter();
        let w = new SnapshotStreamWriter(gzip, res);
        w.finish();
        assert.ok(gzip.end.called, 'stream flushed on normal completion');
        // A late close (normal streams emit close after finish) must NOT re-abort.
        res.emit('close');
        assert.ok(gzip.destroy.notCalled, 'close after finish is ignored');
    });
}

function snapshotBuilderTests(){
    beforeEach(prepareSnapshotBuilder);
    afterEach(restoreSnapshotBuilder);
    describe('SnapshotStreamWriter (backpressure + client-abort)', snapshotStreamWriterTests);
}

describe('SnapshotBuilder', snapshotBuilderTests);
