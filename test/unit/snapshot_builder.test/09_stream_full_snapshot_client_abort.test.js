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

function streamFullSnapshotAbortTests(){
    it('releases the read view and stops when the client disconnects mid-stream', async function(){
        let db = createMockDb();
        db.getLastBlock.resolves(50);
        db.getBlockHashRow.resolves({ ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c' });
        db.doQuery.resolves([{ table_name: 'blocks' }]);
        db.getTableCount.resolves(1);
        let res = new PassThrough();
        res.setHeader = sinon.stub();
        res.on('data', () => {});
        // Simulate the client vanishing exactly when the table read starts.
        db.streamTableRows.callsFake(() => { res.emit('close'); return Readable.from([{ block_index: 50 }]); });

        // Resolves (does not reject): a client abort is swallowed, not a 500.
        await builder.streamFullSnapshot(db, res);
        assert.ok(db.rollbackReadSnapshot.calledOnce, 'read view rolled back on abort');
        assert.ok(db.commitReadSnapshot.notCalled, 'never commits after an abort');
    });
}

function snapshotBuilderTests(){
    beforeEach(prepareSnapshotBuilder);
    afterEach(restoreSnapshotBuilder);
    describe('streamFullSnapshot client-abort', streamFullSnapshotAbortTests);
}

describe('SnapshotBuilder', snapshotBuilderTests);
