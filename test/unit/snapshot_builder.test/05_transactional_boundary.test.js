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

function transactionalFullSnapshotTests(){
    it('full: opens read snapshot before reading the block anchor', async function(){
        let db = createMockDb();
        db.getLastBlock.resolves(50);
        db.getBlockHashRow.resolves({ ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c' });
        db.doQuery.resolves([]); // no tables

        let res = new PassThrough();
        res.setHeader = sinon.stub();
        await new Promise((resolve) => {
            res.on('finish', resolve);
            builder.streamFullSnapshot(db, res);
        });

        assert.ok(db.beginReadSnapshot.calledOnce, 'beginReadSnapshot called once');
        assert.ok(db.beginReadSnapshot.calledBefore(db.getLastBlock), 'snapshot opens before anchor read');
        assert.ok(db.commitReadSnapshot.calledOnce, 'commit releases the read view');
        assert.ok(db.rollbackReadSnapshot.notCalled, 'no rollback on success');
    });

    it('full: commits (releases) the snapshot even on the 404 empty-db path', async function(){
        let db = createMockDb();
        db.getLastBlock.resolves(null);
        let res = createMockRes();
        await builder.streamFullSnapshot(db, res);
        assert.ok(db.beginReadSnapshot.calledOnce);
        assert.ok(db.commitReadSnapshot.calledOnce, 'snapshot released on 404 so the connection is not leaked');
    });

    it('full: rolls back the snapshot if a read throws before streaming', async function(){
        let db = createMockDb();
        db.getLastBlock.resolves(50);
        db.getBlockHashRow.rejects(new Error('boom'));
        let res = createMockRes();
        await assert.rejects(builder.streamFullSnapshot(db, res), /boom/);
        assert.ok(db.rollbackReadSnapshot.calledOnce, 'snapshot rolled back on error');
        assert.ok(db.commitReadSnapshot.notCalled, 'no commit on error');
    });
}

function transactionalIncrementalSnapshotTests(){

    it('incremental: opens read snapshot before reading the block anchor', async function(){
        let db = createMockDb();
        db.getLastBlock.resolves(100);
        db.getBlockHashRow.resolves({ ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c' });
        db.getFirstActionIndex.resolves(500);
        db.doQuery.callsFake(async (query) => {
            if(query.includes('information_schema')) return [{ table_name: 'blocks' }];
            return [];
        });

        let res = new PassThrough();
        res.setHeader = sinon.stub();
        await new Promise((resolve) => {
            res.on('finish', resolve);
            builder.streamIncrementalSnapshot(db, 80, res);
        });

        assert.ok(db.beginReadSnapshot.calledOnce);
        assert.ok(db.beginReadSnapshot.calledBefore(db.getLastBlock));
        assert.ok(db.commitReadSnapshot.calledOnce);
        assert.ok(db.rollbackReadSnapshot.notCalled);
    });

    it('incremental: commits (releases) the snapshot on the 404 path', async function(){
        let db = createMockDb();
        db.getLastBlock.resolves(50);
        let res = createMockRes();
        await builder.streamIncrementalSnapshot(db, 100, res);
        assert.ok(db.beginReadSnapshot.calledOnce);
        assert.ok(db.commitReadSnapshot.calledOnce, 'snapshot released on 404');
    });
}

function snapshotBuilderTests(){
    beforeEach(prepareSnapshotBuilder);
    afterEach(restoreSnapshotBuilder);
    describe('transactional boundary', transactionalFullSnapshotTests);
    describe('transactional boundary', transactionalIncrementalSnapshotTests);
}

describe('SnapshotBuilder', snapshotBuilderTests);
