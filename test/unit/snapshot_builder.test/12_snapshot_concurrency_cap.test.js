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

// Per-Database concurrency cap on the long-lived read-snapshot
// streams, so a bootstrap stampede can never pin every pool connection and
// starve ServerPoller's live-broadcast reads.
let savedEnv;

function prepareConcurrencyEnvironment(){
    savedEnv = {
        DB_POOL_SIZE: process.env.DB_POOL_SIZE,
        MAX_CONCURRENT_SNAPSHOTS: process.env.MAX_CONCURRENT_SNAPSHOTS
    };
    delete process.env.DB_POOL_SIZE;
    delete process.env.MAX_CONCURRENT_SNAPSHOTS;
}

function restoreConcurrencyEnvironment(){
    for(let k of Object.keys(savedEnv)){
        if(savedEnv[k] === undefined) delete process.env[k];
        else process.env[k] = savedEnv[k];
    }
}

function snapshotConcurrencySizingTests(){
    beforeEach(prepareConcurrencyEnvironment);
    afterEach(restoreConcurrencyEnvironment);
    it('defaults the cap to poolSize - 2 (reserves poller + one short-read conn)', function(){
        let db = createMockDb();
        db.connectionPoolParams = { connectionLimit: 5 };
        assert.strictEqual(builder.snapshotCap(db), 3);
    });

    // With no connectionPoolParams to read, the cap falls back to the
    // same per-dbType sizing db.js uses (DB_POOL_SIZE_<DBTYPE> > DB_POOL_SIZE >
    // per-dbType default), so a decoder Database never inherits the indexer's cap.
    it('falls back to per-dbType pool sizing, then DB_POOL_SIZE env', function(){
        let indexerDb  = createMockDb();
        let decoderDb  = createMockDb();
        decoderDb.dbType = 'decoder';
        assert.strictEqual(builder.snapshotCap(indexerDb), poolSizing.DEFAULT_POOL_SIZE.indexer - 2);
        assert.strictEqual(builder.snapshotCap(decoderDb), poolSizing.DEFAULT_POOL_SIZE.decoder - 2);
        process.env.DB_POOL_SIZE = '10';
        assert.strictEqual(builder.snapshotCap(createMockDb()), 8);
        process.env.DB_POOL_SIZE_DECODER = '6';
        assert.strictEqual(builder.snapshotCap(decoderDb), 4);
        delete process.env.DB_POOL_SIZE_DECODER;
    });

    it('honours MAX_CONCURRENT_SNAPSHOTS but clamps to [1, poolSize - 1]', function(){
        let db = createMockDb();
        db.connectionPoolParams = { connectionLimit: 5 };
        process.env.MAX_CONCURRENT_SNAPSHOTS = '2';
        assert.strictEqual(builder.snapshotCap(db), 2);
        // Can never hand the poller's last connection to snapshots.
        process.env.MAX_CONCURRENT_SNAPSHOTS = '99';
        assert.strictEqual(builder.snapshotCap(db), 4);
        // Never below 1 (a 0/negative override would deadlock bootstraps).
        process.env.MAX_CONCURRENT_SNAPSHOTS = '0';
        assert.strictEqual(builder.snapshotCap(db), 1);
    });

    it('cap is per Database instance, floored at 1 for tiny pools', function(){
        let db = createMockDb();
        db.connectionPoolParams = { connectionLimit: 2 };
        assert.strictEqual(builder.snapshotCap(db), 1);
    });
}

function snapshotConcurrencyAdmissionTests(){
    beforeEach(prepareConcurrencyEnvironment);
    afterEach(restoreConcurrencyEnvironment);
    it('rejects a full snapshot with 503 SNAPSHOT_BUSY once the cap is reached, without opening a read view', async function(){
        process.env.MAX_CONCURRENT_SNAPSHOTS = '1';
        let db = createMockDb();

        // First stream: hold beginReadSnapshot open so the slot stays taken.
        let releaseFirst;
        db.beginReadSnapshot = sinon.stub().callsFake(() =>
            new Promise(resolve => { releaseFirst = () => resolve({ _snapshotConn: true }); }));
        let res1 = createMockRes();
        let firstStream = builder.streamFullSnapshot(db, res1);
        await new Promise(setImmediate);

        // Second stream: must be refused up front.
        let res2 = createMockRes();
        await builder.streamFullSnapshot(db, res2);
        assert.ok(res2.status.calledWith(503), 'second request got 503');
        assert.strictEqual(res2.json.firstCall.args[0].code, 'SNAPSHOT_BUSY');
        assert.strictEqual(res2._headers['Retry-After'], '30');
        assert.strictEqual(db.beginReadSnapshot.callCount, 1, 'rejected request never touched the pool');
        assert.strictEqual(builder.snapshotsRejected, 1);

        // Let the first finish (getLastBlock null -> 404 path) and verify the
        // slot is released for the next bootstrap.
        releaseFirst();
        await firstStream;
        assert.strictEqual(builder._inflightSnapshots.size, 0, 'slot released');
        db.beginReadSnapshot = sinon.stub().resolves({ _snapshotConn: true });
        let res3 = createMockRes();
        await builder.streamFullSnapshot(db, res3);
        assert.ok(res3.status.calledWith(404), 'post-release request admitted');
    });
}

function snapshotConcurrencyIsolationTests(){
    beforeEach(prepareConcurrencyEnvironment);
    afterEach(restoreConcurrencyEnvironment);
    it('caps incremental snapshots on the same per-Database semaphore', async function(){
        process.env.MAX_CONCURRENT_SNAPSHOTS = '1';
        let db = createMockDb();
        let releaseFirst;
        db.beginReadSnapshot = sinon.stub().callsFake(() =>
            new Promise(resolve => { releaseFirst = () => resolve({ _snapshotConn: true }); }));
        let inflightFull = builder.streamFullSnapshot(db, createMockRes());
        await new Promise(setImmediate);

        let res = createMockRes();
        await builder.streamIncrementalSnapshot(db, 100, res);
        assert.ok(res.status.calledWith(503), 'incremental refused while full stream holds the slot');
        assert.strictEqual(res.json.firstCall.args[0].code, 'SNAPSHOT_BUSY');

        releaseFirst();
        await inflightFull;
    });

    it('tracks slots independently per Database (one chain cannot starve another)', async function(){
        process.env.MAX_CONCURRENT_SNAPSHOTS = '1';
        let dbA = createMockDb('db_a');
        let releaseA;
        dbA.beginReadSnapshot = sinon.stub().callsFake(() =>
            new Promise(resolve => { releaseA = () => resolve({ _snapshotConn: true }); }));
        let inflightA = builder.streamFullSnapshot(dbA, createMockRes());
        await new Promise(setImmediate);

        // A different Database has its own pool, so its slot is free.
        let dbB = createMockDb('db_b');
        let resB = createMockRes();
        await builder.streamFullSnapshot(dbB, resB);
        assert.ok(resB.status.calledWith(404), 'other Database admitted (hit its normal empty-db 404)');

        releaseA();
        await inflightA;
    });

    it('releases the slot when the stream throws', async function(){
        process.env.MAX_CONCURRENT_SNAPSHOTS = '1';
        let db = createMockDb();
        db.beginReadSnapshot = sinon.stub().rejects(new Error('pool acquire failed'));
        await assert.rejects(() => builder.streamFullSnapshot(db, createMockRes()), /pool acquire failed/);
        assert.strictEqual(builder._inflightSnapshots.size, 0, 'slot released on error');
    });
}

function snapshotBuilderTests(){
    beforeEach(prepareSnapshotBuilder);
    afterEach(restoreSnapshotBuilder);
    describe('snapshot concurrency cap', snapshotConcurrencySizingTests);
    describe('snapshot concurrency cap', snapshotConcurrencyAdmissionTests);
    describe('snapshot concurrency cap', snapshotConcurrencyIsolationTests);
}

describe('SnapshotBuilder', snapshotBuilderTests);
