// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// ClientSync IO/branch coverage. NEW tests only.
// Covers fetchAndApplySchema, bootstrapFromSnapshot, runIncrementalCatchUp,
// incrementalCatchUp, verifyAgainstSource, verifyDecoderCompleteness (catch),
// connectWebSocket/scheduleReconnect, stop, scheduleHeartbeat/flushHeartbeat/
// sendRestHeartbeat, and several small branches.

const assert     = require('assert');
const sinon      = require('sinon');
const axios      = require('axios');
const zlib       = require('zlib');
const proxyquire = require('proxyquire');
const EventEmitter = require('events');
const ClientSync = require('../../src/client/sync');
const { SCHEMA_VERSION } = require('../../src/schema/version');
const Utility    = require('../../src/util');
const HashVerifier = require('../../src/client/hash_verifier');
const realConfig = require('../../src/config');
// A fake database gains the real query methods it lacks, so a query that moved
// into a named db method still reaches the fake's doQuery exactly as before.
const { withDbMixins } = require('../helpers/db_mixins.js');

function createMockDb(overrides){
    return Object.assign({
        dbName:           'test_db',
        dbType:           'indexer',
        getLastBlock:     sinon.stub().resolves(null),
        getBlockHashRow:  sinon.stub().resolves(null),
        doQuery:          sinon.stub().resolves([]),
        getActiveHalt:    sinon.stub().resolves(null),
        getTableCount:    sinon.stub().resolves(0),
        addMissingColumns: sinon.stub().resolves(),
        recordHalt:       sinon.stub().resolves({ block_index: 0 }),
        clearHalt:        sinon.stub().resolves(1)
    }, overrides || {});
}
function createMockApplier(){
    return {
        applyBlock:               sinon.stub().resolves(),
        applyFullSnapshot:        sinon.stub().resolves(),
        applyIncrementalSnapshot: sinon.stub().resolves()
    };
}
function createMockRollback(){
    return { rollback: sinon.stub().resolves() };
}

function makeSync(configOverrides, dbOverrides){
    let db      = createMockDb(dbOverrides);
    let applier = createMockApplier();
    let rb      = createMockRollback();
    let hv      = new HashVerifier();
    let util    = new Utility();
    let config  = Object.assign({
        SYNC_SOURCES:          'http://src1:3006',
        VERIFY_HASHES:         false,
        CLIENT_RECONNECT_DELAY: 5000,
        HASH_CONFIRM_TIMEOUT:  5000,
        SNAPSHOT_MAX_CONTENT:  200 * 1024 * 1024,
        WS_MAX_PAYLOAD:        50 * 1024 * 1024,
        MAX_ROLLBACK_DEPTH:    10,
        GAP_LOG_INTERVAL_MS:   30000
    }, configOverrides || {});
    let sync = new ClientSync('bitcoin', 'mainnet', withDbMixins(db), applier, rb, hv, config, util);
    return { sync, db, applier, rb, hv, util, config };
}

describe('ClientSync: fetchAndApplySchema', function(){
    let sync, db;

    beforeEach(function(){
        ({ sync, db } = makeSync());
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });
    afterEach(function(){ sinon.restore(); });

    it('creates a table that does not yet exist', async function(){
        let getStub = sinon.stub(axios, 'get');
        getStub.resolves({ data: {
            tables: { goodtable: 'CREATE TABLE `goodtable` (id int)' }
        }});
        // information_schema check returns [] → table absent
        db.doQuery.onFirstCall().resolves([]);

        await sync.fetchAndApplySchema('http://src1:3006');

        // Two doQuery calls: info_schema check + CREATE TABLE
        assert.strictEqual(db.doQuery.callCount, 2);
        let createCall = db.doQuery.secondCall.args[0];
        assert.ok(createCall.indexOf('CREATE TABLE') !== -1, 'should issue CREATE TABLE');
    });

    it('calls addMissingColumns when table already exists', async function(){
        sinon.stub(axios, 'get').resolves({ data: {
            tables: { goodtable: 'CREATE TABLE `goodtable` (id int)' }
        }});
        // info_schema returns a row → table exists
        db.doQuery.onFirstCall().resolves([{ TABLE_NAME: 'goodtable' }]);

        await sync.fetchAndApplySchema('http://src1:3006');

        assert.ok(db.addMissingColumns.calledOnce, 'addMissingColumns must be called for existing table');
    });

    it('skips table with empty/falsy createSql', async function(){
        sinon.stub(axios, 'get').resolves({ data: {
            tables: { emptytable: '' }
        }});

        await sync.fetchAndApplySchema('http://src1:3006');

        assert.strictEqual(db.doQuery.called, false, 'empty DDL must be skipped');
    });
});

describe('ClientSync: fetchAndApplySchema', function(){
    let sync, db;

    beforeEach(function(){
        ({ sync, db } = makeSync());
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });
    afterEach(function(){ sinon.restore(); });



    it('rejects invalid table name (e.g. bad-name) and continues', async function(){
        sinon.stub(axios, 'get').resolves({ data: {
            tables: {
                'bad-name':  'CREATE TABLE `bad-name` (id int)',
                validtable:  'CREATE TABLE `validtable` (id int)'
            }
        }});
        db.doQuery.resolves([]);

        await sync.fetchAndApplySchema('http://src1:3006');

        let errorCalls = console.error.getCalls().map(c => c.args[0]);
        assert.ok(errorCalls.some(m => m && m.indexOf('Rejected table name') !== -1),
            'should log rejected table name');
        // validtable still processed: info_schema + CREATE TABLE
        assert.ok(db.doQuery.called, 'valid table should still be processed');
    });

    it('rejects invalid DDL (not CREATE TABLE) and continues', async function(){
        sinon.stub(axios, 'get').resolves({ data: {
            tables: {
                droptable: 'DROP TABLE foo',
                good:      'CREATE TABLE `good` (id int)'
            }
        }});
        db.doQuery.resolves([]);

        await sync.fetchAndApplySchema('http://src1:3006');

        let errorCalls = console.error.getCalls().map(c => c.args[0]);
        assert.ok(errorCalls.some(m => m && m.indexOf('Rejected DDL') !== -1),
            'should log rejected DDL');
    });

    it('records validated source table names for replica-gap checks', async function(){
        sinon.stub(axios, 'get').resolves({ data: {
            tables: {
                goodtable: 'CREATE TABLE `goodtable` (id int)',
                emptytable: '',
                'bad-name': 'CREATE TABLE `bad-name` (id int)'
            }
        }});
        db.doQuery.resolves([]);
        await sync.fetchAndApplySchema('http://src1:3006');

        assert.deepStrictEqual(sync._sourceTables, new Set(['goodtable', 'emptytable']));
    });
});

describe('ClientSync: fetchAndApplySchema', function(){
    let sync, db;

    beforeEach(function(){
        ({ sync, db } = makeSync());
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });
    afterEach(function(){ sinon.restore(); });



    it('swallows per-table doQuery error and continues', async function(){
        sinon.stub(axios, 'get').resolves({ data: {
            tables: {
                t1: 'CREATE TABLE `t1` (id int)',
                t2: 'CREATE TABLE `t2` (id int)'
            }
        }});
        // First table: info_schema check rejects
        db.doQuery.onFirstCall().rejects(new Error('db error'));
        // Second table: info_schema check returns []
        db.doQuery.onSecondCall().resolves([]);
        db.doQuery.resolves([]);

        // Should not throw
        await sync.fetchAndApplySchema('http://src1:3006');
    });

    it('logs outer catch when axios.get rejects', async function(){
        sinon.stub(axios, 'get').rejects(new Error('network fail'));

        await sync.fetchAndApplySchema('http://src1:3006');

        let errorCalls = console.error.getCalls().map(c => c.args[0]);
        assert.ok(errorCalls.some(m => m && m.indexOf('Failed to fetch schema') !== -1),
            'outer catch must log "Failed to fetch schema"');
        assert.strictEqual(sync._sourceTables, null, 'failed fetch must leave source schema unknown');
    });
});

describe('ClientSync: bootstrapFromSnapshot', function(){
    let sync, db, applier;

    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });
    afterEach(function(){ sinon.restore(); });

    it('throws (does not silently return) when no sources configured', async function(){
        ({ sync, db, applier } = makeSync({ SYNC_SOURCES: '' }));

        // A misconfigured replica must not proceed to live-follow on an empty DB.
        await assert.rejects(() => sync.bootstrapFromSnapshot(), /no sync sources configured/);

        let errorCalls = console.error.getCalls().map(c => c.args[0]);
        assert.ok(errorCalls.some(m => m && m.indexOf('No sync sources configured') !== -1));
        assert.strictEqual(applier.applyFullSnapshot.called, false);
    });

    it('happy path with raw (non-gzipped) buffer', async function(){
        ({ sync, db, applier } = makeSync());
        sinon.stub(sync, 'fetchAndApplySchema').resolves();
        let payload = Buffer.from(JSON.stringify({ block_height: 100, tables: {} }));
        sinon.stub(axios, 'get').resolves({ data: payload });

        await sync.bootstrapFromSnapshot();

        assert.ok(applier.applyFullSnapshot.calledOnce, 'applyFullSnapshot must be called');
        assert.strictEqual(sync.lastAppliedBlock, 100);
    });

    it('applies the full snapshot under the shared write lock (M-21)', async function(){
        // The full-snapshot apply is also the runtime recovery fallback for an
        // oversized incremental catch-up, where live-follow is active. It must run
        // under withApplyLock so a concurrent live-block apply or cross-source
        // fallback timer cannot open a second write transaction and clobber the
        // snapshot's DELETE+reload mid-flight.
        ({ sync, db, applier } = makeSync());
        sinon.stub(sync, 'fetchAndApplySchema').resolves();
        let lockSpy = sinon.spy(sync, 'withApplyLock');
        let payload = Buffer.from(JSON.stringify({ block_height: 7, tables: {} }));
        sinon.stub(axios, 'get').resolves({ data: payload });

        await sync.bootstrapFromSnapshot();

        assert.ok(applier.applyFullSnapshot.calledOnce, 'applyFullSnapshot must be called');
        assert.ok(lockSpy.calledOnce, 'applyFullSnapshot must be wrapped in withApplyLock');
        // The lock must wrap the apply, not merely run alongside it.
        assert.ok(lockSpy.calledBefore(applier.applyFullSnapshot),
            'withApplyLock must be entered before applyFullSnapshot runs');
    });
});

describe('ClientSync: bootstrapFromSnapshot', function(){
    let sync, db, applier;

    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });
    afterEach(function(){ sinon.restore(); });



    it('happy path with gzipped buffer', async function(){
        ({ sync, db, applier } = makeSync());
        sinon.stub(sync, 'fetchAndApplySchema').resolves();
        let rawBuf = Buffer.from(JSON.stringify({ block_height: 5, tables: {} }));
        let gzBuf  = zlib.gzipSync(rawBuf);
        sinon.stub(axios, 'get').resolves({ data: gzBuf });

        await sync.bootstrapFromSnapshot();

        assert.ok(applier.applyFullSnapshot.calledOnce);
        assert.strictEqual(sync.lastAppliedBlock, 5);
    });

    it('indexer + 2 sources + VERIFY_HASHES calls verifyAgainstSource', async function(){
        ({ sync, db, applier } = makeSync({
            SYNC_SOURCES: 'http://src1:3006,http://src2:3006',
            VERIFY_HASHES: true
        }));
        sinon.stub(sync, 'fetchAndApplySchema').resolves();
        sinon.stub(sync, 'verifyAgainstSource').resolves();
        let payload = Buffer.from(JSON.stringify({ block_height: 50, tables: {} }));
        sinon.stub(axios, 'get').resolves({ data: payload });

        await sync.bootstrapFromSnapshot();

        assert.ok(sync.verifyAgainstSource.calledOnce);
        assert.strictEqual(sync.verifyAgainstSource.firstCall.args[0], 'http://src2:3006');
    });

    it('decoder + 2 sources calls verifyDecoderCompleteness', async function(){
        ({ sync, db, applier } = makeSync(
            { SYNC_SOURCES: 'http://src1:3006,http://src2:3006', VERIFY_HASHES: false },
            { dbType: 'decoder' }
        ));
        sinon.stub(sync, 'fetchAndApplySchema').resolves();
        sinon.stub(sync, 'verifyDecoderCompleteness').resolves();
        let payload = Buffer.from(JSON.stringify({ block_height: 20, tables: {} }));
        sinon.stub(axios, 'get').resolves({ data: payload });

        await sync.bootstrapFromSnapshot();

        assert.ok(sync.verifyDecoderCompleteness.calledOnce);
    });
});

describe('ClientSync: bootstrapFromSnapshot', function(){
    let sync, db, applier;

    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });
    afterEach(function(){ sinon.restore(); });



    it('catch/retry: rotates sources and THROWS on repeated failure (no swallow)', async function(){
        // BOOTSTRAP_MAX_RETRIES:0 → one rotation round, then propagate. The old code
        // returned normally here, letting start() live-follow an empty replica.
        ({ sync, db, applier } = makeSync({
            SYNC_SOURCES: 'http://src1:3006,http://src2:3006',
            BOOTSTRAP_MAX_RETRIES: 0
        }));
        sinon.stub(sync, 'fetchAndApplySchema').resolves();
        sinon.stub(axios, 'get').rejects(new Error('snap fail'));

        await assert.rejects(() => sync.bootstrapFromSnapshot(), /all sync sources exhausted/);

        let errorCalls = console.error.getCalls().map(c => c.args[0]);
        assert.ok(errorCalls.some(m => m && m.indexOf('All sync sources exhausted') !== -1),
            'must log exhaustion after all retries');
        assert.strictEqual(sync.lastAppliedBlock, null, 'no tip committed on a failed bootstrap');
    });

    it('single source exhausted: THROWS instead of returning success', async function(){
        ({ sync, db, applier } = makeSync({
            SYNC_SOURCES: 'http://src1:3006',
            BOOTSTRAP_MAX_RETRIES: 0
        }));
        sinon.stub(sync, 'fetchAndApplySchema').resolves();
        sinon.stub(axios, 'get').rejects(new Error('snap fail'));

        await assert.rejects(() => sync.bootstrapFromSnapshot(), /all sync sources exhausted/);

        let errorCalls = console.error.getCalls().map(c => c.args[0]);
        assert.ok(errorCalls.some(m => m && m.indexOf('All sync sources exhausted') !== -1));
        assert.strictEqual(sync.lastAppliedBlock, null);
    });
});

describe('ClientSync: bootstrapFromSnapshot', function(){
    let sync, db, applier;

    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });
    afterEach(function(){ sinon.restore(); });



    it('retries the single configured source with backoff, then succeeds', async function(){
        // Transient failure then success: the production single-source topology must
        // recover via in-process retry rather than dying on the first transient 404.
        ({ sync, db, applier } = makeSync({
            SYNC_SOURCES: 'http://src1:3006',
            BOOTSTRAP_MAX_RETRIES: 3
        }));
        let sleep = sinon.stub(sync.util, 'sleep').resolves();   // make backoff instant
        sinon.stub(sync, 'fetchAndApplySchema').resolves();
        let payload = Buffer.from(JSON.stringify({ block_height: 42, tables: {} }));
        let get = sinon.stub(axios, 'get');
        get.onCall(0).rejects(new Error('transient'));
        get.onCall(1).rejects(new Error('transient'));
        get.onCall(2).resolves({ data: payload });

        await sync.bootstrapFromSnapshot();

        assert.ok(applier.applyFullSnapshot.calledOnce, 'eventually applies the snapshot');
        assert.strictEqual(sync.lastAppliedBlock, 42);
        assert.ok(sleep.callCount >= 2, 'backed off between retry rounds');
    });
});
