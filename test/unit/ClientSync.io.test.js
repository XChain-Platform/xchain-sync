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
// Covers _fetchAndApplySchema, _bootstrapFromSnapshot, _runIncrementalCatchUp,
// _incrementalCatchUp, _verifyAgainstSource, _verifyDecoderCompleteness (catch),
// _connectWebSocket/_scheduleReconnect, stop, _scheduleHeartbeat/_flushHeartbeat/
// _sendRestHeartbeat, and several small branches.

const assert     = require('assert');
const sinon      = require('sinon');
const axios      = require('axios');
const zlib       = require('zlib');
const proxyquire = require('proxyquire');
const EventEmitter = require('events');
const ClientSync = require('../../src/ClientSync');
const { SCHEMA_VERSION } = require('../../src/schema-version');
const Utility    = require('../../src/utility');
const HashVerifier = require('../../src/HashVerifier');

// ─── shared helpers ──────────────────────────────────────────────────────────

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
    let sync = new ClientSync('bitcoin', 'mainnet', db, applier, rb, hv, config, util);
    return { sync, db, applier, rb, hv, util, config };
}

// ─────────────────────────────────────────────────────────────────────────────
// 1. _fetchAndApplySchema
// ─────────────────────────────────────────────────────────────────────────────
describe('ClientSync: _fetchAndApplySchema', function(){
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

        await sync._fetchAndApplySchema('http://src1:3006');

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

        await sync._fetchAndApplySchema('http://src1:3006');

        assert.ok(db.addMissingColumns.calledOnce, 'addMissingColumns must be called for existing table');
    });

    it('skips table with empty/falsy createSql', async function(){
        sinon.stub(axios, 'get').resolves({ data: {
            tables: { emptytable: '' }
        }});

        await sync._fetchAndApplySchema('http://src1:3006');

        assert.strictEqual(db.doQuery.called, false, 'empty DDL must be skipped');
    });

    it('rejects invalid table name (e.g. bad-name) and continues', async function(){
        sinon.stub(axios, 'get').resolves({ data: {
            tables: {
                'bad-name':  'CREATE TABLE `bad-name` (id int)',
                validtable:  'CREATE TABLE `validtable` (id int)'
            }
        }});
        db.doQuery.resolves([]);

        await sync._fetchAndApplySchema('http://src1:3006');

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

        await sync._fetchAndApplySchema('http://src1:3006');

        let errorCalls = console.error.getCalls().map(c => c.args[0]);
        assert.ok(errorCalls.some(m => m && m.indexOf('Rejected DDL') !== -1),
            'should log rejected DDL');
    });

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
        await sync._fetchAndApplySchema('http://src1:3006');
    });

    it('logs outer catch when axios.get rejects', async function(){
        sinon.stub(axios, 'get').rejects(new Error('network fail'));

        await sync._fetchAndApplySchema('http://src1:3006');

        let errorCalls = console.error.getCalls().map(c => c.args[0]);
        assert.ok(errorCalls.some(m => m && m.indexOf('Failed to fetch schema') !== -1),
            'outer catch must log "Failed to fetch schema"');
    });
});

// ─────────────────────────────────────────────────────────────────────────────
// 2. _bootstrapFromSnapshot
// ─────────────────────────────────────────────────────────────────────────────
describe('ClientSync: _bootstrapFromSnapshot', function(){
    let sync, db, applier;

    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });
    afterEach(function(){ sinon.restore(); });

    it('throws (does not silently return) when no sources configured', async function(){
        ({ sync, db, applier } = makeSync({ SYNC_SOURCES: '' }));

        // A misconfigured replica must not proceed to live-follow on an empty DB.
        await assert.rejects(() => sync._bootstrapFromSnapshot(), /no sync sources configured/);

        let errorCalls = console.error.getCalls().map(c => c.args[0]);
        assert.ok(errorCalls.some(m => m && m.indexOf('No sync sources configured') !== -1));
        assert.strictEqual(applier.applyFullSnapshot.called, false);
    });

    it('happy path with raw (non-gzipped) buffer', async function(){
        ({ sync, db, applier } = makeSync());
        sinon.stub(sync, '_fetchAndApplySchema').resolves();
        let payload = Buffer.from(JSON.stringify({ block_height: 100, tables: {} }));
        sinon.stub(axios, 'get').resolves({ data: payload });

        await sync._bootstrapFromSnapshot();

        assert.ok(applier.applyFullSnapshot.calledOnce, 'applyFullSnapshot must be called');
        assert.strictEqual(sync.lastAppliedBlock, 100);
    });

    it('happy path with gzipped buffer', async function(){
        ({ sync, db, applier } = makeSync());
        sinon.stub(sync, '_fetchAndApplySchema').resolves();
        let rawBuf = Buffer.from(JSON.stringify({ block_height: 5, tables: {} }));
        let gzBuf  = zlib.gzipSync(rawBuf);
        sinon.stub(axios, 'get').resolves({ data: gzBuf });

        await sync._bootstrapFromSnapshot();

        assert.ok(applier.applyFullSnapshot.calledOnce);
        assert.strictEqual(sync.lastAppliedBlock, 5);
    });

    it('indexer + 2 sources + VERIFY_HASHES calls _verifyAgainstSource', async function(){
        ({ sync, db, applier } = makeSync({
            SYNC_SOURCES: 'http://src1:3006,http://src2:3006',
            VERIFY_HASHES: true
        }));
        sinon.stub(sync, '_fetchAndApplySchema').resolves();
        sinon.stub(sync, '_verifyAgainstSource').resolves();
        let payload = Buffer.from(JSON.stringify({ block_height: 50, tables: {} }));
        sinon.stub(axios, 'get').resolves({ data: payload });

        await sync._bootstrapFromSnapshot();

        assert.ok(sync._verifyAgainstSource.calledOnce);
        assert.strictEqual(sync._verifyAgainstSource.firstCall.args[0], 'http://src2:3006');
    });

    it('decoder + 2 sources calls _verifyDecoderCompleteness', async function(){
        ({ sync, db, applier } = makeSync(
            { SYNC_SOURCES: 'http://src1:3006,http://src2:3006', VERIFY_HASHES: false },
            { dbType: 'decoder' }
        ));
        sinon.stub(sync, '_fetchAndApplySchema').resolves();
        sinon.stub(sync, '_verifyDecoderCompleteness').resolves();
        let payload = Buffer.from(JSON.stringify({ block_height: 20, tables: {} }));
        sinon.stub(axios, 'get').resolves({ data: payload });

        await sync._bootstrapFromSnapshot();

        assert.ok(sync._verifyDecoderCompleteness.calledOnce);
    });

    it('catch/retry: rotates sources and THROWS on repeated failure (no swallow)', async function(){
        // BOOTSTRAP_MAX_RETRIES:0 → one rotation round, then propagate. The old code
        // returned normally here, letting start() live-follow an empty replica.
        ({ sync, db, applier } = makeSync({
            SYNC_SOURCES: 'http://src1:3006,http://src2:3006',
            BOOTSTRAP_MAX_RETRIES: 0
        }));
        sinon.stub(sync, '_fetchAndApplySchema').resolves();
        sinon.stub(axios, 'get').rejects(new Error('snap fail'));

        await assert.rejects(() => sync._bootstrapFromSnapshot(), /all sync sources exhausted/);

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
        sinon.stub(sync, '_fetchAndApplySchema').resolves();
        sinon.stub(axios, 'get').rejects(new Error('snap fail'));

        await assert.rejects(() => sync._bootstrapFromSnapshot(), /all sync sources exhausted/);

        let errorCalls = console.error.getCalls().map(c => c.args[0]);
        assert.ok(errorCalls.some(m => m && m.indexOf('All sync sources exhausted') !== -1));
        assert.strictEqual(sync.lastAppliedBlock, null);
    });

    it('retries the single configured source with backoff, then succeeds', async function(){
        // Transient failure then success: the production single-source topology must
        // recover via in-process retry rather than dying on the first transient 404.
        ({ sync, db, applier } = makeSync({
            SYNC_SOURCES: 'http://src1:3006',
            BOOTSTRAP_MAX_RETRIES: 3
        }));
        let sleep = sinon.stub(sync.util, 'sleep').resolves();   // make backoff instant
        sinon.stub(sync, '_fetchAndApplySchema').resolves();
        let payload = Buffer.from(JSON.stringify({ block_height: 42, tables: {} }));
        let get = sinon.stub(axios, 'get');
        get.onCall(0).rejects(new Error('transient'));
        get.onCall(1).rejects(new Error('transient'));
        get.onCall(2).resolves({ data: payload });

        await sync._bootstrapFromSnapshot();

        assert.ok(applier.applyFullSnapshot.calledOnce, 'eventually applies the snapshot');
        assert.strictEqual(sync.lastAppliedBlock, 42);
        assert.ok(sleep.callCount >= 2, 'backed off between retry rounds');
    });
});

// ─────────────────────────────────────────────────────────────────────────────
// 2b. _bootstrapFromHeight (truncated/fast-chain bootstrap)
// ─────────────────────────────────────────────────────────────────────────────
describe('ClientSync _bootstrapFromHeight', function(){
    let sync, db, applier;

    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
        sinon.stub(console, 'warn');
    });
    afterEach(function(){ sinon.restore(); });

    // axios.get sequence: /status (JSON tip), then per-lookup-table /snapshot-rows
    // pages (empty here), then /snapshot/.../since/<base>?skip_lookups=1 block window
    // (gzip/raw buffer). Branch the stub on the URL.
    function stubTransport(opts){
        opts = opts || {};
        let tip  = (opts.tip  === undefined) ? 1000000 : opts.tip;
        let snap = opts.snapshot || { schema_version: SCHEMA_VERSION.indexer, block_height: tip, since_block: (opts.base === undefined ? tip - 50000 : opts.base), tables: {} };
        sinon.stub(axios, 'get').callsFake(async (url) => {
            if(url.indexOf('/status/') !== -1) return { data: { source_height: tip } };
            if(url.indexOf('/snapshot-rows/') !== -1)
                return { data: Buffer.from(JSON.stringify({ schema_version: SCHEMA_VERSION.indexer, table: 'x', max_id: 0, has_more: false, rows: [] })) };
            if(url.indexOf('/snapshot/') !== -1) return { data: Buffer.from(JSON.stringify(snap)) };
            throw new Error('unexpected url ' + url);
        });
    }

    it('seeds [base..tip] from one incremental snapshot and records the join block', async function(){
        ({ sync, db, applier } = makeSync());
        sinon.stub(sync, '_fetchAndApplySchema').resolves();
        stubTransport({ tip: 1000000, base: 950000 });

        let ok = await sync._bootstrapFromHeight(50000);

        assert.strictEqual(ok, true);
        assert.ok(applier.applyIncrementalSnapshot.calledOnce, 'applies one incremental snapshot');
        assert.strictEqual(sync.lastAppliedBlock, 1000000, 'tip committed');
        assert.strictEqual(sync._bootstrapBase, 950000, 'join block recorded from since_block');
    });

    it('re-pages lookups AFTER applying the block window (closes the T1<T2 FK gap)', async function(){
        // The block window snapshot is taken at the source tip T2, higher than the
        // lookup high-water T1 reached by the first paging pass on a fast chain. A
        // second paging pass after apply pulls the (T1..T2] index_* rows so the
        // terminal recompute and first live block resolve non-NULL consensus hashes.
        ({ sync, db, applier } = makeSync());
        sinon.stub(sync, '_fetchAndApplySchema').resolves();
        let paged = sinon.stub(sync, '_syncLookupTablesPaged').resolves();
        stubTransport({ tip: 1000000, base: 950000 });

        await sync._bootstrapFromHeight(50000);

        assert.ok(paged.calledTwice, 'paged before and re-paged after the block window');
        assert.ok(applier.applyIncrementalSnapshot.calledOnce, 'block window applied once');
        assert.ok(paged.secondCall.calledAfter(applier.applyIncrementalSnapshot.firstCall),
            're-page runs AFTER the block window apply');
    });

    it('clamps base to 0 when depth exceeds the tip', async function(){
        ({ sync, db, applier } = makeSync());
        sinon.stub(sync, '_fetchAndApplySchema').resolves();
        // tip 30, depth 50000 -> base must clamp to 0. Server echoes since_block=0.
        stubTransport({ tip: 30, snapshot: { schema_version: SCHEMA_VERSION.indexer, block_height: 30, since_block: 0, tables: {} } });

        await sync._bootstrapFromHeight(50000);

        let snapCall = axios.get.getCalls().find(c => c.args[0].indexOf('/snapshot/') !== -1);
        assert.ok(snapCall.args[0].indexOf('/since/0') !== -1, 'requests since/0 when depth > tip');
        assert.ok(snapCall.args[0].indexOf('skip_lookups=1') !== -1, 'block window requested with skip_lookups');
        assert.strictEqual(sync._bootstrapBase, 0);
    });

    it('throws when the source tip is unavailable (so the retry wrapper can restart)', async function(){
        ({ sync, db, applier } = makeSync());
        sinon.stub(sync, '_fetchAndApplySchema').resolves();
        sinon.stub(axios, 'get').callsFake(async (url) => {
            if(url.indexOf('/status/') !== -1) return { data: {} }; // no height fields
            return { data: Buffer.from('{}') };
        });

        await assert.rejects(() => sync._bootstrapFromHeight(50000), /source tip unavailable/);
        assert.strictEqual(applier.applyIncrementalSnapshot.called, false);
    });

    it('VERIFY_RECOMPUTE: recomputes the terminal block and HALTS on mismatch', async function(){
        ({ sync, db, applier } = makeSync({ VERIFY_RECOMPUTE: true }));
        sinon.stub(sync, '_fetchAndApplySchema').resolves();
        stubTransport({ tip: 1000000, base: 950000 });
        db.getBlockHashRow.resolves({ ledger_hash: 'lh', actions_hash: 'ah', contract_hash: 'ch' });
        sinon.stub(sync, '_verifyRecompute').resolves([{ field: 'ledger_hash', computed: 'X', committed: 'lh' }]);
        let halt = sinon.stub(sync, '_haltOnDivergence').resolves();

        await sync._bootstrapFromHeight(50000);

        assert.ok(sync._verifyRecompute.calledOnce, 'terminal block recomputed');
        assert.strictEqual(sync._verifyRecompute.firstCall.args[0].block_index, 1000000);
        assert.ok(halt.calledOnce, 'halts durably on a terminal mismatch');
    });

    it('retry wrapper throws (never live-follows empty) when no sources configured', async function(){
        ({ sync } = makeSync({ SYNC_SOURCES: '' }));
        await assert.rejects(() => sync._bootstrapFromHeightRetry(50000), /no sync sources configured/);
    });

    it('retry wrapper retries with backoff then succeeds', async function(){
        ({ sync, db, applier } = makeSync({ BOOTSTRAP_MAX_RETRIES: 3 }));
        let sleep = sinon.stub(sync.util, 'sleep').resolves();
        let fromHeight = sinon.stub(sync, '_bootstrapFromHeight');
        fromHeight.onCall(0).rejects(new Error('transient'));
        fromHeight.onCall(1).resolves(true);

        await sync._bootstrapFromHeightRetry(50000);

        assert.strictEqual(fromHeight.callCount, 2);
        assert.ok(sleep.calledOnce, 'backed off once between rounds');
    });

    it('retry wrapper THROWS on exhaustion', async function(){
        ({ sync } = makeSync({ BOOTSTRAP_MAX_RETRIES: 0 }));
        sinon.stub(sync, '_bootstrapFromHeight').rejects(new Error('boom'));
        await assert.rejects(() => sync._bootstrapFromHeightRetry(50000), /exhausted after 1 round/);
    });
});

// ─────────────────────────────────────────────────────────────────────────────
// 2b-ii. _syncLookupTablesPaged (id-cursor paged lookup sync)
// ─────────────────────────────────────────────────────────────────────────────
describe('ClientSync _syncLookupTablesPaged', function(){
    let sync, db, applier, rt;
    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
        sinon.stub(console, 'warn');
        rt = require('../../src/replicatedTables');
    });
    afterEach(function(){ sinon.restore(); });

    function page(rows, has_more, max_id){
        return { data: Buffer.from(JSON.stringify({ schema_version: SCHEMA_VERSION.indexer, table: 'index_transactions', max_id, has_more, rows })) };
    }

    it('pages one table by id cursor until has_more=false, applying each non-empty page', async function(){
        ({ sync, db, applier } = makeSync());
        sinon.stub(rt, 'getTopology').returns({ index: ['index_transactions'] });
        db.doQuery.resolves([{ m: null }]); // replica empty -> cursor starts at 0
        let get = sinon.stub(axios, 'get');
        get.onCall(0).resolves(page([{ id: 1 }, { id: 2 }], true, 2));
        get.onCall(1).resolves(page([{ id: 3 }], false, 3));

        await sync._syncLookupTablesPaged('http://src:3006');

        assert.strictEqual(get.callCount, 2, 'two pages fetched');
        assert.ok(get.firstCall.args[0].indexOf('after_id=0') !== -1, 'first page from id 0');
        assert.ok(get.secondCall.args[0].indexOf('after_id=2') !== -1, 'cursor advanced to max_id');
        assert.strictEqual(applier.applyIncrementalSnapshot.callCount, 2, 'each non-empty page applied');
        // Pages are applied as a minimal single-table incremental snapshot object.
        assert.deepStrictEqual(
            Object.keys(applier.applyIncrementalSnapshot.firstCall.args[0].tables),
            ['index_transactions']);
    });

    it('catch-up: starts from the replica MAX(id) so only NEW rows are fetched', async function(){
        ({ sync, db, applier } = makeSync());
        sinon.stub(rt, 'getTopology').returns({ index: ['index_transactions'] });
        db.doQuery.resolves([{ m: 8547252 }]); // replica already holds up to this id
        let get = sinon.stub(axios, 'get').resolves(page([], false, 8547252));

        await sync._syncLookupTablesPaged('http://src:3006');

        assert.ok(get.firstCall.args[0].indexOf('after_id=8547252') !== -1, 'cursor starts at replica max id');
        assert.strictEqual(applier.applyIncrementalSnapshot.called, false, 'empty page applies nothing');
    });

    it('throws on a schema-version mismatch (fail closed)', async function(){
        ({ sync, db } = makeSync());
        sinon.stub(rt, 'getTopology').returns({ index: ['index_transactions'] });
        db.doQuery.resolves([{ m: 0 }]);
        sinon.stub(axios, 'get').resolves({ data: Buffer.from(JSON.stringify({ schema_version: 99, has_more: false, rows: [] })) });

        await assert.rejects(() => sync._syncLookupTablesPaged('http://src:3006'), /schema mismatch/);
    });

    it('stops if has_more is true but the cursor cannot advance (no infinite spin)', async function(){
        ({ sync, db, applier } = makeSync());
        sinon.stub(rt, 'getTopology').returns({ index: ['index_transactions'] });
        db.doQuery.resolves([{ m: 0 }]);
        let get = sinon.stub(axios, 'get').resolves(page([{ id: 5 }], true, 0)); // has_more but max_id <= afterId

        await sync._syncLookupTablesPaged('http://src:3006');

        assert.strictEqual(get.callCount, 1, 'stopped after one page despite has_more');
    });

    it('decoder: pages pubkeys by its surrogate id cursor', async function(){
        ({ sync, db, applier } = makeSync({}, { dbType: 'decoder' }));
        sinon.stub(rt, 'getTopology').returns({ index: ['pubkeys'] });
        db.doQuery.resolves([{ m: 42 }]); // replica MAX(id) = 42
        // decoder uses SCHEMA_VERSION.decoder (distinct from indexer); the page must match it.
        let get = sinon.stub(axios, 'get').resolves(
            { data: Buffer.from(JSON.stringify({ schema_version: SCHEMA_VERSION.decoder, table: 'pubkeys', max_id: 42, has_more: false, rows: [] })) });

        await sync._syncLookupTablesPaged('http://src:3006');

        let maxQ = db.doQuery.getCalls().find(c => /MAX\(`id`\)/.test(c.args[0]));
        assert.ok(maxQ, 'queries MAX(id) for pubkeys (surrogate key added by fix #4413)');
        assert.ok(get.firstCall.args[0].indexOf('after_id=42') !== -1, 'cursor starts at replica id high-water');
    });
});

// ─────────────────────────────────────────────────────────────────────────────
// 2b-iii. truncated catch-up uses paged lookups + skip_lookups
// ─────────────────────────────────────────────────────────────────────────────
describe('ClientSync truncated catch-up', function(){
    let sync, db, applier;
    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
        sinon.stub(console, 'warn');
    });
    afterEach(function(){ sinon.restore(); });

    it('truncated chain pages lookups then fetches the block window with skip_lookups=1', async function(){
        // makeSync builds ClientSync('bitcoin','mainnet'); depth keyed by uppercased chain:network.
        ({ sync, db, applier } = makeSync({ SYNC_BOOTSTRAP_DEPTH: { 'BITCOIN:MAINNET': 50000 } }));
        assert.ok(sync._truncatedDepth >= 1, 'chain is in truncated mode');
        let paged = sinon.stub(sync, '_syncLookupTablesPaged').resolves();
        db.getLastBlock.resolves(100);
        sinon.stub(axios, 'get').resolves({ data: Buffer.from(JSON.stringify({ schema_version: SCHEMA_VERSION.indexer, block_height: 105, since_block: 101, tables: {} })) });

        await sync._runIncrementalCatchUp();

        assert.ok(paged.calledTwice, 'lookups paged before AND re-paged after the block window');
        let snapCall = axios.get.getCalls().find(c => c.args[0].indexOf('/snapshot/') !== -1);
        assert.ok(snapCall.args[0].indexOf('skip_lookups=1') !== -1, 'block window fetched with skip_lookups');
    });

    it('re-pages lookups AFTER the block window apply so (T1..T2] FK targets are present before recompute', async function(){
        // Regression: the skip_lookups snapshot is taken at the source tip T2 > the
        // paging high-water T1, so blocks (T1..T2] reference index_* rows not pulled
        // in the first page. The second _syncLookupTablesPaged must run after apply.
        ({ sync, db, applier } = makeSync({ SYNC_BOOTSTRAP_DEPTH: { 'BITCOIN:MAINNET': 50000 } }));
        let paged = sinon.stub(sync, '_syncLookupTablesPaged').resolves();
        db.getLastBlock.resolves(100);
        sinon.stub(axios, 'get').resolves({ data: Buffer.from(JSON.stringify({ schema_version: SCHEMA_VERSION.indexer, block_height: 105, since_block: 101, tables: {} })) });

        await sync._runIncrementalCatchUp();

        assert.ok(paged.calledTwice, 'paged twice: before and after the block window');
        assert.ok(applier.applyIncrementalSnapshot.calledOnce, 'block window applied once');
        assert.ok(paged.secondCall.calledAfter(applier.applyIncrementalSnapshot.firstCall),
            're-page runs AFTER the block window is applied');
    });

    it('full-history chain does NOT page lookups and uses no skip_lookups (unchanged path)', async function(){
        ({ sync, db, applier } = makeSync()); // no depth -> _truncatedDepth 0
        assert.strictEqual(sync._truncatedDepth, 0);
        let paged = sinon.stub(sync, '_syncLookupTablesPaged').resolves();
        db.getLastBlock.resolves(100);
        sinon.stub(axios, 'get').resolves({ data: Buffer.from(JSON.stringify({ schema_version: SCHEMA_VERSION.indexer, block_height: 105, since_block: 101, tables: {} })) });

        await sync._runIncrementalCatchUp();

        assert.ok(paged.notCalled, 'no paged lookup sync for full-history chains');
        let snapCall = axios.get.getCalls().find(c => c.args[0].indexOf('/snapshot/') !== -1);
        assert.ok(snapCall.args[0].indexOf('skip_lookups') === -1, 'no skip_lookups for full-history');
    });

    it('decoder chain is truncated by the same depth and pages lookups + skip_lookups', async function(){
        // Depth is keyed by chain:network (not dbType), so it truncates the decoder too.
        ({ sync, db, applier } = makeSync({ SYNC_BOOTSTRAP_DEPTH: { 'BITCOIN:MAINNET': 50000 } }, { dbType: 'decoder' }));
        assert.ok(sync._truncatedDepth >= 1, 'decoder picks up the chain depth (gate removed)');
        let paged = sinon.stub(sync, '_syncLookupTablesPaged').resolves();
        db.getLastBlock.resolves(100);
        sinon.stub(axios, 'get').resolves({ data: Buffer.from(JSON.stringify({ schema_version: SCHEMA_VERSION.decoder, block_height: 105, since_block: 101, tables: {} })) });

        await sync._runIncrementalCatchUp();

        assert.ok(paged.calledTwice, 'decoder pages lookups before AND re-pages after the window');
        let snapCall = axios.get.getCalls().find(c => c.args[0].indexOf('/snapshot/') !== -1);
        assert.ok(snapCall.args[0].indexOf('skip_lookups=1') !== -1, 'decoder block window fetched with skip_lookups');
    });
});

// ─────────────────────────────────────────────────────────────────────────────
// 2c. _verifyRecompute join-block skip (truncated replica)
// ─────────────────────────────────────────────────────────────────────────────
describe('ClientSync _verifyRecompute join-block skip', function(){
    let sync;
    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });
    afterEach(function(){ sinon.restore(); });

    it('skips recompute for the bootstrap join block (no base-1 predecessor)', async function(){
        ({ sync } = makeSync());
        sync._bootstrapBase = 950000;
        let compute = sinon.stub(sync.blockHasher, 'computeBlockHashes');

        let result = await sync._verifyRecompute({ block_index: 950000 }, { ledger_hash: 'lh' });

        assert.strictEqual(result, null, 'join block treated as clean');
        assert.strictEqual(compute.called, false, 'computeBlockHashes never invoked for the join block');
    });

    it('still recomputes every block above the join block', async function(){
        ({ sync } = makeSync());
        sync._bootstrapBase = 950000;
        let compute = sinon.stub(sync.blockHasher, 'computeBlockHashes')
            .resolves({ ledger_hash: 'lh', actions_hash: 'ah', contract_hash: 'ch' });

        let result = await sync._verifyRecompute({ block_index: 950001 },
            { ledger_hash: 'lh', actions_hash: 'ah', contract_hash: 'ch' });

        assert.strictEqual(result, null, 'matching block is clean');
        assert.ok(compute.calledOnceWith(950001), 'block above the join is recomputed');
    });

    it('full-history replica (null base) recomputes the lowest block too', async function(){
        ({ sync } = makeSync());
        // _bootstrapBase stays null
        let compute = sinon.stub(sync.blockHasher, 'computeBlockHashes')
            .resolves({ ledger_hash: 'lh', actions_hash: 'ah', contract_hash: 'ch' });

        await sync._verifyRecompute({ block_index: 0 }, { ledger_hash: 'lh', actions_hash: 'ah', contract_hash: 'ch' });

        assert.ok(compute.calledOnceWith(0), 'no skip when not a truncated replica');
    });
});

// ─────────────────────────────────────────────────────────────────────────────
// 3. _runIncrementalCatchUp
// ─────────────────────────────────────────────────────────────────────────────
describe('ClientSync: _runIncrementalCatchUp', function(){
    let sync, db, applier;

    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });
    afterEach(function(){ sinon.restore(); });

    it('returns early when no sources configured', async function(){
        ({ sync, db, applier } = makeSync({ SYNC_SOURCES: '' }));

        await sync._runIncrementalCatchUp();

        assert.strictEqual(applier.applyIncrementalSnapshot.called, false);
    });

    it('happy path: dbTip null → sinceBlock=1, calls applyIncrementalSnapshot', async function(){
        ({ sync, db, applier } = makeSync());
        db.getLastBlock.resolves(null);
        let payload = Buffer.from(JSON.stringify({ block_height: 20, tables: {} }));
        sinon.stub(axios, 'get').resolves({ data: payload });

        await sync._runIncrementalCatchUp();

        assert.ok(applier.applyIncrementalSnapshot.calledOnce);
        assert.strictEqual(sync.lastAppliedBlock, 20);
        // URL should have since/1
        let url = axios.get.firstCall.args[0];
        assert.ok(url.indexOf('/since/1') !== -1, 'since=1 when dbTip is null');
    });

    it('happy path: dbTip=10 → sinceBlock=11', async function(){
        ({ sync, db, applier } = makeSync());
        db.getLastBlock.resolves(10);
        let payload = Buffer.from(JSON.stringify({ block_height: 15, tables: {} }));
        sinon.stub(axios, 'get').resolves({ data: payload });

        await sync._runIncrementalCatchUp();

        let url = axios.get.firstCall.args[0];
        assert.ok(url.indexOf('/since/11') !== -1, 'since=11 when dbTip=10');
        assert.strictEqual(sync.lastAppliedBlock, 15);
    });

    it('logs error when axios.get rejects', async function(){
        ({ sync, db, applier } = makeSync());
        db.getLastBlock.resolves(5);
        sinon.stub(axios, 'get').rejects(new Error('inc fail'));

        await sync._runIncrementalCatchUp();

        let errCalls = console.error.getCalls().map(c => c.args[0]);
        assert.ok(errCalls.some(m => m && m.indexOf('Incremental catch-up failed') !== -1));
    });
});

// ─────────────────────────────────────────────────────────────────────────────
// 4. _incrementalCatchUp coalescing
// ─────────────────────────────────────────────────────────────────────────────
describe('ClientSync: _incrementalCatchUp coalescing', function(){
    let sync, db;

    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });
    afterEach(function(){ sinon.restore(); });

    it('calls _runIncrementalCatchUp once when no in-flight', async function(){
        ({ sync, db } = makeSync());
        sinon.stub(sync, '_runIncrementalCatchUp').resolves();

        await sync._incrementalCatchUp(1);

        assert.strictEqual(sync._runIncrementalCatchUp.callCount, 1);
    });

    it('sets _catchUpPending when already in-flight', async function(){
        ({ sync, db } = makeSync());
        // Simulate an in-flight operation with a never-resolving promise so
        // the guard triggers. We only care that _catchUpPending is set and
        // that the call does not start a second runner.
        let neverResolve = new Promise(() => {});
        sync._catchUpInFlight = neverResolve;
        sinon.stub(sync, '_runIncrementalCatchUp').resolves();

        // Call it (do NOT await; the in-flight guard returns synchronously)
        sync._incrementalCatchUp(1);

        assert.strictEqual(sync._catchUpPending, true, '_catchUpPending must be set');
        assert.strictEqual(sync._runIncrementalCatchUp.called, false,
            '_runIncrementalCatchUp must not start when already in-flight');
    });

    it('re-runs once when _catchUpPending and progress was made', async function(){
        ({ sync, db } = makeSync());
        let runCount = 0;
        sync.running = true;
        sinon.stub(sync, '_runIncrementalCatchUp').callsFake(async function(){
            runCount++;
            if(runCount === 1){
                // Simulate progress + pending request arriving during first run
                sync.lastAppliedBlock = (sync.lastAppliedBlock || 0) + 1;
                sync._catchUpPending = true;
            }
            // Second run: no more pending
        });

        await sync._incrementalCatchUp(1);

        assert.strictEqual(sync._runIncrementalCatchUp.callCount, 2,
            'must re-run exactly once when pending + progress');
    });

    it('does not re-run when pending but no progress was made', async function(){
        ({ sync, db } = makeSync());
        sync.running = true;
        sinon.stub(sync, '_runIncrementalCatchUp').callsFake(async function(){
            // Simulate pending arriving but no progress (lastAppliedBlock unchanged)
            sync._catchUpPending = true;
            // lastAppliedBlock stays null
        });

        await sync._incrementalCatchUp(1);

        assert.strictEqual(sync._runIncrementalCatchUp.callCount, 1,
            'must not loop forever when no progress');
    });
});

// ─────────────────────────────────────────────────────────────────────────────
// 5. _verifyAgainstSource
// ─────────────────────────────────────────────────────────────────────────────
describe('ClientSync: _verifyAgainstSource', function(){
    let sync, db;

    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });
    afterEach(function(){ sinon.restore(); });

    it('returns early when dbType is decoder', async function(){
        ({ sync, db } = makeSync({}, { dbType: 'decoder' }));
        sinon.stub(axios, 'get');

        await sync._verifyAgainstSource('http://src1:3006', 100);

        assert.strictEqual(axios.get.called, false);
    });

    it('logs "Hash verification passed" when hashes match', async function(){
        ({ sync, db } = makeSync({ VERIFY_HASHES: true }));
        db.getBlockHashRow.resolves({
            ledger_hash:   'lh1',
            actions_hash:  'ah1',
            contract_hash: 'ch1'
        });
        sinon.stub(axios, 'get').resolves({ data: {
            ledger_hash:   'lh1',
            actions_hash:  'ah1',
            contract_hash: 'ch1',
            table_counts:  null
        }});

        await sync._verifyAgainstSource('http://src1:3006', 100);

        let logCalls = console.log.getCalls().map(c => c.args[0]);
        assert.ok(logCalls.some(m => m && m.indexOf('Hash verification passed') !== -1));
    });

    it('logs "HASH MISMATCH" when hashes differ', async function(){
        ({ sync, db } = makeSync({ VERIFY_HASHES: true }));
        db.getBlockHashRow.resolves({
            ledger_hash:   'lh-local',
            actions_hash:  'ah-local',
            contract_hash: 'ch-local'
        });
        sinon.stub(axios, 'get').resolves({ data: {
            ledger_hash:   'lh-remote',
            actions_hash:  'ah-remote',
            contract_hash: 'ch-remote',
            table_counts:  null
        }});

        await sync._verifyAgainstSource('http://src1:3006', 100);

        let errCalls = console.error.getCalls().map(c => c.args[0]);
        assert.ok(errCalls.some(m => m && m.indexOf('HASH MISMATCH') !== -1));
    });

    it('returns early when localHashes is null', async function(){
        ({ sync, db } = makeSync({ VERIFY_HASHES: true }));
        db.getBlockHashRow.resolves(null);
        sinon.stub(axios, 'get').resolves({ data: {
            ledger_hash:   'lh1',
            actions_hash:  'ah1',
            contract_hash: 'ch1',
            table_counts:  null
        }});

        await sync._verifyAgainstSource('http://src1:3006', 100);

        // No hash comparison logging
        let logCalls = console.log.getCalls().map(c => c.args[0]);
        assert.ok(!logCalls.some(m => m && m.indexOf('Hash verification passed') !== -1));
    });

    it('calls _haltOnDivergence when VERIFY_RECOMPUTE=true and recompute has mismatches', async function(){
        ({ sync, db } = makeSync({ VERIFY_HASHES: true, VERIFY_RECOMPUTE: true }));
        db.getBlockHashRow.resolves({
            ledger_hash:   'lh1',
            actions_hash:  'ah1',
            contract_hash: 'ch1'
        });
        sinon.stub(axios, 'get').resolves({ data: {
            ledger_hash:   'lh1',
            actions_hash:  'ah1',
            contract_hash: 'ch1',
            table_counts:  null
        }});
        // Stub _verifyRecompute to return mismatches
        sinon.stub(sync, '_verifyRecompute').resolves([{ field: 'ledger_hash', computed: 'X', committed: 'lh1' }]);
        sinon.stub(sync, '_haltOnDivergence').resolves();

        await sync._verifyAgainstSource('http://src1:3006', 100);

        assert.ok(sync._haltOnDivergence.calledOnce, '_haltOnDivergence must be called on recompute mismatch');
    });

    it('logs TABLE_COUNT_MISMATCH when source has more rows', async function(){
        ({ sync, db } = makeSync({ VERIFY_HASHES: true }));
        db.getBlockHashRow.resolves({
            ledger_hash:   'lh1',
            actions_hash:  'ah1',
            contract_hash: 'ch1'
        });
        db.getTableCount.resolves(5); // local has 5
        sinon.stub(axios, 'get').resolves({ data: {
            ledger_hash:   'lh1',
            actions_hash:  'ah1',
            contract_hash: 'ch1',
            table_counts:  { blocks: 100 } // remote has 100
        }});

        await sync._verifyAgainstSource('http://src1:3006', 100);

        let errCalls = console.error.getCalls().map(c => c.args[0]);
        assert.ok(errCalls.some(m => m && m.indexOf('TABLE_COUNT_MISMATCH') !== -1));
    });

    it('logs "Table-count verification passed" when counts match', async function(){
        ({ sync, db } = makeSync({ VERIFY_HASHES: true }));
        db.getBlockHashRow.resolves({
            ledger_hash:   'lh1',
            actions_hash:  'ah1',
            contract_hash: 'ch1'
        });
        db.getTableCount.resolves(100); // local same as remote
        sinon.stub(axios, 'get').resolves({ data: {
            ledger_hash:   'lh1',
            actions_hash:  'ah1',
            contract_hash: 'ch1',
            table_counts:  { blocks: 100 }
        }});

        await sync._verifyAgainstSource('http://src1:3006', 100);

        let logCalls = console.log.getCalls().map(c => c.args[0]);
        assert.ok(logCalls.some(m => m && m.indexOf('Table-count verification passed') !== -1));
    });

    it('logs "Hash verification failed" when axios.get rejects', async function(){
        ({ sync, db } = makeSync({ VERIFY_HASHES: true }));
        sinon.stub(axios, 'get').rejects(new Error('net fail'));

        await sync._verifyAgainstSource('http://src1:3006', 100);

        let errCalls = console.error.getCalls().map(c => c.args[0]);
        assert.ok(errCalls.some(m => m && m.indexOf('Hash verification failed') !== -1));
    });
});

// ─────────────────────────────────────────────────────────────────────────────
// 6. _verifyDecoderCompleteness catch branch
// ─────────────────────────────────────────────────────────────────────────────
describe('ClientSync: _verifyDecoderCompleteness catch', function(){
    let sync, db;

    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });
    afterEach(function(){ sinon.restore(); });

    it('logs "Decoder completeness check failed" when axios.get rejects', async function(){
        ({ sync, db } = makeSync({}, { dbType: 'decoder' }));
        sinon.stub(axios, 'get').rejects(new Error('decoder fail'));

        await sync._verifyDecoderCompleteness('http://src2:3006', 100);

        let errCalls = console.error.getCalls().map(c => c.args[0]);
        assert.ok(errCalls.some(m => m && m.indexOf('Decoder completeness check failed') !== -1));
    });
});

// ─────────────────────────────────────────────────────────────────────────────
// 7. stop()
// ─────────────────────────────────────────────────────────────────────────────
describe('ClientSync: stop()', function(){
    let sync;

    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });
    afterEach(function(){ sinon.restore(); });

    it('clears _hbTimer, closes all ws connections, empties wsConns', function(){
        ({ sync } = makeSync());
        sync.running = true;

        let closed = [];
        let ws1 = { close: sinon.stub().callsFake(() => closed.push(1)) };
        let ws2 = { close: () => { closed.push(2); throw new Error('close error'); } };
        sync._hbTimer = setTimeout(() => {}, 9999);
        sync.wsConns  = [ws1, ws2];

        sync.stop();

        assert.strictEqual(sync.running, false);
        assert.strictEqual(sync._hbTimer, null);
        assert.deepStrictEqual(closed, [1, 2], 'both close() calls attempted');
        assert.strictEqual(sync.wsConns.length, 0);
    });

    it('tolerates missing _hbTimer', function(){
        ({ sync } = makeSync());
        sync._hbTimer = null;
        sync.wsConns  = [];

        // Should not throw
        sync.stop();
        assert.strictEqual(sync.running, false);
    });
});

// ─────────────────────────────────────────────────────────────────────────────
// 8. _scheduleHeartbeat / _flushHeartbeat / _sendRestHeartbeat
// ─────────────────────────────────────────────────────────────────────────────
describe('ClientSync: heartbeat', function(){
    let sync, db;

    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });
    afterEach(function(){ sinon.restore(); });

    it('_flushHeartbeat returns early when lastAppliedBlock is null', function(){
        ({ sync, db } = makeSync());
        sync.lastAppliedBlock = null;
        // If it didn't return early it would try to iterate wsConns and sources
        // Just verify it doesn't crash and axios.post is never called
        let postStub = sinon.stub(axios, 'post').resolves();
        sync._flushHeartbeat();
        assert.strictEqual(postStub.called, false);
    });

    it('_flushHeartbeat sends WS message to OPEN connections and clears timer', function(){
        ({ sync, db } = makeSync({ SYNC_SOURCES: 'http://src1:3006' }));
        sync.lastAppliedBlock = 42;
        let sendStub = sinon.stub();
        // OPEN = 1
        sync.wsConns = [{ readyState: 1, send: sendStub }];
        let clock = sinon.useFakeTimers();
        sync._hbTimer = setTimeout(() => {}, 9999);
        sinon.stub(axios, 'post').resolves();

        sync._flushHeartbeat();

        assert.ok(sendStub.calledOnce, 'send must be called for OPEN connection');
        let msg = JSON.parse(sendStub.firstCall.args[0]);
        assert.strictEqual(msg.type, 'heartbeat');
        assert.strictEqual(msg.appliedBlock, 42);
        assert.strictEqual(sync._hbTimer, null, 'timer must be cleared');
        assert.strictEqual(sync._hbLastSentBlock, 42);
        clock.restore();
    });

    it('_scheduleHeartbeat flushes immediately when _hbLastSentBlock is null', function(){
        ({ sync, db } = makeSync({ SYNC_SOURCES: 'http://src1:3006' }));
        sync.lastAppliedBlock  = 10;
        sync._hbLastSentBlock  = null;
        sinon.stub(sync, '_flushHeartbeat');

        sync._scheduleHeartbeat();

        assert.ok(sync._flushHeartbeat.calledOnce, 'must flush immediately on first heartbeat');
    });

    it('_scheduleHeartbeat flushes immediately when >= 10 blocks advanced', function(){
        ({ sync, db } = makeSync({ SYNC_SOURCES: 'http://src1:3006' }));
        sync.lastAppliedBlock = 110;
        sync._hbLastSentBlock = 100; // delta = 10
        sinon.stub(sync, '_flushHeartbeat');

        sync._scheduleHeartbeat();

        assert.ok(sync._flushHeartbeat.calledOnce);
    });

    it('_scheduleHeartbeat arms a 5s timer when delta < 10 and no timer running', function(){
        ({ sync, db } = makeSync({ SYNC_SOURCES: 'http://src1:3006' }));
        sync.lastAppliedBlock = 105;
        sync._hbLastSentBlock = 100; // delta = 5
        sync._hbTimer = null;
        let clock = sinon.useFakeTimers();
        sinon.stub(sync, '_flushHeartbeat');

        sync._scheduleHeartbeat();

        assert.strictEqual(sync._flushHeartbeat.called, false, 'must not flush immediately');
        assert.notStrictEqual(sync._hbTimer, null, 'timer must be set');

        clock.tick(5000);

        assert.ok(sync._flushHeartbeat.calledOnce, 'must flush after 5s timer');
        clock.restore();
    });

    it('_sendRestHeartbeat posts to correct URL with Bearer header when api key set', async function(){
        ({ sync, db } = makeSync({ SYNC_SOURCES: 'http://src1:3006', SYNC_API_KEY: 'mykey' }));
        sync.lastAppliedBlock = 99;
        let postStub = sinon.stub(axios, 'post').resolves();

        await sync._sendRestHeartbeat('http://src1:3006');

        assert.ok(postStub.calledOnce);
        let [url, body, opts] = postStub.firstCall.args;
        assert.ok(url.indexOf('/validator-heartbeat/') !== -1);
        assert.strictEqual(opts.headers['Authorization'], 'Bearer mykey');
    });

    it('_sendRestHeartbeat omits Authorization header when no api key', async function(){
        ({ sync, db } = makeSync({ SYNC_SOURCES: 'http://src1:3006' }));
        sync.config['SYNC_API_KEY'] = '';
        let postStub = sinon.stub(axios, 'post').resolves();

        await sync._sendRestHeartbeat('http://src1:3006');

        let [, , opts] = postStub.firstCall.args;
        assert.ok(!opts.headers['Authorization'], 'no Authorization header when no api key');
    });
});

// ─────────────────────────────────────────────────────────────────────────────
// 9. _connectWebSocket / _scheduleReconnect
// ─────────────────────────────────────────────────────────────────────────────
describe('ClientSync: _connectWebSocket', function(){
    // Use proxyquire to inject a fake ws constructor
    let ClientSyncWS, fakeWsInstance, FakeWS;

    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');

        // Build a fresh fake WS EventEmitter for each test
        fakeWsInstance = new EventEmitter();
        fakeWsInstance.close    = sinon.stub();
        fakeWsInstance.send     = sinon.stub();
        fakeWsInstance.readyState = 1; // OPEN

        FakeWS = function(url, opts){
            // Attach the singleton fake instance
            Object.setPrototypeOf(fakeWsInstance, FakeWS.prototype);
            return fakeWsInstance;
        };
        // Give FakeWS the same OPEN constant as the real ws
        FakeWS.OPEN = 1;
        Object.setPrototypeOf(FakeWS.prototype, EventEmitter.prototype);

        ClientSyncWS = proxyquire('../../src/ClientSync', { ws: FakeWS });
    });
    afterEach(function(){ sinon.restore(); });

    function makeSyncWS(configOverrides){
        let db      = createMockDb();
        let applier = createMockApplier();
        let rb      = createMockRollback();
        let hv      = new HashVerifier();
        let util    = new Utility();
        let config  = Object.assign({
            SYNC_SOURCES:           'http://src1:3006',
            VERIFY_HASHES:          false,
            CLIENT_RECONNECT_DELAY: 5000,
            HASH_CONFIRM_TIMEOUT:   5000,
            SNAPSHOT_MAX_CONTENT:   200 * 1024 * 1024,
            WS_MAX_PAYLOAD:         50 * 1024 * 1024,
            MAX_ROLLBACK_DEPTH:     10,
            GAP_LOG_INTERVAL_MS:    30000
        }, configOverrides || {});
        let sync = new ClientSyncWS('bitcoin', 'mainnet', db, applier, rb, hv, config, util);
        return { sync, db, applier };
    }

    it('stores ws in wsConns and registers handlers', function(){
        let { sync } = makeSyncWS();
        sync.running = true;

        sync._connectWebSocket('http://src1:3006', 0);

        assert.strictEqual(sync.wsConns[0], fakeWsInstance);
        // Verify handlers registered
        assert.ok(fakeWsInstance.listenerCount('open')    > 0);
        assert.ok(fakeWsInstance.listenerCount('message') > 0);
        assert.ok(fakeWsInstance.listenerCount('close')   > 0);
        assert.ok(fakeWsInstance.listenerCount('error')   > 0);
    });

    it('uses ?sync_mode=infra-only query string when SYNC_MODE env is set', function(){
        let { sync } = makeSyncWS();
        let envKey = 'SYNC_MODE_BITCOIN';
        process.env[envKey] = 'infra-only';

        try {
            sync._connectWebSocket('http://src1:3006', 0);
            let logCalls = console.log.getCalls().map(c => c.args[0]);
            assert.ok(logCalls.some(m => m && m.indexOf('infra-only') !== -1),
                'infra-only mode must be in the log output');
        } finally {
            delete process.env[envKey];
        }
    });

    it('triggers _scheduleReconnect when WS constructor throws', function(){
        // Make FakeWS throw on construction
        let ThrowWS = function(){ throw new Error('ws construct fail'); };
        ThrowWS.OPEN = 1;
        let CSThrow = proxyquire('../../src/ClientSync', { ws: ThrowWS });
        let db = createMockDb(), applier = createMockApplier(), rb = createMockRollback();
        let hv = new HashVerifier(), util = new Utility();
        let config = {
            SYNC_SOURCES: 'http://src1:3006', VERIFY_HASHES: false,
            CLIENT_RECONNECT_DELAY: 9999999, HASH_CONFIRM_TIMEOUT: 5000,
            WS_MAX_PAYLOAD: 50*1024*1024, MAX_ROLLBACK_DEPTH: 10,
            SNAPSHOT_MAX_CONTENT: 200*1024*1024, GAP_LOG_INTERVAL_MS: 30000
        };
        let syncT = new CSThrow('bitcoin', 'mainnet', db, applier, rb, hv, config, util);
        syncT.running = false; // _scheduleReconnect no-ops when not running

        // Should not throw
        syncT._connectWebSocket('http://src1:3006', 0);

        let errCalls = console.error.getCalls().map(c => c.args[0]);
        assert.ok(errCalls.some(m => m && m.indexOf('WebSocket connection error') !== -1));
    });

    it('message handler: calls _handleEvent for a valid block event', async function(){
        let { sync } = makeSyncWS();
        sync.running = true;
        sinon.stub(sync, '_handleEvent').resolves();

        sync._connectWebSocket('http://src1:3006', 0);

        let validEvent = JSON.stringify({ type: 'block', block_index: 5, ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c' });
        await fakeWsInstance.emit('message', Buffer.from(validEvent));

        assert.ok(sync._handleEvent.calledOnce, '_handleEvent must be called for valid event');
    });

    it('message handler: logs invalid WS event', async function(){
        let { sync } = makeSyncWS();
        sync.running = true;
        sinon.stub(sync, '_handleEvent').resolves();

        sync._connectWebSocket('http://src1:3006', 0);

        let invalidEvent = JSON.stringify({ type: 'unknown_type', block_index: 5 });
        await fakeWsInstance.emit('message', Buffer.from(invalidEvent));

        let errCalls = console.error.getCalls().map(c => c.args[0]);
        assert.ok(errCalls.some(m => m && m.indexOf('Invalid WS event') !== -1));
    });

    it('message handler: logs on malformed JSON', async function(){
        let { sync } = makeSyncWS();
        sync.running = true;
        sinon.stub(sync, '_handleEvent').resolves();

        sync._connectWebSocket('http://src1:3006', 0);

        await fakeWsInstance.emit('message', Buffer.from('{not json'));

        // Malformed JSON is caught at parse time (before the serialized event chain).
        let errCalls = console.error.getCalls().map(c => c.args[0]);
        assert.ok(errCalls.some(m => m && m.indexOf('Error parsing WebSocket message') !== -1));
    });

    it('message handler: logs when _handleEvent rejects', async function(){
        let { sync } = makeSyncWS();
        sync.running = true;
        sinon.stub(sync, '_handleEvent').rejects(new Error('handle boom'));

        sync._connectWebSocket('http://src1:3006', 0);

        // A well-formed event passes validation and reaches the serialized event
        // chain; its rejection is surfaced by the chain's .catch.
        await fakeWsInstance.emit('message', Buffer.from(JSON.stringify({ type: 'block', block_index: 5 })));
        await sync._wsEventChain;   // let the .then(_handleEvent).catch settle

        let errCalls = console.error.getCalls().map(c => c.args[0]);
        assert.ok(errCalls.some(m => m && m.indexOf('Error handling WebSocket message') !== -1));
    });

    it('close handler: calls _scheduleReconnect', function(){
        let { sync } = makeSyncWS();
        sync.running = false; // prevent actual reconnect timer

        sync._connectWebSocket('http://src1:3006', 0);
        fakeWsInstance.emit('close');

        // Log output should include disconnect message
        let logCalls = console.log.getCalls().map(c => c.args[0]);
        assert.ok(logCalls.some(m => m && m.indexOf('WebSocket disconnected') !== -1));
    });

    it('error handler: logs the error message', function(){
        let { sync } = makeSyncWS();

        sync._connectWebSocket('http://src1:3006', 0);
        fakeWsInstance.emit('error', { message: 'ECONNREFUSED' });

        let errCalls = console.error.getCalls().map(c => c.args[0]);
        assert.ok(errCalls.some(m => m && m.indexOf('WebSocket error') !== -1));
    });

    it('_scheduleReconnect: no-op when running=false', function(){
        let { sync } = makeSyncWS();
        sync.running = false;
        let clock = sinon.useFakeTimers();
        sinon.stub(sync, '_connectWebSocket');

        sync._scheduleReconnect('http://src1:3006', 0);
        clock.tick(10000);

        assert.strictEqual(sync._connectWebSocket.called, false);
        clock.restore();
    });

    it('_scheduleReconnect: reconnects after CLIENT_RECONNECT_DELAY when running=true', function(){
        let { sync } = makeSyncWS({ CLIENT_RECONNECT_DELAY: 100 });
        sync.running = true;
        let clock = sinon.useFakeTimers();

        // Stub _connectWebSocket so it doesn't actually try to create another WS
        sinon.stub(sync, '_connectWebSocket');
        // Only the schedule, not the one registered in constructor
        sync._scheduleReconnect('http://src1:3006', 0);
        assert.strictEqual(sync._connectWebSocket.called, false);

        clock.tick(101);

        assert.ok(sync._connectWebSocket.calledOnce, 'must reconnect after delay');
        clock.restore();
    });

    it('_connectWebSockets iterates all sources', function(){
        let { sync } = makeSyncWS({ SYNC_SOURCES: 'http://src1:3006,http://src2:3006' });
        sinon.stub(sync, '_connectWebSocket');

        sync._connectWebSockets();

        assert.strictEqual(sync._connectWebSocket.callCount, 2);
        assert.strictEqual(sync._connectWebSocket.firstCall.args[0], 'http://src1:3006');
        assert.strictEqual(sync._connectWebSocket.secondCall.args[0], 'http://src2:3006');
    });

    it('open handler: logs connected message', function(){
        let { sync } = makeSyncWS();
        sync.running = false;

        sync._connectWebSocket('http://src1:3006', 0);
        fakeWsInstance.emit('open');

        let logCalls = console.log.getCalls().map(c => c.args[0]);
        assert.ok(logCalls.some(m => m && m.indexOf('WebSocket connected') !== -1));
    });
});

// ─────────────────────────────────────────────────────────────────────────────
// 10a. _handleEvent lastKnownServerBlock branches
// ─────────────────────────────────────────────────────────────────────────────
describe('ClientSync: _handleEvent lastKnownServerBlock branches', function(){
    let sync, db;

    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });
    afterEach(function(){ sinon.restore(); });

    it('does not update lastKnownServerBlock on block event when incoming <= current', async function(){
        ({ sync, db } = makeSync());
        sync.lastKnownServerBlock = 200;
        sinon.stub(sync, '_handleBlock').resolves();

        await sync._handleEvent({ type: 'block', block_index: 100 }, 0);

        assert.strictEqual(sync.lastKnownServerBlock, 200, 'must not decrease lastKnownServerBlock');
    });

    it('does not update lastKnownServerBlock on status event when incoming <= current', async function(){
        ({ sync, db } = makeSync());
        sync.lastKnownServerBlock = 200;
        sync.lastAppliedBlock = 200;
        sinon.stub(sync, '_incrementalCatchUp').resolves();

        await sync._handleEvent({ type: 'status', block_height: 100 }, 0);

        assert.strictEqual(sync.lastKnownServerBlock, 200, 'must not decrease lastKnownServerBlock');
    });
});

// ─────────────────────────────────────────────────────────────────────────────
// 10b. Misc branch coverage (_haltOnDivergence, _verifyRecompute, clearHalt, _safeParse)
// ─────────────────────────────────────────────────────────────────────────────
describe('ClientSync: misc branch coverage', function(){
    let sync, db;

    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });
    afterEach(function(){ sinon.restore(); });

    it('_haltOnDivergence: uses defaults when mismatches/sources are falsy', async function(){
        ({ sync, db } = makeSync());

        await sync._haltOnDivergence(50, null, null, 'test-reason');

        assert.deepStrictEqual(sync._halted.mismatches, []);
        assert.deepStrictEqual(sync._halted.sources, []);
    });

    it('_haltOnDivergence: logs on recordHalt failure but still halts in memory', async function(){
        ({ sync, db } = makeSync());
        db.recordHalt.rejects(new Error('db error'));

        await sync._haltOnDivergence(50, [], [], 'test-reason');

        assert.ok(sync.isHalted(), 'must still be halted even when recordHalt fails');
        let errCalls = console.error.getCalls().map(c => c.args[0]);
        assert.ok(errCalls.some(m => m && m.indexOf('CRITICAL') !== -1));
    });

    it('_verifyRecompute: returns null for decoder dbType', async function(){
        ({ sync, db } = makeSync({}, { dbType: 'decoder' }));

        let result = await sync._verifyRecompute({ block_index: 5 });

        assert.strictEqual(result, null);
    });

    it('_safeParse: returns raw string when JSON.parse fails', function(){
        ({ sync, db } = makeSync());

        let result = sync._safeParse('not-valid-json{{{');

        assert.strictEqual(result, 'not-valid-json{{{');
    });

    it('clearHalt: logs without "was halted" when _halted is null', async function(){
        ({ sync, db } = makeSync());
        sync._halted = null;

        await sync.clearHalt();

        let logCalls = console.log.getCalls().map(c => c.args[0]);
        assert.ok(logCalls.some(m => m && m.indexOf('CLEARED') !== -1));
        // No "was halted at block" suffix
        assert.ok(!logCalls.some(m => m && m.indexOf('was halted at block') !== -1));
    });

    it('clearHalt: logs error when db.clearHalt throws', async function(){
        ({ sync, db } = makeSync());
        db.clearHalt.rejects(new Error('db fail'));
        sync._halted = { blockIndex: 5, reason: 'test', mismatches: [], sources: [], at: '2026-01-01' };

        await sync.clearHalt();

        let errCalls = console.error.getCalls().map(c => c.args[0]);
        assert.ok(errCalls.some(m => m && m.indexOf('clearHalt persistence failed') !== -1));
        assert.strictEqual(sync._halted, null, '_halted must still be cleared');
    });

    it('_flushHeartbeat: swallows ws.send error', function(){
        ({ sync, db } = makeSync({ SYNC_SOURCES: 'http://src1:3006' }));
        sync.lastAppliedBlock = 10;
        // WS that throws on send
        sync.wsConns = [{ readyState: 1, send: () => { throw new Error('send fail'); } }];
        sinon.stub(axios, 'post').resolves();

        // Should not throw
        sync._flushHeartbeat();

        assert.strictEqual(sync._hbLastSentBlock, 10);
    });

    it('_verifyTableCounts: treats NaN local count as 0', async function(){
        ({ sync, db } = makeSync());
        // getTableCount returns NaN (non-finite)
        db.getTableCount.resolves(NaN);

        let mismatches = await sync._verifyTableCounts({ blocks: 5 });

        // local=NaN → reset to 0 → remote(5) > local(0) → mismatch
        assert.strictEqual(mismatches.length, 1);
        assert.strictEqual(mismatches[0].localCount, 0);
    });

    it('constructor: validatorId falls back to "unknown" when hostname() is empty', function(){
        // Requires proxyquire to intercept the inline require('os') in the constructor
        const fakeOs = { hostname: () => '' };
        const ClientSyncOS = proxyquire('../../src/ClientSync', { os: fakeOs });
        const savedId = process.env.VALIDATOR_ID;
        delete process.env.VALIDATOR_ID;

        let db2 = createMockDb();
        let s = new ClientSyncOS('bitcoin', 'mainnet', db2, createMockApplier(), createMockRollback(),
            new HashVerifier(), {
                SYNC_SOURCES: 'http://src:3006', VERIFY_HASHES: false, CLIENT_RECONNECT_DELAY: 5000,
                HASH_CONFIRM_TIMEOUT: 5000, SNAPSHOT_MAX_CONTENT: 1000, WS_MAX_PAYLOAD: 1000,
                MAX_ROLLBACK_DEPTH: 10, GAP_LOG_INTERVAL_MS: 30000
            }, new Utility());

        assert.strictEqual(s.validatorId, 'unknown');

        if(savedId !== undefined) process.env.VALIDATOR_ID = savedId;
    });
});

// ─────────────────────────────────────────────────────────────────────────────
// 10. Small branches
// ─────────────────────────────────────────────────────────────────────────────
describe('ClientSync: small branches', function(){
    let sync, db, applier;

    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });
    afterEach(function(){ sinon.restore(); });

    // _handleBlock decoder gap (lines 722-726)
    it('_handleBlock (decoder): logs gap and triggers catch-up when blockIndex > lastApplied+1', async function(){
        ({ sync, db, applier } = makeSync({}, { dbType: 'decoder' }));
        sync.lastAppliedBlock = 10;
        sync.lastHashes = { block_hash: 'h10' };
        sinon.stub(sync, '_incrementalCatchUp').resolves();

        // blockIndex=13, gap of 2
        await sync._handleBlock({ type: 'block', block_index: 13, block_hash: 'h13' }, 0);

        assert.ok(sync._incrementalCatchUp.calledOnce, 'catch-up must be triggered on decoder gap');
        assert.strictEqual(applier.applyBlock.called, false);
    });

    // Strict-mode cross-source timeout (lines 750-751)
    it('_handleBlock: STRICT mode logs rejection and deletes pending on timeout', async function(){
        ({ sync, db, applier } = makeSync({
            SYNC_SOURCES: 'http://src1:3006,http://src2:3006',
            VERIFY_HASHES: true,
            HASH_CONFIRM_TIMEOUT: 200,
            HASH_CONFIRM_STRICT: true
        }));
        sync.lastAppliedBlock = 100;
        sync.lastHashes = { ledger_hash: 'lh100', actions_hash: 'ah100', contract_hash: 'ch100' };
        // Make chain continuity pass
        sinon.stub(sync.hashVerifier, 'verifyChainContinuity').returns({ valid: true });

        let clock = sinon.useFakeTimers();

        let event = {
            type: 'block', block_index: 101,
            ledger_hash: 'lh101', actions_hash: 'ah101', contract_hash: 'ch101'
        };
        // Source 0 sends, source 1 never sends
        await sync._handleBlock(event, 0);

        assert.strictEqual(applier.applyBlock.called, false, 'must not apply before timeout');

        // Tick past timeout
        await clock.tickAsync(300);

        // With STRICT mode, block should be rejected (not applied)
        assert.strictEqual(applier.applyBlock.called, false, 'STRICT mode must not apply on timeout');
        assert.strictEqual(sync.pendingHashes.has(101), false, 'pending must be deleted after timeout');

        let errCalls = console.error.getCalls().map(c => c.args[0]);
        assert.ok(errCalls.some(m => m && m.indexOf('STRICT') !== -1), 'STRICT log must appear');

        clock.restore();
    });

    // Halt on divergence from cross-source mismatch (lines 769-774)
    it('_handleBlock: HALT_ON_DIVERGENCE=true calls _haltOnDivergence on hash mismatch', async function(){
        ({ sync, db, applier } = makeSync({
            SYNC_SOURCES: 'http://src1:3006,http://src2:3006',
            VERIFY_HASHES: true,
            HALT_ON_DIVERGENCE: true,
            HASH_CONFIRM_TIMEOUT: 5000
        }));
        sync.lastAppliedBlock = 100;
        sync.lastHashes = { ledger_hash: 'lh100', actions_hash: 'ah100', contract_hash: 'ch100' };
        sinon.stub(sync.hashVerifier, 'verifyChainContinuity').returns({ valid: true });
        sinon.stub(sync, '_haltOnDivergence').resolves();

        let event0 = {
            type: 'block', block_index: 101,
            ledger_hash: 'lhA', actions_hash: 'ahA', contract_hash: 'chA'
        };
        let event1 = {
            type: 'block', block_index: 101,
            ledger_hash: 'lhB', actions_hash: 'ahB', contract_hash: 'chB'
        };

        await sync._handleBlock(event0, 0);
        await sync._handleBlock(event1, 1);

        assert.ok(sync._haltOnDivergence.calledOnce, '_haltOnDivergence must be called on hash mismatch');
        assert.strictEqual(applier.applyBlock.called, false, 'must not apply contested blocks');
    });

    // _applyBlockEvent decoder lastHashes (line 892)
    it('_applyBlockEvent sets lastHashes to {block_hash} for decoder dbType', async function(){
        ({ sync, db, applier } = makeSync({}, { dbType: 'decoder' }));
        sync.lastAppliedBlock = null;

        await sync._applyBlockEvent({
            block_index: 5,
            block_hash:  'bh5',
            ledger_hash: 'lh5',
            actions_hash: 'ah5',
            contract_hash: 'ch5'
        });

        assert.deepStrictEqual(sync.lastHashes, { block_hash: 'bh5' });
    });

    // _handleReorg max-depth exceeded → fail closed (durable halt, no rollback)
    it('_handleReorg: exceeds MAX_ROLLBACK_DEPTH → HALTS and does NOT rollback', async function(){
        let rb;
        ({ sync, db, applier, rb } = makeSync({ MAX_ROLLBACK_DEPTH: 5 }));
        sync.lastAppliedBlock = 100;

        // rb.rollback is already a sinon stub (from createMockRollback)
        let rollbackStub = rb.rollback;

        // Reorg to block 10 → depth = 100 - 10 + 1 = 91 > 5
        await sync._handleReorg({ type: 'reorg', block_index: 10 });

        assert.strictEqual(rollbackStub.called, false, 'rollback must NOT be called when depth exceeded');
        // Fail closed: record a durable halt rather than returning and serving the fork.
        assert.ok(sync.isHalted(), 'must halt durably when reorg depth exceeds the ceiling');
        assert.strictEqual(sync.getHaltInfo().reason, 'max-rollback-depth-exceeded');
        assert.strictEqual(db.recordHalt.calledOnce, true, 'durable halt must be persisted');
    });

    // The decoder track has no VERIFY_RECOMPUTE/VERIFY_STATE_HASH net, so the
    // max-depth halt is its only guard against permanently serving the fork.
    it('_handleReorg: decoder track also HALTS on max-depth exceed', async function(){
        let rb;
        ({ sync, db, applier, rb } = makeSync({ MAX_ROLLBACK_DEPTH: 5 }, { dbType: 'decoder' }));
        sync.lastAppliedBlock = 100;

        await sync._handleReorg({ type: 'reorg', block_index: 10 }); // depth = 91 > 5

        assert.strictEqual(rb.rollback.called, false);
        assert.ok(sync.isHalted(), 'decoder must halt durably; it has no recompute fallback');
        assert.strictEqual(sync.getHaltInfo().reason, 'max-rollback-depth-exceeded');
        assert.strictEqual(db.recordHalt.firstCall.args[0], 'decoder');
    });
});
