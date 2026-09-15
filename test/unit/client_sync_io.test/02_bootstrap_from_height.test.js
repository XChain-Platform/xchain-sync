// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

const assert     = require('assert');
const sinon      = require('sinon');
const axios      = require('axios');
const ClientSync = require('../../../src/client/sync');
const { SCHEMA_VERSION } = require('../../../src/schema/version');
const Utility    = require('../../../src/util');
const HashVerifier = require('../../../src/client/hash_verifier');
const { withDbMixins } = require('../../helpers/db_mixins.js');

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
describe('ClientSync bootstrapFromHeight', function(){
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
        sinon.stub(sync, 'fetchAndApplySchema').resolves();
        stubTransport({ tip: 1000000, base: 950000 });

        let ok = await sync.bootstrapFromHeight(50000);

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
        sinon.stub(sync, 'fetchAndApplySchema').resolves();
        let paged = sinon.stub(sync, 'syncLookupTablesPaged').resolves();
        stubTransport({ tip: 1000000, base: 950000 });

        await sync.bootstrapFromHeight(50000);

        assert.ok(paged.calledTwice, 'paged before and re-paged after the block window');
        assert.ok(applier.applyIncrementalSnapshot.calledOnce, 'block window applied once');
        assert.ok(paged.secondCall.calledAfter(applier.applyIncrementalSnapshot.firstCall),
            're-page runs AFTER the block window apply');
    });
});

describe('ClientSync bootstrapFromHeight', function(){
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



    it('clamps base to 0 when depth exceeds the tip', async function(){
        ({ sync, db, applier } = makeSync());
        sinon.stub(sync, 'fetchAndApplySchema').resolves();
        // tip 30, depth 50000 -> base must clamp to 0. Server echoes since_block=0.
        stubTransport({ tip: 30, snapshot: { schema_version: SCHEMA_VERSION.indexer, block_height: 30, since_block: 0, tables: {} } });

        await sync.bootstrapFromHeight(50000);

        let snapCall = axios.get.getCalls().find(c => c.args[0].indexOf('/snapshot/') !== -1);
        assert.ok(snapCall.args[0].indexOf('/since/0') !== -1, 'requests since/0 when depth > tip');
        assert.ok(snapCall.args[0].indexOf('skip_lookups=1') !== -1, 'block window requested with skip_lookups');
        assert.strictEqual(sync._bootstrapBase, 0);
    });

    it('throws when the source tip is unavailable (so the retry wrapper can restart)', async function(){
        ({ sync, db, applier } = makeSync());
        sinon.stub(sync, 'fetchAndApplySchema').resolves();
        sinon.stub(axios, 'get').callsFake(async (url) => {
            if(url.indexOf('/status/') !== -1) return { data: {} }; // no height fields
            return { data: Buffer.from('{}') };
        });

        await assert.rejects(() => sync.bootstrapFromHeight(50000), /source tip unavailable/);
        assert.strictEqual(applier.applyIncrementalSnapshot.called, false);
    });
});

describe('ClientSync bootstrapFromHeight', function(){
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



    it('VERIFY_RECOMPUTE: recomputes the terminal block and HALTS on mismatch', async function(){
        ({ sync, db, applier } = makeSync({ VERIFY_RECOMPUTE: true }));
        sinon.stub(sync, 'fetchAndApplySchema').resolves();
        stubTransport({ tip: 1000000, base: 950000 });
        db.getBlockHashRow.resolves({ ledger_hash: 'lh', actions_hash: 'ah', contract_hash: 'ch' });
        sinon.stub(sync, 'verifyRecompute').resolves([{ field: 'ledger_hash', computed: 'X', committed: 'lh' }]);
        let halt = sinon.stub(sync, 'haltOnDivergence').resolves();

        await sync.bootstrapFromHeight(50000);

        assert.ok(sync.verifyRecompute.calledOnce, 'terminal block recomputed');
        assert.strictEqual(sync.verifyRecompute.firstCall.args[0].block_index, 1000000);
        assert.ok(halt.calledOnce, 'halts durably on a terminal mismatch');
    });

    it('retry wrapper throws (never live-follows empty) when no sources configured', async function(){
        ({ sync } = makeSync({ SYNC_SOURCES: '' }));
        await assert.rejects(() => sync.bootstrapFromHeightRetry(50000), /no sync sources configured/);
    });
});

describe('ClientSync bootstrapFromHeight', function(){
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



    it('retry wrapper retries with backoff then succeeds', async function(){
        ({ sync, db, applier } = makeSync({ BOOTSTRAP_MAX_RETRIES: 3 }));
        let sleep = sinon.stub(sync.util, 'sleep').resolves();
        let fromHeight = sinon.stub(sync, 'bootstrapFromHeight');
        fromHeight.onCall(0).rejects(new Error('transient'));
        fromHeight.onCall(1).resolves(true);

        await sync.bootstrapFromHeightRetry(50000);

        assert.strictEqual(fromHeight.callCount, 2);
        assert.ok(sleep.calledOnce, 'backed off once between rounds');
    });

    it('retry wrapper THROWS on exhaustion', async function(){
        ({ sync } = makeSync({ BOOTSTRAP_MAX_RETRIES: 0 }));
        sinon.stub(sync, 'bootstrapFromHeight').rejects(new Error('boom'));
        await assert.rejects(() => sync.bootstrapFromHeightRetry(50000), /exhausted after 1 round/);
    });
});
