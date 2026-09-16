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
const realConfig = require('../../../src/config');
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
describe('ClientSync truncated catch-up', function(){
    let sync, db, applier;
    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
        sinon.stub(console, 'warn');
    });
    afterEach(function(){ sinon.restore(); });

    // Regression: the two ends of this lookup disagreed. config.js keyed the map by the
    // env-var spelling ('DOGE:TESTNET') while ClientSync asked by the hub's `cfg.coin`
    // ('dogecoin'), so the documented key missed and depth fell through to 0, which is
    // the FULL-snapshot branch. Drive the real getConfig() parse against the real
    // constructor, in both directions, so a re-divergence cannot pass.


    it('truncated chain pages lookups then fetches the block window with skip_lookups=1', async function(){
        // makeSync builds ClientSync('bitcoin','mainnet'); depth keyed '<TICKER>:<NETWORK>'.
        ({ sync, db, applier } = makeSync({ SYNC_BOOTSTRAP_DEPTH: { 'BTC:MAINNET': 50000 } }));
        assert.ok(sync._truncatedDepth >= 1, 'chain is in truncated mode');
        let paged = sinon.stub(sync, 'syncLookupTablesPaged').resolves();
        db.getLastBlock.resolves(100);
        sinon.stub(axios, 'get').resolves({ data: Buffer.from(JSON.stringify({ schema_version: SCHEMA_VERSION.indexer, block_height: 105, since_block: 101, tables: {} })) });

        await sync.runIncrementalCatchUp();

        assert.ok(paged.calledTwice, 'lookups paged before AND re-paged after the block window');
        let snapCall = axios.get.getCalls().find(c => c.args[0].indexOf('/snapshot/') !== -1);
        assert.ok(snapCall.args[0].indexOf('skip_lookups=1') !== -1, 'block window fetched with skip_lookups');
    });

    it('re-pages lookups AFTER the block window apply so (T1..T2] FK targets are present before recompute', async function(){
        // Regression: the skip_lookups snapshot is taken at the source tip T2 > the
        // paging high-water T1, so blocks (T1..T2] reference index_* rows not pulled
        // in the first page. The second syncLookupTablesPaged must run after apply.
        ({ sync, db, applier } = makeSync({ SYNC_BOOTSTRAP_DEPTH: { 'BTC:MAINNET': 50000 } }));
        let paged = sinon.stub(sync, 'syncLookupTablesPaged').resolves();
        db.getLastBlock.resolves(100);
        sinon.stub(axios, 'get').resolves({ data: Buffer.from(JSON.stringify({ schema_version: SCHEMA_VERSION.indexer, block_height: 105, since_block: 101, tables: {} })) });

        await sync.runIncrementalCatchUp();

        assert.ok(paged.calledTwice, 'paged twice: before and after the block window');
        assert.ok(applier.applyIncrementalSnapshot.calledOnce, 'block window applied once');
        assert.ok(paged.secondCall.calledAfter(applier.applyIncrementalSnapshot.firstCall),
            're-page runs AFTER the block window is applied');
    });
});

describe('ClientSync truncated catch-up', function(){
    let sync, db, applier;
    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
        sinon.stub(console, 'warn');
    });
    afterEach(function(){ sinon.restore(); });

    // Regression: the two ends of this lookup disagreed. config.js keyed the map by the
    // env-var spelling ('DOGE:TESTNET') while ClientSync asked by the hub's `cfg.coin`
    // ('dogecoin'), so the documented key missed and depth fell through to 0, which is
    // the FULL-snapshot branch. Drive the real getConfig() parse against the real
    // constructor, in both directions, so a re-divergence cannot pass.


    it('full-history chain does NOT page lookups and uses no skip_lookups (unchanged path)', async function(){
        ({ sync, db, applier } = makeSync()); // no depth -> _truncatedDepth 0
        assert.strictEqual(sync._truncatedDepth, 0);
        let paged = sinon.stub(sync, 'syncLookupTablesPaged').resolves();
        db.getLastBlock.resolves(100);
        sinon.stub(axios, 'get').resolves({ data: Buffer.from(JSON.stringify({ schema_version: SCHEMA_VERSION.indexer, block_height: 105, since_block: 101, tables: {} })) });

        await sync.runIncrementalCatchUp();

        assert.ok(paged.notCalled, 'no paged lookup sync for full-history chains');
        let snapCall = axios.get.getCalls().find(c => c.args[0].indexOf('/snapshot/') !== -1);
        assert.ok(snapCall.args[0].indexOf('skip_lookups') === -1, 'no skip_lookups for full-history');
    });

    it('decoder chain is truncated by the same depth and pages lookups + skip_lookups', async function(){
        // Depth is keyed by chain:network (not dbType), so it truncates the decoder too.
        ({ sync, db, applier } = makeSync({ SYNC_BOOTSTRAP_DEPTH: { 'BTC:MAINNET': 50000 } }, { dbType: 'decoder' }));
        assert.ok(sync._truncatedDepth >= 1, 'decoder picks up the chain depth (gate removed)');
        let paged = sinon.stub(sync, 'syncLookupTablesPaged').resolves();
        db.getLastBlock.resolves(100);
        sinon.stub(axios, 'get').resolves({ data: Buffer.from(JSON.stringify({ schema_version: SCHEMA_VERSION.decoder, block_height: 105, since_block: 101, tables: {} })) });

        await sync.runIncrementalCatchUp();

        assert.ok(paged.calledTwice, 'decoder pages lookups before AND re-pages after the window');
        let snapCall = axios.get.getCalls().find(c => c.args[0].indexOf('/snapshot/') !== -1);
        assert.ok(snapCall.args[0].indexOf('skip_lookups=1') !== -1, 'decoder block window fetched with skip_lookups');
    });
});

describe('ClientSync truncated catch-up', function(){
    let sync, db, applier;
    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
        sinon.stub(console, 'warn');
    });
    afterEach(function(){ sinon.restore(); });

    // Regression: the two ends of this lookup disagreed. config.js keyed the map by the
    // env-var spelling ('DOGE:TESTNET') while ClientSync asked by the hub's `cfg.coin`
    // ('dogecoin'), so the documented key missed and depth fell through to 0, which is
    // the FULL-snapshot branch. Drive the real getConfig() parse against the real
    // constructor, in both directions, so a re-divergence cannot pass.
    describe('depth-key resolution against the real config parse', function(){
        const DEPTH_ENV = ['SYNC_BOOTSTRAP_DEPTH_DOGE_TESTNET', 'SYNC_BOOTSTRAP_DEPTH_DOGECOIN_TESTNET'];
        let saved = {};
        beforeEach(function(){
            for(let k of DEPTH_ENV){ saved[k] = process.env[k]; delete process.env[k]; }
        });
        afterEach(function(){
            for(let k of DEPTH_ENV){
                if(saved[k] === undefined) delete process.env[k]; else process.env[k] = saved[k];
            }
        });

        function depthFor(envKey){
            process.env[envKey] = '50000';
            let cfg = Object.assign(realConfig.getConfig(), { SYNC_SOURCES: 'http://src1:3006', MAX_ROLLBACK_DEPTH: 10 });
            // 'dogecoin' is the form the hub publishes as cfg.coin and SyncService passes on
            let s = new ClientSync('dogecoin', 'testnet', withDbMixins(createMockDb()), createMockApplier(),
                createMockRollback(), new HashVerifier(), cfg, new Utility());
            return s._truncatedDepth;
        }

        it('the documented SYNC_BOOTSTRAP_DEPTH_DOGE_TESTNET reaches a hub-named "dogecoin" chain', function(){
            assert.strictEqual(depthFor('SYNC_BOOTSTRAP_DEPTH_DOGE_TESTNET'), 50000);
        });
        it('the full-name SYNC_BOOTSTRAP_DEPTH_DOGECOIN_TESTNET resolves to the same depth', function(){
            assert.strictEqual(depthFor('SYNC_BOOTSTRAP_DEPTH_DOGECOIN_TESTNET'), 50000);
        });
    });
});
