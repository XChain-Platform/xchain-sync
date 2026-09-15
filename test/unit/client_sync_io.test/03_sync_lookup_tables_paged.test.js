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
describe('ClientSync syncLookupTablesPaged', function(){
    let sync, db, applier, rt;
    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
        sinon.stub(console, 'warn');
        rt = require('../../../src/schema/replicated_tables');
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

        await sync.syncLookupTablesPaged('http://src:3006');

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

        await sync.syncLookupTablesPaged('http://src:3006');

        assert.ok(get.firstCall.args[0].indexOf('after_id=8547252') !== -1, 'cursor starts at replica max id');
        assert.strictEqual(applier.applyIncrementalSnapshot.called, false, 'empty page applies nothing');
    });

    it('throws on a schema-version mismatch (fail closed)', async function(){
        ({ sync, db } = makeSync());
        sinon.stub(rt, 'getTopology').returns({ index: ['index_transactions'] });
        db.doQuery.resolves([{ m: 0 }]);
        sinon.stub(axios, 'get').resolves({ data: Buffer.from(JSON.stringify({ schema_version: 99, has_more: false, rows: [] })) });

        await assert.rejects(() => sync.syncLookupTablesPaged('http://src:3006'), /schema mismatch/);
    });
});

describe('ClientSync syncLookupTablesPaged', function(){
    let sync, db, applier, rt;
    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
        sinon.stub(console, 'warn');
        rt = require('../../../src/schema/replicated_tables');
    });
    afterEach(function(){ sinon.restore(); });

    function page(rows, has_more, max_id){
        return { data: Buffer.from(JSON.stringify({ schema_version: SCHEMA_VERSION.indexer, table: 'index_transactions', max_id, has_more, rows })) };
    }



    it('stops if has_more is true but the cursor cannot advance (no infinite spin)', async function(){
        ({ sync, db, applier } = makeSync());
        sinon.stub(rt, 'getTopology').returns({ index: ['index_transactions'] });
        db.doQuery.resolves([{ m: 0 }]);
        let get = sinon.stub(axios, 'get').resolves(page([{ id: 5 }], true, 0)); // has_more but max_id <= afterId

        await sync.syncLookupTablesPaged('http://src:3006');

        assert.strictEqual(get.callCount, 1, 'stopped after one page despite has_more');
    });

    it('decoder: pages pubkeys by its surrogate id cursor', async function(){
        ({ sync, db, applier } = makeSync({}, { dbType: 'decoder' }));
        sinon.stub(rt, 'getTopology').returns({ index: ['pubkeys'] });
        db.doQuery.resolves([{ m: 42 }]); // replica MAX(id) = 42
        // decoder uses SCHEMA_VERSION.decoder (distinct from indexer); the page must match it.
        let get = sinon.stub(axios, 'get').resolves(
            { data: Buffer.from(JSON.stringify({ schema_version: SCHEMA_VERSION.decoder, table: 'pubkeys', max_id: 42, has_more: false, rows: [] })) });

        await sync.syncLookupTablesPaged('http://src:3006');

        let maxQ = db.doQuery.getCalls().find(c => /MAX\(`id`\)/.test(c.args[0]));
        assert.ok(maxQ, 'queries MAX(id) for pubkeys (surrogate key added by fix #4413)');
        assert.ok(get.firstCall.args[0].indexOf('after_id=42') !== -1, 'cursor starts at replica id high-water');
    });
});
