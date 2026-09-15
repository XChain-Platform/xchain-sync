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
// A hole below the replica's high-water mark is unreachable from the ordinary
// cursor, which is how the BTC mainnet index_transactions gap (blocks
// 961908-963876, every block's state_hash row) survived four weeks of sweeps
// that reported it on every pass. These pin the repair, and pin that the count
// check which FOUND it stays strict on the lookups.
describe('ClientSync lookup-hole repair and count-check scoping @regression', function(){
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

    it('fromZero pages from id 0, never consulting the high-water mark', async function(){
        ({ sync, db, applier } = makeSync());
        sinon.stub(rt, 'getTopology').returns({ index: ['index_transactions'] });
        // A replica holding rows up to id 500 with a hole below it. The ordinary
        // path would start at 500 and skip the hole forever.
        db.doQuery.resolves([{ m: 500 }]);
        let get = sinon.stub(axios, 'get');
        get.onCall(0).resolves(page([{ id: 1 }], false, 1));

        await sync.syncLookupTablesPaged('http://src:3006', { fromZero: new Set(['index_transactions']) });

        assert.ok(get.firstCall.args[0].indexOf('after_id=0') !== -1,
            'a repair pass must start at id 0, or the hole stays unreachable');
        assert.strictEqual(db.doQuery.called, false,
            'the MAX(id) probe is skipped entirely on a repair pass');
    });

    it('leaves tables not named in fromZero on the ordinary high-water cursor', async function(){
        ({ sync, db, applier } = makeSync());
        sinon.stub(rt, 'getTopology').returns({ index: ['index_transactions'] });
        db.doQuery.resolves([{ m: 500 }]);
        let get = sinon.stub(axios, 'get');
        get.onCall(0).resolves(page([], false, 500));

        await sync.syncLookupTablesPaged('http://src:3006', { fromZero: new Set(['index_addresses']) });

        assert.ok(get.firstCall.args[0].indexOf('after_id=500') !== -1,
            'an unnamed table keeps the cheap cursor-seeded page');
    });
});

describe('ClientSync lookup-hole repair and count-check scoping @regression', function(){
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



    // a repair pass must tell ClientApplier to fail loud on a row IGNORE
    // silently ate for any reason other than its own PRIMARY key, or a from-zero
    // pass over a short lookup table can go on reporting the identical short count
    // forever with nothing in the journal to explain why (the production RDOGE
    // index_statuses case this ticket exists for). The ordinary cursor-seeded path
    // must NOT pay for that extra check: every block re-sends these tables' rows
    // by design, so it stays on the cheap, silent INSERT IGNORE contract.
    it('a repairing (fromZero) pass asks ClientApplier for the strict IGNORE check', async function(){
        ({ sync, db, applier } = makeSync());
        sinon.stub(rt, 'getTopology').returns({ index: ['index_statuses'] });
        db.doQuery.resolves([{ m: null }]);
        let get = sinon.stub(axios, 'get');
        get.onCall(0).resolves(
            { data: Buffer.from(JSON.stringify({
                schema_version: SCHEMA_VERSION.indexer, table: 'index_statuses',
                max_id: 3, has_more: false,
                rows: [{ id: 1, status: 'open' }, { id: 2, status: 'closed' }, { id: 3, status: 'completed' }]
            })) });

        await sync.syncLookupTablesPaged('http://src:3006', { fromZero: new Set(['index_statuses']) });

        assert.strictEqual(applier.applyIncrementalSnapshot.callCount, 1);
        assert.deepStrictEqual(applier.applyIncrementalSnapshot.firstCall.args[1], { strictIgnoreCheck: true },
            'a repair pass must opt into the strict post-INSERT IGNORE check');
    });

    it('the ordinary high-water cursor path never asks for the strict IGNORE check', async function(){
        ({ sync, db, applier } = makeSync());
        sinon.stub(rt, 'getTopology').returns({ index: ['index_statuses'] });
        db.doQuery.resolves([{ m: 0 }]);
        let get = sinon.stub(axios, 'get');
        get.onCall(0).resolves(
            { data: Buffer.from(JSON.stringify({
                schema_version: SCHEMA_VERSION.indexer, table: 'index_statuses',
                max_id: 1, has_more: false, rows: [{ id: 1, status: 'open' }]
            })) });

        await sync.syncLookupTablesPaged('http://src:3006');

        assert.strictEqual(applier.applyIncrementalSnapshot.callCount, 1);
        assert.strictEqual(applier.applyIncrementalSnapshot.firstCall.args[1], undefined,
            'the hot streaming/catch-up path must not pay for the extra SHOW WARNINGS round trip');
    });
});

describe('ClientSync lookup-hole repair and count-check scoping @regression', function(){
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



    it('excludes the events operational log from the count check, whose counts cannot converge', async function(){
        ({ sync, db } = makeSync());
        db.getTableCount = sinon.stub().resolves(0);
        let mismatches = await sync.verifyTableCounts({ events: 415 }, undefined, {});
        assert.deepStrictEqual(mismatches, [],
            'events is applied INSERT IGNORE over an independently generated id, so a delta is structural');
    });

    it('KEEPS index_transactions strict, because that check is what found the mainnet hole', async function(){
        ({ sync, db } = makeSync());
        db.getTableCount = sinon.stub().resolves(186003);
        let mismatches = await sync.verifyTableCounts({ index_transactions: 187972 }, undefined, {});
        assert.strictEqual(mismatches.length, 1, 'a short lookup must still be reported');
        assert.strictEqual(mismatches[0].table, 'index_transactions');
        assert.strictEqual(mismatches[0].delta, 1969);
    });
});
