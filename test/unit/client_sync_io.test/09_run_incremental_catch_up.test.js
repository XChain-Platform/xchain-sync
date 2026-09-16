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
describe('ClientSync: runIncrementalCatchUp', function(){
    let sync, db, applier;

    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });
    afterEach(function(){ sinon.restore(); });

    it('returns early when no sources configured', async function(){
        ({ sync, db, applier } = makeSync({ SYNC_SOURCES: '' }));

        await sync.runIncrementalCatchUp();

        assert.strictEqual(applier.applyIncrementalSnapshot.called, false);
    });

    it('happy path: dbTip null → sinceBlock=1, calls applyIncrementalSnapshot', async function(){
        ({ sync, db, applier } = makeSync());
        db.getLastBlock.resolves(null);
        let payload = Buffer.from(JSON.stringify({ block_height: 20, tables: {} }));
        sinon.stub(axios, 'get').resolves({ data: payload });

        await sync.runIncrementalCatchUp();

        assert.ok(applier.applyIncrementalSnapshot.calledOnce);
        assert.strictEqual(sync.lastAppliedBlock, 20);
        let url = axios.get.firstCall.args[0];
        assert.ok(url.indexOf('/since/1') !== -1, 'since=1 when dbTip is null');
    });

    // A fail-soft tip read answers null for a DB blip too, and null is what an EMPTY
    // replica answers, so the resume cursor would collapse to since/1 and re-request
    // the whole history into the plain-INSERT ledger tables.
    it('reads the resume cursor fail-CLOSED (opts.rethrow)', async function(){
        ({ sync, db, applier } = makeSync());
        db.getLastBlock.resolves(10);
        sinon.stub(axios, 'get').resolves({ data: Buffer.from(JSON.stringify({ block_height: 15, tables: {} })) });

        await sync.runIncrementalCatchUp();

        assert.deepStrictEqual(db.getLastBlock.firstCall.args[1], { rethrow: true });
    });

    it('aborts the pass (no request, no apply) when the tip read faults', async function(){
        ({ sync, db, applier } = makeSync());
        let err = new Error('server has gone away'); err.errno = 2006;
        db.getLastBlock.rejects(err);
        sinon.stub(axios, 'get').resolves({ data: Buffer.from(JSON.stringify({ block_height: 15, tables: {} })) });

        // Caught by the pass's own catch: it must NOT reject out of the in-flight
        // runner, whose callers (WS event handlers) have no catch of their own.
        await sync.runIncrementalCatchUp();

        assert.strictEqual(axios.get.called, false, 'no snapshot window requested');
        assert.strictEqual(applier.applyIncrementalSnapshot.called, false);
    });
});

describe('ClientSync: runIncrementalCatchUp', function(){
    let sync, db, applier;

    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });
    afterEach(function(){ sinon.restore(); });



    it('happy path: dbTip=10 → sinceBlock=11', async function(){
        ({ sync, db, applier } = makeSync());
        db.getLastBlock.resolves(10);
        let payload = Buffer.from(JSON.stringify({ block_height: 15, tables: {} }));
        sinon.stub(axios, 'get').resolves({ data: payload });

        await sync.runIncrementalCatchUp();

        let url = axios.get.firstCall.args[0];
        assert.ok(url.indexOf('/since/11') !== -1, 'since=11 when dbTip=10');
        assert.strictEqual(sync.lastAppliedBlock, 15);
    });

    it('logs error when axios.get rejects', async function(){
        ({ sync, db, applier } = makeSync());
        db.getLastBlock.resolves(5);
        sinon.stub(axios, 'get').rejects(new Error('inc fail'));

        await sync.runIncrementalCatchUp();

        let errCalls = console.error.getCalls().map(c => c.args[0]);
        assert.ok(errCalls.some(m => m && m.indexOf('Incremental catch-up failed') !== -1));
    });
});
