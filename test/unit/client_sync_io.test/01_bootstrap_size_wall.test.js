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
// ─────────────────────────────────────────────────────────────────────────────
// 2a. bootstrap size wall + rate limit
//
// The DOGE:testnet replica on origin-host froze for weeks because a full-history
// snapshot outgrew SNAPSHOT_MAX_CONTENT: every bootstrap round hit the same wall,
// BootstrapExhaustedError exited the process, systemd restarted it, and the loop
// drained the source's hourly full-snapshot budget until every request 429'd.
// ─────────────────────────────────────────────────────────────────────────────
describe('ClientSync: bootstrap size wall', function(){
    let sync, db, applier;

    function sizeError(){
        let e = new Error('maxContentLength size of 209715200 exceeded');
        e.code = 'ERR_FR_MAX_CONTENT_LENGTH_EXCEEDED';
        return e;
    }

    function rateLimitError(headers){
        let e = new Error('Request failed with status code 429');
        e.response = { status: 429, headers: headers || {} };
        return e;
    }

    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
        sinon.stub(console, 'warn');
    });
    afterEach(function(){ sinon.restore(); });

    it('an oversized full snapshot halts durably instead of burning retry rounds', async function(){
        ({ sync, db, applier } = makeSync({
            SYNC_SOURCES: 'http://src1:3006',
            BOOTSTRAP_MAX_RETRIES: 5
        }));
        let sleep = sinon.stub(sync.util, 'sleep').resolves();
        sinon.stub(sync, 'fetchAndApplySchema').resolves();
        let get = sinon.stub(axios, 'get').rejects(sizeError());

        await assert.rejects(() => sync.bootstrapFromSnapshot(), /snapshot-too-large/);

        // One attempt, not six: retrying cannot shrink the payload, and each extra
        // request is one more token off the source's hourly snapshot budget.
        assert.strictEqual(get.callCount, 1, 'must not retry an unshrinkable payload');
        assert.strictEqual(sleep.callCount, 0, 'must not back off into another round');
        assert.strictEqual(sync.isHalted(), true, 'halts rather than exiting for a restart');
        assert.strictEqual(sync.getHaltInfo().reason, 'snapshot-too-large');
        assert.strictEqual(applier.applyFullSnapshot.called, false);
    });

    it('persists the halt so the restart lands idle rather than back at the wall', async function(){
        ({ sync, db, applier } = makeSync({ SYNC_SOURCES: 'http://src1:3006' }));
        sinon.stub(sync.util, 'sleep').resolves();
        sinon.stub(sync, 'fetchAndApplySchema').resolves();
        sinon.stub(axios, 'get').rejects(sizeError());

        await assert.rejects(() => sync.bootstrapFromSnapshot(), /operator must clear the halt/);

        assert.ok(db.recordHalt.calledOnce, 'halt must survive the process');
        assert.strictEqual(db.recordHalt.firstCall.args[2], 'snapshot-too-large');
        // The message must name the operator's actual remedy, since no automated
        // path can choose between truncating the replica and raising the ceiling.
        let logged = console.error.getCalls().map(c => String(c.args[0])).join('\n');
        assert.ok(logged.indexOf('SYNC_BOOTSTRAP_DEPTH') !== -1, 'names the truncation remedy');
        assert.ok(logged.indexOf('SNAPSHOT_MAX_CONTENT') !== -1, 'names the ceiling remedy');
    });
});

describe('ClientSync: bootstrap size wall', function(){
    let sync, db, applier;

    function sizeError(){
        let e = new Error('maxContentLength size of 209715200 exceeded');
        e.code = 'ERR_FR_MAX_CONTENT_LENGTH_EXCEEDED';
        return e;
    }

    function rateLimitError(headers){
        let e = new Error('Request failed with status code 429');
        e.response = { status: 429, headers: headers || {} };
        return e;
    }

    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
        sinon.stub(console, 'warn');
    });
    afterEach(function(){ sinon.restore(); });



    it('a rotating multi-source bootstrap stops at the first size wall', async function(){
        // Every source serves the same chain, so the second source is the same wall.
        ({ sync, db, applier } = makeSync({
            SYNC_SOURCES: 'http://src1:3006,http://src2:3006',
            BOOTSTRAP_MAX_RETRIES: 5
        }));
        sinon.stub(sync.util, 'sleep').resolves();
        sinon.stub(sync, 'fetchAndApplySchema').resolves();
        let get = sinon.stub(axios, 'get').rejects(sizeError());

        await assert.rejects(() => sync.bootstrapFromSnapshot(), /snapshot-too-large/);

        assert.strictEqual(get.callCount, 1, 'must not rotate onto an identical payload');
    });

    it('a generic transport failure still rotates and retries (no over-halting)', async function(){
        ({ sync, db, applier } = makeSync({
            SYNC_SOURCES: 'http://src1:3006',
            BOOTSTRAP_MAX_RETRIES: 2
        }));
        sinon.stub(sync.util, 'sleep').resolves();
        sinon.stub(sync, 'fetchAndApplySchema').resolves();
        sinon.stub(axios, 'get').rejects(new Error('ECONNRESET'));

        await assert.rejects(() => sync.bootstrapFromSnapshot(), /all sync sources exhausted/);

        assert.strictEqual(sync.isHalted(), false, 'a transient fault must not halt');
        assert.strictEqual(db.recordHalt.called, false);
    });
});

describe('ClientSync: bootstrap size wall', function(){
    let sync, db, applier;

    function sizeError(){
        let e = new Error('maxContentLength size of 209715200 exceeded');
        e.code = 'ERR_FR_MAX_CONTENT_LENGTH_EXCEEDED';
        return e;
    }

    function rateLimitError(headers){
        let e = new Error('Request failed with status code 429');
        e.response = { status: 429, headers: headers || {} };
        return e;
    }

    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
        sinon.stub(console, 'warn');
    });
    afterEach(function(){ sinon.restore(); });



    it('a 429 is reported with the wait the source advertised', async function(){
        ({ sync, db, applier } = makeSync({
            SYNC_SOURCES: 'http://src1:3006',
            BOOTSTRAP_MAX_RETRIES: 0
        }));
        sinon.stub(sync.util, 'sleep').resolves();
        sinon.stub(sync, 'fetchAndApplySchema').resolves();
        sinon.stub(axios, 'get').rejects(rateLimitError({ 'retry-after': '2400' }));

        await assert.rejects(() => sync.bootstrapFromSnapshot(), /all sync sources exhausted/);

        let logged = console.error.getCalls().map(c => String(c.args[0])).join('\n');
        assert.ok(logged.indexOf('HTTP 429') !== -1, 'the 429 must be named, not buried in the axios dump');
        assert.ok(logged.indexOf('2400s') !== -1, 'the advertised wait must be readable');
        // A 429 is transient (the hourly window reopens), so it must not halt.
        assert.strictEqual(sync.isHalted(), false);
    });

    it('falls back to RateLimit-Reset when Retry-After is absent', function(){
        ({ sync } = makeSync());
        assert.strictEqual(sync.rateLimitRetryAfterSeconds(rateLimitError({ 'ratelimit-reset': '900' })), 900);
        assert.strictEqual(sync.rateLimitRetryAfterSeconds(rateLimitError({})), 0);
        assert.strictEqual(sync.rateLimitRetryAfterSeconds(new Error('nope')), null);
    });

    it('the size wall has one definition, shared with the incremental fallback', function(){
        ({ sync } = makeSync());
        assert.strictEqual(sync.isContentLengthOverflow(sizeError()), true);
        // axios raises the same wall by message alone on some transports.
        assert.strictEqual(sync.isContentLengthOverflow(new Error('maxContentLength exceeded')), true);
        assert.strictEqual(sync.isContentLengthOverflow(new Error('ECONNRESET')), false);
        assert.strictEqual(sync.isContentLengthOverflow(null), false);
    });
});
