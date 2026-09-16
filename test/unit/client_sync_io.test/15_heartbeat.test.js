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
describe('ClientSync: heartbeat', function(){
    let sync, db;

    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });
    afterEach(function(){ sinon.restore(); });

    it('flushHeartbeat returns early when lastAppliedBlock is null', function(){
        ({ sync, db } = makeSync());
        sync.lastAppliedBlock = null;
        // If it didn't return early it would try to iterate wsConns and sources
        // Just verify it doesn't crash and axios.post is never called
        let postStub = sinon.stub(axios, 'post').resolves();
        sync.flushHeartbeat();
        assert.strictEqual(postStub.called, false);
    });

    it('flushHeartbeat sends WS message to OPEN connections and clears timer', function(){
        ({ sync, db } = makeSync({ SYNC_SOURCES: 'http://src1:3006' }));
        sync.lastAppliedBlock = 42;
        let sendStub = sinon.stub();
        // OPEN = 1
        sync.wsConns = [{ readyState: 1, send: sendStub }];
        let clock = sinon.useFakeTimers();
        sync._hbTimer = setTimeout(() => {}, 9999);
        sinon.stub(axios, 'post').resolves();

        sync.flushHeartbeat();

        assert.ok(sendStub.calledOnce, 'send must be called for OPEN connection');
        let msg = JSON.parse(sendStub.firstCall.args[0]);
        assert.strictEqual(msg.type, 'heartbeat');
        assert.strictEqual(msg.appliedBlock, 42);
        assert.strictEqual(sync._hbTimer, null, 'timer must be cleared');
        assert.strictEqual(sync._hbLastSentBlock, 42);
        clock.restore();
    });

    it('scheduleHeartbeat flushes immediately when _hbLastSentBlock is null', function(){
        ({ sync, db } = makeSync({ SYNC_SOURCES: 'http://src1:3006' }));
        sync.lastAppliedBlock  = 10;
        sync._hbLastSentBlock  = null;
        sinon.stub(sync, 'flushHeartbeat');

        sync.scheduleHeartbeat();

        assert.ok(sync.flushHeartbeat.calledOnce, 'must flush immediately on first heartbeat');
    });
});

describe('ClientSync: heartbeat', function(){
    let sync, db;

    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });
    afterEach(function(){ sinon.restore(); });



    it('scheduleHeartbeat flushes immediately when >= 10 blocks advanced', function(){
        ({ sync, db } = makeSync({ SYNC_SOURCES: 'http://src1:3006' }));
        sync.lastAppliedBlock = 110;
        sync._hbLastSentBlock = 100; // delta = 10
        sinon.stub(sync, 'flushHeartbeat');

        sync.scheduleHeartbeat();

        assert.ok(sync.flushHeartbeat.calledOnce);
    });

    it('scheduleHeartbeat arms a 5s timer when delta < 10 and no timer running', function(){
        ({ sync, db } = makeSync({ SYNC_SOURCES: 'http://src1:3006' }));
        sync.lastAppliedBlock = 105;
        sync._hbLastSentBlock = 100; // delta = 5
        sync._hbTimer = null;
        let clock = sinon.useFakeTimers();
        sinon.stub(sync, 'flushHeartbeat');

        sync.scheduleHeartbeat();

        assert.strictEqual(sync.flushHeartbeat.called, false, 'must not flush immediately');
        assert.notStrictEqual(sync._hbTimer, null, 'timer must be set');

        clock.tick(5000);

        assert.ok(sync.flushHeartbeat.calledOnce, 'must flush after 5s timer');
        clock.restore();
    });

    it('sendRestHeartbeat posts to correct URL with Bearer header when the upstream key is set', async function(){
        ({ sync, db } = makeSync({ SYNC_SOURCES: 'http://src1:3006', SYNC_UPSTREAM_KEY: 'upkey' }));
        sync.lastAppliedBlock = 99;
        let postStub = sinon.stub(axios, 'post').resolves();

        await sync.sendRestHeartbeat('http://src1:3006');

        assert.ok(postStub.calledOnce);
        let [url, body, opts] = postStub.firstCall.args;
        assert.ok(url.indexOf('/validator-heartbeat/') !== -1);
        assert.strictEqual(opts.headers['Authorization'], 'Bearer upkey');
    });
});

describe('ClientSync: heartbeat', function(){
    let sync, db;

    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });
    afterEach(function(){ sinon.restore(); });



    it('sendRestHeartbeat omits Authorization header when no upstream key', async function(){
        ({ sync, db } = makeSync({ SYNC_SOURCES: 'http://src1:3006' }));
        sync.config['SYNC_UPSTREAM_KEY'] = '';
        let postStub = sinon.stub(axios, 'post').resolves();

        await sync.sendRestHeartbeat('http://src1:3006');

        let [, , opts] = postStub.firstCall.args;
        assert.ok(!opts.headers['Authorization'], 'no Authorization header when no upstream key');
    });

    // The split this pair exists to hold: SYNC_API_KEY guards this process's OWN api,
    // so leaking it upstream is what forced every client onto the server's value.
    it('sendRestHeartbeat does NOT send the inbound SYNC_API_KEY upstream', async function(){
        ({ sync, db } = makeSync({ SYNC_SOURCES: 'http://src1:3006', SYNC_API_KEY: 'inbound-only' }));
        let postStub = sinon.stub(axios, 'post').resolves();

        await sync.sendRestHeartbeat('http://src1:3006');

        let [, , opts] = postStub.firstCall.args;
        assert.ok(!opts.headers['Authorization'],
            'the inbound guard key must never be presented to a source server');
    });

    it('upstreamHeaders carries the upstream key to snapshot reads, not just heartbeats', async function(){
        ({ sync, db } = makeSync({ SYNC_SOURCES: 'http://src1:3006', SYNC_UPSTREAM_KEY: 'upkey' }));
        assert.strictEqual(sync.upstreamHeaders()['Authorization'], 'Bearer upkey');

        ({ sync, db } = makeSync({ SYNC_SOURCES: 'http://src1:3006' }));
        assert.deepStrictEqual(sync.upstreamHeaders(), {},
            'no header at all when the source tier is keyless');
    });
});
