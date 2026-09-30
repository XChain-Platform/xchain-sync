// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// Covers shared armed source setup. One part of armed_source_handshake.test.js.
const assert     = require('assert');
const http       = require('http');
const express    = require('express');
const WebSocket  = require('ws');
const sinon      = require('sinon');
const proxyquire = require('proxyquire');
const { createApiKeyMiddleware, safeEqual } = require('../../../../src/http/middleware');
const ClientSync      = require('../../../../src/client/sync');
const HashVerifier    = require('../../../../src/client/hash_verifier');
const Utility         = require('../../../../src/util');
const checkpointReads = require('../../../../src/db/state_checkpoints');

const PORT       = 19477;
const SERVER_KEY = 'armed-source-key';

// Enough of a ClientSync to exercise the outbound credential path. The replication
// collaborators are never reached: every assertion here is decided by the source's
// guard, at or before the response.
function makeClient(configOverrides){
    const noop   = () => {};
    const db     = { query: async () => [], getConnection: async () => ({ release: noop }) };
    const stub   = { apply: noop, rollback: noop };
    const config = Object.assign({
        SYNC_SOURCES:           'http://127.0.0.1:' + PORT,
        VERIFY_HASHES:          false,
        CLIENT_RECONNECT_DELAY: 5000,
        HASH_CONFIRM_TIMEOUT:   5000,
        SNAPSHOT_MAX_CONTENT:   16 * 1024 * 1024,
        WS_MAX_PAYLOAD:         16 * 1024 * 1024,
        MAX_ROLLBACK_DEPTH:     10,
        GAP_LOG_INTERVAL_MS:    30000
    }, configOverrides || {});
    return new ClientSync('bitcoin', 'mainnet', db, stub, stub, new HashVerifier(), config, new Utility());
}

function registerArmedSourceHooks(){
    let server, wss;

    // An ARMED source. The REST side mounts the SAME createApiKeyMiddleware api.js
    // mounts app-wide. The WS side is a transcription of api.js's upgrade guard,
    // because that handler is inline in startApi and not exported: it shares the
    // real safeEqual, but a change to the guard's SHAPE would not fail here. What
    // these cases are authoritative about is the CLIENT half, which is what the
    // credential split changed.
    before(function(done){
        const app = express();
        app.use(createApiKeyMiddleware(SERVER_KEY));
        app.get('/snapshot/:dbType/:chain/:network', (req, res) => res.json({ ok: true, rows: [] }));
        app.get('/health', (req, res) => res.json({ status: 'healthy' }));

        server = http.createServer(app);
        wss    = new WebSocket.Server({ noServer: true });

        server.on('upgrade', (request, socket, head) => {
            const authHeader = request.headers['authorization'];
            if(!authHeader || !safeEqual(authHeader, 'Bearer ' + SERVER_KEY)){
                socket.write('HTTP/1.1 401 Unauthorized\r\n\r\n');
                socket.destroy();
                return;
            }
            wss.handleUpgrade(request, socket, head, (ws) => wss.emit('connection', ws, request));
        });

        server.listen(PORT, '127.0.0.1', done);
    });

    after(function(done){
        if(wss) wss.close();
        server.close(done);
    });
}

// Boots the REAL startApi() with the network edges stubbed out. Hands back the
// express app it built, the raw `server.on('upgrade', ...)` handler (captured
// off the fake http.Server so it can be invoked directly, since it is declared
// inline in startApi and never exported), and the fake WebSocket.Server so a
// test can see whether handleUpgrade was ever reached. Same shape as the
// api_rate_limit_proxy_security suite's bootRealApi, plus the upgrade capture:
// without capturing the real handler, the WS guard above was only ever
// exercised by registerArmedSourceHooks's hand-transcribed copy, which shares
// safeEqual but not the handler's own shape.
async function bootRealApi(envOverrides){
    let prior = {};
    for(let key of Object.keys(envOverrides || {})){
        prior[key] = process.env[key];
        let val = envOverrides[key];
        if(val === undefined) delete process.env[key];
        else process.env[key] = val;
    }

    let capturedApp     = null;
    let upgradeHandler  = null;
    let fakeServer = {
        on: (event, handler) => { if(event === 'upgrade') upgradeHandler = handler; },
        listen: () => {}
    };
    let fakeWss = {
        on:            () => {},
        clients:       new Set(),
        handleUpgrade: sinon.spy((request, socket, head, cb) => {}),
        emit:          () => {}
    };

    class SyncServiceStub {
        start(){ return Promise.resolve(); }
        getBroadcaster(){ return { addSubscription: () => {} }; }
        getChains(){ return []; }
        getDatabase(){ return { fake: true }; }
        getClientSync(){ return null; }
    }

    // startApi arms long-lived intervals; a test process must not inherit them.
    // express-rate-limit's MemoryStore also calls setInterval and unrefs the
    // handle it gets back, so the stub returns an unref-able stand-in rather
    // than null.
    let intervals = sinon.stub(global, 'setInterval').returns({ unref(){}, ref(){} });

    try {
        let api = proxyquire('../../../../src/api', {
            'http': { createServer: (app) => { capturedApp = app; return fakeServer; } },
            'ws':   { Server: function(){ return fakeWss; } },
            './sync_service': SyncServiceStub
        });
        await api.startApi();
    } finally {
        intervals.restore();
        for(let key of Object.keys(prior)){
            if(prior[key] === undefined) delete process.env[key];
            else process.env[key] = prior[key];
        }
    }

    return { app: capturedApp, upgradeHandler, wss: fakeWss };
}

// Puts a real startApi()-built app on a loopback port for HTTP assertions.
async function listenRealApi(app){
    let server = await new Promise((resolve) => {
        let s = app.listen(0, '127.0.0.1', () => resolve(s));
    });
    return {
        port:  server.address().port,
        close: () => new Promise((resolve) => server.close(resolve))
    };
}

function makeUpgradeReq(headers){
    return { headers: headers || {}, url: '/subscribe/indexer/bitcoin/mainnet' };
}

// Enough of a raw net.Socket for the guard's rejection path: it only ever
// writes the 401 status line and destroys the connection.
function makeSocket(){
    return {
        written:   [],
        destroyed: false,
        write(chunk){ this.written.push(String(chunk)); },
        destroy(){ this.destroyed = true; }
    };
}

describe('Security: /halt/clear and the WebSocket upgrade guard, against the REAL api.js', function(){
    let harness = null;

    afterEach(async function(){
        if(harness) await harness.close();
        harness = null;
        sinon.restore();
    });

    describe('POST /halt/clear/:dbType/:chain/:network', function(){

        it('401s when no SYNC_API_KEY is configured at all, even carrying a header', async function(){
            let { app } = await bootRealApi({ SYNC_API_KEY: undefined, SYNC_MODE: 'client' });
            harness = await listenRealApi(app);

            let res = await fetch('http://127.0.0.1:' + harness.port + '/halt/clear/indexer/bitcoin/mainnet', {
                method:  'POST',
                headers: { authorization: 'Bearer whatever' }
            });
            assert.strictEqual(res.status, 401);
        });

        it('401s on the wrong key once a key IS configured', async function(){
            let { app } = await bootRealApi({ SYNC_API_KEY: 'test-halt-key', SYNC_MODE: 'client' });
            harness = await listenRealApi(app);

            let res = await fetch('http://127.0.0.1:' + harness.port + '/halt/clear/indexer/bitcoin/mainnet', {
                method:  'POST',
                headers: { authorization: 'Bearer wrong-key' }
            });
            assert.strictEqual(res.status, 401);
        });

        it('403s in server mode even carrying the correct key: halt-clear applies to client mode only', async function(){
            let { app } = await bootRealApi({ SYNC_API_KEY: 'test-halt-key', SYNC_MODE: 'server' });
            harness = await listenRealApi(app);

            let res = await fetch('http://127.0.0.1:' + harness.port + '/halt/clear/indexer/bitcoin/mainnet', {
                method:  'POST',
                headers: { authorization: 'Bearer test-halt-key' }
            });
            assert.strictEqual(res.status, 403);
        });
    });

    describe('WebSocket upgrade guard (server.on(\'upgrade\', ...))', function(){

        it('401s a keyless upgrade once a key IS configured', async function(){
            let { upgradeHandler } = await bootRealApi({ SYNC_API_KEY: 'test-ws-key' });
            let socket = makeSocket();

            upgradeHandler(makeUpgradeReq(), socket, Buffer.alloc(0));

            assert.strictEqual(socket.destroyed, true);
            assert.ok(socket.written.join('').includes('401'));
        });

        it('401s an upgrade carrying the wrong key', async function(){
            let { upgradeHandler } = await bootRealApi({ SYNC_API_KEY: 'test-ws-key' });
            let socket = makeSocket();

            upgradeHandler(makeUpgradeReq({ authorization: 'Bearer nope' }), socket, Buffer.alloc(0));

            assert.strictEqual(socket.destroyed, true);
            assert.ok(socket.written.join('').includes('401'));
        });

        it('does not reject a correctly-keyed upgrade: it reaches wss.handleUpgrade instead', async function(){
            let { upgradeHandler, wss } = await bootRealApi({ SYNC_API_KEY: 'test-ws-key' });
            let socket = makeSocket();

            upgradeHandler(makeUpgradeReq({ authorization: 'Bearer test-ws-key' }), socket, Buffer.alloc(0));

            assert.strictEqual(socket.written.length, 0, 'no 401 status line should be written on a valid key');
            assert.ok(wss.handleUpgrade.called);
        });
    });
});

// The signed-checkpoint reads api.js's /checkpoint/... routes call through to
// (src/db/state_checkpoints.js, a Database mixin). Exercised directly against
// the mixin's own SQL, not the HTTP layer above it, since what varies between
// the three reads is the query shape and its bound params, not how api.js
// turns a 404/500 around them.
describe('Unit: state_checkpoints reads (SQL shape, limit, empty result)', function(){

    function fakeDb(rows){
        return { doQuery: sinon.stub().resolves(rows) };
    }

    describe('getLatestCheckpoint', function(){

        it('takes the newest row overall: DESC by block_index then checkpoint_seq, capped at one', async function(){
            let db = fakeDb([{ block_index: 5 }]);
            await checkpointReads.getLatestCheckpoint.call(db);

            let [sql, params] = db.doQuery.firstCall.args;
            assert.ok(/FROM state_checkpoints\b/.test(sql));
            assert.ok(/ORDER BY block_index DESC, checkpoint_seq DESC/.test(sql));
            assert.ok(/LIMIT 1\s*$/.test(sql.trim()));
            assert.strictEqual(params, undefined);
        });

        it('returns an empty array when the table has no rows', async function(){
            let db = fakeDb([]);
            let rows = await checkpointReads.getLatestCheckpoint.call(db);
            assert.deepStrictEqual(rows, []);
        });
    });

    describe('findCheckpointsInRange', function(){

        it('bounds block_index to [from, to], keeps only the max checkpoint_seq per height, and passes the caller limit through', async function(){
            let db = fakeDb([]);
            await checkpointReads.findCheckpointsInRange.call(db, 10, 20, 500);

            let [sql, params] = db.doQuery.firstCall.args;
            assert.ok(/WHERE block_index >= \? AND block_index <= \?/.test(sql));
            assert.ok(/checkpoint_seq = \(SELECT MAX\(s2\.checkpoint_seq\) FROM state_checkpoints s2 WHERE s2\.block_index = sc\.block_index\)/.test(sql));
            assert.ok(/ORDER BY block_index ASC LIMIT \?/.test(sql));
            assert.deepStrictEqual(params, [10, 20, 500]);
        });

        it('returns an empty array when nothing falls in range', async function(){
            let db = fakeDb([]);
            let rows = await checkpointReads.findCheckpointsInRange.call(db, 1, 2, 10);
            assert.deepStrictEqual(rows, []);
        });
    });

    describe('getCheckpointAtHeight', function(){

        it('filters on the exact height and takes the newest checkpoint_seq there', async function(){
            let db = fakeDb([]);
            await checkpointReads.getCheckpointAtHeight.call(db, 42);

            let [sql, params] = db.doQuery.firstCall.args;
            assert.ok(/WHERE block_index=\?/.test(sql));
            assert.ok(/ORDER BY checkpoint_seq DESC LIMIT 1/.test(sql));
            assert.deepStrictEqual(params, [42]);
        });

        it('returns an empty array when no checkpoint exists at that height', async function(){
            let db = fakeDb([]);
            let rows = await checkpointReads.getCheckpointAtHeight.call(db, 999);
            assert.deepStrictEqual(rows, []);
        });
    });
});

module.exports = { PORT, SERVER_KEY, makeClient, registerArmedSourceHooks };
