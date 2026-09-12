// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.
//
// Rehearsal for arming the sync SERVER tier.
//
// Until the credential split, only the heartbeat ever attached an Authorization
// header, so setting SYNC_API_KEY on a source would have 401'd every snapshot
// read and the WebSocket upgrade of every client replicating from it. That is a
// fleet-wide replication stop, and it is invisible to the unit tests because
// they stub axios. These cases drive the REAL guard over a REAL socket.
//
// The ordering the rollout depends on is asserted directly: a client WITHOUT the
// upstream key must fail against an armed source. That is the failure an operator
// would otherwise discover by causing it.
//
// No database: the handshake is decided before any query runs, so this file runs
// anywhere, unlike the rest of test/integration.

const assert    = require('assert');
const http      = require('http');
const express   = require('express');
const axios     = require('axios');
const WebSocket = require('ws');

const { createApiKeyMiddleware, safeEqual } = require('../../src/middleware');
const ClientSync   = require('../../src/ClientSync');
const HashVerifier = require('../../src/HashVerifier');
const Utility      = require('../../src/utility');

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

describe('Integration: armed source handshake (server-tier rollout rehearsal)', function(){

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

    it('REST: a client carrying the upstream key reads a snapshot from an armed source', async function(){
        const sync = makeClient({ SYNC_UPSTREAM_KEY: SERVER_KEY });
        const url  = 'http://127.0.0.1:' + PORT + '/snapshot/indexer/bitcoin/mainnet';

        const res = await axios.get(url, { headers: sync._upstreamHeaders(), timeout: 5000 });

        assert.strictEqual(res.status, 200);
        assert.strictEqual(res.data.ok, true);
    });

    it('REST: the same read WITHOUT the upstream key is refused, which is the rollout order', async function(){
        const sync = makeClient({});           // the state every client is in today
        const url  = 'http://127.0.0.1:' + PORT + '/snapshot/indexer/bitcoin/mainnet';

        let status = null;
        try {
            await axios.get(url, { headers: sync._upstreamHeaders(), timeout: 5000 });
        } catch(e){
            status = e.response ? e.response.status : null;
        }
        assert.strictEqual(status, 401,
            'arming a source before its clients hold SYNC_UPSTREAM_KEY stops replication');
    });

    it('REST: the inbound guard key is NOT accepted upstream, so the two are truly separate', async function(){
        const sync = makeClient({ SYNC_API_KEY: 'this-guards-my-own-api' });
        const url  = 'http://127.0.0.1:' + PORT + '/snapshot/indexer/bitcoin/mainnet';

        let status = null;
        try {
            await axios.get(url, { headers: sync._upstreamHeaders(), timeout: 5000 });
        } catch(e){
            status = e.response ? e.response.status : null;
        }
        assert.strictEqual(status, 401, 'SYNC_API_KEY must never be presented to a source');
    });

    it('WS: the upgrade succeeds only when the client carries the upstream key', function(done){
        const sync  = makeClient({ SYNC_UPSTREAM_KEY: SERVER_KEY });
        const wsUrl = 'ws://127.0.0.1:' + PORT + '/subscribe/indexer/bitcoin/mainnet';
        const ws    = new WebSocket(wsUrl, { headers: sync._upstreamHeaders() });

        ws.on('open',  () => { ws.close(); done(); });
        ws.on('error', (e) => done(new Error('armed source refused a keyed client: ' + e.message)));
    });

    it('WS: the upgrade is refused without it, so streaming sync stops too, not just bootstrap', function(done){
        const sync  = makeClient({});
        const wsUrl = 'ws://127.0.0.1:' + PORT + '/subscribe/indexer/bitcoin/mainnet';
        const ws    = new WebSocket(wsUrl, { headers: sync._upstreamHeaders() });

        ws.on('open',  () => { ws.close(); done(new Error('armed source accepted a keyless upgrade')); });
        ws.on('error', () => done());
    });
});
