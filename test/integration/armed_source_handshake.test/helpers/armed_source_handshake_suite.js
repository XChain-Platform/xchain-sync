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
const http      = require('http');
const express   = require('express');
const WebSocket = require('ws');
const { createApiKeyMiddleware, safeEqual } = require('../../../../src/http/middleware');
const ClientSync   = require('../../../../src/client/sync');
const HashVerifier = require('../../../../src/client/hash_verifier');
const Utility      = require('../../../../src/util');

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

module.exports = { PORT, SERVER_KEY, makeClient, registerArmedSourceHooks };
