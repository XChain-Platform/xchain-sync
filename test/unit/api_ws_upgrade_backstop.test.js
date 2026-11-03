// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// WebSocket upgrades arrive as 'upgrade' events outside the express middleware
// chain. They must still spend the same per-IP backstop budget REST uses:
// wrong keys, malformed paths and valid subscribes all count, and an IP
// already refused on REST is refused on upgrade too.

const assert       = require('assert');
const http         = require('http');
const EventEmitter = require('events');
const { createApp, createUpgradeHandler } = require('../../src/api');

const KEY   = 'ws-backstop-key';
const LIMIT = 5;
const PATH  = '/subscribe/indexer/dogecoin/regtest';

function provider() {
    return {
        isReady:                () => true,
        getHubConfigAgeSeconds: () => 0,
        getChains:              () => [],
        getDatabase:            () => ({}),
        getBroadcaster:         () => ({ addSubscription() {} }),
        getSnapshotBuilder:     () => null,
        getPoller:              () => null,
        getTransparencyLog:     () => null,
        getClientSync:          () => null
    };
}

const cfg = () => ({
    SYNC_MODE: 'server',
    SYNC_API_KEY: KEY,
    TRUST_PROXY: false,
    SNAPSHOT_RATE_FULL: 100,
    SNAPSHOT_RATE_INCR: 100,
    TRANSPARENCY_RATE_LIMIT: 100
});

function fakeSocket(remoteAddress) {
    let s = new EventEmitter();
    s.remoteAddress = remoteAddress;
    s.destroyed = false;
    s.written = '';
    s.write = (data) => { s.written += String(data); return true; };
    s.destroy = () => { s.destroyed = true; };
    return s;
}

function upgradeRequest(remoteAddress, { url = PATH, authorization } = {}) {
    let socket = fakeSocket(remoteAddress);
    let req = new http.IncomingMessage(socket);
    req.method = 'GET';
    req.url = url;
    req.headers = { connection: 'Upgrade', upgrade: 'websocket' };
    if (authorization) req.headers.authorization = authorization;
    return { req, socket };
}

function statusOf(socket) {
    let m = /^HTTP\/1\.1 (\d{3})/.exec(socket.written);
    return m ? Number(m[1]) : null;
}

// Fresh app, server and upgrade handler per case so each backstop bucket
// starts empty. The fake upgrade sockets claim 127.0.0.1 so they share a
// bucket with the real REST probes sent over loopback.
async function withHarness(fn) {
    let app = createApp(provider(), cfg());
    let server = http.createServer(app);
    let admitted = [];
    let wss = {
        handleUpgrade(req, socket) { admitted.push(socket); },
        emit() {}
    };
    let handle = createUpgradeHandler(provider(), wss, app);
    await new Promise((r) => server.listen(0, '127.0.0.1', r));
    try {
        let base = 'http://127.0.0.1:' + server.address().port;
        let upgrade = async (ip, opts) => {
            let { req, socket } = upgradeRequest(ip, opts);
            await handle(req, socket, Buffer.alloc(0));
            return { socket, admitted: admitted.includes(socket) };
        };
        await fn({ base, upgrade });
    } finally {
        server.closeAllConnections();
        await new Promise((r) => server.close(r));
    }
}

const good = { authorization: 'Bearer ' + KEY };
const bad  = (i) => ({ authorization: 'Bearer wrong' + i });

// Pins the backstop to LIMIT requests per minute for the enclosing describe.
function useSmallBackstop() {
    let prev;
    before(function () {
        prev = process.env.SYNC_RATE_LIMIT_RPM;
        process.env.SYNC_RATE_LIMIT_RPM = String(LIMIT);
    });
    after(function () {
        if (prev === undefined) delete process.env.SYNC_RATE_LIMIT_RPM;
        else process.env.SYNC_RATE_LIMIT_RPM = prev;
    });
}

describe('WebSocket upgrades spend the app-wide backstop limiter', function () {
    useSmallBackstop();

    it('answers 429, not 401, once wrong-key upgrades drain the budget', async function () {
        await withHarness(async ({ upgrade }) => {
            for (let i = 0; i < LIMIT; i++) {
                let r = await upgrade('127.0.0.1', bad(i));
                assert.strictEqual(statusOf(r.socket), 401, 'upgrade ' + i);
                assert.strictEqual(r.socket.destroyed, true);
            }
            let r = await upgrade('127.0.0.1', bad(LIMIT));
            assert.strictEqual(statusOf(r.socket), 429);
            assert.match(r.socket.written, /RATE_LIMITED/);
            assert.strictEqual(r.socket.destroyed, true);
            assert.strictEqual(r.admitted, false);
        });
    });

    it('shares one bucket with REST: drained by upgrades, REST and a valid upgrade are refused', async function () {
        await withHarness(async ({ base, upgrade }) => {
            for (let i = 0; i < LIMIT; i++) await upgrade('127.0.0.1', bad(i));
            let rest = await fetch(base + '/health', { headers: good });
            assert.strictEqual(rest.status, 429);
            await rest.arrayBuffer();
            let r = await upgrade('127.0.0.1', good);
            assert.strictEqual(statusOf(r.socket), 429);
            assert.strictEqual(r.admitted, false);
        });
    });

    it('refuses a valid upgrade from an IP already answered 429 on REST', async function () {
        await withHarness(async ({ base, upgrade }) => {
            for (let i = 0; i <= LIMIT; i++) {
                let res = await fetch(base + '/health', { headers: { authorization: 'Bearer wrong' + i } });
                assert.strictEqual(res.status, i < LIMIT ? 401 : 429, 'REST ' + i);
                await res.arrayBuffer();
            }
            let r = await upgrade('127.0.0.1', good);
            assert.strictEqual(statusOf(r.socket), 429);
            assert.strictEqual(r.admitted, false);
        });
    });

});

describe('WebSocket upgrade backstop covers every upgrade shape', function () {
    useSmallBackstop();

    it('counts malformed upgrade paths against the budget', async function () {
        await withHarness(async ({ upgrade }) => {
            for (let i = 0; i < LIMIT; i++) {
                let r = await upgrade('127.0.0.1', Object.assign({ url: '/nope/' + i }, good));
                assert.strictEqual(r.socket.destroyed, true);
                assert.strictEqual(r.admitted, false);
            }
            let r = await upgrade('127.0.0.1', good);
            assert.strictEqual(statusOf(r.socket), 429);
            assert.strictEqual(r.admitted, false);
        });
    });

    it('admits valid upgrades within budget and limits valid subscribe churn', async function () {
        await withHarness(async ({ upgrade }) => {
            for (let i = 0; i < LIMIT; i++) {
                let r = await upgrade('127.0.0.1', good);
                assert.strictEqual(r.admitted, true, 'upgrade ' + i);
                assert.strictEqual(r.socket.written, '');
                assert.strictEqual(r.socket.destroyed, false);
            }
            let r = await upgrade('127.0.0.1', good);
            assert.strictEqual(statusOf(r.socket), 429);
            assert.strictEqual(r.admitted, false);
        });
    });

    it('leaves a different IP unaffected', async function () {
        await withHarness(async ({ upgrade }) => {
            for (let i = 0; i <= LIMIT; i++) await upgrade('127.0.0.1', bad(i));
            let r = await upgrade('10.0.0.9', good);
            assert.strictEqual(r.admitted, true);
            assert.strictEqual(r.socket.written, '');
        });
    });
});
