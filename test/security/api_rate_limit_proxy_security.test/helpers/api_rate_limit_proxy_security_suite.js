// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// Shared HTTP harness and hook registration. One part of api_rate_limit_proxy_security.test.js.
const assert     = require('assert');
const sinon      = require('sinon');
const express    = require('express');
const proxyquire = require('proxyquire');
const { trustProxyHops, snapshotKey, createRateLimiters } = require('../../../../src/api');

// A stand-in for the co-located Apache: the socket peer is loopback and the
// real client address arrives appended to X-Forwarded-For.
const PROXY_HOST = '127.0.0.1';

const CLIENT_A = '198.51.100.7';
const CLIENT_B = '198.51.100.8';
const SPOOFED  = '203.0.113.66';

// Boots a throwaway server carrying the same proxy-trust seam and the same
// snapshot limiter the service mounts. The echo route reports both the IP
// express resolved and the exact bucket key the limiter charged.
async function startHarness(options){
    let cfg = Object.assign({
        SNAPSHOT_RATE_FULL:     100,
        SNAPSHOT_RATE_INCR:     100,
        TRANSPARENCY_RATE_LIMIT: 100
    }, options.cfg || {});

    let app = express();
    app.set('trust proxy', trustProxyHops(options.trustProxy));

    let limiters = createRateLimiters(cfg);
    app.use(limiters.backstopLimiter);
    app.get('/snapshot/:dbType/:chain/:network', limiters.fullSnapshotLimiter, (req, res) => {
        res.json({ ip: req.ip, key: snapshotKey(req) });
    });

    let server = await new Promise((resolve) => {
        let s = app.listen(0, PROXY_HOST, () => resolve(s));
    });

    return {
        port:  server.address().port,
        close: () => new Promise((resolve) => server.close(resolve))
    };
}

// One request through the harness. forwardedFor is the X-Forwarded-For value as
// it would look leaving Apache (client-supplied entries first, real client last).
async function get(harness, forwardedFor, path){
    let headers = {};
    if(forwardedFor) headers['x-forwarded-for'] = forwardedFor;
    let res  = await fetch('http://' + PROXY_HOST + ':' + harness.port + (path || '/snapshot/indexer/BTC/mainnet'), { headers });
    let body = null;
    try { body = await res.json(); } catch(e){ body = null; }
    return { status: res.status, body };
}

// Boots the REAL startApi() with the network edges stubbed out, and hands back
// the express app it built. Nothing else reaches the production wiring: the
// harness above builds its own app, so without this a deleted app.set() in
// startApi would go unnoticed.
async function bootRealApi(trustProxyEnv, extraEnv){
    let env   = Object.assign({ TRUST_PROXY: trustProxyEnv }, extraEnv || {});
    let prior = {};
    for(let key of Object.keys(env)){
        prior[key] = process.env[key];
        if(env[key] === undefined) delete process.env[key];
        else process.env[key] = env[key];
    }

    let capturedApp = null;
    let fakeServer  = { on: () => {}, listen: () => {} };
    let fakeWss     = { on: () => {}, clients: new Set(), handleUpgrade: () => {} };

    class SyncServiceStub {
        start(){ return Promise.resolve(); }
        getBroadcaster(){ return null; }
        getChains(){ return []; }
        getDatabase(){ return null; }
    }

    // startApi arms three long-lived intervals; a test process must not inherit them.
    let intervals = sinon.stub(global, 'setInterval').returns(null);

    try {
        let api = proxyquire('../../../../src/api', {
            'http': { createServer: (app) => { capturedApp = app; return fakeServer; } },
            'ws':   { Server: function(){ return fakeWss; } },
            './SyncService': SyncServiceStub
        });
        await api.startApi();
    } finally {
        intervals.restore();
        for(let key of Object.keys(prior)){
            if(prior[key] === undefined) delete process.env[key];
            else process.env[key] = prior[key];
        }
    }

    return capturedApp;
}

// Puts a real startApi()-built app on a loopback port. Its snapshot route 404s
// (the stubbed SyncService owns no databases), which is fine: the limiter has
// already charged the bucket by then, so 404 means "served" and 429 means
// "rate limited" just as they would against a real source.
async function listenRealApi(app){
    let server = await new Promise((resolve) => {
        let s = app.listen(0, PROXY_HOST, () => resolve(s));
    });
    return {
        port:  server.address().port,
        close: () => new Promise((resolve) => server.close(resolve))
    };
}

function registerHooks(beforeHook, afterHook) {
    beforeEach(beforeHook);
    afterEach(afterHook);
}

module.exports = {
    assert,
    bootRealApi,
    CLIENT_A,
    CLIENT_B,
    get,
    listenRealApi,
    PROXY_HOST,
    registerHooks,
    sinon,
    SPOOFED,
    startHarness,
    trustProxyHops
};
