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
// Behind a co-located reverse proxy, express resolved every
// request to 127.0.0.1 because 'trust proxy' was never set, so the snapshot
// rate limiters keyed one shared global bucket and a single caller could drain
// every validator's snapshot budget. These tests drive real HTTP through the
// production limiter instances (createRateLimiters) and the production key
// function (snapshotKey), not a re-declaration of them.

// src/api.js is the process entry point and patches console as it loads. The unit
// tier's bootstrap opts out of that; the security tier has no bootstrap, so this file
// opts out itself, or every later suite that stubs console stops seeing logger output.
process.env.XCHAIN_LOG_PATCH = '0';
const {
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
} = require('./api_rate_limit_proxy_security.test/helpers/api_rate_limit_proxy_security_suite');

let harness = null;

function beforeHook() {
    // express-rate-limit logs a validation notice when it sees an
    // X-Forwarded-For header with 'trust proxy' off; that is the very
    // combination two of these tests exercise on purpose.
    sinon.stub(console, 'error');
    sinon.stub(console, 'warn');
}

async function afterHook() {
    if(harness) await harness.close();
    harness = null;
    sinon.restore();
}

describe('API rate-limit proxy trust security', function(){
    registerHooks(beforeHook, afterHook);

    // ── trustProxyHops: one hop, never true ──

    describe('trustProxyHops', function(){

        it('returns exactly one hop when TRUST_PROXY is on', function(){
            assert.strictEqual(trustProxyHops(true), 1);
        });

        it('never returns true, which express-rate-limit rejects as permissive', function(){
            assert.notStrictEqual(trustProxyHops(true), true);
        });

        it('disables forwarded-header trust when TRUST_PROXY is off', function(){
            assert.strictEqual(trustProxyHops(false), false);
            assert.strictEqual(trustProxyHops(undefined), false);
            assert.strictEqual(trustProxyHops(''), false);
        });
    });

});

describe('API rate-limit proxy trust security', function(){
    registerHooks(beforeHook, afterHook);

    // ── the setting reaches the app startApi actually serves ──

    describe('startApi wiring', function(){

        it('sets one trusted hop on the served app when TRUST_PROXY is on', async function(){
            let app = await bootRealApi('true');
            assert.ok(app, 'startApi did not build an express app');
            assert.strictEqual(app.get('trust proxy'), 1);
        });

        it('leaves the served app untrusting when TRUST_PROXY is unset', async function(){
            let app = await bootRealApi(undefined);
            assert.ok(app, 'startApi did not build an express app');
            assert.strictEqual(app.get('trust proxy'), false);
        });

        // End to end, against the app the service actually serves:
        // exhausting one validator's snapshot budget must not 429 the next.
        it('does not let one client drain the snapshot budget of another', async function(){
            let app = await bootRealApi('true', { SNAPSHOT_RATE_FULL: '2' });
            harness = await listenRealApi(app);

            assert.strictEqual((await get(harness, CLIENT_A)).status, 404);
            assert.strictEqual((await get(harness, CLIENT_A)).status, 404);
            assert.strictEqual((await get(harness, CLIENT_A)).status, 429, 'client A should have exhausted its own budget');
            assert.strictEqual((await get(harness, CLIENT_B)).status, 404, 'client B was 429d by a shared global bucket');
        });

        it('shares one bucket across forged headers when TRUST_PROXY is unset', async function(){
            let app = await bootRealApi(undefined, { SNAPSHOT_RATE_FULL: '2' });
            harness = await listenRealApi(app);

            assert.strictEqual((await get(harness, CLIENT_A)).status, 404);
            assert.strictEqual((await get(harness, CLIENT_A)).status, 404);
            assert.strictEqual((await get(harness, CLIENT_B)).status, 429, 'a directly-exposed process must not believe the header');
        });
    });

});
