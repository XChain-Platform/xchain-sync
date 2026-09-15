// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

const assert  = require('assert');
const axios   = require('axios');
const express = require('express');
const { trustProxyHops, createRateLimiters } = require('../../../src/api');
const { defineRestApiSuite } = require('./rest_api_harness');

function registerTests() {
    // Regression coverage for proxy-trust/rate-limit wiring, driven through THIS
    // suite's own app-building path rather than only the dedicated security suite:
    // boots a second, disposable server on the exact trustProxyHops/createRateLimiters
    // imported at the top of this file, so a regression in either seam fails here
    // too, not only in apiRateLimitProxy.security.test.js. Before this harness
    // derived its wiring from src/api.js it built its own app with no trust-proxy
    // setting and no limiter at all, so this class of bug could not have failed
    // any test in this file.
    describe('Proxy-trust rate-limit wiring', function() {
        let proxyServer;

        afterEach(async function() {
            if (proxyServer) await new Promise(resolve => proxyServer.close(resolve));
            proxyServer = null;
        });

        async function bootProxyHarness(trustProxy) {
            let proxyApp = express();
            proxyApp.set('trust proxy', trustProxyHops(trustProxy));
            let proxyLimiters = createRateLimiters({ SNAPSHOT_RATE_FULL: 2, SNAPSHOT_RATE_INCR: 100, TRANSPARENCY_RATE_LIMIT: 100000 });
            proxyApp.get('/snapshot/:dbType/:chain/:network', proxyLimiters.fullSnapshotLimiter, (req, res) => {
                res.json({ ip: req.ip });
            });
            proxyServer = await new Promise(resolve => {
                let s = proxyApp.listen(0, '127.0.0.1', () => resolve(s));
            });
            return proxyServer.address().port;
        }

        async function hit(port, forwardedFor) {
            let headers = forwardedFor ? { 'x-forwarded-for': forwardedFor } : {};
            return axios.get('http://127.0.0.1:' + port + '/snapshot/indexer/bitcoin/mainnet', {
                headers, validateStatus: () => true
            });
        }

        it('gives independent snapshot buckets to distinct forwarded clients when TRUST_PROXY is on', async function() {
            let port = await bootProxyHarness(true);
            assert.strictEqual((await hit(port, '198.51.100.10')).status, 200);
            assert.strictEqual((await hit(port, '198.51.100.10')).status, 200);
            assert.strictEqual((await hit(port, '198.51.100.10')).status, 429, 'client should have exhausted its own budget');
            assert.strictEqual((await hit(port, '198.51.100.11')).status, 200, 'a second forwarded client must not share the first client bucket');
        });

        it('collapses every caller onto the socket address when TRUST_PROXY is off', async function() {
            let port = await bootProxyHarness(false);
            assert.strictEqual((await hit(port, '198.51.100.20')).status, 200);
            assert.strictEqual((await hit(port, '198.51.100.20')).status, 200);
            let res = await hit(port, '198.51.100.21');
            assert.strictEqual(res.status, 429, 'an unset trust proxy must ignore the forwarded header and share one bucket');
        });
    });
}

defineRestApiSuite(registerTests);
