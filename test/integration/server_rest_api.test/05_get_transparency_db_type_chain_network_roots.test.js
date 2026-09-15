// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

const assert = require('assert');
const axios  = require('axios');
const { defineRestApiSuite } = require('./helpers/rest_api_harness');

function registerTests(state) {
    describe('GET /transparency/:dbType/:chain/:network/roots', function() {
        let baseUrl, log;

        before(function() {
            baseUrl = state.baseUrl;
            log = state.log;
        });

        it('returns paginated transparency log', async function() {
            for (let i = 1; i <= 20; i++) {
                await log.recordBlock(i, 1700000000 + i, 'lh' + i, 'ah' + i, 'ch' + i);
            }

            let res = await axios.get(baseUrl + '/transparency/indexer/bitcoin/mainnet/roots?page=0&limit=10');
            assert.strictEqual(res.status, 200);
            assert.strictEqual(res.data.total, 20);
            assert.strictEqual(res.data.results.length, 10);
            assert.strictEqual(res.data.page, 0);
            assert.strictEqual(res.data.limit, 10);
        });
    });
}

defineRestApiSuite(registerTests);
