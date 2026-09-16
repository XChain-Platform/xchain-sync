// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

const assert   = require('assert');
const axios    = require('axios');
const fixtures = require('./helpers/fixtures');
const { defineRestApiSuite } = require('./server_rest_api.test/helpers/rest_api_harness');

function registerTests(state) {
    describe('GET /status', function() {
        let sourceDb, baseUrl;

        before(function() {
            sourceDb = state.sourceDb;
            baseUrl = state.baseUrl;
        });

        it('returns status for all chains', async function() {
            await fixtures.seedBlocks(sourceDb, 1, 3);
            let res = await axios.get(baseUrl + '/status');
            assert.strictEqual(res.status, 200);
            assert.ok(res.data.bitcoin);
            assert.ok(res.data.bitcoin.mainnet);
            assert.strictEqual(res.data.bitcoin.mainnet.block_height, 3);
            assert.ok(res.data.bitcoin.mainnet.ledger_hash);
            assert.ok(res.data.last_updated);
        });

        it('returns null values when no blocks', async function() {
            let res = await axios.get(baseUrl + '/status');
            assert.strictEqual(res.data.bitcoin.mainnet.block_height, null);
        });
    });
}

defineRestApiSuite(registerTests);
