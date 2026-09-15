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
const fixtures = require('../helpers/fixtures');
const { defineRestApiSuite } = require('./rest_api_harness');

function registerTests(state) {
    describe('GET /status/:dbType/:chain/:network', function() {
        let sourceDb, baseUrl;

        before(function() {
            sourceDb = state.sourceDb;
            baseUrl = state.baseUrl;
        });

        it('returns status for specific chain', async function() {
            await fixtures.seedBlocks(sourceDb, 1, 5);
            let res = await axios.get(baseUrl + '/status/indexer/bitcoin/mainnet');
            assert.strictEqual(res.status, 200);
            assert.strictEqual(res.data.block_height, 5);
            assert.strictEqual(res.data.chain, 'bitcoin');
            assert.strictEqual(res.data.network, 'mainnet');
        });

        it('returns 404 for unknown chain', async function() {
            try {
                await axios.get(baseUrl + '/status/indexer/unknown/chain');
                assert.fail('Should have thrown');
            } catch (e) {
                assert.strictEqual(e.response.status, 404);
            }
        });
    });
}

defineRestApiSuite(registerTests);
