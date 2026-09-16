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
const zlib     = require('zlib');
const fixtures = require('../helpers/fixtures');
const { defineRestApiSuite } = require('./helpers/rest_api_harness');

function registerTests(state) {
    describe('GET /snapshot/:dbType/:chain/:network', function() {
        let sourceDb, baseUrl;

        before(function() {
            sourceDb = state.sourceDb;
            baseUrl = state.baseUrl;
        });

        it('returns gzip-compressed full snapshot', async function() {
            await fixtures.seedBlocks(sourceDb, 1, 5);
            let res = await axios.get(baseUrl + '/snapshot/indexer/bitcoin/mainnet', {
                responseType: 'arraybuffer',
                decompress: false
            });
            assert.strictEqual(res.status, 200);

            let json = zlib.gunzipSync(Buffer.from(res.data)).toString();
            let snapshot = JSON.parse(json);

            assert.strictEqual(snapshot.block_height, 5);
            assert.ok(snapshot.tables);
            assert.ok(snapshot.tables.blocks);
            assert.strictEqual(snapshot.tables.blocks.length, 5);
            assert.ok(snapshot.tables.transactions);
            assert.ok(snapshot.tables.credits);
        });

        it('returns 404 when no blocks', async function() {
            let res = await axios.get(baseUrl + '/snapshot/indexer/bitcoin/mainnet', {
                validateStatus: () => true
            });
            assert.strictEqual(res.status, 404);
        });
    });
}

defineRestApiSuite(registerTests);
