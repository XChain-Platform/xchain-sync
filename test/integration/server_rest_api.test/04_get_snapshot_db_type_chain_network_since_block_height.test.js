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
const { defineRestApiSuite } = require('./rest_api_harness');

function registerTests(state) {
    describe('GET /snapshot/:dbType/:chain/:network/since/:blockHeight', function() {
        let sourceDb, baseUrl;

        before(function() {
            sourceDb = state.sourceDb;
            baseUrl = state.baseUrl;
        });

        it('returns incremental snapshot', async function() {
            await fixtures.seedBlocks(sourceDb, 1, 10);
            let res = await axios.get(baseUrl + '/snapshot/indexer/bitcoin/mainnet/since/6', {
                responseType: 'arraybuffer',
                decompress: false
            });

            let json = zlib.gunzipSync(Buffer.from(res.data)).toString();
            let snapshot = JSON.parse(json);

            assert.strictEqual(snapshot.block_height, 10);
            assert.strictEqual(snapshot.since_block, 6);
            assert.ok(snapshot.tables);
            if (snapshot.tables.blocks) {
                assert.strictEqual(snapshot.tables.blocks.length, 5);
            }
        });

        it('returns 400 for invalid blockHeight', async function() {
            try {
                await axios.get(baseUrl + '/snapshot/indexer/bitcoin/mainnet/since/abc');
                assert.fail('Should have thrown');
            } catch (e) {
                assert.strictEqual(e.response.status, 400);
            }
        });
    });
}

defineRestApiSuite(registerTests);
