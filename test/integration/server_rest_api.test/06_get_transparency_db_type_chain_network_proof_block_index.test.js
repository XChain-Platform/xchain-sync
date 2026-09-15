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
const { defineRestApiSuite } = require('./rest_api_harness');

function registerTests(state) {
    describe('GET /transparency/:dbType/:chain/:network/proof/:block_index', function() {
        let baseUrl, log;

        before(function() {
            baseUrl = state.baseUrl;
            log = state.log;
        });

        afterEach(function() {
            state.testSyncMode = 'server';
        });

        it('returns Merkle inclusion proof for a committed block', async function() {
            for (let i = 1; i <= 100; i++) {
                await log.recordBlock(i, 1700000000 + i, 'lh' + i, 'ah' + i, 'ch' + i);
            }

            let res = await axios.get(baseUrl + '/transparency/indexer/bitcoin/mainnet/proof/50');
            assert.strictEqual(res.status, 200);
            assert.strictEqual(res.data.blockIndex, 50);
            assert.strictEqual(res.data.epoch, 1);
            assert.ok(res.data.merkleRoot, 'should have merkleRoot');
            assert.ok(Array.isArray(res.data.proof), 'proof should be an array');
            assert.strictEqual(res.data.verified, true);
        });

        it('returns 403 in client mode', async function() {
            state.testSyncMode = 'client';
            try {
                await axios.get(baseUrl + '/transparency/indexer/bitcoin/mainnet/proof/50');
                assert.fail('Should have thrown');
            } catch (e) {
                assert.strictEqual(e.response.status, 403);
            }
        });

        it('returns 400 for decoder dbType', async function() {
            try {
                await axios.get(baseUrl + '/transparency/decoder/bitcoin/mainnet/proof/50');
                assert.fail('Should have thrown');
            } catch (e) {
                assert.strictEqual(e.response.status, 400);
            }
        });

        it('returns 404 for unknown chain/network', async function() {
            try {
                await axios.get(baseUrl + '/transparency/indexer/litecoin/mainnet/proof/50');
                assert.fail('Should have thrown');
            } catch (e) {
                assert.strictEqual(e.response.status, 404);
            }
        });
    });
}

defineRestApiSuite(registerTests);
