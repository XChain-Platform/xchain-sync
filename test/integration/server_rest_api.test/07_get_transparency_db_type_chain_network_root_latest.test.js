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

function bindState(state, target) {
    target.baseUrl = state.baseUrl;
    target.log = state.log;
}

function registerCommittedRootTests(state) {
    describe('GET /transparency/:dbType/:chain/:network/root/latest', function() {
        const values = {};

        before(function() {
            bindState(state, values);
        });

        afterEach(function() {
            state.testSyncMode = 'server';
        });

        it('returns latest Merkle root after an epoch is committed', async function() {
            for (let i = 1; i <= 100; i++) {
                await values.log.recordBlock(i, 1700000000 + i, 'lh' + i, 'ah' + i, 'ch' + i);
            }

            let res = await axios.get(values.baseUrl + '/transparency/indexer/bitcoin/mainnet/root/latest');
            assert.strictEqual(res.status, 200);
            assert.strictEqual(Number(res.data.epoch), 1);
            assert.ok(res.data.merkle_root, 'should have merkle_root');
            assert.strictEqual(Number(res.data.start_block), 1);
            assert.strictEqual(Number(res.data.end_block), 100);
        });

        it('returns null epoch and merkle_root when log is empty', async function() {
            let res = await axios.get(values.baseUrl + '/transparency/indexer/bitcoin/mainnet/root/latest');
            assert.strictEqual(res.status, 200);
            assert.strictEqual(res.data.epoch, null);
            assert.strictEqual(res.data.merkle_root, null);
        });
    });
}

function registerLatestRootErrors(state) {
    describe('GET /transparency/:dbType/:chain/:network/root/latest', function() {
        let baseUrl;

        before(function() {
            baseUrl = state.baseUrl;
        });

        afterEach(function() {
            state.testSyncMode = 'server';
        });

        it('returns 403 in client mode', async function() {
            state.testSyncMode = 'client';
            try {
                await axios.get(baseUrl + '/transparency/indexer/bitcoin/mainnet/root/latest');
                assert.fail('Should have thrown');
            } catch (e) {
                assert.strictEqual(e.response.status, 403);
            }
        });

        it('returns 400 for decoder dbType', async function() {
            try {
                await axios.get(baseUrl + '/transparency/decoder/bitcoin/mainnet/root/latest');
                assert.fail('Should have thrown');
            } catch (e) {
                assert.strictEqual(e.response.status, 400);
            }
        });

        it('returns 404 for unknown chain/network', async function() {
            try {
                await axios.get(baseUrl + '/transparency/indexer/litecoin/mainnet/root/latest');
                assert.fail('Should have thrown');
            } catch (e) {
                assert.strictEqual(e.response.status, 404);
            }
        });
    });
}

defineRestApiSuite((state) => {
    registerCommittedRootTests(state);
    registerLatestRootErrors(state);
});
