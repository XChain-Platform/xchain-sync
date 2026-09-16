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
    describe('GET /schema/:dbType/:chain/:network', function() {
        let baseUrl;

        before(function() {
            baseUrl = state.baseUrl;
        });

        it('returns table DDLs', async function() {
            let res = await axios.get(baseUrl + '/schema/indexer/bitcoin/mainnet');
            assert.strictEqual(res.status, 200);
            assert.ok(res.data.tables);
            assert.ok(res.data.tables.blocks);
            assert.ok(res.data.tables.blocks.includes('CREATE TABLE'));
            assert.ok(res.data.tables.transactions);
            assert.ok(Object.keys(res.data.tables).length >= 10);
        });
    });
}

defineRestApiSuite(registerTests);
