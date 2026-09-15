// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// Covers upper page limits. One part of transparency_page.test.js.
const assert = require('assert');
const TransparencyLog = require('../../../../src/server/transparency_log');
const { createMockDb, registerHooks } = require('./helpers/transparency_page_suite');

describe('Boundary: Transparency Log Pagination', function(){
    registerHooks();
    describe('limit parameter', function(){
        it('limit=1000: at max', async function(){
            let log = new TransparencyLog(createMockDb(100));
            let result = await log.getPage(0, 1000);
            assert.strictEqual(result.limit, 1000);
        });

        it('limit=1001: clamped to 1000', async function(){
            let log = new TransparencyLog(createMockDb(100));
            let result = await log.getPage(0, 1001);
            assert.strictEqual(result.limit, 1000);
        });

        it('limit=999999: clamped to 1000', async function(){
            let log = new TransparencyLog(createMockDb(100));
            let result = await log.getPage(0, 999999);
            assert.strictEqual(result.limit, 1000);
        });

        it('limit="abc": defaults to 100', async function(){
            let log = new TransparencyLog(createMockDb(100));
            let result = await log.getPage(0, 'abc');
            assert.strictEqual(result.limit, 100);
        });

        it('limit=undefined: defaults to 100', async function(){
            let log = new TransparencyLog(createMockDb(100));
            let result = await log.getPage(0);
            assert.strictEqual(result.limit, 100);
        });
    });
});
