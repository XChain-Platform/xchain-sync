// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// Covers lower page limits. One part of transparency_page.test.js.
const assert = require('assert');
const TransparencyLog = require('../../../../src/server/transparency_log');
const { createMockDb, registerHooks } = require('./helpers/transparency_page_suite');

describe('Boundary: Transparency Log Pagination', function(){
    registerHooks();
    describe('limit parameter', function(){
        it('limit=1: minimum valid', async function(){
            let log = new TransparencyLog(createMockDb(100));
            let result = await log.getPage(0, 1);
            assert.strictEqual(result.limit, 1);
        });

        it('limit=0: falls back to 100 (0 is falsy)', async function(){
            let log = new TransparencyLog(createMockDb(100));
            let result = await log.getPage(0, 0);
            assert.strictEqual(result.limit, 100);
        });

        it('limit=-1: clamped to 1', async function(){
            let log = new TransparencyLog(createMockDb(100));
            let result = await log.getPage(0, -1);
            assert.strictEqual(result.limit, 1);
        });

        it('limit=100: default value', async function(){
            let log = new TransparencyLog(createMockDb(100));
            let result = await log.getPage(0, 100);
            assert.strictEqual(result.limit, 100);
        });

        it('limit=999: just below max', async function(){
            let log = new TransparencyLog(createMockDb(100));
            let result = await log.getPage(0, 999);
            assert.strictEqual(result.limit, 999);
        });
    });
});
