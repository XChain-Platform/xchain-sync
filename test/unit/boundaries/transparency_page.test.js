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
const TransparencyLog = require('../../../src/server/transparency_log');
const { createMockDb, registerHooks } = require('./transparency_page.test/helpers/transparency_page_suite');

describe('Boundary: Transparency Log Pagination', function(){
    registerHooks();
    describe('page parameter', function(){
        it('page=0: offset is 0', async function(){
            let log = new TransparencyLog(createMockDb(100));
            let result = await log.getPage(0, 10);
            assert.strictEqual(result.page, 0);
            let selectCall = log.db.doQuery.getCalls().find(c => c.args[0].includes('LIMIT'));
            assert.strictEqual(selectCall.args[1][1], 0); // offset
        });

        it('page=-1: clamped to 0', async function(){
            let log = new TransparencyLog(createMockDb(100));
            let result = await log.getPage(-1, 10);
            assert.strictEqual(result.page, 0);
        });

        it('page=-100: clamped to 0', async function(){
            let log = new TransparencyLog(createMockDb(100));
            let result = await log.getPage(-100, 10);
            assert.strictEqual(result.page, 0);
        });

        it('page=1: offset equals limit', async function(){
            let log = new TransparencyLog(createMockDb(100));
            let result = await log.getPage(1, 25);
            assert.strictEqual(result.page, 1);
            let selectCall = log.db.doQuery.getCalls().find(c => c.args[0].includes('LIMIT'));
            assert.strictEqual(selectCall.args[1][1], 25); // offset = 1 * 25
        });

        it('page="abc": defaults to 0', async function(){
            let log = new TransparencyLog(createMockDb(100));
            let result = await log.getPage('abc', 10);
            assert.strictEqual(result.page, 0);
        });

        it('page=undefined: defaults to 0', async function(){
            let log = new TransparencyLog(createMockDb(100));
            let result = await log.getPage(undefined, 10);
            assert.strictEqual(result.page, 0);
        });

        it('page=1.5: truncated to 1 by parseInt', async function(){
            let log = new TransparencyLog(createMockDb(100));
            let result = await log.getPage(1.5, 10);
            assert.strictEqual(result.page, 1);
        });

        it('page=999999: no upper clamp, valid high page', async function(){
            let log = new TransparencyLog(createMockDb(100));
            let result = await log.getPage(999999, 10);
            assert.strictEqual(result.page, 999999);
        });
    });
});
