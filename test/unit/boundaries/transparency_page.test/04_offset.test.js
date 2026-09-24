// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// Covers offset calculations. One part of transparency_page.test.js.
const assert = require('assert');
const TransparencyLog = require('../../../../src/server/transparency_log');
const { createMockDb, registerHooks } = require('./helpers/transparency_page_suite');

describe('Boundary: Transparency Log Pagination', function(){
    registerHooks();
    describe('offset calculation', function(){
        it('page=0, limit=10: offset=0', async function(){
            let log = new TransparencyLog(createMockDb(100));
            await log.getPage(0, 10);
            let selectCall = log.db.doQuery.getCalls().find(c => c.args[0].includes('LIMIT'));
            assert.strictEqual(selectCall.args[1][1], 0);
        });

        it('page=5, limit=20: offset=100', async function(){
            let log = new TransparencyLog(createMockDb(1000));
            await log.getPage(5, 20);
            let selectCall = log.db.doQuery.getCalls().find(c => c.args[0].includes('LIMIT'));
            assert.strictEqual(selectCall.args[1][1], 100);
        });
    });
});
