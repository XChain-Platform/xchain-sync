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
const sinon  = require('sinon');
const TransparencyLog = require('../../../src/TransparencyLog');

function createMockDb(total){
    return {
        doQuery: sinon.stub().callsFake(async (query, args) => {
            if(query.includes('COUNT'))
                return [{ total: total || 0 }];
            return [];
        })
    };
}

describe('Boundary: Transparency Log Pagination', function(){

    afterEach(function(){ sinon.restore(); });

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
