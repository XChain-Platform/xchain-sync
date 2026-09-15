// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// Covers retry exhaustion. One part of circuit_breaker.test.js.
const assert = require('assert');
const sinon  = require('sinon');
const { createDatabase, registerHooks } = require('./helpers/circuit_breaker_suite');

describe('Boundary: Circuit Breaker', function(){
    let db, pool;
    registerHooks();

    describe('max retry attempts (30)', function(){
        it('throws after 30 attempts without reaching circuit threshold', async function(){
            let callCount = 0;
            pool = {
                getConnection: sinon.stub().callsFake(async () => {
                    callCount++;
                    throw new Error('fail');
                }),
                end: sinon.stub()
            };
            db = createDatabase(pool);
            sinon.stub(db.util, 'sleep').resolves();
            // Set high threshold so circuit doesn't trip first
            db.circuitThreshold = 100;

            await assert.rejects(
                () => db.getConnection(),
                (err) => {
                    let msg = (err && err.message) ? err.message : String(err);
                    return msg.includes('30 attempts');
                }
            );
            assert.strictEqual(callCount, 30);
        });
    });
});
