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
const { createDatabase, registerHooks } = require('./circuit_breaker.test/helpers/circuit_breaker_suite');

describe('Boundary: Circuit Breaker', function(){
    let db, pool;
    registerHooks();

    describe('failure threshold (10)', function(){
        it('9 failures: circuit stays closed', async function(){
            let conn = { release: sinon.stub() };
            let callCount = 0;
            pool = {
                getConnection: sinon.stub().callsFake(async () => {
                    callCount++;
                    if(callCount <= 9) throw new Error('fail');
                    return conn;
                }),
                end: sinon.stub()
            };
            db = createDatabase(pool);
            // Override sleep to be instant
            sinon.stub(db.util, 'sleep').resolves();

            let result = await db.getConnection();
            assert.strictEqual(result, conn);
            assert.strictEqual(db.circuitState, 'closed');
            assert.strictEqual(db.circuitFailures, 0);
        });

        it('10 failures: circuit opens and throws', async function(){
            pool = {
                getConnection: sinon.stub().rejects(new Error('fail')),
                end: sinon.stub()
            };
            db = createDatabase(pool);
            sinon.stub(db.util, 'sleep').resolves();

            await assert.rejects(
                () => db.getConnection(),
                (err) => {
                    let msg = (err && err.message) ? err.message : String(err);
                    return msg.includes('Circuit breaker opened');
                }
            );
            assert.strictEqual(db.circuitState, 'open');
            assert.strictEqual(db.circuitFailures, 10);
        });
    });
});
