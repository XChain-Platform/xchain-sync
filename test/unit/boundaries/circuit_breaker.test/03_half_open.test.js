// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// Covers half-open recovery. One part of circuit_breaker.test.js.
const assert = require('assert');
const sinon  = require('sinon');
const { createDatabase, registerHooks } = require('./helpers/circuit_breaker_suite');

describe('Boundary: Circuit Breaker', function(){
    let db, pool;
    registerHooks();

    describe('half-open recovery', function(){
        it('transitions to half-open after cooldown expires', async function(){
            let conn = { release: sinon.stub() };
            pool = {
                getConnection: sinon.stub().resolves(conn),
                end: sinon.stub()
            };
            db = createDatabase(pool);

            // Simulate open circuit with expired cooldown
            db.circuitState = 'open';
            db.circuitFailures = 10;
            db.circuitOpenUntil = Date.now() - 1; // expired

            let result = await db.getConnection();
            assert.strictEqual(result, conn);
            assert.strictEqual(db.circuitState, 'closed');
            assert.strictEqual(db.circuitFailures, 0);
        });

        it('re-opens on failure during half-open', async function(){
            pool = {
                getConnection: sinon.stub().rejects(new Error('still down')),
                end: sinon.stub()
            };
            db = createDatabase(pool);
            sinon.stub(db.util, 'sleep').resolves();

            // Simulate half-open state
            db.circuitState = 'open';
            db.circuitFailures = 9;
            db.circuitOpenUntil = Date.now() - 1;

            await assert.rejects(() => db.getConnection());
            assert.strictEqual(db.circuitState, 'open');
        });
    });
});
