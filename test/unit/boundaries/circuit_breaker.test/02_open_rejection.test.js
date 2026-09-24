// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// Covers open-circuit rejection. One part of circuit_breaker.test.js.
const assert = require('assert');
const sinon  = require('sinon');
const { createDatabase, registerHooks } = require('./helpers/circuit_breaker_suite');

describe('Boundary: Circuit Breaker', function(){
    let db, pool;
    registerHooks();

    describe('circuit open rejection', function(){
        it('rejects immediately during cooldown', async function(){
            pool = {
                getConnection: sinon.stub().rejects(new Error('fail')),
                end: sinon.stub()
            };
            db = createDatabase(pool);
            sinon.stub(db.util, 'sleep').resolves();

            // Open the circuit
            try { await db.getConnection(); } catch(e) {}
            assert.strictEqual(db.circuitState, 'open');

            // Set cooldown to future
            db.circuitOpenUntil = Date.now() + 30000;

            await assert.rejects(
                () => db.getConnection(),
                (err) => {
                    let msg = (err && err.message) ? err.message : String(err);
                    return msg.includes('Circuit breaker open');
                }
            );
        });
    });
});
