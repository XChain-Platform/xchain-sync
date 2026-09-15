// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// Covers backoff timing. One part of circuit_breaker.test.js.
const assert = require('assert');
const sinon  = require('sinon');
const { createDatabase, registerHooks } = require('./helpers/circuit_breaker_suite');

describe('Boundary: Circuit Breaker', function(){
    let db, pool;
    registerHooks();

    describe('backoff delay calculation', function(){
        it('uses correct exponential progression', async function(){
            let delays = [];
            pool = {
                getConnection: sinon.stub().rejects(new Error('fail')),
                end: sinon.stub()
            };
            db = createDatabase(pool);
            db.circuitThreshold = 100; // prevent circuit from tripping
            sinon.stub(db.util, 'sleep').callsFake(async (ms) => { delays.push(ms); });
            sinon.stub(Math, 'random').returns(0); // no jitter

            try { await db.getConnection(); } catch(e) {}

            // First 6 delays: 500, 1000, 2000, 4000, 8000, 15000 (capped)
            assert.strictEqual(delays[0], 500);
            assert.strictEqual(delays[1], 1000);
            assert.strictEqual(delays[2], 2000);
            assert.strictEqual(delays[3], 4000);
            assert.strictEqual(delays[4], 8000);
            assert.strictEqual(delays[5], 15000); // capped at maxDelay

            // All subsequent delays should also be 15000 (capped)
            for(let i = 6; i < delays.length; i++){
                assert.strictEqual(delays[i], 15000);
            }
        });

        it('adds up to 30% jitter', async function(){
            let delays = [];
            pool = {
                getConnection: sinon.stub().rejects(new Error('fail')),
                end: sinon.stub()
            };
            db = createDatabase(pool);
            db.circuitThreshold = 100;
            sinon.stub(db.util, 'sleep').callsFake(async (ms) => { delays.push(ms); });
            sinon.stub(Math, 'random').returns(1); // max jitter

            try { await db.getConnection(); } catch(e) {}

            // First delay: 500 + floor(1 * 500 * 0.3) = 500 + 150 = 650
            assert.strictEqual(delays[0], 650);
            // Second: 1000 + floor(1 * 1000 * 0.3) = 1000 + 300 = 1300
            assert.strictEqual(delays[1], 1300);
        });
    });
});
