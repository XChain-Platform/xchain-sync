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
const Utility = require('../../../src/utility');

// The mariadb import is at module level, so proxyquire injects a mock pool to test the circuit breaker logic without a real connection.
const proxyquire = require('proxyquire');

function createDatabase(poolStub){
    let mockMariadb = {
        createPool: sinon.stub().returns(poolStub)
    };
    let Database = proxyquire('../../../src/db', { 'mariadb': mockMariadb });
    let util = new Utility();
    return new Database('localhost', 3306, 'testdb', 'user', 'pass', util);
}

describe('Boundary: Circuit Breaker', function(){

    let db, pool;

    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });

    afterEach(function(){ sinon.restore(); });

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

    describe('circuit open rejection', function(){
        it('rejects immediately during cooldown', async function(){
            pool = {
                getConnection: sinon.stub().rejects(new Error('fail')),
                end: sinon.stub()
            };
            db = createDatabase(pool);
            sinon.stub(db.util, 'sleep').resolves();

            try { await db.getConnection(); } catch(e) {}
            assert.strictEqual(db.circuitState, 'open');

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

    describe('half-open recovery', function(){
        it('transitions to half-open after cooldown expires', async function(){
            let conn = { release: sinon.stub() };
            pool = {
                getConnection: sinon.stub().resolves(conn),
                end: sinon.stub()
            };
            db = createDatabase(pool);

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

            db.circuitState = 'open';
            db.circuitFailures = 9;
            db.circuitOpenUntil = Date.now() - 1;

            await assert.rejects(() => db.getConnection());
            assert.strictEqual(db.circuitState, 'open');
        });
    });

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

    describe('transaction connection bypass', function(){
        it('returns transactionConnection when set (bypasses pool)', async function(){
            let txConn = { release: sinon.stub() };
            pool = {
                getConnection: sinon.stub().rejects(new Error('should not be called')),
                end: sinon.stub()
            };
            db = createDatabase(pool);
            db.transactionConnection = txConn;

            let result = await db.getConnection();
            assert.strictEqual(result, txConn);
            assert.strictEqual(pool.getConnection.called, false);
        });
    });
});
