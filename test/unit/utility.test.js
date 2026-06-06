// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available —
// contact legal@dankest.llc.

const assert = require('assert');
const sinon  = require('sinon');
const Utility = require('../../src/utility');

describe('Utility', function(){

    let util;

    beforeEach(function(){
        util = new Utility();
        sinon.stub(console, 'error');
    });

    afterEach(function(){
        sinon.restore();
    });

    describe('isNull', function(){
        it('returns true for null', function(){
            assert.strictEqual(util.isNull(null), true);
        });
        it('returns true for undefined', function(){
            assert.strictEqual(util.isNull(undefined), true);
        });
        it('returns true for empty string', function(){
            assert.strictEqual(util.isNull(''), true);
        });
        it('returns false for 0', function(){
            assert.strictEqual(util.isNull(0), false);
        });
        it('returns false for false', function(){
            assert.strictEqual(util.isNull(false), false);
        });
        it('returns false for whitespace string', function(){
            assert.strictEqual(util.isNull(' '), false);
        });
        it('returns false for a normal string', function(){
            assert.strictEqual(util.isNull('hello'), false);
        });
    });

    describe('jsonStringify', function(){
        it('serializes a plain object', function(){
            let result = util.jsonStringify({ a: 1, b: 'two' });
            assert.strictEqual(result, '{"a":1,"b":"two"}');
        });
        it('converts BigInt values to strings', function(){
            let result = util.jsonStringify({ amount: BigInt('99999999999999999') });
            assert.strictEqual(result, '{"amount":"99999999999999999"}');
        });
        it('handles nested BigInt values', function(){
            let result = util.jsonStringify({ outer: { inner: BigInt(42) } });
            let parsed = JSON.parse(result);
            assert.strictEqual(parsed.outer.inner, '42');
        });
        it('handles null values', function(){
            let result = util.jsonStringify({ a: null });
            assert.strictEqual(result, '{"a":null}');
        });
    });

    describe('getDataHash', function(){
        it('returns a 64-char hex string', function(){
            let hash = util.getDataHash({ foo: 'bar' });
            assert.strictEqual(hash.length, 64);
            assert.match(hash, /^[0-9a-f]{64}$/);
        });
        it('returns identical hash for identical input', function(){
            let a = util.getDataHash({ x: 1 });
            let b = util.getDataHash({ x: 1 });
            assert.strictEqual(a, b);
        });
        it('returns different hash for different input', function(){
            let a = util.getDataHash({ x: 1 });
            let b = util.getDataHash({ x: 2 });
            assert.notStrictEqual(a, b);
        });
        it('handles BigInt fields', function(){
            let hash = util.getDataHash({ amount: BigInt(100) });
            assert.strictEqual(hash.length, 64);
        });
    });

    describe('throwError', function(){
        it('throws an Error with the given message', function(){
            assert.throws(() => util.throwError('test error'), { message: 'test error' });
        });
        it('logs to console.error', function(){
            try { util.throwError('msg'); } catch(e){}
            assert.strictEqual(console.error.calledOnce, true);
        });
    });

    describe('logError', function(){
        it('logs to console.error', function(){
            util.logError('some error', { extra: 'info' });
            assert.strictEqual(console.error.calledOnce, true);
        });
    });

    describe('sleep', function(){
        it('resolves after the specified delay', async function(){
            let clock = sinon.useFakeTimers();
            let resolved = false;
            util.sleep(100).then(() => { resolved = true; });
            assert.strictEqual(resolved, false);
            await clock.tickAsync(100);
            assert.strictEqual(resolved, true);
            clock.restore();
        });
    });

    describe('startTimer / getTimer', function(){
        it('returns millisecond format for < 1 second', function(){
            let clock = sinon.useFakeTimers(1000);
            let timer = util.startTimer();
            clock.tick(500);
            assert.strictEqual(util.getTimer(timer), '500ms');
            clock.restore();
        });
        it('returns second format for < 60 seconds', function(){
            let clock = sinon.useFakeTimers(1000);
            let timer = util.startTimer();
            clock.tick(5000);
            assert.strictEqual(util.getTimer(timer), '5s');
            clock.restore();
        });
        it('returns minute format for >= 60 seconds', function(){
            let clock = sinon.useFakeTimers(1000);
            let timer = util.startTimer();
            clock.tick(125000); // 2m 5s
            assert.strictEqual(util.getTimer(timer), '2m 5s');
            clock.restore();
        });
    });
});
