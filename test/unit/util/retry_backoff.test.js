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
const sinon = require('sinon');
const { backoffDelay, errorText, OutageLog } = require('../../../src/util/retry_backoff');

function delays(base, count){
    return Array.from({ length: count }, (_, attempt) => backoffDelay(attempt, base));
}

function logLines(){
    return console.log.callCount + console.warn.callCount + console.error.callCount;
}

describe('retry_backoff: backoffDelay', function(){
    it('doubles from the base delay and stops at the 60s cap', function(){
        assert.deepStrictEqual(delays(1000, 9), [1000, 2000, 4000, 8000, 16000, 32000, 60000, 60000, 60000]);
        assert.deepStrictEqual(delays(5000, 6), [5000, 10000, 20000, 40000, 60000, 60000]);
    });

    it('never retries in a tight loop when the configured delay is tiny or zero', function(){
        assert.deepStrictEqual(delays(0, 4), [0, 2000, 4000, 8000]);
        assert.deepStrictEqual(delays(1, 3), [1, 2000, 4000]);
        assert.ok(delays(0, 40).slice(1).every(d => d >= 1000 && d <= 60000));
    });

    it('keeps a configured delay above the cap as a fixed delay', function(){
        assert.deepStrictEqual(delays(9999999, 3), [9999999, 9999999, 9999999]);
    });

    it('falls back to 1s for a missing base and stays finite for huge attempt counts', function(){
        assert.strictEqual(backoffDelay(0, undefined), 1000);
        assert.strictEqual(backoffDelay(1e6, 1000), 60000);
    });
});

describe('retry_backoff: errorText', function(){
    it('keeps the code, message and HTTP status and drops the request config', function(){
        const e = new Error('Request failed');
        e.code = 'ERR_BAD_RESPONSE';
        e.response = { status: 503 };
        e.config = { headers: { Authorization: 'Bearer never-printed' } };
        assert.strictEqual(errorText(e), 'ERR_BAD_RESPONSE Request failed HTTP 503');
        const refused = Object.assign(new Error('connect ECONNREFUSED 127.0.0.1:3006'), { code: 'ECONNREFUSED' });
        assert.strictEqual(errorText(refused), 'connect ECONNREFUSED 127.0.0.1:3006');
        assert.strictEqual(errorText(null), 'unknown error');
        assert.strictEqual(errorText('closed'), 'closed');
    });
});

describe('retry_backoff: OutageLog', function(){
    let clock;
    beforeEach(function(){
        sinon.stub(console, 'log'); sinon.stub(console, 'warn'); sinon.stub(console, 'error');
        clock = sinon.useFakeTimers();
    });
    afterEach(function(){ clock.restore(); sinon.restore(); });

    it('logs the start, one summary per 5 minutes and the end of an outage', function(){
        const outage = new OutageLog('Source X');
        for(let t = 0; t < 10 * 60 * 1000; t += 100){
            outage.failure('ECONNREFUSED', 1000);
            clock.tick(100);
        }
        assert.strictEqual(console.warn.callCount, 2, 'start plus one summary in 10 minutes');
        assert.match(console.warn.firstCall.args[0], /^Source X unavailable \(ECONNREFUSED\); next attempt in 1s/);
        assert.match(console.warn.secondCall.args[0], /still unavailable after 3001 failed attempt\(s\) over 5m/);
        assert.strictEqual(outage.recovered(), true);
        assert.match(console.log.lastCall.args[0], /Source X available again after 6000 failed attempt\(s\) over 10m/);
        assert.strictEqual(outage.active, false);
        assert.strictEqual(logLines(), 3);
    });

    it('is silent on recovery when there was no outage and honours its level', function(){
        const outage = new OutageLog('Status check', { level: 'error' });
        assert.strictEqual(outage.recovered(), false);
        outage.failure('timeout');
        assert.strictEqual(console.error.callCount, 1);
        assert.strictEqual(logLines(), 1);
    });
});
