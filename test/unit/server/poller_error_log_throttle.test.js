// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// A server poller whose source database stays down keeps polling at its
// interval, keeps /health's consecutive-error count exact, and logs a bounded
// number of lines instead of one stack per poll.

const assert = require('assert');
const sinon = require('sinon');
const ServerPoller = require('../../../src/server/poller');
const Utility = require('../../../src/util');

const TEN_MINUTES = 10 * 60 * 1000;

function makePoller(){
    const poller = Object.create(ServerPoller.prototype);
    Object.assign(poller, {
        chain: 'bitcoin', network: 'mainnet', dbType: 'indexer',
        config: { BLOCK_POLL_INTERVAL: 3000 }, util: new Utility(),
        pollErrorCount: 0, pollErrorLoggedAt: 0, running: true
    });
    poller.updateStatus = sinon.stub().resolves();
    return poller;
}

describe('ServerPoller error log throttle @regression', function(){
    let clock;
    beforeEach(function(){
        sinon.stub(console, 'log'); sinon.stub(console, 'warn'); sinon.stub(console, 'error');
        clock = sinon.useFakeTimers();
    });
    afterEach(function(){ clock.restore(); sinon.restore(); });

    it('logs a source outage in a few lines while polling at its interval', async function(){
        const poller = makePoller();
        const down = Object.assign(new Error('connect ECONNREFUSED'), { code: 'ECONNREFUSED' });
        let up = false;
        poller.poll = sinon.stub().callsFake(() => up ? Promise.resolve(0) : Promise.reject(down));
        const loop = poller.pollLoop();

        await clock.tickAsync(TEN_MINUTES);
        assert.ok(poller.poll.callCount >= 195, 'still polls every 3s: ' + poller.poll.callCount);
        assert.strictEqual(poller.pollErrorCount, poller.poll.callCount, '/health sees every consecutive error');
        const errors = console.error.callCount;
        assert.ok(errors >= 1 && errors <= 3, errors + ' error lines in 10 minutes');
        assert.match(String(console.error.firstCall.args[0]), /consecutive errors: 1;/);

        up = true;
        await clock.tickAsync(3000);
        assert.strictEqual(poller.pollErrorCount, 0);
        assert.ok(console.log.getCalls().some(c => /recovered after \d+ consecutive error/.test(c.args[0])));
        assert.ok(console.error.callCount + console.warn.callCount + console.log.callCount < 50);

        poller.running = false;
        await clock.tickAsync(3000);
        await loop;
    });
});
