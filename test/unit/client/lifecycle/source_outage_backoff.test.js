// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// A replica whose sync source goes down must retry on a growing, capped delay
// and log a bounded number of lines, both when the WebSocket closes (reconnect
// loop) and when it stays open but silent (stale-stream status checks).

const assert = require('assert');
const EventEmitter = require('events');
const proxyquire = require('proxyquire');
const sinon = require('sinon');
const Utility = require('../../../../src/util');
const HashVerifier = require('../../../../src/client/hash_verifier');

const SOURCE = 'http://source:3006';
const TEN_MINUTES = 10 * 60 * 1000;
const STALE_MS = 180000;
const EXPECTED_GAPS = [1000, 2000, 4000, 8000, 16000, 32000, 60000];

function refusal(){
    const e = new Error('connect ECONNREFUSED 10.0.0.1:3006');
    e.code = 'ECONNREFUSED';
    // Shaped like an HTTP client error, which carries its request headers.
    e.config = { headers: { Authorization: 'Bearer outage-test-secret' } };
    return e;
}

// A fake origin: while down, every new socket errors and closes once its
// handlers are on (a microtask, so the refusal takes no fake-clock time).
function loadClientSync(get){
    const origin = { up: true, sockets: [] };
    function FakeWebSocket(){
        const ws = new EventEmitter();
        ws.close = sinon.stub();
        ws.terminate = sinon.stub();
        ws.createdAt = Date.now();
        origin.sockets.push(ws);
        if(!origin.up){
            Promise.resolve().then(() => { ws.emit('error', refusal()); ws.emit('close'); });
        }
        return ws;
    }
    FakeWebSocket.OPEN = 1;
    const ClientSync = proxyquire('../../../../src/client/sync', { ws: FakeWebSocket, axios: { get } });
    return { ClientSync, origin };
}

function makeSync(ClientSync){
    const db = {
        dbName: 'test_db', dbType: 'indexer',
        getLastBlock: sinon.stub().resolves(10),
        getBlockHashRow: sinon.stub().resolves(null),
        getActiveHalt: sinon.stub().resolves(null),
        doQuery: sinon.stub().resolves([]),
        recordHalt: sinon.stub().resolves()
    };
    const config = {
        SYNC_SOURCES: SOURCE, VERIFY_HASHES: false, VERIFY_RECOMPUTE: false,
        VERIFY_STATE_HASH: false, VERIFY_STATE_COMMITMENT: false,
        CLIENT_RECONNECT_DELAY: 1000, CLIENT_SOURCE_STALE_MS: STALE_MS,
        HASH_CONFIRM_TIMEOUT: 5000, MAX_ROLLBACK_DEPTH: 10,
        WS_MAX_PAYLOAD: 1024 * 1024, SNAPSHOT_MAX_CONTENT: 1024 * 1024, GAP_LOG_INTERVAL_MS: 30000
    };
    const sync = new ClientSync('bitcoin', 'mainnet', db, {},
        { rollback: sinon.stub().resolves() }, new HashVerifier(), config, new Utility());
    sync.running = true;
    sync.lastAppliedBlock = 10;
    sinon.stub(sync, 'incrementalCatchUp').resolves();
    return sync;
}

function logLines(){
    return [console.log, console.warn, console.error]
        .reduce((all, fn) => all.concat(fn.getCalls().map(c => c.args.map(String).join(' '))), []);
}

function resetLogs(){ console.log.resetHistory(); console.warn.resetHistory(); console.error.resetHistory(); }

function gaps(times){ return times.slice(1).map((t, i) => t - times[i]); }

function sendStatus(ws, height){
    ws.emit('message', Buffer.from(JSON.stringify({
        type: 'status', block_height: height, source_block_height: height
    })));
}

async function reconnectsWithBackoff(clock){
    const { ClientSync, origin } = loadClientSync(sinon.stub());
    const sync = makeSync(ClientSync);
    sync.connectWebSocket(SOURCE, 0);
    origin.sockets[0].emit('open');

    resetLogs();
    origin.up = false;
    origin.sockets[0].emit('close');
    const droppedAt = Date.now();
    await clock.tickAsync(TEN_MINUTES);

    const attempts = origin.sockets.slice(1).map(ws => ws.createdAt);
    const delays = gaps([droppedAt].concat(attempts));
    assert.deepStrictEqual(delays.slice(0, EXPECTED_GAPS.length), EXPECTED_GAPS);
    assert.ok(delays.slice(EXPECTED_GAPS.length).every(d => d === 60000), 'delays stay at the cap: ' + delays);
    assert.ok(attempts.length <= 20, attempts.length + ' reconnect attempts in 10 minutes');
    const lines = logLines();
    assert.ok(lines.length < 50, lines.length + ' log lines in 10 minutes of downtime');
    assert.ok(lines.every(l => !l.includes('outage-test-secret')), 'request headers never reach the log');

    // The origin returns: the next attempt connects, resumes after the committed
    // tip, and a later drop starts again from the base delay.
    origin.up = true;
    const before = origin.sockets.length;
    await clock.tickAsync(60000);
    assert.strictEqual(origin.sockets.length, before + 1);
    const live = origin.sockets[origin.sockets.length - 1];
    live.emit('open');
    await clock.tickAsync(0);
    assert.ok(sync.incrementalCatchUp.calledOnceWithExactly(11));
    assert.ok(logLines().some(l => l.includes('available again')), 'the recovery is logged once');

    origin.up = false;
    live.emit('close');
    const reDroppedAt = Date.now();
    await clock.tickAsync(1000);
    assert.strictEqual(origin.sockets[origin.sockets.length - 1].createdAt - reDroppedAt, 1000);
    sync.stop();
}

async function staleChecksBackOffWhileUnreachable(clock){
    const checks = [];
    const get = sinon.stub().callsFake(() => { checks.push(Date.now()); return Promise.reject(refusal()); });
    const { ClientSync, origin } = loadClientSync(get);
    const sync = makeSync(ClientSync);
    sync.connectWebSocket(SOURCE, 0);
    const ws = origin.sockets[0];
    ws.emit('open');
    sendStatus(ws, 10);
    const lastEventAt = Date.now();

    resetLogs();
    await clock.tickAsync(STALE_MS + TEN_MINUTES);

    assert.strictEqual(checks[0] - lastEventAt, STALE_MS + 1, 'the first check runs when the stream goes stale');
    const delays = gaps(checks);
    assert.deepStrictEqual(delays.slice(0, EXPECTED_GAPS.length), EXPECTED_GAPS);
    assert.ok(delays.slice(EXPECTED_GAPS.length).every(d => d === 60000), 'delays stay at the cap: ' + delays);
    assert.ok(checks.length <= 20, checks.length + ' status checks in 10 minutes');
    const lines = logLines();
    assert.ok(lines.length < 50, lines.length + ' log lines in 10 minutes of downtime');
    assert.ok(lines.every(l => !l.includes('outage-test-secret')), 'request headers never reach the log');
    assert.strictEqual(ws.terminate.called, false, 'a failed status read never drops the stream');

    // The origin returns with a higher tip: the stalled stream is replaced and
    // the replica resumes after its committed tip.
    get.callsFake(() => Promise.resolve({ data: { source_height: 20, block_height: 20 } }));
    await clock.tickAsync(60000);
    assert.strictEqual(ws.terminate.calledOnce, true);
    await clock.tickAsync(1000);
    const next = origin.sockets[origin.sockets.length - 1];
    assert.notStrictEqual(next, ws);
    next.emit('open');
    await clock.tickAsync(0);
    assert.ok(sync.incrementalCatchUp.calledOnceWithExactly(11));
    sync.stop();
}

async function staleChecksBackOffWhileTipIsFlat(clock){
    const checks = [];
    const get = sinon.stub().callsFake(() => {
        checks.push(Date.now());
        return Promise.resolve({ data: { source_height: 10, block_height: 10 } });
    });
    const { ClientSync, origin } = loadClientSync(get);
    const sync = makeSync(ClientSync);
    sync.connectWebSocket(SOURCE, 0);
    const ws = origin.sockets[0];
    ws.emit('open');
    sendStatus(ws, 10);

    resetLogs();
    await clock.tickAsync(STALE_MS + TEN_MINUTES);

    assert.ok(checks.length <= 20, checks.length + ' status checks in 10 minutes');
    assert.strictEqual(logLines().length, 0);
    assert.strictEqual(ws.terminate.called, false);

    // A live event resets the backoff: the next stale check runs one stale window later.
    sendStatus(ws, 11);
    const eventAt = Date.now();
    const before = checks.length;
    await clock.tickAsync(STALE_MS + 1);
    assert.strictEqual(checks.length, before + 1);
    assert.strictEqual(checks[checks.length - 1] - eventAt, STALE_MS + 1);
    sync.stop();
}

describe('ClientSync source outage backoff @regression', function(){
    let clock;
    beforeEach(function(){
        sinon.stub(console, 'log'); sinon.stub(console, 'warn'); sinon.stub(console, 'error');
        clock = sinon.useFakeTimers();
    });
    afterEach(function(){ clock.restore(); sinon.restore(); });

    it('reconnects to a closed origin on a doubling delay capped at 60s, with bounded logs', function(){ return reconnectsWithBackoff(clock); });
    it('re-checks a silent stream against an unreachable origin with backoff, not every millisecond', function(){ return staleChecksBackOffWhileUnreachable(clock); });
    it('re-checks a reachable origin whose tip has not moved with backoff and no log lines', function(){ return staleChecksBackOffWhileTipIsFlat(clock); });
});
