// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// Pins the container exit path. `docker stop` sends SIGTERM to node (PID 1 via the
// Dockerfile's exec-form CMD) and this drain is everything that happens between
// that signal and the process ending: without it the poll/apply loops die wherever
// they stand, which on a replica is an aborted apply transaction.

const assert = require('assert');
const { createShutdown, createSyncDrain, closeServer, resolveTimeoutMs, DEFAULT_SHUTDOWN_TIMEOUT_MS } = require('../../src/shutdown');
const SyncService = require('../../src/SyncService');
const { waitFor } = require('../e2e/helpers/waitFor');

const silentLog = { log(){}, warn(){}, error(){} };

function makeServer(order){
    return {
        closed: false,
        idleDropped: false,
        close(cb){ this.closed = true; order.push('server.close'); setImmediate(cb); },
        closeIdleConnections(){ this.idleDropped = true; }
    };
}

// A SyncService with its constructor bypassed: start() needs a live hub, and the
// property this test is about is the fan-out in stop(), not discovery.
function makeSyncService({ pollers = [], clientSyncs = [], databases = [] } = {}){
    const svc = Object.create(SyncService.prototype);
    svc.ready       = true;
    svc.pollers     = new Map(pollers.map((p, i) => ['chain' + i + ':regtest:indexer', p]));
    svc.clientSyncs = new Map(clientSyncs.map((s, i) => ['chain' + i + ':regtest:indexer', s]));
    svc.databases   = new Map(databases.map((db, i) => ['chain' + i + ':regtest:indexer', { db, config: {}, dbType: 'indexer' }]));
    return svc;
}

function makeDb(order, name){
    return { closed: false, async close(){ this.closed = true; order.push('close:' + name); } };
}

describe('graceful shutdown', function(){

    describe('createShutdown', function(){

        it('runs the drain and exits zero when it completes', async function(){
            const codes = [];
            let drained = false;
            const shutdown = createShutdown({
                drain: async () => { drained = true; },
                exit: (c) => codes.push(c),
                log: silentLog
            });
            // The handler is fire-and-forget, so wait on the thing it produces -
            // an exit code - rather than on a guessed number of milliseconds. The
            // code is pushed after the drain has settled, so its arrival IS the
            // completion signal a sleep was standing in for.
            shutdown('SIGTERM');
            await waitFor(() => codes.length > 0, 2000, 5);
            assert.strictEqual(drained, true);
            assert.deepStrictEqual(codes, [0]);
        });

        it('is idempotent: a second signal does not re-enter the drain', async function(){
            const codes = [];
            let calls = 0;
            const shutdown = createShutdown({
                drain: async () => { calls++; await new Promise((r) => setTimeout(r, 20)); },
                exit: (c) => codes.push(c),
                log: silentLog
            });
            // Both signals are delivered before the first drain can finish, so a
            // re-entry would have bumped `calls` synchronously; waiting for the
            // single exit code proves the drain ran to completion without one.
            shutdown('SIGTERM');
            shutdown('SIGINT');
            await waitFor(() => codes.length > 0, 2000, 5);
            assert.strictEqual(calls, 1);
            assert.deepStrictEqual(codes, [0]);
        });

        // The reason the handler is safe to install at all: registering one REMOVES
        // node's default terminate, so without this bound a hung drain turns every
        // stop into a container that lingers until the supervisor's grace expires.
        it('hard-exits non-zero when the drain overruns its budget', async function(){
            const codes = [];
            const shutdown = createShutdown({
                drain: () => new Promise(() => {}),
                timeoutMs: 20,
                exit: (c) => codes.push(c),
                log: silentLog
            });
            shutdown('SIGTERM');
            await waitFor(() => codes.length > 0, 2000, 5);
            assert.deepStrictEqual(codes, [1]);
        });

        it('exits non-zero when the drain throws, and only once', async function(){
            const codes = [];
            const shutdown = createShutdown({
                drain: async () => { throw new Error('pool refused to close'); },
                timeoutMs: 50,
                exit: (c) => codes.push(c),
                log: silentLog
            });
            shutdown('SIGTERM');
            // Deliberately a fixed window rather than a poll, and longer than the
            // 50ms budget: the "only once" half of this claim is that the timeout
            // timer produced NO second exit after the throw already produced one,
            // so the budget has to be allowed to expire. Waiting for the first
            // code instead would return before the timer could misfire and assert
            // nothing.
            await new Promise((r) => setTimeout(r, 120));
            assert.deepStrictEqual(codes, [1]);
        });
    });

    describe('resolveTimeoutMs', function(){
        it('prefers an explicit budget, then the env var, then the default', function(){
            assert.strictEqual(resolveTimeoutMs(1234, {}), 1234);
            assert.strictEqual(resolveTimeoutMs(undefined, { SHUTDOWN_TIMEOUT_MS: '4321' }), 4321);
            assert.strictEqual(resolveTimeoutMs(undefined, {}), DEFAULT_SHUTDOWN_TIMEOUT_MS);
        });

        it('stays under Docker\'s 10s default stop grace', function(){
            assert.ok(DEFAULT_SHUTDOWN_TIMEOUT_MS < 10000);
        });
    });

    describe('closeServer', function(){
        it('drops idle keep-alive sockets that would otherwise hold close() open', async function(){
            const server = makeServer([]);
            await closeServer(server);
            assert.strictEqual(server.closed, true);
            assert.strictEqual(server.idleDropped, true);
        });

        it('resolves on a missing server rather than hanging the drain', async function(){
            await closeServer(null);
            await closeServer({});
        });
    });

    describe('SyncService.stop', function(){

        it('marks the service not-ready before anything else', async function(){
            const order = [];
            const db  = makeDb(order, 'db0');
            const svc = makeSyncService({ databases: [db] });
            let readyWhenPoolClosed = null;
            db.close = async function(){ readyWhenPoolClosed = svc.ready; this.closed = true; };

            await svc.stop();
            assert.strictEqual(svc.ready, false, '/health must report not-ready for the whole drain window');
            assert.strictEqual(readyWhenPoolClosed, false);
        });

        it('stops every poller and client sync and closes every pool', async function(){
            const order   = [];
            const pollers = [{ stopped: false, stop(){ this.stopped = true; order.push('poller0'); } },
                             { stopped: false, stop(){ this.stopped = true; order.push('poller1'); } }];
            const syncs   = [{ stopped: false, stop(){ this.stopped = true; order.push('sync0'); } }];
            const dbs     = [makeDb(order, 'db0'), makeDb(order, 'db1')];
            const svc     = makeSyncService({ pollers, clientSyncs: syncs, databases: dbs });

            await svc.stop();

            assert.ok(pollers.every(p => p.stopped), 'every poller must be told to stop');
            assert.ok(syncs.every(s => s.stopped), 'every client sync must be told to stop');
            assert.ok(dbs.every(d => d.closed), 'every pool must be released');

            // Pools last: a poller mid-iteration still needs its connection to finish
            // or roll back the statement it is on.
            for(const name of ['db0', 'db1']){
                assert.ok(order.indexOf('close:' + name) > order.indexOf('poller0'),
                    name + ' must close after the pollers were stopped');
                assert.ok(order.indexOf('close:' + name) > order.indexOf('sync0'),
                    name + ' must close after the client syncs were stopped');
            }
        });

        it('closes a shared db handle exactly once', async function(){
            const order = [];
            const shared = makeDb(order, 'shared');
            let calls = 0;
            shared.close = async function(){ calls++; this.closed = true; };
            const svc = makeSyncService({ databases: [shared, shared] });
            await svc.stop();
            assert.strictEqual(calls, 1);
        });

        it('clears the hub re-poll and metric timers so nothing rebuilds pools mid-drain', async function(){
            const svc = makeSyncService({});
            svc._hubRepollTimer      = setInterval(() => {}, 100000);
            svc._stateTreeMetricTimer = setInterval(() => {}, 100000);
            await svc.stop();
            assert.strictEqual(svc._hubRepollTimer, null);
            assert.strictEqual(svc._stateTreeMetricTimer, null);
        });

        it('keeps going when one pool refuses to close', async function(){
            const order = [];
            const bad   = { closed: false, async close(){ throw new Error('pool wedged'); } };
            const good  = makeDb(order, 'good');
            const svc   = makeSyncService({ databases: [bad, good] });
            await svc.stop();
            assert.strictEqual(good.closed, true, 'one wedged pool must not strand the rest');
        });

        it('is safe to call twice', async function(){
            const order = [];
            const svc = makeSyncService({ databases: [makeDb(order, 'db0')] });
            await svc.stop();
            await svc.stop();
            assert.strictEqual(svc.ready, false);
        });
    });

    describe('createSyncDrain', function(){

        it('clears entry-point timers, closes sockets, drains the server, then stops the service', async function(){
            const order = [];
            const server = makeServer(order);
            let terminated = 0;
            const wss = {
                closed: false,
                clients: new Set([{ terminate(){ terminated++; } }, { terminate(){ terminated++; } }]),
                close(){ this.closed = true; order.push('wss.close'); }
            };
            const svc = makeSyncService({ databases: [makeDb(order, 'db0')] });
            const original = svc.stop.bind(svc);
            svc.stop = async () => { order.push('syncService.stop'); return original(); };

            const timer = setInterval(() => { order.push('tick'); }, 5);
            const drain = createSyncDrain({ syncService: svc, server, wss, timers: [timer], log: silentLog });

            await drain();

            assert.strictEqual(wss.closed, true);
            assert.strictEqual(terminated, 2, 'subscriber sockets must be closed, not left to die with the process');
            assert.strictEqual(server.closed, true);
            assert.strictEqual(svc.ready, false);
            assert.ok(order.indexOf('wss.close') < order.indexOf('server.close'));
            assert.ok(order.indexOf('server.close') < order.indexOf('syncService.stop'),
                'stop serving before the poll loops and pools go away');
            assert.ok(!order.includes('tick'), 'the entry-point intervals must be cleared first');
        });

        it('drains a partially-built process without throwing', async function(){
            const drain = createSyncDrain({ log: silentLog });
            await drain();
        });
    });
});
