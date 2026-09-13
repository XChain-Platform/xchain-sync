/*********************************************************************
 *
 * Copyright © 2025–2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC – https://dankest.llc
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This file is part of XChain Platform. Licensed under the GNU Affero
 * General Public License v3.0 or later; see LICENSE.md. A commercial
 * license (without AGPL source-disclosure terms) is available -
 * contact legal@dankest.llc.
 *
 **********************************************************************
 *
 * XChain Sync - Graceful shutdown
 *
 * Bounded, idempotent drain for SIGTERM/SIGINT. The Dockerfile CMD runs node as
 * PID 1, so `docker stop` delivers SIGTERM here; without a handler node's
 * default action terminates the process wherever the poll/apply loops happen to
 * be, which on a client replica means an aborted apply transaction and a
 * partially written block on every reboot and rolling upgrade.
 *
 * Registering a handler REMOVES node's default terminate, so every handler
 * built here carries its own hard-exit timer: a drain that hangs must still end
 * the process, or a stop becomes an indefinitely lingering container under any
 * supervisor with a long or unbounded grace period. That is strictly worse than
 * the SIGKILL this replaces, so the bound is not optional.
 *
 ********************************************************************/

// Hard-exit budget for the whole drain. Docker's default stop grace is 10s and
// xchain-node issues a bare `docker stop`, so the default sits under it: an
// overrun that ends in our own logged exit is diagnosable, one that ends in the
// daemon's SIGKILL is not. SHUTDOWN_TIMEOUT_MS overrides.
const DEFAULT_SHUTDOWN_TIMEOUT_MS = 8000;

function resolveTimeoutMs(timeoutMs, env){
    if(Number.isFinite(timeoutMs) && timeoutMs > 0) return timeoutMs;
    const raw = parseInt((env || process.env).SHUTDOWN_TIMEOUT_MS, 10);
    return (Number.isFinite(raw) && raw > 0) ? raw : DEFAULT_SHUTDOWN_TIMEOUT_MS;
}

// Close an http.Server and resolve once it has stopped listening. Idle keep-alive
// sockets would otherwise hold close() open indefinitely while no request is in
// flight, so they are dropped explicitly; requests already being served are left
// to finish, which is the whole point of draining rather than exiting.
function closeServer(server){
    return new Promise((resolve) => {
        if(!server || typeof server.close !== 'function') return resolve();
        let settled = false;
        const done = () => { if(!settled){ settled = true; resolve(); } };
        try {
            server.close(done);
            if(typeof server.closeIdleConnections === 'function') server.closeIdleConnections();
        } catch(_){
            done();
        }
    });
}

/**
 * Build an idempotent signal handler that runs `drain` under a hard-exit bound.
 *
 * @param {object}   opts
 * @param {function} opts.drain      async work to finish before exiting
 * @param {number}   [opts.timeoutMs] hard-exit budget (default SHUTDOWN_TIMEOUT_MS / 8000)
 * @param {function} [opts.exit]     process-exit seam (tests pass their own)
 * @param {object}   [opts.log]      console-shaped logger
 * @returns {function(string): void} handler to register on SIGTERM / SIGINT
 */
function createShutdown({ drain, timeoutMs, exit, log } = {}){
    const onExit  = exit || ((code) => process.exit(code));
    const logger  = log || console;
    const budget  = resolveTimeoutMs(timeoutMs);
    let signalled = false;

    return function shutdown(signal){
        // A second signal must not restart the sequence: re-entering would stop the
        // loops and close pools underneath a drain already using them.
        if(signalled){
            logger.log('Shutdown already in progress; ignoring ' + (signal || 'signal') + '.');
            return;
        }
        signalled = true;
        logger.log('Received ' + (signal || 'signal') + ', draining (hard exit in ' + budget + 'ms)...');

        let finished = false;
        const timer = setTimeout(() => {
            if(finished) return;
            finished = true;
            // Non-zero: the drain did NOT complete, so work was cut off exactly as a
            // SIGKILL would have cut it. A clean drain below exits 0.
            logger.error('Shutdown drain exceeded ' + budget + 'ms; exiting hard.');
            onExit(1);
        }, budget);

        Promise.resolve().then(() => drain()).then(
            () => {
                if(finished) return;
                finished = true;
                clearTimeout(timer);
                logger.log('Shutdown drain complete; exiting.');
                onExit(0);
            },
            (err) => {
                if(finished) return;
                finished = true;
                clearTimeout(timer);
                logger.error('Shutdown drain failed:', err);
                onExit(1);
            }
        );
    };
}

/**
 * The sync service's drain, as its own function so the exit path is unit-testable
 * without booting startApi().
 *
 * Order is load-bearing: stop serving before stopping the loops, and stop the
 * loops before their pools go away (SyncService.stop closes the pools last for
 * the same reason).
 *
 * @param {object}   opts
 * @param {object}   opts.syncService  SyncService instance
 * @param {object}   opts.server       http.Server returned by server.listen()
 * @param {object}   [opts.wss]        ws.Server attached to that server
 * @param {number[]} [opts.timers]     interval handles owned by the entry point
 * @param {object}   [opts.log]        console-shaped logger
 */
function createSyncDrain({ syncService, server, wss, timers, log } = {}){
    const logger = log || console;
    return async function drain(){
        // Clear the entry point's own status/eviction intervals first: both reach
        // into the broadcaster the stop below tears down.
        for(const timer of (timers || [])){
            if(timer) clearInterval(timer);
        }

        // Close subscriber sockets before the poll loops stop, so a replica client
        // sees a clean close and reconnects elsewhere rather than sitting on a
        // socket that has silently stopped receiving blocks.
        if(wss && typeof wss.close === 'function'){
            try {
                if(wss.clients && typeof wss.clients.forEach === 'function')
                    wss.clients.forEach((client) => { try { client.terminate(); } catch(_){} });
                wss.close();
            } catch(err){
                logger.warn('Shutdown: closing the WebSocket server failed: ' + (err && err.message ? err.message : err));
            }
        }

        await closeServer(server);

        if(syncService && typeof syncService.stop === 'function') await syncService.stop();
    };
}

module.exports = {
    DEFAULT_SHUTDOWN_TIMEOUT_MS,
    resolveTimeoutMs,
    closeServer,
    createShutdown,
    createSyncDrain
};
