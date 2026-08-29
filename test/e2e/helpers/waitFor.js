// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

async function waitFor(fn, timeout = 15000, interval = 100) {
    let start = Date.now();
    while (Date.now() - start < timeout) {
        if (await fn()) return true;
        await new Promise(r => setTimeout(r, interval));
    }
    throw new Error('waitFor timeout after ' + timeout + 'ms');
}

async function waitForReplicaBlock(replicaDb, expectedBlock, timeout = 15000) {
    await waitFor(async () => {
        let block = await replicaDb.getLastBlock();
        return block !== null && block >= expectedBlock;
    }, timeout);
}

async function waitForServerStatus(axios, serverUrl, chain, network, expectedBlock, timeout = 15000) {
    await waitFor(async () => {
        try {
            let res = await axios.get(serverUrl + '/status/indexer/' + chain + '/' + network, { timeout: 3000 });
            return res.data && res.data.block_height >= expectedBlock;
        } catch (e) {
            return false;
        }
    }, timeout);
}

async function waitForTableCount(db, table, expectedCount, timeout = 15000) {
    await waitFor(async () => {
        let count = await db.getTableCount(table);
        return count >= expectedCount;
    }, timeout);
}

// Bounded drain that reports rather than throws: resolves true as soon as `fn`
// holds, false when the budget runs out. For windows whose OUTCOME is the thing
// being recorded rather than a precondition being met: a perf shortfall (a
// subscriber dropped under backpressure) is the number being measured, and a
// settle window that ends with no close frame is a socket that was accepted.
// Assertions on a precondition use waitFor, which throws.
async function drainUntil(fn, timeout = 15000, interval = 50) {
    let start = Date.now();
    while (Date.now() - start < timeout) {
        if (await fn()) return true;
        await new Promise(r => setTimeout(r, interval));
    }
    return false;
}

// Wait until the client has OBSERVED its link to a source going away. This is
// the anchor for "and now nothing should arrive" assertions: once the client has
// seen the close, no further data can reach it over that socket, so the window
// is a fact instead of a guess. It also fails loudly (waitFor throws) when the
// link that was supposed to be severed is still open.
async function waitForClientDisconnect(client, sourceIndex = 0, timeout = 15000) {
    await waitFor(() => !client.isConnected(sourceIndex), timeout);
}

// Wait until the client has FINISHED handling `target` cumulative events of
// `type`. Take the baseline with client.getEventsHandled(type) BEFORE the step
// that triggers the events, or events handled in between are counted twice over.
// Use this before asserting that a client did NOT act on something: the count
// only advances once each event's handler has settled, so the assertion runs
// against a client that genuinely had its chance, not against one whose socket
// silently never connected.
async function waitForClientEvents(client, type, target, timeout = 15000) {
    await waitFor(() => client.getEventsHandled(type) >= target, timeout);
}

// Wait until the server's poll loop has recorded `n` more failed cycles. Proves
// the server actually felt a source-DB outage (and survived it) instead of
// inferring that from elapsed time.
async function waitForServerPollFailures(server, n = 1, timeout = 30000) {
    let base = server.pollFailures;
    await waitFor(() => server.pollFailures >= base + n, timeout);
}

module.exports = {
    waitFor,
    drainUntil,
    waitForReplicaBlock,
    waitForServerStatus,
    waitForTableCount,
    waitForClientDisconnect,
    waitForClientEvents,
    waitForServerPollFailures
};
