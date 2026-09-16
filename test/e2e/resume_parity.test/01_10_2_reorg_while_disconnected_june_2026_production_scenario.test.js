// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.
//
// E2E: Disconnect/resume PARITY (test-framework program P2, slice S2).
//
// The property under test everywhere here: no matter how a replica's
// connection history played out (clean resume, reorg during downtime, torn
// bootstrap, divergence halt, schema drift, flapping), it must end either
// BYTE-IDENTICAL to the source (assertReplicaByteIdentical: full replicated
// table set + per-block recompute conformance) or DURABLY HALTED, never
// silently diverged. June 2026 production background: five replicas halted
// holding orphaned pre-reorg blocks their origins had deleted; the remediation
// was a re-seed. 10.2 pins that whole lifecycle as a regression test.

'use strict';

const assert = require('assert');
const fixtures = require('../helpers/fixtures');
const testDb   = require('../helpers/testDb');
const ServerProcess = require('../helpers/serverProcess');
const ClientProcess = require('../helpers/clientProcess');
const { waitFor, waitForReplicaBlock } = require('../helpers/waitFor');
const { assertReplicaByteIdentical } = require('../helpers/assertions');
const {
    ResumeParityLifecycle,
    SERVER_PORT,
    waitForConnected
} = require('./helpers/resume_parity_harness');

let sourceDb, replicaDb, server, client;
const lifecycle = new ResumeParityLifecycle((state) => {
    sourceDb = state.sourceDb;
    replicaDb = state.replicaDb;
    server = state.server;
    client = state.client;
});

async function reorgWhileDisconnected(){
    this.timeout(90000);

    await fixtures.seedBlocks(sourceDb, 1, 25);
    server = lifecycle.setServer(new ServerProcess(sourceDb, SERVER_PORT));
    await server.start();

    client = lifecycle.setClient(new ClientProcess(replicaDb, server.getUrl()));
    await client.start();
    await waitForReplicaBlock(replicaDb, 25, 15000);

    // Disconnect. The source reorgs below the replica's tip: blocks
    // 18-25 are orphaned and replaced by a different chain 18-28.
    await client.stop();
    await fixtures.deleteBlocksFrom(sourceDb, 18);
    // indexOffset: replacement transactions get FRESH global indexes,
    // as on a real chain; they must not collide with the orphans.
    await fixtures.seedBlocks(sourceDb, 18, 28, { creditAmount: '7777', indexOffset: 5000 });

    // Resume. The replica still holds the orphaned 18-25; the reorg
    // event is long gone (only live subscribers saw it).
    client = lifecycle.setClient(new ClientProcess(replicaDb, server.getUrl()));
    await client.connectLive();
    await waitForConnected(client);
    await fixtures.seedBlocks(sourceDb, 29, 29, { indexOffset: 5000 });

    // Give the client ample time to react (gap catch-up + applies).
    // The verdict is only SETTLED once no catch-up is in flight: the
    // catch-up commits the applied range FIRST and runs the join-block
    // recompute (and persists any halt to sync_halt) AFTER, so sampling
    // mid-catch-up sees either b>=29 with the halt still pending (byte-
    // identity assert on a half-judged replica) or isHalted() true with
    // the durable sync_halt row not yet committed.
    await waitFor(async () => {
        if (client.sync._catchUpInFlight) return false;
        if (client.sync.isHalted()) return true;
        let b = await replicaDb.getLastBlock();
        return b !== null && b >= 29;
    }, 30000).catch(() => {});

    // THE PROPERTY: never silently diverged. Either the client halted,
    // or the replica is byte-identical to the post-reorg source.
    if (!client.sync.isHalted()) {
        await assertReplicaByteIdentical(sourceDb, replicaDb);
    } else {
        // Halted: the halt must be durable, and remediation must work.
        let halt = await replicaDb.getActiveHalt('indexer');
        assert.ok(halt, 'an in-memory halt must also be recorded durably in sync_halt');
    }

    // Remediation (what production did): stop, wipe, re-bootstrap.
    await client.stop();
    await testDb.truncateAll(replicaDb);
    await replicaDb.clearHalt('indexer');
    client = lifecycle.setClient(new ClientProcess(replicaDb, server.getUrl()));
    await client.start();
    await waitForReplicaBlock(replicaDb, 29, 30000);
    await assertReplicaByteIdentical(sourceDb, replicaDb);
}

function registerReorgWhileDisconnected(){
    describe('10.2 Reorg while disconnected (June 2026 production scenario)', function() {
        it('a replica holding orphaned blocks must not silently follow the new chain; a re-seed converges it', reorgWhileDisconnected);
    });
}

describe('E2E: Disconnect/Resume Parity', function() {
    lifecycle.registerHooks();
    registerReorgWhileDisconnected();
});
