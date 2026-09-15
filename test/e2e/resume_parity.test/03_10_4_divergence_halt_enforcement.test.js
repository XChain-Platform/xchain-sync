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
const { waitFor, waitForReplicaBlock, waitForClientEvents } = require('../helpers/waitFor');
const { assertReplicaByteIdentical } = require('../helpers/assertions');
const { ResumeParityLifecycle, SERVER_PORT } = require('./helpers/resume_parity_harness');

let sourceDb, replicaDb, server, client;
const lifecycle = new ResumeParityLifecycle((state) => {
    sourceDb = state.sourceDb;
    replicaDb = state.replicaDb;
    server = state.server;
    client = state.client;
});

async function startDivergenceScenario(){
    await fixtures.seedBlocks(sourceDb, 1, 10);
    server = lifecycle.setServer(new ServerProcess(sourceDb, SERVER_PORT));
    await server.start();
    // Take manual control of polling so the tampered block below
    // cannot be broadcast before the tamper lands.
    clearInterval(server.pollInterval);

    client = lifecycle.setClient(new ClientProcess(replicaDb, server.getUrl()));
    await client.start();
    await server.poll();
    await waitForReplicaBlock(replicaDb, 10, 15000);

    // Seed block 11, then corrupt its RAW DATA on the source after its
    // hashes were committed (a silently corrupted origin, the same
    // class as the production DOUBLE-promotion bug): the streamed
    // block's rows can no longer reproduce the committed ledger hash.
    // The committed hash chain itself stays intact, so descendant
    // blocks remain internally consistent.
    await fixtures.seedBlocks(sourceDb, 11, 11);
    await sourceDb.doQuery(
        "UPDATE credits SET amount = '31337' WHERE action_index = 1100");
    await server.poll();

    await waitFor(async () => client.sync.isHalted(), 15000);
}

async function exerciseHaltedPaths(seenBlocks){
    // …and FURTHER blocks must be refused on BOTH paths (the June
    // half-enforced-halt regression: replicas kept advancing via
    // catch-up while "halted").
    await fixtures.seedBlocks(sourceDb, 12, 14);
    await server.poll();                          // live path
    await client.incrementalCatchUp(11).catch(() => {}); // catch-up path
    // The refusal is only proven once the halted client has actually
    // handled the three live blocks. Waiting on the events instead of a
    // duration means a client that never received them fails here rather
    // than reporting its silence as a successful refusal.
    await waitForClientEvents(client, 'block', seenBlocks + 3, 15000);
    return replicaDb.getLastBlock();
}

async function repairSourceAndReseed(){
    // Operator remediation. Two steps, BOTH required:
    // 1. Fix the source's corrupted row (the origin itself).
    // 2. Re-seed the replica: the live path APPLIES a block before
    //    recomputing, so the divergent block's rows are already in the
    //    replica even though the cursor never advanced.
    //    Clear-halt-alone is NOT enough (exactly the June production
    //    lesson).
    await sourceDb.doQuery(
        "UPDATE credits SET amount = '1000' WHERE action_index = 1100");
    // Later seeds rebuilt the source's balances while the corrupted
    // amount was live; rebuild them now that the ledger is honest.
    await require('../../../src/db/balance_helpers').rebuildBalances(sourceDb);
    await client.stop();
    await testDb.truncateAll(replicaDb);
    client = lifecycle.setClient(new ClientProcess(replicaDb, server.getUrl()));
    await client.start();
    await server.poll();
    await waitForReplicaBlock(replicaDb, 14, 15000);
}

async function divergenceHaltEnforcement(){
    this.timeout(60000);

    await startDivergenceScenario();

    // Durable halt record on the replica.
    let halt = await replicaDb.getActiveHalt('indexer');
    assert.ok(halt, 'sync_halt row must exist');
    assert.strictEqual(Number(halt.block_index), 11);
    assert.strictEqual(halt.reason, 'local-recompute-divergence');

    // The divergent block must not have advanced the cursor…
    assert.strictEqual(client.getLastAppliedBlock(), 10);

    let seenBlocks = client.getEventsHandled('block');
    let replicaTip = await exerciseHaltedPaths(seenBlocks);
    assert.ok(replicaTip <= 11,
        'halted replica must not advance past the divergence (tip=' + replicaTip + ')');

    await repairSourceAndReseed();
    await assertReplicaByteIdentical(sourceDb, replicaDb);
}

function registerDivergenceHaltEnforcement(){
    describe('10.4 Divergence halt enforcement', function() {
        it('halts durably on recompute divergence; live AND catch-up refuse to advance; clearHalt resumes', divergenceHaltEnforcement);
    });
}

describe('E2E: Disconnect/Resume Parity', function() {
    lifecycle.registerHooks();
    registerDivergenceHaltEnforcement();
});
