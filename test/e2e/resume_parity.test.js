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
const sinon  = require('sinon');
const fixtures = require('./helpers/fixtures');
const ServerProcess = require('./helpers/serverProcess');
const ClientProcess = require('./helpers/clientProcess');
const { waitForReplicaBlock } = require('./helpers/waitFor');
const { assertReplicaByteIdentical } = require('./helpers/assertions');
const {
    createStubWatchdog,
    ResumeParityLifecycle,
    SERVER_PORT,
    waitForConnected,
    waitForQuiesce
} = require('./resume_parity.test/helpers/resume_parity_harness');

// Unit coverage for the watchdog itself: exercised in isolation (no DB/docker
// infra, no real hook timeout) so it runs under the plain unit/CI venues too,
// not only a full e2e pass. Lives in this file because the fix does.
describe('resume-parity before-all stub watchdog', function() {
    afterEach(function() { sinon.restore(); });

    it('skips the stub once abandoned before it was ever applied, leaving console stubbable', async function() {
        const watchdog = createStubWatchdog(5);
        await new Promise(function(r) { setTimeout(r, 30); }); // let it fire
        const applied = watchdog.stubConsole();
        assert.strictEqual(applied, false, 'stub must be skipped once abandoned');
        // The "next suite" must be able to wrap console cleanly - this is the
        // actual production symptom (an "already wrapped" throw).
        assert.doesNotThrow(function() {
            sinon.stub(console, 'log');
            sinon.stub(console, 'error');
        });
    });

    it('undoes an already-applied stub once abandoned, leaving console stubbable', async function() {
        const watchdog = createStubWatchdog(5);
        const applied = watchdog.stubConsole(); // applies immediately, well inside grace
        assert.strictEqual(applied, true);
        await new Promise(function(r) { setTimeout(r, 30); }); // let the watchdog fire and restore
        assert.doesNotThrow(function() {
            sinon.stub(console, 'log');
            sinon.stub(console, 'error');
        });
    });

    it('leaves a normal, on-time stub in place (cancel path never abandons it)', function() {
        const watchdog = createStubWatchdog(60000);
        const applied = watchdog.stubConsole();
        watchdog.cancel();
        assert.strictEqual(applied, true);
        assert.ok(console.log.isSinonProxy, 'a timely stub must stay applied for the suite body');
    });
});

let sourceDb, replicaDb, replicaBDb, server, client, clientB;
const lifecycle = new ResumeParityLifecycle((state) => {
    sourceDb = state.sourceDb;
    replicaDb = state.replicaDb;
    replicaBDb = state.replicaBDb;
    server = state.server;
    client = state.client;
    clientB = state.clientB;
});

function registerResumeParity(){
    describe('10.1 Resume parity vs a control replica', function() {
        it('a resumed replica converges byte-identically to one that never disconnected', async function() {
            this.timeout(60000);

            await fixtures.seedBlocks(sourceDb, 1, 20);
            server = lifecycle.setServer(new ServerProcess(sourceDb, SERVER_PORT));
            await server.start();

            // Replica A (will disconnect) and replica B (control, stays live).
            client = lifecycle.setClient(new ClientProcess(replicaDb, server.getUrl()));
            await client.start();
            clientB = lifecycle.setClientB(new ClientProcess(replicaBDb, server.getUrl()));
            await clientB.start();
            await waitForReplicaBlock(replicaDb, 20, 15000);
            await waitForReplicaBlock(replicaBDb, 20, 15000);

            // A disconnects; the chain advances 30 blocks; B follows live.
            await client.stop();
            await fixtures.seedBlocks(sourceDb, 21, 50);
            await waitForReplicaBlock(replicaBDb, 50, 30000);

            // A resumes: fresh client session over the same replica DB (the
            // production restart shape: gap detection + incremental catch-up).
            client = lifecycle.setClient(new ClientProcess(replicaDb, server.getUrl()));
            await client.connectLive();
            await waitForConnected(client);
            // A new block must arrive for gap detection to notice the deficit.
            await fixtures.seedBlocks(sourceDb, 51, 51);
            await waitForReplicaBlock(replicaDb, 51, 30000);
            await waitForReplicaBlock(replicaBDb, 51, 30000);
            await waitForQuiesce(sourceDb, replicaDb);
            await waitForQuiesce(sourceDb, replicaBDb);

            // Both replicas must be byte-identical to the source, and thereby
            // to each other: resume path ≡ always-connected path.
            await assertReplicaByteIdentical(sourceDb, replicaDb);
            await assertReplicaByteIdentical(sourceDb, replicaBDb);
        });
    });
}

describe('E2E: Disconnect/Resume Parity', function() {
    lifecycle.registerHooks();
    registerResumeParity();
});
