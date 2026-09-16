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

const fixtures = require('../helpers/fixtures');
const ServerProcess = require('../helpers/serverProcess');
const ClientProcess = require('../helpers/clientProcess');
const { waitForReplicaBlock } = require('../helpers/waitFor');
const { assertReplicaByteIdentical } = require('../helpers/assertions');
const { ResumeParityLifecycle, SERVER_PORT } = require('./helpers/resume_parity_harness');

let sourceDb, replicaDb, server, client;
const lifecycle = new ResumeParityLifecycle((state) => {
    sourceDb = state.sourceDb;
    replicaDb = state.replicaDb;
    server = state.server;
    client = state.client;
});

function registerTornBootstrapState(){
    describe('10.3 Torn bootstrap state', function() {
        it('re-bootstrapping over a torn replica converges byte-identically', async function() {
            this.timeout(60000);

            await fixtures.seedBlocks(sourceDb, 1, 30);
            server = lifecycle.setServer(new ServerProcess(sourceDb, SERVER_PORT));
            await server.start();

            client = lifecycle.setClient(new ClientProcess(replicaDb, server.getUrl()));
            await client.bootstrap();

            // Tear the replica mid-state: drop arbitrary rows from several
            // tables (simulates a bootstrap killed partway through an apply).
            await client.stop();
            await replicaDb.doQuery('DELETE FROM credits WHERE action_index > 1500');
            await replicaDb.doQuery('DELETE FROM transactions WHERE block_index > 22');
            await replicaDb.doQuery('DELETE FROM balances');

            // A fresh bootstrap over the torn state must produce a replica
            // indistinguishable from one bootstrapped cleanly.
            client = lifecycle.setClient(new ClientProcess(replicaDb, server.getUrl()));
            await client.start();
            await waitForReplicaBlock(replicaDb, 30, 30000);
            await assertReplicaByteIdentical(sourceDb, replicaDb);
        });
    });
}

describe('E2E: Disconnect/Resume Parity', function() {
    lifecycle.registerHooks();
    registerTornBootstrapState();
});
