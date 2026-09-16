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

function registerConnectionFlapping(){
    describe('10.6 Connection flapping', function() {
        it('repeated disconnect/reconnect cycles end byte-identical', async function() {
            this.timeout(90000);

            await fixtures.seedBlocks(sourceDb, 1, 10);
            server = lifecycle.setServer(new ServerProcess(sourceDb, SERVER_PORT));
            await server.start();

            client = lifecycle.setClient(new ClientProcess(replicaDb, server.getUrl()));
            await client.start();
            await waitForReplicaBlock(replicaDb, 10, 15000);

            let tip = 10;
            for (let cycle = 0; cycle < 5; cycle++) {
                await client.stop();
                // Advance the chain a deterministic-but-varied amount per cycle.
                let next = tip + 3 + cycle;
                await fixtures.seedBlocks(sourceDb, tip + 1, next);
                tip = next;

                client = lifecycle.setClient(new ClientProcess(replicaDb, server.getUrl()));
                await client.connectLive();
                await waitForConnected(client);
                // One more block so gap detection fires on reconnect.
                tip += 1;
                await fixtures.seedBlocks(sourceDb, tip, tip);
                await waitForReplicaBlock(replicaDb, tip, 20000);
            }

            await assertReplicaByteIdentical(sourceDb, replicaDb);
        });
    });
}

describe('E2E: Disconnect/Resume Parity', function() {
    lifecycle.registerHooks();
    registerConnectionFlapping();
});
