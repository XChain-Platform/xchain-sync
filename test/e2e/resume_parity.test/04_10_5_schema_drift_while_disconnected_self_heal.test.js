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

function registerSchemaDriftSelfHeal(){
    describe('10.5 Schema drift while disconnected (self-heal)', function() {
        it('a replica missing a replicated table re-applies the source schema and converges', async function() {
            this.timeout(60000);

            // Blocks 1-10 carry NO debits (default debitAmount '0'), so the
            // debits table is empty history; the replica losing it costs no
            // rows, exactly like production's anchor_actions wedge (a table
            // whose first-ever row arrives while the replica's schema predates
            // it; errno 1146 on apply).
            await fixtures.seedBlocks(sourceDb, 1, 10);
            server = lifecycle.setServer(new ServerProcess(sourceDb, SERVER_PORT));
            await server.start();

            client = lifecycle.setClient(new ClientProcess(replicaDb, server.getUrl()));
            await client.start();
            await waitForReplicaBlock(replicaDb, 10, 15000);

            // Disconnect, then lose the (empty) replicated table.
            await client.stop();
            await replicaDb.doQuery('DROP TABLE debits');

            client = lifecycle.setClient(new ClientProcess(replicaDb, server.getUrl()));
            await client.connectLive();
            await waitForConnected(client);
            // The FIRST debits rows in history arrive while the table is missing.
            await fixtures.seedBlocks(sourceDb, 11, 15, { debitAmount: '5' });

            // The apply hits 1146, self-heals the schema from the source's
            // /schema endpoint, retries, and converges.
            await waitForReplicaBlock(replicaDb, 15, 30000);
            await assertReplicaByteIdentical(sourceDb, replicaDb);
        });
    });
}

describe('E2E: Disconnect/Resume Parity', function() {
    lifecycle.registerHooks();
    registerSchemaDriftSelfHeal();
});
