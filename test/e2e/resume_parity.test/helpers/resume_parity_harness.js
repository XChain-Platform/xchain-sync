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

const sinon  = require('sinon');
const setup  = require('../../helpers/setup');
const testDb = require('../../helpers/testDb');
const { waitFor } = require('../../helpers/waitFor');

const SERVER_PORT = 29500;
const REPLICA_B_NAME = 'xchain_e2e_replica_b';

// Wait until a resumed client's WebSocket is actually OPEN; a block seeded
// before the subscription lands broadcasts to nobody, and nothing re-delivers
// it until the NEXT block triggers gap detection.
async function waitForConnected(c) {
    await waitFor(() => c.sync.wsConns && c.sync.wsConns[0] && c.sync.wsConns[0].readyState === 1, 10000);
}

// Blocks land before the rest of a snapshot's tables within one apply, so a
// tip check alone can observe a mid-apply replica. Quiesce on row-count
// parity of the busiest tables before running the byte oracle.
async function waitForQuiesce(src, rep, timeout) {
    await waitFor(async () => {
        for (let t of ['blocks', 'transactions', 'credits']) {
            if (await testDb.getRowCount(src, t) !== await testDb.getRowCount(rep, t)) return false;
        }
        return true;
    }, timeout || 15000);
}

// mocha's hook timeout cannot cancel a still-running async before()
// body - once the timer fires, mocha marks the hook failed and moves on, but
// our setup code keeps executing in the background and will still reach the
// sinon.stub(console, ...) call. That leaves a live stub with nobody left to
// restore it (mocha only runs `after` when `before` succeeded), so the next
// file's before hook then throws "Attempted to wrap log which is already
// wrapped." This watchdog races the setup itself, inside our own code, so we
// can react before mocha gives up: after graceMs with no explicit disarm, it
// flags the stub as abandoned (skipping it if not yet applied, or undoing it
// immediately via sinon.restore() if it already was).
function createStubWatchdog(graceMs) {
    const state = { abandoned: false, stubbed: false };
    const timer = setTimeout(function() {
        state.abandoned = true;
        if (state.stubbed) sinon.restore();
    }, graceMs);
    return {
        state: state,
        cancel: function() { clearTimeout(timer); },
        stubConsole: function() {
            if (state.abandoned) return false;
            if (!process.env.E2E_VERBOSE) {
                sinon.stub(console, 'log');
                sinon.stub(console, 'error');
                state.stubbed = true;
            }
            return true;
        }
    };
}

class ResumeParityLifecycle {
    constructor(assignState){
        this.assignState = assignState;
        this.sourceDb = null;
        this.replicaDb = null;
        this.replicaBDb = null;
        this.server = null;
        this.client = null;
        this.clientB = null;
    }

    publish(){
        this.assignState({
            sourceDb: this.sourceDb,
            replicaDb: this.replicaDb,
            replicaBDb: this.replicaBDb,
            server: this.server,
            client: this.client,
            clientB: this.clientB
        });
    }

    async setup(){
        // Grace window kept under the mocha hook timeout above so the
        // watchdog always gets to react first (see createStubWatchdog).
        const watchdog = createStubWatchdog(55000);
        try {
            await setup.globalSetup();
            this.sourceDb  = setup.getSourceDb();
            this.replicaDb = setup.getReplicaDb();

            // Second replica (the "control" that never disconnects), same MariaDB
            // endpoint as the primary replica, name-scoped.
            this.replicaBDb = await testDb.createDatabase(REPLICA_B_NAME,
                testDb.REPLICA_DB_HOST, testDb.REPLICA_DB_PORT,
                testDb.REPLICA_DB_USER, testDb.REPLICA_DB_PASS);
            await testDb.seedSchema(this.replicaBDb);
            this.publish();
            watchdog.stubConsole();
        } finally {
            watchdog.cancel();
        }
    }

    async teardown(){
        sinon.restore();
        if (this.client)  await this.client.stop();
        if (this.clientB) await this.clientB.stop();
        if (this.server)  await this.server.stop();
        if (this.replicaBDb) await this.replicaBDb.close();
        await testDb.dropDatabase(REPLICA_B_NAME,
            testDb.REPLICA_DB_HOST, testDb.REPLICA_DB_PORT,
            testDb.REPLICA_DB_USER, testDb.REPLICA_DB_PASS);
        await setup.globalTeardown();
    }

    async reset(){
        if (this.client)  { await this.client.stop();  this.client = null; }
        if (this.clientB) { await this.clientB.stop(); this.clientB = null; }
        if (this.server)  { await this.server.stop(); this.server = null; }
        await setup.resetDatabases();
        await testDb.truncateAll(this.replicaBDb);
        this.publish();
    }

    registerHooks(){
        const lifecycle = this;
        before(async function() { this.timeout(60000); await lifecycle.setup(); });
        after(async function() { await lifecycle.teardown(); });
        beforeEach(async function() { this.timeout(30000); await lifecycle.reset(); });
    }

    setServer(server){ this.server = server; this.publish(); return server; }
    setClient(client){ this.client = client; this.publish(); return client; }
    setClientB(client){ this.clientB = client; this.publish(); return client; }
}

module.exports = {
    createStubWatchdog,
    REPLICA_B_NAME,
    ResumeParityLifecycle,
    SERVER_PORT,
    waitForConnected,
    waitForQuiesce
};
