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
// E2E: PROPERTY-GRADE interleaving parity (test-framework program P2, S3).
//
// resume-parity.test.js pins six hand-picked disconnect/resume scenarios.
// This suite generalizes them: a seeded PRNG drives a random schedule of
// {grow, reorg, disconnect, reconnect, server-restart, idle} steps against a
// live server/client pair, then quiesces and demands the S1/S2 property:
//
//   the replica is BYTE-IDENTICAL to the source (full replicated set +
//   per-block recompute conformance), OR it is durably HALTED — and a halt is
//   legitimate only when a reorg crossed a DELIVERY INTERRUPTION: it fired
//   inside an explicit disconnect window, or its replacement blocks were
//   still in flight when a disconnect/server-restart cut the stream (either
//   way the replica is left on an orphaned tip the live event stream never
//   corrected, and the catch-up join recompute rightly halts on resume).
//   A remediation (wipe + re-bootstrap) must then converge it.
//
// Failures print the seed: PARITY_SEED=<n> replays the exact schedule.
// PARITY_RUNS / PARITY_STEPS tune breadth and depth.

'use strict';

const assert = require('assert');
const sinon  = require('sinon');
const setup    = require('./helpers/setup');
const fixtures = require('./helpers/fixtures');
const testDb   = require('./helpers/testDb');
const ServerProcess = require('./helpers/serverProcess');
const ClientProcess = require('./helpers/clientProcess');
const { waitFor, waitForReplicaBlock } = require('./helpers/waitFor');
const { assertReplicaByteIdentical } = require('./helpers/assertions');

const SERVER_PORT = 29550;
const RUNS  = parseInt(process.env.PARITY_RUNS  || '2');
const STEPS = parseInt(process.env.PARITY_STEPS || '14');
const FIXED_SEED = process.env.PARITY_SEED ? parseInt(process.env.PARITY_SEED) : null;

function mulberry32(seed) {
    return function () {
        seed |= 0; seed = (seed + 0x6D2B79F5) | 0;
        let t = Math.imul(seed ^ (seed >>> 15), 1 | seed);
        t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
        return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
    };
}

describe('E2E: Interleaved disconnect/reorg/restart parity (property)', function() {

    let sourceDb, replicaDb, server, client;

    before(async function() {
        // Schema seeding over the shared remote-DB venue can exceed a minute
        // under load; on CI's local service containers this is headroom.
        this.timeout(180000);
        await setup.globalSetup();
        sourceDb  = setup.getSourceDb();
        replicaDb = setup.getReplicaDb();
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });

    after(async function() {
        sinon.restore();
        if (client) await client.stop();
        if (server) await server.stop();
        await setup.globalTeardown();
    });

    beforeEach(async function() {
        this.timeout(30000);
        if (client) { await client.stop(); client = null; }
        if (server) { await server.stop(); server = null; }
        await setup.resetDatabases();
    });

    async function waitForConnected(c) {
        await waitFor(() => c.sync.wsConns && c.sync.wsConns[0] && c.sync.wsConns[0].readyState === 1, 10000);
    }

    async function waitForQuiesce(timeout) {
        await waitFor(async () => {
            for (let t of ['blocks', 'transactions', 'credits']) {
                if (await testDb.getRowCount(sourceDb, t) !== await testDb.getRowCount(replicaDb, t)) return false;
            }
            return true;
        }, timeout || 20000);
    }

    // A reorg is "absorbed" once the replica's tip blocks row equals the
    // source's — same height + same consensus hash ids means the chained
    // per-block recompute already verified everything beneath it. Checked at
    // delivery interruptions (disconnect / server-restart): a reorg whose
    // replacement blocks were still in flight leaves the replica on an
    // orphaned tip, and the catch-up join recompute will (correctly) halt on
    // resume — the same class as a reorg inside an explicit disconnect window.
    async function replicaAbsorbedSourceTip() {
        const q = 'SELECT block_index, ledger_hash_id, actions_hash_id, contract_hash_id ' +
                  'FROM blocks ORDER BY block_index DESC LIMIT 1';
        let [src] = await sourceDb.doQuery(q);
        let [rep] = await replicaDb.doQuery(q);
        if (!src || !rep) return false;
        return ['block_index', 'ledger_hash_id', 'actions_hash_id', 'contract_hash_id']
            .every(c => String(src[c]) === String(rep[c]));
    }

    async function runSchedule(seed) {
        const rng = mulberry32(seed);
        const pick = (arr) => arr[Math.floor(rng() * arr.length)];
        const trace = [];

        // Genesis state: 10 blocks, server up, client live.
        let tip = 10;
        let generation = 0;            // bumped per reorg → fresh index space
        let connected = true;
        // Halt legitimacy: a reorg crossed a delivery interruption — it fired
        // while disconnected, or its replacement blocks were unabsorbed when a
        // disconnect/server-restart cut the stream. (Over-approximates by one
        // sided checks at the interruption point, never under-approximates: a
        // halt with no interruption since genesis/remediation stays a failure.)
        let reorgCrossedInterruption = false;
        let pendingReorg = false;      // a reorg not yet verified absorbed
        await fixtures.seedBlocks(sourceDb, 1, tip);
        server = new ServerProcess(sourceDb, SERVER_PORT);
        await server.start();
        client = new ClientProcess(replicaDb, server.getUrl());
        await client.start();
        await waitForReplicaBlock(replicaDb, tip, 15000);

        const seedOpts = () => generation
            ? { indexOffset: generation * 10000, creditAmount: String(1000 + generation) }
            : {};

        for (let step = 0; step < STEPS; step++) {
            // Weighted step choice: growth dominates, like a real chain.
            const roll = rng();
            let action;
            if      (roll < 0.45) action = 'grow';
            else if (roll < 0.60) action = 'reorg';
            else if (roll < 0.72) action = 'disconnect';
            else if (roll < 0.87) action = 'reconnect';
            else if (roll < 0.94) action = 'serverRestart';
            else                  action = 'idle';

            if (action === 'grow') {
                const n = 1 + Math.floor(rng() * 3);
                await fixtures.seedBlocks(sourceDb, tip + 1, tip + n, seedOpts());
                tip += n;
                trace.push('grow+' + n + '→' + tip);

            } else if (action === 'reorg') {
                const depth = 1 + Math.floor(rng() * 3);
                if (tip - depth < 2) { trace.push('reorg-skipped'); continue; }
                const from = tip - depth + 1;
                await fixtures.deleteBlocksFrom(sourceDb, from);
                generation += 1;       // replacement rows: fresh global indexes
                const regrow = depth + Math.floor(rng() * 2); // ≥ depth: chain regrows
                await fixtures.seedBlocks(sourceDb, from, from + regrow - 1, seedOpts());
                tip = from + regrow - 1;
                if (!connected) reorgCrossedInterruption = true;
                pendingReorg = true;
                trace.push('reorg@' + from + ' regrow' + regrow + '→' + tip);

            } else if (action === 'disconnect') {
                if (!connected) { trace.push('disc-noop'); continue; }
                await client.stop();
                connected = false;
                // stop() drained in-flight applies — the replica's state for
                // this window is final. An unabsorbed reorg here can orphan-
                // join on resume, so a later halt is legitimate.
                if (pendingReorg) {
                    if (await replicaAbsorbedSourceTip()) pendingReorg = false;
                    else reorgCrossedInterruption = true;
                }
                trace.push('disconnect');

            } else if (action === 'reconnect') {
                if (connected) { trace.push('reconn-noop'); continue; }
                client = new ClientProcess(replicaDb, server.getUrl());
                await client.connectLive();
                await waitForConnected(client);
                connected = true;
                trace.push('reconnect');

            } else if (action === 'serverRestart') {
                await server.stop();
                // The WS stream is cut even though the harness still counts
                // the client as connected — same interruption hazard as a
                // disconnect for a reorg whose replacements were in flight.
                if (pendingReorg) {
                    if (await replicaAbsorbedSourceTip()) pendingReorg = false;
                    else reorgCrossedInterruption = true;
                }
                server = new ServerProcess(sourceDb, SERVER_PORT);
                await server.start();
                trace.push('server-restart');

            } else {
                await new Promise(r => setTimeout(r, 300));
                trace.push('idle');
            }

            // A client that halted mid-schedule (reorg crossed a disconnect)
            // gets the production remediation immediately: wipe + re-bootstrap.
            if (connected && client.sync.isHalted()) {
                assert.ok(reorgCrossedInterruption,
                    'seed=' + seed + ' [' + trace.join(' ') + ']: client halted (' +
                    JSON.stringify(client.sync.getHaltInfo()) +
                    ') without a reorg crossing a delivery interruption — halts are only legitimate there');
                await client.stop();
                await testDb.truncateAll(replicaDb);
                client = new ClientProcess(replicaDb, server.getUrl());
                await client.start();
                connected = true;
                reorgCrossedInterruption = false;
                pendingReorg = false;  // re-bootstrap put the replica on the source chain
                trace.push('HALT→remediated');
            }
        }

        // Quiesce: ensure live, nudge gap detection with one more block, wait.
        if (!connected) {
            client = new ClientProcess(replicaDb, server.getUrl());
            await client.connectLive();
            await waitForConnected(client);
            connected = true;
        }
        await fixtures.seedBlocks(sourceDb, tip + 1, tip + 1, seedOpts());
        tip += 1;

        await waitFor(async () => {
            if (client.sync.isHalted()) return true;
            let b = await replicaDb.getLastBlock();
            return b !== null && b >= tip;
        }, 30000);

        if (client.sync.isHalted()) {
            assert.ok(reorgCrossedInterruption,
                'seed=' + seed + ' [' + trace.join(' ') + ']: halted at quiesce without a reorg crossing a delivery interruption');
            let halt = await replicaDb.getActiveHalt('indexer');
            assert.ok(halt, 'seed=' + seed + ': in-memory halt must be durable');
            // Production remediation must converge.
            await client.stop();
            await testDb.truncateAll(replicaDb);
            client = new ClientProcess(replicaDb, server.getUrl());
            await client.start();
            await waitForReplicaBlock(replicaDb, tip, 30000);
        }

        await waitForQuiesce();
        try {
            await assertReplicaByteIdentical(sourceDb, replicaDb);
        } catch (e) {
            e.message = 'seed=' + seed + ' [' + trace.join(' ') + ']\n' + e.message;
            throw e;
        }
        return trace;
    }

    for (let run = 0; run < RUNS; run++) {
        it('randomized schedule run ' + (run + 1) + ' ends byte-identical or legitimately halted', async function() {
            this.timeout(180000);
            const seed = FIXED_SEED !== null ? FIXED_SEED : 977000 + run * 1313;
            await runSchedule(seed);
        });
    }
});
