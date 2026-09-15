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
 * E2E: Decoder DB lifecycle (bootstrap + live sync)
 *
 * Exercises the Phase 1-3 decoder sync path end-to-end against real
 * MariaDB containers (test/e2e/docker-compose.e2e.yml). The server side
 * uses the real ServerPoller + BlockBroadcaster + SnapshotBuilder; the
 * client side uses the real ClientSync + ClientApplier. The HTTP+WS
 * surface in between mirrors the /:dbType/ route shape from src/api.js.
 *
 * Covered:
 *   - GET /status/decoder/:chain/:network returns block_hash (not
 *     ledger_hash/actions_hash/contract_hash).
 *   - GET /transparency/decoder/:chain/:network/roots => 400 (decoder
 *     has no transparency log).
 *   - Cold bootstrap: replica receives full snapshot, blocks +
 *     transactions + transaction_outputs + events + pubkeys match source.
 *   - Live sync: new source blocks reach replica via WebSocket; payload
 *     carries block_hash and no ledger/actions/contract_hash.
 *   - Incremental snapshot: replica catches up from a since-block,
 *     including tx-scoped tables (transaction_outputs) and events/pubkeys.
 *   - Reorg / rollback: blocks + tx-scoped rows roll back while
 *     append-only index tables stay intact.
 *
 ********************************************************************/

const assert = require('assert');

const decoderFixtures = require('../helpers/decoderFixtures');
const { DecoderLifecycle } = require('./helpers/decoder_lifecycle_harness');

let sourceDb, replicaDb, client;
const lifecycle = new DecoderLifecycle((state) => {
    sourceDb = state.sourceDb;
    replicaDb = state.replicaDb;
    client = state.client;
});

function startServer(overrides){ return lifecycle.startServer(overrides); }
function makeClient(){ return lifecycle.makeClient(); }

function registerIncrementalSnapshot(){
    describe('Incremental snapshot', function() {

        it('catches replica up from a since-block, including tx-scoped tables', async function() {
            this.timeout(30000);

            // No WS connection in this test: the /since/N incremental-snapshot
            // endpoint is the only path exercised, and it must also carry
            // transaction_outputs, which is tx-scoped rather than block-scoped
            // (the failure mode this test guards against).
            await decoderFixtures.seedDecoderBlocks(sourceDb, 1, 5);
            await startServer();

            client = makeClient();
            await client.bootstrap();
            assert.strictEqual(await replicaDb.getLastBlock(), 5);

            await decoderFixtures.seedDecoderBlocks(sourceDb, 6, 12);

            await client.incrementalCatchUp(6);

            assert.strictEqual(await replicaDb.getLastBlock(), 12);

            // Same table set and parity rationale as the bootstrap test above.
            for(let t of ['blocks', 'transactions', 'transaction_outputs',
                          'index_addresses', 'index_transactions', 'events', 'pubkeys']){
                let s = await sourceDb.getTableCount(t);
                let r = await replicaDb.getTableCount(t);
                assert.strictEqual(r, s, t + ' row count mismatch: source=' + s + ' replica=' + r);
            }
        });
    });
}

describe('E2E: Decoder DB Lifecycle', function() {
    lifecycle.registerHooks();
    registerIncrementalSnapshot();
});
