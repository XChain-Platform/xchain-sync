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

function registerReorgRollback(){
    describe('Reorg / rollback', function() {

        it('rolls back blocks + tx-scoped rows and keeps index tables intact', async function() {
            this.timeout(30000);

            await decoderFixtures.seedDecoderBlocks(sourceDb, 1, 10);
            await startServer();
            client = makeClient();
            await client.bootstrap();
            assert.strictEqual(await replicaDb.getLastBlock(), 10);

            let initialAddrCount = await replicaDb.getTableCount('index_addresses');

            // rollback(8) removes blocks 8, 9, 10, leaving the tip at 7.
            await client.rollback(8);

            assert.strictEqual(await replicaDb.getLastBlock(), 7);
            assert.strictEqual(await replicaDb.getTableCount('blocks'),       7);
            assert.strictEqual(await replicaDb.getTableCount('transactions'), 7);

            // Tx-scoped: seedDecoderBlocks creates one transaction_output per
            // block, so post-rollback we should see 7 left (was 10).
            assert.strictEqual(await replicaDb.getTableCount('transaction_outputs'), 7);

            // Index tables are append-only and should NOT be touched by rollback.
            assert.strictEqual(
                await replicaDb.getTableCount('index_addresses'),
                initialAddrCount,
                'index_addresses must not change on rollback'
            );

            let srcHead = await sourceDb.getBlockHashRow(7);
            let rplHead = await replicaDb.getBlockHashRow(7);
            assert.strictEqual(rplHead.block_hash, srcHead.block_hash);
        });
    });
}

describe('E2E: Decoder DB Lifecycle', function() {
    lifecycle.registerHooks();
    registerReorgRollback();
});
