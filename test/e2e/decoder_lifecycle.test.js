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
const axios  = require('axios');

const decoderFixtures = require('./helpers/decoderFixtures');
const { waitFor }      = require('./helpers/waitFor');
const {
    CHAIN,
    DecoderLifecycle,
    NETWORK,
    SERVER_PORT
} = require('./decoder_lifecycle.test/helpers/decoder_lifecycle_harness');

let sourceDb, replicaDb, client;
const lifecycle = new DecoderLifecycle((state) => {
    sourceDb = state.sourceDb;
    replicaDb = state.replicaDb;
    client = state.client;
});

function startServer(overrides){
    return lifecycle.startServer(overrides);
}

function registerRestSurface(){
    describe('REST surface', function() {

        it('GET /status/decoder/... returns block_hash (no indexer hashes)', async function() {
            this.timeout(15000);
            await decoderFixtures.seedDecoderBlocks(sourceDb, 1, 3);
            await startServer();

            await waitFor(async () => {
                let r = await axios.get('http://127.0.0.1:' + SERVER_PORT + '/status/decoder/' + CHAIN + '/' + NETWORK);
                return r.data.block_height === 3;
            }, 10000);

            let res = await axios.get('http://127.0.0.1:' + SERVER_PORT + '/status/decoder/' + CHAIN + '/' + NETWORK);
            assert.strictEqual(res.data.dbType, 'decoder');
            assert.strictEqual(res.data.block_height, 3);
            assert.ok(res.data.block_hash, 'block_hash should be present');
            assert.strictEqual(res.data.ledger_hash, undefined,   'decoder status must not expose ledger_hash');
            assert.strictEqual(res.data.actions_hash, undefined,  'decoder status must not expose actions_hash');
            assert.strictEqual(res.data.contract_hash, undefined, 'decoder status must not expose contract_hash');
        });

        it('GET /transparency/decoder/... returns 400 (indexer-only)', async function() {
            this.timeout(10000);
            await startServer();

            try {
                await axios.get('http://127.0.0.1:' + SERVER_PORT + '/transparency/decoder/' + CHAIN + '/' + NETWORK + '/roots');
                assert.fail('Expected 400');
            } catch(e){
                assert.strictEqual(e.response.status, 400);
                assert.match(e.response.data.error, /indexer-only/i);
            }
        });
    });
}

describe('E2E: Decoder DB Lifecycle', function() {
    lifecycle.registerHooks();
    registerRestSurface();
});
