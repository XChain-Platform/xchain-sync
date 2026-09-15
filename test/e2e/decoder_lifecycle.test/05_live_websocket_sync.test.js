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

const assert    = require('assert');
const WebSocket = require('ws');

const decoderFixtures = require('../helpers/decoderFixtures');
const { waitFor }      = require('../helpers/waitFor');
const {
    CHAIN,
    DecoderLifecycle,
    NETWORK,
    SERVER_PORT
} = require('./helpers/decoder_lifecycle_harness');

let sourceDb, replicaDb, client;
const lifecycle = new DecoderLifecycle((state) => {
    sourceDb = state.sourceDb;
    replicaDb = state.replicaDb;
    client = state.client;
});

function startServer(overrides){ return lifecycle.startServer(overrides); }
function makeClient(){ return lifecycle.makeClient(); }

function registerLiveBlockSync(){
    describe('Live WebSocket sync', function() {

        it('replica receives decoder block events for blocks added post-bootstrap', async function() {
            this.timeout(30000);

            await decoderFixtures.seedDecoderBlocks(sourceDb, 1, 5);
            await startServer();

            client = makeClient();
            await client.bootstrap();
            await client.connectLive();
            // Poll the client's socket to OPEN before seeding: blocks broadcast
            // while the handshake is still in flight are never re-sent.
            await waitFor(() => client.sync.wsConns[0] && client.sync.wsConns[0].readyState === WebSocket.OPEN, 10000);

            await decoderFixtures.seedDecoderBlocks(sourceDb, 6, 8);

            await waitFor(async () => {
                let b = await replicaDb.getLastBlock();
                return b !== null && b >= 8;
            }, 20000);

            let last = await replicaDb.getLastBlock();
            assert.strictEqual(last, 8);

            let head = await replicaDb.getBlockHashRow(8);
            assert.ok(head.block_hash, 'replica head should have block_hash');
            let src = await sourceDb.getBlockHashRow(8);
            assert.strictEqual(head.block_hash, src.block_hash);
        });
    });
}

function registerLivePayloadShape(){
    describe('Live WebSocket sync', function() {
        it('decoder block payload carries block_hash and omits indexer hashes', async function() {
            this.timeout(15000);

            await decoderFixtures.seedDecoderBlocks(sourceDb, 1, 2);
            await startServer();

            // Subscribe with a raw WS client so we can inspect the wire format.
            let messages = [];
            let ws = new WebSocket('ws://127.0.0.1:' + SERVER_PORT + '/subscribe/decoder/' + CHAIN + '/' + NETWORK);
            ws.on('message', (data) => {
                try { messages.push(JSON.parse(data.toString())); } catch(e){}
            });
            // Poll the socket to OPEN: block 3 is broadcast exactly once.
            await waitFor(() => ws.readyState === WebSocket.OPEN, 10000);

            await decoderFixtures.seedDecoderBlocks(sourceDb, 3, 3);

            await waitFor(() => messages.some(m => m.type === 'block' && m.block_index === 3), 10000);

            let block = messages.find(m => m.type === 'block' && m.block_index === 3);
            assert.ok(block, 'should have received block 3');
            assert.strictEqual(block.dbType, 'decoder');
            assert.ok(block.block_hash, 'decoder payload should include block_hash');
            assert.strictEqual(block.ledger_hash, undefined,
                'decoder payload must not include ledger_hash');
            assert.strictEqual(block.actions_hash, undefined,
                'decoder payload must not include actions_hash');
            assert.strictEqual(block.contract_hash, undefined,
                'decoder payload must not include contract_hash');

            // Decoder payloads carry transactions + transaction_outputs (not actions).
            assert.ok(block.data, 'payload should have data');
            assert.ok(block.data.transactions, 'payload should include transactions');
            assert.strictEqual(block.data.actions, undefined,
                'decoder payload must not include actions');

            ws.close();
        });
    });
}

describe('E2E: Decoder DB Lifecycle', function() {
    lifecycle.registerHooks();
    registerLiveBlockSync();
    registerLivePayloadShape();
});
