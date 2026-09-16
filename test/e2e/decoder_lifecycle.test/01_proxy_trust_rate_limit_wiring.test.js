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

const decoderFixtures = require('../helpers/decoderFixtures');
const {
    CHAIN,
    CLIENT_A,
    CLIENT_B,
    DecoderLifecycle,
    NETWORK,
    SERVER_PORT,
    SPOOFED
} = require('./helpers/decoder_lifecycle_harness');

let sourceDb;
const lifecycle = new DecoderLifecycle((state) => { sourceDb = state.sourceDb; });

function startServer(overrides){
    return lifecycle.startServer(overrides);
}

function registerProxyTrustRateLimitWiring(){
    // Regression coverage for the two seams this harness shares with src/api.js.
    // A hand-rolled app with no 'trust proxy' setting resolves every request to
    // the proxy loopback address, so all callers share one snapshot bucket and
    // the first to arrive can 429 the rest; nothing else in this file would
    // notice that, because the routes answer identically either way.
    describe('Proxy-trust rate-limit wiring', function() {

        async function snapshotHit(forwardedFor){
            return axios.get('http://127.0.0.1:' + SERVER_PORT + '/snapshot/decoder/' + CHAIN + '/' + NETWORK, {
                headers: { 'x-forwarded-for': forwardedFor },
                validateStatus: () => true
            });
        }

        // The limiter charges its bucket before the handler runs, but an empty
        // source answers 404, so seed blocks and let 200 mean served.
        beforeEach(async function(){
            this.timeout(15000);
            await decoderFixtures.seedDecoderBlocks(sourceDb, 1, 3);
        });

        it('gives distinct forwarded clients independent snapshot budgets', async function(){
            this.timeout(20000);
            await startServer({ TRUST_PROXY: true, SNAPSHOT_RATE_FULL: 2 });

            assert.strictEqual((await snapshotHit(CLIENT_A)).status, 200);
            assert.strictEqual((await snapshotHit(CLIENT_A)).status, 200);
            assert.strictEqual((await snapshotHit(CLIENT_A)).status, 429, 'the client should have exhausted its own budget');
            assert.strictEqual((await snapshotHit(CLIENT_B)).status, 200,
                'a distinct client was refused, so one shared bucket is serving every caller');
        });

        it('stops at the one address the proxy vouched for, so a forged prefix buys no budget', async function(){
            this.timeout(20000);
            await startServer({ TRUST_PROXY: true, SNAPSHOT_RATE_FULL: 2 });

            assert.strictEqual((await snapshotHit(CLIENT_A)).status, 200);
            assert.strictEqual((await snapshotHit(SPOOFED + ', ' + CLIENT_A)).status, 200);
            assert.strictEqual((await snapshotHit('203.0.113.1, 203.0.113.2, ' + CLIENT_A)).status, 429,
                'entries left of the proxy-appended address were believed and minted a fresh bucket');
        });
    });
}

describe('E2E: Decoder DB Lifecycle', function() {
    lifecycle.registerHooks();
    registerProxyTrustRateLimitWiring();
});
