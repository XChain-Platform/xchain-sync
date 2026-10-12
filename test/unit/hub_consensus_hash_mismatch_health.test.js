'use strict';

// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

const assert = require('assert');
const http   = require('http');

const HubClient = require('../../src/hub/client');
const coins     = require('../../src/coins');

function trueHashes(){
    const out = {};
    for(const network of coins.NETWORKS) out[network] = coins.consensusHashes(network);
    return out;
}

function provider(hubClient, ready = true){
    return {
        hubClient,
        isReady:                () => ready,
        getHubConfigAgeSeconds: () => null,
        getChains:              () => [],
        getDatabase:            () => null,
        getBroadcaster:         () => null,
        getSnapshotBuilder:     () => null,
        getPoller:              () => null,
        getTransparencyLog:     () => null,
        getClientSync:          () => null
    };
}

function cfg(){
    return {
        SYNC_MODE: 'client',
        SYNC_API_KEY: '',
        TRUST_PROXY: false,
        SNAPSHOT_RATE_FULL: 100,
        SNAPSHOT_RATE_INCR: 100,
        TRANSPARENCY_RATE_LIMIT: 100
    };
}

async function readHealth(hubClient, ready = true){
    const { createApp } = require('../../src/api');
    const server = http.createServer(createApp(provider(hubClient, ready), cfg()));
    await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
    try {
        const response = await fetch('http://127.0.0.1:' + server.address().port + '/health');
        return { status: response.status, body: await response.json() };
    } finally {
        server.closeAllConnections();
        await new Promise((resolve) => server.close(resolve));
    }
}

describe('hub consensus hash mismatch health reporting', function(){
    it('records unknown, matching, and mismatching hub hash states', function(){
        const hub = new HubClient([]);
        assert.strictEqual(hub.hubConsensusHashMismatch, null);
        assert.deepStrictEqual(hub.hubConsensusHashMismatchDetails, []);

        hub.checkHubConsensusHash(null);
        assert.strictEqual(hub.hubConsensusHashMismatch, null);
        assert.deepStrictEqual(hub.hubConsensusHashMismatchDetails, []);

        hub.checkHubConsensusHash({});
        assert.strictEqual(hub.hubConsensusHashMismatch, null);
        assert.deepStrictEqual(hub.hubConsensusHashMismatchDetails, []);

        hub.checkHubConsensusHash({ testnet: { UNKNOWN: 'a'.repeat(64) } });
        assert.strictEqual(hub.hubConsensusHashMismatch, null);
        assert.deepStrictEqual(hub.hubConsensusHashMismatchDetails, []);

        hub.checkHubConsensusHash(trueHashes());
        assert.strictEqual(hub.hubConsensusHashMismatch, false);
        assert.deepStrictEqual(hub.hubConsensusHashMismatchDetails, []);

        const drifted = trueHashes();
        drifted.testnet = Object.assign({}, drifted.testnet, { BTC: 'f'.repeat(64) });
        hub.checkHubConsensusHash(drifted);
        assert.strictEqual(hub.hubConsensusHashMismatch, true);
        assert.deepStrictEqual(hub.hubConsensusHashMismatchDetails, [
            'BTC/testnet: hub ' + 'f'.repeat(64) + ' vs bundled ' + trueHashes().testnet.BTC
        ]);

        hub.checkHubConsensusHash(trueHashes());
        assert.strictEqual(hub.hubConsensusHashMismatch, false);
        assert.deepStrictEqual(hub.hubConsensusHashMismatchDetails, []);
    });
});

describe('hub consensus hash mismatch HTTP health reporting', function(){
    it('publishes unknown as the final two keys while starting', async function(){
        const health = await readHealth(new HubClient([]), false);
        assert.strictEqual(health.status, 503);
        assert.strictEqual(health.body.status, 'starting');
        assert.strictEqual(health.body.hub_consensus_hash_mismatch, null);
        assert.deepStrictEqual(health.body.hub_consensus_hash_mismatch_details, []);
        assert.deepStrictEqual(Object.keys(health.body).slice(-2), [
            'hub_consensus_hash_mismatch', 'hub_consensus_hash_mismatch_details'
        ]);
    });

    it('publishes a confirmed match without degrading health', async function(){
        const hub = new HubClient([]);
        hub.checkHubConsensusHash(trueHashes());

        const health = await readHealth(hub);
        assert.strictEqual(health.status, 200);
        assert.strictEqual(health.body.status, 'healthy');
        assert.strictEqual(health.body.hub_consensus_hash_mismatch, false);
        assert.deepStrictEqual(health.body.hub_consensus_hash_mismatch_details, []);
    });

    it('publishes mismatch detail without degrading health', async function(){
        const hub = new HubClient([]);
        const drifted = trueHashes();
        drifted.regtest = Object.assign({}, drifted.regtest, { LTC: 'e'.repeat(64) });
        hub.checkHubConsensusHash(drifted);

        const health = await readHealth(hub);
        assert.strictEqual(health.status, 200);
        assert.strictEqual(health.body.status, 'healthy');
        assert.strictEqual(health.body.hub_consensus_hash_mismatch, true);
        assert.strictEqual(health.body.hub_consensus_hash_mismatch_details.length, 1);
        assert.match(health.body.hub_consensus_hash_mismatch_details[0], /^LTC\/regtest:/);
        assert.deepStrictEqual(Object.keys(health.body).slice(-2), [
            'hub_consensus_hash_mismatch', 'hub_consensus_hash_mismatch_details'
        ]);
    });
});
