// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later

const assert = require('assert');
const sinon = require('sinon');
const axios = require('axios');
const HubClient = require('../../../src/hub/client');
const coins = require('../../../src/coins');

const ERROR_ENVELOPE = { error: 'config store unavailable' };

function driftedHashes(){
    const hashes = {};
    for(const network of coins.NETWORKS) hashes[network] = coins.consensusHashes(network);
    hashes.testnet = Object.assign({}, hashes.testnet, { BTC: 'f'.repeat(64) });
    return hashes;
}

function wrapped(configs, seq, watermark, extra){
    return Object.assign({ configs, seq, watermark, coin_consensus_hashes: driftedHashes() }, extra || {});
}

function registerHooks(){
    beforeEach(function(){
        sinon.stub(console, 'error');
    });
    afterEach(function(){ sinon.restore(); });
}

describe('HubClient getallconfigs error envelope @regression', function(){
    registerHooks();

    it('keeps cached config, cursor, freshness, and mismatch state after a failed poll', async function(){
        const hub = new HubClient('localhost', 10000);
        const post = sinon.stub(axios, 'post');
        post.onCall(0).resolves({ data: { result: wrapped({ bitcoin: { mainnet: {} } }, 5, 5000) } });
        await hub.getallconfigs();
        assert.strictEqual(hub.hubConsensusHashMismatch, true);

        const sentinel = 1234;
        hub.lastSuccessfulFetchAt = sentinel;
        const configs = hub.configs;
        const details = hub.hubConsensusHashMismatchDetails.slice();
        post.onCall(1).resolves({ data: { result: ERROR_ENVELOPE } });

        assert.strictEqual(await hub.getallconfigs(), null);
        assert.strictEqual(hub.configs, configs);
        assert.strictEqual(hub.lastWatermark, 5000);
        assert.strictEqual(hub.lastSuccessfulFetchAt, sentinel);
        assert.strictEqual(hub.hubConsensusHashMismatch, true);
        assert.deepStrictEqual(hub.hubConsensusHashMismatchDetails, details);
    });

    it('does not stamp freshness on a first error envelope', async function(){
        const hub = new HubClient('localhost', 10000);
        sinon.stub(axios, 'post').resolves({ data: { result: ERROR_ENVELOPE } });
        assert.strictEqual(await hub.getallconfigs(), null);
        assert.strictEqual(hub.lastSuccessfulFetchAt, null);
        assert.strictEqual(hub.configs, null);
    });
});

describe('HubClient getallconfigs error envelope refetches @regression', function(){
    registerHooks();

    it('rejects an error envelope returned by the regression refetch', async function(){
        const hub = new HubClient('localhost', 10000);
        const post = sinon.stub(axios, 'post');
        post.onCall(0).resolves({ data: { result: wrapped({ bitcoin: {} }, 5, 5000) } });
        await hub.getallconfigs();
        hub.lastSuccessfulFetchAt = 1234;
        const configs = hub.configs;
        post.onCall(1).resolves({ data: { result: wrapped({ litecoin: {} }, 1, 100) } });
        post.onCall(2).resolves({ data: { result: ERROR_ENVELOPE } });

        assert.strictEqual(await hub.getallconfigs(), null);
        assert.strictEqual(hub.configs, configs);
        assert.strictEqual(hub.lastWatermark, 5000);
        assert.strictEqual(hub.lastSuccessfulFetchAt, 1234);
        assert.strictEqual(hub.hubConsensusHashMismatch, true);
        assert.strictEqual(post.getCall(2).args[1].params.since_updated_at, 0);
    });

    it('rejects an error envelope returned by the endpoint-change refetch', async function(){
        const hub = new HubClient(['http://a:1', 'http://b:2']);
        const post = sinon.stub(axios, 'post');
        post.resolves({ data: { result: wrapped({ bitcoin: {} }, 5, 5000) } });
        await hub.getallconfigs();
        hub.lastSuccessfulFetchAt = 1234;
        const configs = hub.configs;
        post.resetBehavior();
        post.withArgs('http://a:1').rejects(new Error('down'));
        post.withArgs('http://b:2').onFirstCall().resolves({ data: { result: wrapped({ litecoin: {} }, 6, 6000) } });
        post.withArgs('http://b:2').onSecondCall().resolves({ data: { result: ERROR_ENVELOPE } });

        assert.strictEqual(await hub.getallconfigs(), null);
        assert.strictEqual(hub.configs, configs);
        assert.strictEqual(hub.lastWatermark, 5000);
        assert.strictEqual(hub.lastSuccessfulFetchAt, 1234);
        assert.strictEqual(hub.hubConsensusHashMismatch, true);
    });

    it('accepts a wrapped config result that also has an error field', async function(){
        const hub = new HubClient('localhost', 10000);
        const result = wrapped({ bitcoin: { mainnet: {} } }, 5, 5000, { error: 'advisory' });
        sinon.stub(axios, 'post').resolves({ data: { result } });

        assert.deepStrictEqual(await hub.getallconfigs(), result.configs);
        assert.strictEqual(typeof hub.lastSuccessfulFetchAt, 'number');
    });
});
