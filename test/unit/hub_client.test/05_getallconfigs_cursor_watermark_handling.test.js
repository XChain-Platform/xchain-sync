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
const sinon  = require('sinon');
const axios  = require('axios');
const HubClient = require('../../../src/hub/client');

describe('HubClient', function(){

    let hub;

    beforeEach(function(){
        hub = new HubClient('localhost', 10000);
        sinon.stub(console, 'error');
    });

    afterEach(function(){
        sinon.restore();
    });

    describe('getallconfigs cursor + watermark handling', function(){
        it('records lastSuccessfulFetchAt on a successful bare-map fetch and resets the cursor', async function(){
            sinon.stub(axios, 'post').resolves({ data: { result: { bitcoin: { mainnet: {} } } } });
            await hub.getallconfigs();
            assert.strictEqual(typeof hub.lastSuccessfulFetchAt, 'number');
            assert.strictEqual(hub.lastWatermark, 0);
            assert.strictEqual(hub.lastSeq, 0);
        });

        it('unwraps a { configs, seq } payload (no watermark) and resets the cursor', async function(){
            sinon.stub(axios, 'post').resolves({ data: { result: {
                configs: { bitcoin: { mainnet: {} } }, seq: 5
            } } });
            let r = await hub.getallconfigs();
            assert.deepStrictEqual(r, { bitcoin: { mainnet: {} } });
            assert.strictEqual(hub.lastSeq, 5);
            assert.strictEqual(hub.lastWatermark, 0);
        });
    });
});

describe('HubClient', function(){

    let hub;

    beforeEach(function(){
        hub = new HubClient('localhost', 10000);
        sinon.stub(console, 'error');
    });

    afterEach(function(){
        sinon.restore();
    });

    describe('getallconfigs cursor + watermark handling', function(){
        it('merges a delta against the cursor it previously sent', async function(){
            let stub = sinon.stub(axios, 'post');
            // First fetch: full tree + watermark advances the cursor.
            stub.onCall(0).resolves({ data: { result: {
                configs: { btc: { main: { 'xchain-indexer': { name: 'a' } } } },
                seq: 1, watermark: 1000
            } } });
            let first = await hub.getallconfigs();
            assert.strictEqual(hub.lastWatermark, 1000);
            assert.ok(first.btc.main['xchain-indexer']);

            // Second fetch: client now sends since_updated_at=1000; hub replies with a delta
            // that adds a module to an existing branch AND introduces a brand-new coin/network.
            stub.onCall(1).resolves({ data: { result: {
                configs: {
                    btc: { main: { 'xchain-decoder': { name: 'b' } } },
                    ltc: { test: { 'xchain-indexer': { name: 'c' } } }
                },
                seq: 2, watermark: 2000
            } } });
            let second = await hub.getallconfigs();
            // Delta merged into the cache: both modules present.
            assert.ok(second.btc.main['xchain-indexer'], 'kept the prior module');
            assert.ok(second.btc.main['xchain-decoder'], 'merged the delta module');
            assert.ok(second.ltc.test['xchain-indexer'], 'created the brand-new coin branch');
            assert.strictEqual(hub.lastWatermark, 2000);
            // The second request echoed the cursor one second behind the stored
            // watermark (the overlap), not the raw watermark.
            assert.strictEqual(stub.secondCall.args[1].params.since_updated_at, 999);
        });
    });
});

describe('HubClient', function(){

    let hub;

    beforeEach(function(){
        hub = new HubClient('localhost', 10000);
        sinon.stub(console, 'error');
    });

    afterEach(function(){
        sinon.restore();
    });

    describe('getallconfigs cursor + watermark handling', function(){
        it('treats a watermarked payload as a full tree on the first fetch (no cursor sent yet)', async function(){
            sinon.stub(axios, 'post').resolves({ data: { result: {
                configs: { btc: { main: {} } }, seq: 1, watermark: 500
            } } });
            let r = await hub.getallconfigs();
            assert.deepStrictEqual(r, { btc: { main: {} } });
            assert.strictEqual(hub.lastWatermark, 500);
        });

        it('resets the cursor and re-fetches full when it fails over to a different endpoint', async function(){
            // A wall-clock cursor from hub A is not valid against hub B (each stamps
            // updated_at at its own apply time), so on failover the client must discard
            // the stale-cursor delta and re-fetch the full tree from the new endpoint.
            let h = new HubClient(['http://a:1', 'http://b:2']);
            let stub = sinon.stub(axios, 'post');
            // Poll 1: endpoint A serves a full tree and advances the cursor to 1000.
            stub.withArgs('http://a:1').onFirstCall().resolves({ data: { result: {
                configs: { btc: { main: { 'xchain-indexer': { name: 'a' } } } }, seq: 1, watermark: 1000
            } } });
            // Poll 2: endpoint A is down; endpoint B serves a different full tree (wm 2000).
            stub.withArgs('http://a:1').onSecondCall().rejects({ code: 'ECONNREFUSED' });
            stub.withArgs('http://b:2').resolves({ data: { result: {
                configs: { ltc: { test: { 'xchain-indexer': { name: 'b' } } } }, seq: 2, watermark: 2000
            } } });

            await h.getallconfigs();
            assert.strictEqual(h.lastWatermark, 1000);
            assert.strictEqual(h._watermarkEndpointIdx, 0);

            let second = await h.getallconfigs();
            // The client re-requested the full tree from B with a reset cursor.
            let resetCall = stub.getCalls().find(c =>
                c.args[0] === 'http://b:2' && c.args[1].params.since_updated_at === 0);
            assert.ok(resetCall, 'expected a since_updated_at=0 re-fetch against the new endpoint');
            // Full replace from B: A's cached branch is gone (not cross-hub merged).
            assert.ok(second.ltc.test['xchain-indexer'], 'has the new endpoint tree');
            assert.strictEqual(second.btc, undefined, 'dropped the old endpoint tree (no stale merge)');
            assert.strictEqual(h.lastWatermark, 2000);
            assert.strictEqual(h._watermarkEndpointIdx, 1);
        });
    });
});
