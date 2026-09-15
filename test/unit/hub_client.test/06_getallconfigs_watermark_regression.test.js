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

    describe('getallconfigs watermark regression', function(){
        it('discards the cache and re-fetches full when the same endpoint serves a lower watermark', async function(){
            let stub = sinon.stub(axios, 'post');
            // First fetch: full tree, watermark advances to 5000.
            stub.onCall(0).resolves({ data: { result: {
                configs: { btc: { main: { 'xchain-indexer': { name: 'a' } } } },
                seq: 5, watermark: 5000
            } } });
            let first = await hub.getallconfigs();
            assert.ok(first.btc.main['xchain-indexer']);
            assert.strictEqual(hub.lastWatermark, 5000);

            // Second fetch: same endpoint answers (no failover), but the hub restarted
            // and its watermark regressed below what it served before.
            stub.onCall(1).resolves({ data: { result: {
                configs: { ltc: { test: { 'xchain-indexer': { name: 'b' } } } },
                seq: 1, watermark: 100
            } } });
            // Regression re-fetch (since_updated_at: 0) after the regression is detected.
            stub.onCall(2).resolves({ data: { result: {
                configs: { ltc: { test: { 'xchain-indexer': { name: 'b' } } } },
                seq: 1, watermark: 100
            } } });
            let second = await hub.getallconfigs();

            // Cache was discarded (not merged): the old btc branch is gone.
            assert.strictEqual(second.btc, undefined, 'dropped the stale cached branch');
            assert.ok(second.ltc.test['xchain-indexer'], 'has the restored hub tree');
            assert.strictEqual(hub.lastWatermark, 100);
            assert.strictEqual(hub.lastSeq, 1);
            // The re-fetch after regression asked for the full tree.
            assert.strictEqual(stub.getCall(2).args[1].params.since_updated_at, 0);
            assert.ok(console.error.getCalls().some((c) => /HUB CONFIG REGRESSION/.test(String(c.args[0]))));
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

    describe('getallconfigs watermark regression', function(){
        it('does not treat a regressed-but-unwrapped (no seq/configs) payload as a regression', async function(){
            let stub = sinon.stub(axios, 'post');
            stub.onCall(0).resolves({ data: { result: {
                configs: { btc: { main: {} } }, seq: 5, watermark: 5000
            } } });
            await hub.getallconfigs();
            // Bare-map (older hub) response: no seq/configs wrapper, so it cannot regress.
            stub.onCall(1).resolves({ data: { result: { ltc: { test: {} } } } });
            let second = await hub.getallconfigs();
            assert.deepStrictEqual(second, { ltc: { test: {} } });
            assert.strictEqual(hub.lastWatermark, 0);
            assert.ok(!console.error.getCalls().some((c) => /HUB CONFIG REGRESSION/.test(String(c.args[0]))));
        });
    });
});
