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
const HubClient = require('../../src/hub/client');

describe('HubClient', function(){

    let hub;

    beforeEach(function(){
        hub = new HubClient('localhost', 10000);
        sinon.stub(console, 'error');
    });

    afterEach(function(){
        sinon.restore();
    });

    describe('constructor', function(){
        it('builds the correct URL', function(){
            assert.strictEqual(hub.urls[0], 'http://localhost:10000');
        });
    });

    describe('ping', function(){
        it('returns true on successful response', async function(){
            sinon.stub(axios, 'post').resolves({ data: { result: true } });
            let result = await hub.ping();
            assert.strictEqual(result, true);
        });

        it('returns false on error', async function(){
            sinon.stub(axios, 'post').rejects(new Error('timeout'));
            let result = await hub.ping();
            assert.strictEqual(result, false);
        });

        it('returns falsy when result is null', async function(){
            sinon.stub(axios, 'post').resolves({ data: { result: null } });
            let result = await hub.ping();
            assert.ok(!result);
        });

        it('sends correct JSON-RPC payload', async function(){
            let stub = sinon.stub(axios, 'post').resolves({ data: { result: true } });
            await hub.ping();
            let payload = stub.firstCall.args[1];
            assert.strictEqual(payload.method, 'ping');
            assert.strictEqual(payload.jsonrpc, '2.0');
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

    describe('getallconfigs', function(){
        it('returns parsed result on success', async function(){
            let mockResult = { bitcoin: { mainnet: {} } };
            sinon.stub(axios, 'post').resolves({ data: { result: mockResult } });
            let result = await hub.getallconfigs();
            assert.deepStrictEqual(result, mockResult);
        });

        it('returns null on error', async function(){
            sinon.stub(axios, 'post').rejects(new Error('fail'));
            let result = await hub.getallconfigs();
            assert.strictEqual(result, null);
        });

        it('returns null when no result in response', async function(){
            sinon.stub(axios, 'post').resolves({ data: {} });
            let result = await hub.getallconfigs();
            assert.strictEqual(result, null);
        });
    });
});
