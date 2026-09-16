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

    describe('call multi-endpoint fallback', function(){
        it('falls back to the next endpoint and stickies the good one', async function(){
            let h = new HubClient(['http://bad:1', 'http://good:2']);
            let stub = sinon.stub(axios, 'post');
            stub.withArgs('http://bad:1').rejects({ code: 'ECONNREFUSED' });
            stub.withArgs('http://good:2').resolves({ data: { result: 'ok' } });

            let r = await h.call({});
            assert.strictEqual(r, 'ok');
            assert.strictEqual(h._lastGoodIdx, 1);
            assert.deepStrictEqual(h.lastFailures, ['http://bad:1 → ECONNREFUSED']);

            // Next call starts at the sticky good endpoint.
            stub.resetHistory();
            await h.call({});
            assert.strictEqual(stub.firstCall.args[0], 'http://good:2');
        });

        it('returns null and records failures when every endpoint fails', async function(){
            let h = new HubClient(['http://a:1', 'http://b:2']);
            let stub = sinon.stub(axios, 'post');
            stub.rejects(new Error('down'));
            let r = await h.call({});
            assert.strictEqual(r, null);
            assert.strictEqual(h.lastFailures.length, 2);
        });
    });
});
