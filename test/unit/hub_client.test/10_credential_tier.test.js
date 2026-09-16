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

    // The hub redacts secret-bearing config params (rpc/DB passwords) unless the
    // caller asks with include_secrets. Sync is one of only two services that
    // genuinely needs them: extractDbConfigs turns this tree into the
    // replication sources' db_user/db_pass, so a redacted response points every
    // source at a password of "[redacted]".
    describe('credential tier', function(){

        const savedKeys = {};
        beforeEach(function(){
            for(const k of ['HUB_API_KEY', 'HUB_CONFIG_SECRETS_API_KEY']){
                savedKeys[k] = process.env[k];
                delete process.env[k];
            }
        });
        afterEach(function(){
            for(const [k, v] of Object.entries(savedKeys)){
                if(v === undefined) delete process.env[k];
                else process.env[k] = v;
            }
        });

        it('asks for secrets on the initial fetch', async function(){
            let post = sinon.stub(axios, 'post').resolves({ data: { result: {} } });
            await hub.getallconfigs();
            assert.deepStrictEqual(post.firstCall.args[1].params,
                { since_updated_at: 0, include_secrets: true });
        });

        it('keeps asking on the delta poll, cursor and all', async function(){
            let post = sinon.stub(axios, 'post')
                .resolves({ data: { result: { configs: {}, seq: 1, watermark: 5000 } } });
            await hub.getallconfigs();
            await hub.getallconfigs();
            // The overlap sends the cursor one second behind the stored watermark.
            assert.deepStrictEqual(post.secondCall.args[1].params,
                { since_updated_at: 4999, include_secrets: true });
        });

        it('sends HUB_CONFIG_SECRETS_API_KEY when the hub splits the credential tier', async function(){
            process.env.HUB_API_KEY = 'bulk-key';
            process.env.HUB_CONFIG_SECRETS_API_KEY = 'secrets-key';
            let post = sinon.stub(axios, 'post').resolves({ data: { result: {} } });
            await hub.getallconfigs();
            assert.strictEqual(post.firstCall.args[2].headers['x-api-key'], 'secrets-key');
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

    describe('credential tier', function(){

        const savedKeys = {};
        beforeEach(function(){
            for(const k of ['HUB_API_KEY', 'HUB_CONFIG_SECRETS_API_KEY']){
                savedKeys[k] = process.env[k];
                delete process.env[k];
            }
        });
        afterEach(function(){
            for(const [k, v] of Object.entries(savedKeys)){
                if(v === undefined) delete process.env[k];
                else process.env[k] = v;
            }
        });

        it('falls back to the bulk key when the hub does not split the tier', async function(){
            process.env.HUB_API_KEY = 'bulk-key';
            let post = sinon.stub(axios, 'post').resolves({ data: { result: {} } });
            await hub.getallconfigs();
            assert.strictEqual(post.firstCall.args[2].headers['x-api-key'], 'bulk-key');
        });

        it('names the cause once when the hub redacts anyway, not once per poll', async function(){
            sinon.stub(axios, 'post').resolves({ data: { result: {
                configs: {}, seq: 1, watermark: 5000, secrets_redacted: true, redacted_params: 6
            } } });
            await hub.getallconfigs();
            await hub.getallconfigs();
            // console.error is stubbed by the outer beforeEach.
            let hits = console.error.getCalls().filter((c) => /CREDENTIAL-REDACTED/.test(String(c.args[0])));
            assert.strictEqual(hits.length, 1);
            assert.ok(hits[0].args[0].includes('6 params withheld'), hits[0].args[0]);
        });

        it('says nothing when the hub served the credentials', async function(){
            sinon.stub(axios, 'post').resolves({ data: { result: {
                configs: {}, seq: 1, watermark: 5000, secrets_redacted: false, redacted_params: 0
            } } });
            await hub.getallconfigs();
            assert.ok(!console.error.getCalls().some((c) => /CREDENTIAL-REDACTED/.test(String(c.args[0]))));
        });
    });
});
