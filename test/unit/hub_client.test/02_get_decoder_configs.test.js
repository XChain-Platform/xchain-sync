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

    describe('getDecoderConfigs', function(){
        it('extracts xchain-decoder entries', async function(){
            sinon.stub(axios, 'post').resolves({ data: { result: {
                bitcoin: { mainnet: { 'xchain-decoder': { db_host: 'dh', db_port: '3309', name: 'dec', user: 'u', pass: 'p' } } }
            }}});
            let configs = await hub.getDecoderConfigs();
            assert.strictEqual(configs.length, 1);
            assert.strictEqual(configs[0].dbType, 'decoder');
            assert.strictEqual(configs[0].db_port, 3309);
        });

        it('skips non-object coin/network values defensively', async function(){
            sinon.stub(axios, 'post').resolves({ data: { result: {
                bitcoin: 'not-an-object',
                litecoin: { mainnet: 'also-not-an-object' }
            }}});
            let configs = await hub.getDecoderConfigs();
            assert.deepStrictEqual(configs, []);
        });
    });
});
