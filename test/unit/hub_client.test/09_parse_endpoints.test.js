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

    describe('parseEndpoints', function(){
        it('parses a comma-separated HUB_VALIDATORS list, prefixing bare hosts', function(){
            let urls = HubClient.parseEndpoints({ HUB_VALIDATORS: 'http://a:1, b:2 ,' });
            assert.deepStrictEqual(urls, ['http://a:1', 'http://b:2']);
        });
        it('prefixes bare HUB_VALIDATORS hosts with https when configured', function(){
            let urls = HubClient.parseEndpoints({ HUB_VALIDATORS: 'b:2', HUB_PROTOCOL: 'https' });
            assert.deepStrictEqual(urls, ['https://b:2']);
        });
        it('falls back to HUB_API_HOST/HUB_PORT when no validators are set', function(){
            assert.deepStrictEqual(
                HubClient.parseEndpoints({ HUB_API_HOST: 'h', HUB_PORT: 9000 }),
                ['http://h:9000']);
        });
        it('uses https in the fallback path when HUB_PROTOCOL is https', function(){
            assert.deepStrictEqual(
                HubClient.parseEndpoints({ HUB_API_HOST: 'h', HUB_PORT: 9000, HUB_PROTOCOL: 'https' }),
                ['https://h:9000']);
        });
        it('defaults host/port/proto when nothing is configured', function(){
            assert.deepStrictEqual(HubClient.parseEndpoints({}), ['http://localhost:10000']);
        });
    });
});
