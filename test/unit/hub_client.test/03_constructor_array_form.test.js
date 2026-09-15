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

    describe('constructor: array form', function(){
        it('uses an explicit endpoint array verbatim', function(){
            let h = new HubClient(['https://a:1', 'http://b:2']);
            assert.deepStrictEqual(h.urls, ['https://a:1', 'http://b:2']);
        });

        it('honors https via the legacy port arg', function(){
            let h = new HubClient('host', 'https');
            assert.strictEqual(h.urls[0], 'https://host:10000');
        });

        it('defaults a non-numeric port to 10000', function(){
            let h = new HubClient('host', 'notaport');
            assert.strictEqual(h.urls[0], 'http://host:10000');
        });
    });
});
