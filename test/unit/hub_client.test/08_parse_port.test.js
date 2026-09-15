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

    describe('parsePort', function(){
        it('parses a numeric primary', function(){
            assert.strictEqual(HubClient.parsePort('3307', undefined), 3307);
        });
        it('uses the fallback when primary is absent/empty', function(){
            assert.strictEqual(HubClient.parsePort('', '3308'), 3308);
            assert.strictEqual(HubClient.parsePort(undefined, 3309), 3309);
        });
        it('preserves a literal 0 (does not fall through to default)', function(){
            assert.strictEqual(HubClient.parsePort(0, 9999), 0);
        });
        it('defaults to 3306 when both are absent', function(){
            assert.strictEqual(HubClient.parsePort(undefined, undefined), 3306);
        });
        it('defaults to 3306 for a non-numeric or negative value', function(){
            assert.strictEqual(HubClient.parsePort('abc', undefined), 3306);
            assert.strictEqual(HubClient.parsePort('-1', undefined), 3306);
        });
    });
});
