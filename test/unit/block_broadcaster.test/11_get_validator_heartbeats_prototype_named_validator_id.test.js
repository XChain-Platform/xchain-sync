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
const BlockBroadcaster = require('../../../src/server/block_broadcaster');

let broadcaster, config;

describe('BlockBroadcaster getValidatorHeartbeats with prototype-named validator ids', function(){
    beforeEach(function(){
        config = { WS_MAX_PER_IP: 3, WS_BACKPRESSURE_LIMIT: 50 };
        sinon.stub(console, 'log');
    });

    afterEach(function(){
        sinon.restore();
    });

    it('keeps a __proto__ validator visible as an own key and does not re-parent the map', function(){
        broadcaster = new BlockBroadcaster(config);
        broadcaster.recordValidatorHeartbeat('bitcoin', 'regtest', 'indexer', '__proto__', 5, null);
        broadcaster.recordValidatorHeartbeat('bitcoin', 'regtest', 'indexer', 'ok', 5, null);
        let r = broadcaster.getValidatorHeartbeats('bitcoin', 'regtest', 'indexer');

        assert.strictEqual(r.total, 2);
        assert.strictEqual(Object.keys(r.validators).length, r.total);
        assert.ok(Object.keys(r.validators).includes('__proto__'));
        assert.ok(Object.keys(r.validators).includes('ok'));
        assert.ok(Object.hasOwn(r.validators, '__proto__'));
        assert.strictEqual(r.validators['__proto__'].applied_height, 5);
        assert.strictEqual(Object.getPrototypeOf(r.validators), Object.prototype);
        assert.strictEqual(({}).applied_height, undefined);
        assert.ok(Object.hasOwn(JSON.parse(JSON.stringify(r.validators)), '__proto__'));
    });

    it('reports a roster member named __proto__ as absent when it never reported', function(){
        config.EXPECTED_VALIDATORS = ['__proto__'];
        broadcaster = new BlockBroadcaster(config);
        let r = broadcaster.getValidatorHeartbeats('bitcoin', 'regtest', 'indexer');
        assert.ok(Object.hasOwn(r.validators, '__proto__'));
        assert.strictEqual(r.validators['__proto__'].status, 'absent');
    });

    it('does not overwrite a reporting __proto__ roster member with absent', function(){
        config.EXPECTED_VALIDATORS = ['__proto__'];
        broadcaster = new BlockBroadcaster(config);
        broadcaster.recordValidatorHeartbeat('bitcoin', 'regtest', 'indexer', '__proto__', 5, null);
        let r = broadcaster.getValidatorHeartbeats('bitcoin', 'regtest', 'indexer');
        assert.strictEqual(r.validators['__proto__'].status, 'unknown');
        assert.strictEqual(r.validators['__proto__'].applied_height, 5);
    });

    it('does not treat inherited Object.prototype names as already reported roster members', function(){
        config.EXPECTED_VALIDATORS = ['constructor', 'toString'];
        broadcaster = new BlockBroadcaster(config);
        let r = broadcaster.getValidatorHeartbeats('bitcoin', 'regtest', 'indexer');
        assert.strictEqual(r.validators['constructor'].status, 'absent');
        assert.strictEqual(r.validators['toString'].status, 'absent');
    });
});
