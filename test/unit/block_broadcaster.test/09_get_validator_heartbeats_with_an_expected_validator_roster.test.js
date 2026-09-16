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
const WebSocket = require('ws');
const BlockBroadcaster = require('../../../src/server/block_broadcaster');

function mockWs(ip){
    let ws = {
        readyState: WebSocket.OPEN,
        bufferedAmount: 0,
        _syncBuffered: 0,
        _syncChain: null,
        _syncNetwork: null,
        _syncIp: null,
        send: sinon.stub(),
        close: sinon.stub(),
        on: sinon.stub()
    };
    return ws;
}

function mockReq(ip){
    return { headers: {}, socket: { remoteAddress: ip || '127.0.0.1' } };
}

let broadcaster, config;

function registerHooks(){
    beforeEach(function(){
        config = { WS_MAX_PER_IP: 3, WS_BACKPRESSURE_LIMIT: 50 };
        broadcaster = new BlockBroadcaster(config);
        sinon.stub(console, 'log');
    });

    afterEach(function(){
        sinon.restore();
    });
}

describe('BlockBroadcaster', function(){
    registerHooks();

    describe('getValidatorHeartbeats with an expected-validator roster', function(){
        let rostered;
        beforeEach(function(){
            rostered = new BlockBroadcaster({ EXPECTED_VALIDATORS: ['val-a', 'val-b', 'val-c'] });
        });

        it('surfaces roster members that have never reported as status absent', function(){
            rostered.recordValidatorHeartbeat('bitcoin', 'mainnet', 'indexer', 'val-a', 500, null);
            let res = rostered.getValidatorHeartbeats('bitcoin', 'mainnet', 'indexer');
            // expected_total is the denominator; total counts only the one that reported.
            assert.strictEqual(res.expected_total, 3);
            assert.strictEqual(res.total, 1);
            // val-b and val-c never POSTed → absent, with null lag/last_seen.
            assert.strictEqual(res.validators['val-b'].status, 'absent');
            assert.strictEqual(res.validators['val-b'].lag_blocks, null);
            assert.strictEqual(res.validators['val-b'].last_seen, null);
            assert.strictEqual(res.validators['val-c'].status, 'absent');
        });

        it('reports all roster members absent when none have reported', function(){
            let res = rostered.getValidatorHeartbeats('bitcoin', 'mainnet', 'indexer');
            assert.strictEqual(res.expected_total, 3);
            assert.strictEqual(res.total, 0);
            assert.strictEqual(Object.keys(res.validators).length, 3);
            assert.ok(['val-a','val-b','val-c'].every(id => res.validators[id].status === 'absent'));
        });

        it('does not duplicate a reporting roster member as absent', function(){
            rostered.updateStatus('bitcoin', 'mainnet', { dbType: 'indexer', block_height: 510 });
            rostered.recordValidatorHeartbeat('bitcoin', 'mainnet', 'indexer', 'val-a', 500, null);
            let res = rostered.getValidatorHeartbeats('bitcoin', 'mainnet', 'indexer');
            assert.strictEqual(res.validators['val-a'].status, 'known');
            assert.strictEqual(res.validators['val-a'].lag_blocks, 10);
        });

        it('reports a non-roster reporter alongside absent roster members', function(){
            rostered.recordValidatorHeartbeat('bitcoin', 'mainnet', 'indexer', 'rogue', 500, null);
            let res = rostered.getValidatorHeartbeats('bitcoin', 'mainnet', 'indexer');
            // total counts the off-roster reporter; expected_total stays the roster size.
            assert.strictEqual(res.total, 1);
            assert.strictEqual(res.expected_total, 3);
            assert.ok(res.validators['rogue']);
            assert.strictEqual(res.validators['val-a'].status, 'absent');
        });
    });
});
