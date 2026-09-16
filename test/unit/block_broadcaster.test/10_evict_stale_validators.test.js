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

    describe('evictStaleValidators', function(){
        it('transitions an entry past the TTL to stale instead of deleting it', function(){
            broadcaster.updateStatus('bitcoin', 'mainnet', { dbType: 'indexer', block_height: 510 });
            broadcaster.recordValidatorHeartbeat('bitcoin', 'mainnet', 'indexer', 'val-a', 500, null);
            // Backdate last_seen so the entry is older than the threshold.
            let map = broadcaster.validatorHeartbeats.get('bitcoin:mainnet:indexer');
            map.get('val-a').last_seen = Date.now() - 100000;

            broadcaster.evictStaleValidators(60000);

            // Still present in the map and surfaced as 'stale' with its last applied_height.
            // Stale entries are excluded from total so a going-stale validator
            // decrements the count (the operator-visible erosion signal).
            let res = broadcaster.getValidatorHeartbeats('bitcoin', 'mainnet', 'indexer');
            assert.strictEqual(res.total, 0);
            assert.ok(res.validators['val-a']);
            assert.strictEqual(res.validators['val-a'].status, 'stale');
            assert.strictEqual(res.validators['val-a'].applied_height, 500);
            assert.ok(res.validators['val-a'].evicted_at);
        });

        it('leaves a fresh entry untouched', function(){
            broadcaster.recordValidatorHeartbeat('bitcoin', 'mainnet', 'indexer', 'val-a', 500, null);
            broadcaster.evictStaleValidators(60000);
            let res = broadcaster.getValidatorHeartbeats('bitcoin', 'mainnet', 'indexer');
            assert.strictEqual(res.validators['val-a'].status, 'unknown');
        });

        it('restores a stale validator to known/unknown on the next heartbeat', function(){
            broadcaster.recordValidatorHeartbeat('bitcoin', 'mainnet', 'indexer', 'val-a', 500, null);
            let map = broadcaster.validatorHeartbeats.get('bitcoin:mainnet:indexer');
            map.get('val-a').last_seen = Date.now() - 100000;
            broadcaster.evictStaleValidators(60000);
            assert.strictEqual(map.get('val-a').status, 'stale');

            // A new POST overwrites the entry with a fresh, status-less record.
            broadcaster.recordValidatorHeartbeat('bitcoin', 'mainnet', 'indexer', 'val-a', 520, null);
            let res = broadcaster.getValidatorHeartbeats('bitcoin', 'mainnet', 'indexer');
            assert.strictEqual(res.validators['val-a'].status, 'unknown');
            assert.strictEqual(map.get('val-a').status, undefined);
        });
    });
});

describe('BlockBroadcaster', function(){
    registerHooks();

    describe('evictStaleValidators', function(){
        it('hard-removes a non-roster entry that stays stale past a second TTL window', function(){
            broadcaster.recordValidatorHeartbeat('bitcoin', 'mainnet', 'indexer', 'rogue', 500, null);
            let map = broadcaster.validatorHeartbeats.get('bitcoin:mainnet:indexer');
            map.get('rogue').last_seen = Date.now() - 100000;
            broadcaster.evictStaleValidators(60000);          // active → stale
            assert.strictEqual(map.get('rogue').status, 'stale');

            // Backdate evicted_at beyond a second threshold window → hard-removed.
            map.get('rogue').evicted_at = Date.now() - 100000;
            broadcaster.evictStaleValidators(60000);
            assert.strictEqual(broadcaster.validatorHeartbeats.has('bitcoin:mainnet:indexer'), false);
        });

        it('keeps a roster member visible as stale indefinitely', function(){
            let rostered = new BlockBroadcaster({ EXPECTED_VALIDATORS: ['val-a'] });
            rostered.recordValidatorHeartbeat('bitcoin', 'mainnet', 'indexer', 'val-a', 500, null);
            let map = rostered.validatorHeartbeats.get('bitcoin:mainnet:indexer');
            map.get('val-a').last_seen = Date.now() - 100000;
            rostered.evictStaleValidators(60000);             // active → stale
            // Even long past the second window, a roster member is not hard-removed.
            map.get('val-a').evicted_at = Date.now() - 100000;
            rostered.evictStaleValidators(60000);
            assert.strictEqual(map.get('val-a').status, 'stale');
        });
    });
});
