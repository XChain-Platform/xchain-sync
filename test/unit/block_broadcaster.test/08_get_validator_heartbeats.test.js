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

    describe('getValidatorHeartbeats', function(){
        it('returns empty structure with zero counts when no validators reported', function(){
            let res = broadcaster.getValidatorHeartbeats('bitcoin', 'mainnet', 'indexer');
            assert.deepStrictEqual(res, { validators: {}, total: 0, expected_total: null, unknown_count: 0 });
        });

        it('marks a validator unknown when source height is not yet known', function(){
            // No status data → sourceHeight null → lag_blocks null → status 'unknown'.
            broadcaster.recordValidatorHeartbeat('bitcoin', 'mainnet', 'indexer', 'val-a', 500, null);
            let res = broadcaster.getValidatorHeartbeats('bitcoin', 'mainnet', 'indexer');
            assert.strictEqual(res.total, 1);
            assert.strictEqual(res.unknown_count, 1);
            assert.strictEqual(res.validators['val-a'].lag_blocks, null);
            assert.strictEqual(res.validators['val-a'].status, 'unknown');
        });

        it('marks a validator known with a computed lag once source height is set', function(){
            broadcaster.updateStatus('bitcoin', 'mainnet', { dbType: 'indexer', block_height: 510 });
            broadcaster.recordValidatorHeartbeat('bitcoin', 'mainnet', 'indexer', 'val-b', 500, null);
            let res = broadcaster.getValidatorHeartbeats('bitcoin', 'mainnet', 'indexer');
            assert.strictEqual(res.total, 1);
            assert.strictEqual(res.unknown_count, 0);
            assert.strictEqual(res.validators['val-b'].lag_blocks, 10);
            assert.strictEqual(res.validators['val-b'].status, 'known');
        });

        it('reports a caught-up validator as known with lag 0', function(){
            broadcaster.updateStatus('bitcoin', 'mainnet', { dbType: 'indexer', block_height: 500 });
            broadcaster.recordValidatorHeartbeat('bitcoin', 'mainnet', 'indexer', 'val-c', 500, null);
            let res = broadcaster.getValidatorHeartbeats('bitcoin', 'mainnet', 'indexer');
            assert.strictEqual(res.validators['val-c'].lag_blocks, 0);
            assert.strictEqual(res.validators['val-c'].status, 'known');
            assert.strictEqual(res.unknown_count, 0);
        });

        it('counts only the unknown-lag validators in unknown_count across a mixed set', function(){
            broadcaster.updateStatus('bitcoin', 'mainnet', { dbType: 'indexer', block_height: 600 });
            broadcaster.recordValidatorHeartbeat('bitcoin', 'mainnet', 'indexer', 'known-1', 600, null);
            broadcaster.recordValidatorHeartbeat('bitcoin', 'mainnet', 'indexer', 'known-2', 590, null);
            // A different chain has no status data, so its validator stays unknown, but
            // it must not bleed into the bitcoin/mainnet tally.
            broadcaster.recordValidatorHeartbeat('litecoin', 'mainnet', 'indexer', 'unk-1', 100, null);
            let res = broadcaster.getValidatorHeartbeats('bitcoin', 'mainnet', 'indexer');
            assert.strictEqual(res.total, 2);
            assert.strictEqual(res.unknown_count, 0);
            let ltc = broadcaster.getValidatorHeartbeats('litecoin', 'mainnet', 'indexer');
            assert.strictEqual(ltc.total, 1);
            assert.strictEqual(ltc.unknown_count, 1);
        });
    });
});
