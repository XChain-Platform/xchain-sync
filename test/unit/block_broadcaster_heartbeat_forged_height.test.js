// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// A heartbeat reports how far a replica has applied. A forged applied height
// (unsafe integer, or anything above the tip this server serves) must never make
// reported lag negative, must never be stored as a height above the tip, and must
// never make a forged-ahead validator read as caught up (lag 0, status known).

const assert = require('assert');
const sinon  = require('sinon');
const WebSocket = require('ws');
const BlockBroadcaster = require('../../src/server/block_broadcaster');

const TIP = 120675;
const FORGED = [1e21, 9007199254740993, TIP + 1000000, -1, 1.5, '1', TIP + 1];

function mockWs(){
    return {
        readyState: WebSocket.OPEN,
        bufferedAmount: 0,
        send: sinon.stub(),
        close: sinon.stub(),
        on: sinon.stub()
    };
}

function messageHandler(ws){
    let call = ws.on.getCalls().find(c => c.args[0] === 'message');
    return call ? call.args[1] : null;
}

// Send the raw JSON text so values like 9007199254740993 go through JSON.parse
// exactly as they would on the wire.
function heartbeat(handler, raw){
    handler(Buffer.from('{"type":"heartbeat","appliedBlock":' + JSON.stringify(raw) + '}'));
}

function assertSaneLag(sub){
    if(sub.appliedBlock !== null){
        assert.ok(Number.isSafeInteger(sub.appliedBlock), 'appliedBlock must be a safe integer, got ' + sub.appliedBlock);
        assert.ok(sub.appliedBlock <= TIP, 'appliedBlock ' + sub.appliedBlock + ' is above the tip ' + TIP);
    }
    if(sub.lag !== null)
        assert.ok(sub.lag >= 0, 'lag must never be negative, got ' + sub.lag);
}

let broadcaster;

function registerHooks(){
    beforeEach(function(){
        broadcaster = new BlockBroadcaster({ WS_MAX_PER_IP: 10, WS_BACKPRESSURE_MAX_BYTES: 16777216, WS_BACKPRESSURE_STALL_MS: 30000 });
        sinon.stub(console, 'log');
    });

    afterEach(function(){
        sinon.restore();
    });
}

describe('BlockBroadcaster forged heartbeat heights', function(){
    registerHooks();

    describe('WebSocket heartbeat', function(){
        let ws, handler;

        beforeEach(function(){
            ws = mockWs();
            broadcaster.addSubscription(ws, { headers: {}, socket: { remoteAddress: '127.0.0.3' } },
                'litecoin', 'regtest', 'full', 'indexer');
            handler = messageHandler(ws);
            broadcaster.broadcast('litecoin', 'regtest', { type: 'block', dbType: 'indexer', block_index: TIP }, null);
            heartbeat(handler, TIP - 6);
        });

        for(let raw of FORGED){
            it('rejects appliedBlock ' + JSON.stringify(raw) + ' and keeps the prior value', function(){
                heartbeat(handler, raw);
                let sub = broadcaster.getSubscribers('litecoin', 'regtest', 'indexer')[0];
                assertSaneLag(sub);
                assert.strictEqual(sub.appliedBlock, TIP - 6);
                assert.strictEqual(sub.lag, 6);
                assert.strictEqual(sub.lagStatus, 'known');
            });
        }

        it('accepts a heartbeat at tip - 1 and reports lag 1', function(){
            heartbeat(handler, TIP - 1);
            let sub = broadcaster.getSubscribers('litecoin', 'regtest', 'indexer')[0];
            assert.strictEqual(sub.appliedBlock, TIP - 1);
            assert.strictEqual(sub.lag, 1);
            assert.strictEqual(sub.lagStatus, 'known');
        });

        it('accepts a heartbeat at the tip and reports lag 0', function(){
            heartbeat(handler, TIP);
            let sub = broadcaster.getSubscribers('litecoin', 'regtest', 'indexer')[0];
            assert.strictEqual(sub.lag, 0);
            assert.strictEqual(sub.lagStatus, 'known');
        });

        it('accepts a height up to the served database tip when it runs ahead of the last pushed block', function(){
            broadcaster.updateStatus('litecoin', 'regtest', { dbType: 'indexer', block_height: TIP, source_block_height: TIP + 3, measured_at: Date.now() });
            heartbeat(handler, TIP + 3);
            let sub = broadcaster.getSubscribers('litecoin', 'regtest', 'indexer')[0];
            assert.strictEqual(sub.appliedBlock, TIP + 3);
            assert.strictEqual(sub.lag, 0);
            assert.strictEqual(sub.lagStatus, 'known');
        });

        it('reports lag unknown, never negative, when a stored height is above a tip that later fell', function(){
            heartbeat(handler, TIP);
            // A reorg rewinds the served tip below what the replica last reported.
            broadcaster.broadcast('litecoin', 'regtest', { type: 'block', dbType: 'indexer', block_index: TIP - 10 }, null);
            let sub = broadcaster.getSubscribers('litecoin', 'regtest', 'indexer')[0];
            assert.strictEqual(sub.lag, null);
            assert.strictEqual(sub.lagStatus, 'unknown');
        });
    });
});

describe('BlockBroadcaster forged heartbeat heights', function(){
    registerHooks();

    describe('REST validator heartbeat', function(){
        beforeEach(function(){
            broadcaster.updateStatus('litecoin', 'regtest', { dbType: 'indexer', block_height: TIP, measured_at: Date.now() });
        });

        for(let raw of FORGED){
            it('refuses applied_height ' + JSON.stringify(raw) + ' and never lists it as caught up', function(){
                let accepted = broadcaster.recordValidatorHeartbeat('litecoin', 'regtest', 'indexer', 'v-forged', raw, null);
                assert.strictEqual(accepted, false);
                let v = broadcaster.getValidatorHeartbeats('litecoin', 'regtest', 'indexer').validators['v-forged'];
                if(v){
                    assert.ok(v.applied_height <= TIP, 'stored applied_height above the tip: ' + v.applied_height);
                    assert.ok(!(v.lag_blocks === 0 && v.status === 'known'), 'forged-ahead validator reads as caught up');
                }
            });
        }

        it('keeps the prior entry when a forged height follows a valid one', function(){
            assert.strictEqual(broadcaster.recordValidatorHeartbeat('litecoin', 'regtest', 'indexer', 'v1', TIP - 1, null), true);
            broadcaster.recordValidatorHeartbeat('litecoin', 'regtest', 'indexer', 'v1', 1e21, null);
            let v = broadcaster.getValidatorHeartbeats('litecoin', 'regtest', 'indexer').validators.v1;
            assert.strictEqual(v.applied_height, TIP - 1);
            assert.strictEqual(v.lag_blocks, 1);
            assert.strictEqual(v.status, 'known');
        });

        it('reports a stored height above a tip that later fell as unknown, not caught up', function(){
            broadcaster.recordValidatorHeartbeat('litecoin', 'regtest', 'indexer', 'v2', TIP, null);
            broadcaster.updateStatus('litecoin', 'regtest', { dbType: 'indexer', block_height: TIP - 10, measured_at: Date.now() });
            let res = broadcaster.getValidatorHeartbeats('litecoin', 'regtest', 'indexer');
            assert.strictEqual(res.validators.v2.lag_blocks, null);
            assert.strictEqual(res.validators.v2.status, 'unknown');
            assert.strictEqual(res.unknown_count, 1);
        });

        it('accepts any safe height while the served tip is not yet known', function(){
            let fresh = new BlockBroadcaster({});
            assert.strictEqual(fresh.recordValidatorHeartbeat('litecoin', 'regtest', 'indexer', 'v3', 500, null), true);
            assert.strictEqual(fresh.recordValidatorHeartbeat('litecoin', 'regtest', 'indexer', 'v3', 9007199254740993, null), false);
            assert.strictEqual(fresh.getValidatorHeartbeats('litecoin', 'regtest', 'indexer').validators.v3.applied_height, 500);
        });
    });
});
