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

// statusData is overwrite-only and ServerPoller.updateStatus writes it only on a
// SUCCESSFUL database read, with its callers swallowing the rejection. So a failed,
// hung or stopped poller leaves the last HEALTHY object in the cache and every
// reader keeps re-serving it: the 60s broadcast, the new-subscriber snapshot, the
// validator-lag view and REST /status. Status events keep arriving on time, so the
// consumer's own silence timeout never fires and lag_blocks 0 is certified forever.
const FRESH = { dbType: 'indexer', block_height: 200, source_block_height: 200,
                replica_stale: false, replica_seconds_behind: 2 };

describe('BlockBroadcaster', function(){
    registerHooks();

    describe('getStatus freshness expiry', function(){
        it('returns a freshly measured status unchanged', function(){
            broadcaster.config['SYNC_STATUS_MAX_AGE_MS'] = 180000;
            broadcaster.updateStatus('bitcoin', 'mainnet',
                Object.assign({}, FRESH, { measured_at: Date.now() }));

            let status = broadcaster.getStatus('bitcoin', 'mainnet', 'indexer');
            assert.strictEqual(status.replica_stale, false, 'a live measurement still certifies');
            assert.strictEqual(status.replica_seconds_behind, 2);
            assert.strictEqual(status.status_stale, undefined);
        });

        it('expires the freshness verdict of a status measured before the poller stopped', function(){
            broadcaster.config['SYNC_STATUS_MAX_AGE_MS'] = 180000;
            broadcaster.updateStatus('bitcoin', 'mainnet',
                Object.assign({}, FRESH, { measured_at: Date.now() - 200000 }));

            let status = broadcaster.getStatus('bitcoin', 'mainnet', 'indexer');
            assert.strictEqual(status.replica_stale, true,
                'a measurement this old must not keep certifying freshness');
            assert.strictEqual(status.replica_seconds_behind, null, 'the lag figure is unknowable now');
            assert.strictEqual(status.status_stale, true);
            assert.ok(status.measured_age_ms >= 200000, 'the age is reported for the operator');
            assert.strictEqual(status.block_height, 200,
                'heights are kept: they are the diagnostics the outage needs');
        });
    });
});

describe('BlockBroadcaster', function(){
    registerHooks();

    describe('getStatus freshness expiry', function(){
        it('does not mutate the cached object, so a recovered poll is not poisoned', function(){
            broadcaster.config['SYNC_STATUS_MAX_AGE_MS'] = 180000;
            let cached = Object.assign({}, FRESH, { measured_at: Date.now() - 200000 });
            broadcaster.updateStatus('bitcoin', 'mainnet', cached);

            broadcaster.getStatus('bitcoin', 'mainnet', 'indexer');

            assert.strictEqual(cached.replica_stale, false, 'the demotion is a copy, never in place');
            assert.strictEqual(cached.status_stale, undefined);
        });

        it('leaves an UNDATED status alone rather than demoting on a guess', function(){
            broadcaster.updateStatus('bitcoin', 'mainnet', Object.assign({}, FRESH));
            let status = broadcaster.getStatus('bitcoin', 'mainnet', 'indexer');
            assert.strictEqual(status.replica_stale, false);
            assert.strictEqual(status.status_stale, undefined);
        });

        it('broadcastStatus publishes the expired verdict, not the cached healthy one', function(){
            broadcaster.config['SYNC_STATUS_MAX_AGE_MS'] = 180000;
            broadcaster.updateStatus('bitcoin', 'mainnet',
                Object.assign({}, FRESH, { measured_at: Date.now() - 200000 }));
            let ws = mockWs();
            broadcaster.addSubscription(ws, mockReq(), 'bitcoin', 'mainnet');
            ws.send.resetHistory();

            broadcaster.broadcastStatus('bitcoin', 'mainnet');

            let sent = JSON.parse(ws.send.firstCall.args[0]);
            assert.strictEqual(sent.replica_stale, true,
                'the periodic rebroadcast is the path that certified freshness after polling failed');
            assert.strictEqual(sent.status_stale, true);
        });
    });
});
