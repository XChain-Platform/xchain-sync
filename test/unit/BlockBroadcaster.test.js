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
const BlockBroadcaster = require('../../src/BlockBroadcaster');

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

describe('BlockBroadcaster', function(){

    let broadcaster, config;

    beforeEach(function(){
        config = { WS_MAX_PER_IP: 3, WS_BACKPRESSURE_LIMIT: 50 };
        broadcaster = new BlockBroadcaster(config);
        sinon.stub(console, 'log');
    });

    afterEach(function(){
        sinon.restore();
    });

    describe('addSubscription', function(){
        it('adds ws to subscribers set', function(){
            let ws = mockWs();
            let req = mockReq('1.1.1.1');
            let result = broadcaster.addSubscription(ws, req, 'bitcoin', 'mainnet');
            assert.strictEqual(result, true);
            assert.strictEqual(broadcaster.getSubscriberCount('bitcoin', 'mainnet'), 1);
        });

        it('sets metadata on ws', function(){
            let ws = mockWs();
            broadcaster.addSubscription(ws, mockReq('1.1.1.1'), 'bitcoin', 'mainnet');
            assert.strictEqual(ws._syncChain, 'bitcoin');
            assert.strictEqual(ws._syncNetwork, 'mainnet');
            assert.strictEqual(ws._syncIp, '1.1.1.1');
        });

        it('rejects when per-IP limit exceeded', function(){
            let req = mockReq('2.2.2.2');
            for(let i = 0; i < 3; i++){
                broadcaster.addSubscription(mockWs(), req, 'bitcoin', 'mainnet');
            }
            let ws4 = mockWs();
            let result = broadcaster.addSubscription(ws4, req, 'bitcoin', 'mainnet');
            assert.strictEqual(result, false);
            assert.strictEqual(ws4.close.calledOnce, true);
        });

        it('sends initial status if available', function(){
            broadcaster.updateStatus('bitcoin', 'mainnet', { block_height: 100 });
            let ws = mockWs();
            broadcaster.addSubscription(ws, mockReq(), 'bitcoin', 'mainnet');
            assert.strictEqual(ws.send.calledOnce, true);
            let sent = JSON.parse(ws.send.firstCall.args[0]);
            assert.strictEqual(sent.type, 'status');
            assert.strictEqual(sent.block_height, 100);
        });

        it('does not send status if none available', function(){
            let ws = mockWs();
            broadcaster.addSubscription(ws, mockReq(), 'bitcoin', 'mainnet');
            assert.strictEqual(ws.send.called, false);
        });

        it('registers close and error handlers', function(){
            let ws = mockWs();
            broadcaster.addSubscription(ws, mockReq(), 'bitcoin', 'mainnet');
            assert.strictEqual(ws.on.calledWith('close'), true);
            assert.strictEqual(ws.on.calledWith('error'), true);
        });

        it('uses x-forwarded-for when TRUST_PROXY is true', function(){
            let trustedBroadcaster = new BlockBroadcaster({ WS_MAX_PER_IP: 3, WS_BACKPRESSURE_LIMIT: 50, TRUST_PROXY: true });
            let ws = mockWs();
            let req = { headers: { 'x-forwarded-for': '9.9.9.9' }, socket: { remoteAddress: '1.1.1.1' } };
            trustedBroadcaster.addSubscription(ws, req, 'bitcoin', 'mainnet');
            assert.strictEqual(ws._syncIp, '9.9.9.9');
        });
    });

    describe('removeSubscription', function(){
        it('removes from subscribers and ipConnections', function(){
            let ws = mockWs();
            broadcaster.addSubscription(ws, mockReq('3.3.3.3'), 'bitcoin', 'mainnet');
            assert.strictEqual(broadcaster.getSubscriberCount('bitcoin', 'mainnet'), 1);
            broadcaster.removeSubscription(ws);
            assert.strictEqual(broadcaster.getSubscriberCount('bitcoin', 'mainnet'), 0);
        });

        it('clears metadata on ws', function(){
            let ws = mockWs();
            broadcaster.addSubscription(ws, mockReq(), 'bitcoin', 'mainnet');
            broadcaster.removeSubscription(ws);
            assert.strictEqual(ws._syncChain, null);
            assert.strictEqual(ws._syncNetwork, null);
        });

        it('handles ws with no metadata gracefully', function(){
            let ws = mockWs();
            broadcaster.removeSubscription(ws); // should not throw
        });

        it('cleans up empty sets from maps', function(){
            let ws = mockWs();
            broadcaster.addSubscription(ws, mockReq('4.4.4.4'), 'bitcoin', 'mainnet');
            broadcaster.removeSubscription(ws);
            assert.strictEqual(broadcaster.subscribers.has('bitcoin:mainnet'), false);
            assert.strictEqual(broadcaster.ipConnections.has('4.4.4.4'), false);
        });
    });

    describe('broadcast', function(){
        it('sends to all subscribers of a chain/network', function(){
            let ws1 = mockWs(), ws2 = mockWs();
            broadcaster.addSubscription(ws1, mockReq('5.5.5.5'), 'bitcoin', 'mainnet');
            broadcaster.addSubscription(ws2, mockReq('6.6.6.6'), 'bitcoin', 'mainnet');
            broadcaster.broadcast('bitcoin', 'mainnet', { type: 'block', data: 'x' });
            assert.strictEqual(ws1.send.calledOnce, true);
            assert.strictEqual(ws2.send.calledOnce, true);
        });

        it('does not send to other chain/network', function(){
            let ws1 = mockWs(), ws2 = mockWs();
            broadcaster.addSubscription(ws1, mockReq('5.5.5.5'), 'bitcoin', 'mainnet');
            broadcaster.addSubscription(ws2, mockReq('6.6.6.6'), 'litecoin', 'mainnet');
            broadcaster.broadcast('bitcoin', 'mainnet', { type: 'block' });
            assert.strictEqual(ws1.send.calledOnce, true);
            assert.strictEqual(ws2.send.called, false);
        });

        it('does nothing when no subscribers', function(){
            broadcaster.broadcast('bitcoin', 'mainnet', { type: 'block' }); // should not throw
        });

        it('infra-only subscriber receives only infra tables, filtered from event.data', function(){
            // Regression (#3621/#3874): the gate filtered event.tables (always
            // undefined for block payloads — rows live under event.data), so
            // infra-only subscribers silently received the FULL block.
            let full = mockWs(), infra = mockWs();
            broadcaster.addSubscription(full,  mockReq('7.7.7.7'), 'bitcoin', 'mainnet');
            broadcaster.addSubscription(infra, mockReq('8.8.8.8'), 'bitcoin', 'mainnet');
            infra._syncMode = 'infra-only';

            let event = { type: 'block', block_index: 5, data: {
                blocks:  [{ id: 1 }],   // infra table
                actions: [{ id: 2 }]    // non-infra table
            }};
            broadcaster.broadcast('bitcoin', 'mainnet', event, new Set(['blocks']));

            // Full subscriber still gets every table.
            let fullMsg = JSON.parse(full.send.firstCall.args[0]);
            assert.deepStrictEqual(Object.keys(fullMsg.data).sort(), ['actions', 'blocks']);

            // Infra-only subscriber gets ONLY the infra table, under `data`.
            let infraMsg = JSON.parse(infra.send.firstCall.args[0]);
            assert.strictEqual(infraMsg.sync_mode, 'infra-only');
            assert.deepStrictEqual(Object.keys(infraMsg.data), ['blocks']);
            assert.ok(!('actions' in infraMsg.data), 'non-infra table must be filtered out');
        });

        it('infra-only subscriber with no matching infra tables gets an empty data set (not the full block)', function(){
            let infra = mockWs();
            broadcaster.addSubscription(infra, mockReq('9.9.9.9'), 'bitcoin', 'mainnet');
            infra._syncMode = 'infra-only';
            let event = { type: 'block', block_index: 6, data: { actions: [{ id: 1 }] } };
            broadcaster.broadcast('bitcoin', 'mainnet', event, new Set(['blocks']));
            let infraMsg = JSON.parse(infra.send.firstCall.args[0]);
            assert.deepStrictEqual(infraMsg.data, {});
        });

        it('encodes binary columns in the updated_rows channel (same wire codec as data)', function(){
            let ws = mockWs();
            broadcaster.addSubscription(ws, mockReq('1.2.3.4'), 'bitcoin', 'mainnet');
            let event = { type: 'block', block_index: 7, data: { blocks: [{ id: 1 }] },
                updated_rows: { attests: [{ action_index: 9, payload: Buffer.from('hi') }] } };
            broadcaster.broadcast('bitcoin', 'mainnet', event);
            let msg = JSON.parse(ws.send.firstCall.args[0]);
            // Buffer must serialize to the base64 binary sentinel, not {"type":"Buffer"}.
            assert.strictEqual(msg.updated_rows.attests[0].payload.__xbin__, Buffer.from('hi').toString('base64'));
        });

        it('infra-only subscriber receives the infra subset of updated_rows', function(){
            let infra = mockWs();
            broadcaster.addSubscription(infra, mockReq('2.3.4.5'), 'bitcoin', 'mainnet');
            infra._syncMode = 'infra-only';
            let event = { type: 'block', block_index: 8, data: { stakes: [{ id: 1 }] },
                updated_rows: {
                    stakes:          [{ action_index: 1 }],  // infra table → kept
                    contract_stakes: [{ action_index: 2 }]   // non-infra    → dropped
                }};
            broadcaster.broadcast('bitcoin', 'mainnet', event, new Set(['stakes']));
            let msg = JSON.parse(infra.send.firstCall.args[0]);
            assert.deepStrictEqual(Object.keys(msg.updated_rows), ['stakes']);
        });
    });

    describe('broadcastStatus', function(){
        it('sends status to all subscribers', function(){
            broadcaster.updateStatus('bitcoin', 'mainnet', { block_height: 200 });
            let ws = mockWs();
            broadcaster.addSubscription(ws, mockReq(), 'bitcoin', 'mainnet');
            ws.send.resetHistory();
            broadcaster.broadcastStatus('bitcoin', 'mainnet');
            assert.strictEqual(ws.send.calledOnce, true);
            let sent = JSON.parse(ws.send.firstCall.args[0]);
            assert.strictEqual(sent.type, 'status');
        });

        it('does nothing when no status data', function(){
            let ws = mockWs();
            broadcaster.addSubscription(ws, mockReq(), 'bitcoin', 'mainnet');
            ws.send.resetHistory();
            broadcaster.broadcastStatus('bitcoin', 'mainnet');
            assert.strictEqual(ws.send.called, false);
        });
    });

    describe('_send', function(){
        it('skips non-OPEN WebSocket', function(){
            let ws = mockWs();
            ws.readyState = WebSocket.CLOSED;
            broadcaster._send(ws, 'test');
            assert.strictEqual(ws.send.called, false);
        });

        it('closes ws when backpressure limit exceeded', function(){
            let ws = mockWs();
            ws._syncIp = 'test';
            ws._syncChain = 'bitcoin';
            ws._syncNetwork = 'mainnet';
            ws.bufferedAmount = 100;
            ws._syncBuffered = config.WS_BACKPRESSURE_LIMIT + 1;
            broadcaster._send(ws, 'test');
            assert.strictEqual(ws.close.calledOnce, true);
        });

        it('resets backpressure counter when buffer is clear', function(){
            let ws = mockWs();
            ws.bufferedAmount = 0;
            ws._syncBuffered = 10;
            broadcaster._send(ws, 'test');
            assert.strictEqual(ws._syncBuffered, 0);
        });
    });

    describe('applied-block tracking', function(){

        // Retrieve the inbound 'message' handler addSubscription registered on a ws.
        function messageHandler(ws){
            let call = ws.on.getCalls().find(c => c.args[0] === 'message');
            return call ? call.args[1] : null;
        }

        it('initialises _syncLastSentBlock and _syncAppliedBlock to null', function(){
            let ws = mockWs();
            broadcaster.addSubscription(ws, mockReq('7.7.7.1'), 'bitcoin', 'mainnet');
            assert.strictEqual(ws._syncLastSentBlock, null);
            assert.strictEqual(ws._syncAppliedBlock, null);
        });

        it('updates _syncAppliedBlock when a heartbeat message arrives', function(){
            let ws = mockWs();
            broadcaster.addSubscription(ws, mockReq('7.7.7.2'), 'bitcoin', 'mainnet');
            let handler = messageHandler(ws);
            assert.ok(handler, 'message handler should be registered');
            handler(JSON.stringify({ type: 'heartbeat', appliedBlock: 42 }));
            assert.strictEqual(ws._syncAppliedBlock, 42);
        });

        it('ignores malformed or unknown inbound messages', function(){
            let ws = mockWs();
            broadcaster.addSubscription(ws, mockReq('7.7.7.3'), 'bitcoin', 'mainnet');
            let handler = messageHandler(ws);
            handler('not json');
            handler(JSON.stringify({ type: 'something-else', appliedBlock: 99 }));
            handler(JSON.stringify({ type: 'heartbeat' })); // no appliedBlock
            assert.strictEqual(ws._syncAppliedBlock, null);
        });

        it('updates _syncLastSentBlock to the block height on broadcast', function(){
            let ws1 = mockWs(), ws2 = mockWs();
            broadcaster.addSubscription(ws1, mockReq('7.7.7.4'), 'bitcoin', 'mainnet');
            broadcaster.addSubscription(ws2, mockReq('7.7.7.5'), 'bitcoin', 'mainnet');
            broadcaster.broadcast('bitcoin', 'mainnet', { type: 'block', block_index: 850000 });
            assert.strictEqual(ws1._syncLastSentBlock, 850000);
            assert.strictEqual(ws2._syncLastSentBlock, 850000);
        });

        it('does not advance _syncLastSentBlock for non-block events', function(){
            let ws = mockWs();
            broadcaster.addSubscription(ws, mockReq('7.7.7.6'), 'bitcoin', 'mainnet');
            broadcaster.broadcast('bitcoin', 'mainnet', { type: 'reorg', block_index: 850001 });
            assert.strictEqual(ws._syncLastSentBlock, null);
        });

        it('reports null appliedBlock and lag for a subscriber with no heartbeat', function(){
            let ws = mockWs();
            broadcaster.addSubscription(ws, mockReq('7.7.7.7'), 'bitcoin', 'mainnet');
            broadcaster.broadcast('bitcoin', 'mainnet', { type: 'block', block_index: 100 });
            let subs = broadcaster.getSubscribers('bitcoin', 'mainnet');
            assert.strictEqual(subs.length, 1);
            assert.strictEqual(subs[0].ip, '7.7.7.7');
            assert.strictEqual(subs[0].lastSentBlock, 100);
            assert.strictEqual(subs[0].appliedBlock, null);
            assert.strictEqual(subs[0].lag, null);
            assert.strictEqual(subs[0].heartbeatReceived, false);
            assert.strictEqual(subs[0].lagStatus, 'unknown');
        });

        it('reports lag = lastSentBlock - appliedBlock after a heartbeat', function(){
            let ws = mockWs();
            broadcaster.addSubscription(ws, mockReq('7.7.7.8'), 'bitcoin', 'mainnet');
            broadcaster.broadcast('bitcoin', 'mainnet', { type: 'block', block_index: 100 });
            messageHandler(ws)(JSON.stringify({ type: 'heartbeat', appliedBlock: 97 }));
            let subs = broadcaster.getSubscribers('bitcoin', 'mainnet');
            assert.strictEqual(subs[0].lastSentBlock, 100);
            assert.strictEqual(subs[0].appliedBlock, 97);
            assert.strictEqual(subs[0].lag, 3);
            assert.strictEqual(subs[0].heartbeatReceived, true);
            assert.strictEqual(subs[0].lagStatus, 'known');
        });

        it('reports heartbeatReceived true even when caught up (lag 0)', function(){
            let ws = mockWs();
            broadcaster.addSubscription(ws, mockReq('7.7.7.9'), 'bitcoin', 'mainnet');
            broadcaster.broadcast('bitcoin', 'mainnet', { type: 'block', block_index: 100 });
            messageHandler(ws)(JSON.stringify({ type: 'heartbeat', appliedBlock: 100 }));
            let subs = broadcaster.getSubscribers('bitcoin', 'mainnet');
            // lag 0 (caught up) must be distinguishable from lag null (unknown):
            // heartbeatReceived is the signal that disambiguates the two.
            assert.strictEqual(subs[0].lag, 0);
            assert.strictEqual(subs[0].heartbeatReceived, true);
            // caught up (lag 0) is 'known', distinct from a never-heartbeated 'unknown'.
            assert.strictEqual(subs[0].lagStatus, 'known');
        });

        it('returns an empty array for an unknown chain/network', function(){
            assert.deepStrictEqual(broadcaster.getSubscribers('unknown', 'test'), []);
        });
    });

    describe('getSubscriberCount', function(){
        it('returns count for specific chain/network', function(){
            broadcaster.addSubscription(mockWs(), mockReq('a.a.a.1'), 'bitcoin', 'mainnet');
            broadcaster.addSubscription(mockWs(), mockReq('a.a.a.2'), 'bitcoin', 'mainnet');
            assert.strictEqual(broadcaster.getSubscriberCount('bitcoin', 'mainnet'), 2);
        });

        it('returns 0 for unknown chain/network', function(){
            assert.strictEqual(broadcaster.getSubscriberCount('unknown', 'test'), 0);
        });

        it('returns total across all chains when no args', function(){
            broadcaster.addSubscription(mockWs(), mockReq('b.b.b.1'), 'bitcoin', 'mainnet');
            broadcaster.addSubscription(mockWs(), mockReq('b.b.b.2'), 'litecoin', 'mainnet');
            assert.strictEqual(broadcaster.getSubscriberCount(), 2);
        });
    });

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
            // A different chain has no status data, so its validator stays unknown — but
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

    describe('evictStaleValidators', function(){
        it('transitions an entry past the TTL to stale instead of deleting it', function(){
            broadcaster.updateStatus('bitcoin', 'mainnet', { dbType: 'indexer', block_height: 510 });
            broadcaster.recordValidatorHeartbeat('bitcoin', 'mainnet', 'indexer', 'val-a', 500, null);
            // Backdate last_seen so the entry is older than the threshold.
            let map = broadcaster.validatorHeartbeats.get('bitcoin:mainnet:indexer');
            map.get('val-a').last_seen = Date.now() - 100000;

            broadcaster.evictStaleValidators(60000);

            // Still present in the map and surfaced as 'stale' with its last applied_height.
            let res = broadcaster.getValidatorHeartbeats('bitcoin', 'mainnet', 'indexer');
            assert.strictEqual(res.total, 1);
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
