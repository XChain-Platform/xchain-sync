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
            // undefined for block payloads (rows live under event.data), so
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

    });
});

describe('BlockBroadcaster', function(){
    registerHooks();

    describe('broadcast', function(){
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
});
