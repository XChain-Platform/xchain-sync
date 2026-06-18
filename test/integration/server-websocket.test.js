// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

const assert    = require('assert');
const sinon     = require('sinon');
const http      = require('http');
const WebSocket = require('ws');
const express   = require('express');
const setup     = require('./helpers/setup');
const testDb    = require('./helpers/testDb');
const fixtures  = require('./helpers/fixtures');
const ServerPoller     = require('../../src/ServerPoller');
const BlockBroadcaster = require('../../src/BlockBroadcaster');
const TransparencyLog  = require('../../src/TransparencyLog');

const WS_PORT = 19200;

describe('Integration: WebSocket Broadcasting', function() {

    let sourceDb, broadcaster, poller, log, server, wss;

    before(async function() {
        await setup.globalSetup();
        sourceDb = setup.getSourceDb();

        let config = {
            WS_MAX_PER_IP: 3,
            WS_BACKPRESSURE_LIMIT: 50,
            BLOCK_POLL_INTERVAL: 100
        };

        broadcaster = new BlockBroadcaster(config);
        log = new TransparencyLog(sourceDb);
        poller = new ServerPoller('bitcoin', 'mainnet', sourceDb, broadcaster, log, config, testDb.util);

        // Create HTTP + WS server
        let app = express();
        server = http.createServer(app);
        wss = new WebSocket.Server({ noServer: true });

        server.on('upgrade', (request, socket, head) => {
            let match = request.url.match(/^\/subscribe\/([^\/]+)\/([^\/]+)\/([^\/\?]+)/);
            if (!match) { socket.destroy(); return; }
            wss.handleUpgrade(request, socket, head, (ws) => {
                broadcaster.addSubscription(ws, request, match[2], match[3], 'full', match[1]);
            });
        });

        await new Promise(resolve => server.listen(WS_PORT, resolve));

        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });

    after(async function() {
        sinon.restore();
        await new Promise(resolve => server.close(resolve));
        await setup.globalTeardown();
    });

    beforeEach(async function() {
        await testDb.truncateAll(sourceDb);
    });

    // Helper: connect a WS client and return {ws, messages}
    function connectWs(chain, network) {
        return new Promise((resolve, reject) => {
            let messages = [];
            let ws = new WebSocket('ws://127.0.0.1:' + WS_PORT + '/subscribe/indexer/' + chain + '/' + network);
            ws.on('open', () => resolve({ ws, messages }));
            ws.on('message', (data) => messages.push(JSON.parse(data.toString())));
            ws.on('error', reject);
        });
    }

    // Helper: wait until messages array has at least n entries
    function waitForMessages(messages, n, timeout = 5000) {
        return new Promise((resolve, reject) => {
            let timer = setTimeout(() => reject(new Error('Timeout waiting for ' + n + ' messages, got ' + messages.length)), timeout);
            let check = () => {
                if (messages.length >= n) { clearTimeout(timer); resolve(); }
                else setTimeout(check, 50);
            };
            check();
        });
    }

    describe('subscribe and receive block', function() {
        it('receives block event after new block is polled', async function() {
            await fixtures.seedBlocks(sourceDb, 1, 1);
            poller.lastPolledBlock = 0;

            let { ws, messages } = await connectWs('bitcoin', 'mainnet');
            try {
                await poller._poll();
                await waitForMessages(messages, 1);

                // Find the block message (skip any status messages)
                let blockMsg = messages.find(m => m.type === 'block');
                assert.ok(blockMsg);
                assert.strictEqual(blockMsg.block_index, 1);
                assert.strictEqual(blockMsg.chain, 'bitcoin');
                assert.strictEqual(blockMsg.network, 'mainnet');
                assert.ok(blockMsg.data);
            } finally {
                ws.close();
            }
        });

        it('receives reorg event', async function() {
            await fixtures.seedBlocks(sourceDb, 1, 10);
            poller.lastPolledBlock = 10;
            await fixtures.deleteBlocksFrom(sourceDb, 8);

            let { ws, messages } = await connectWs('bitcoin', 'mainnet');
            try {
                await poller._poll();
                await waitForMessages(messages, 1);

                let reorgMsg = messages.find(m => m.type === 'reorg');
                assert.ok(reorgMsg);
                assert.strictEqual(reorgMsg.block_index, 8);
            } finally {
                ws.close();
            }
        });
    });

    describe('initial status on connect', function() {
        it('sends status message when status data exists', async function() {
            broadcaster.updateStatus('bitcoin', 'mainnet', {
                block_height: 42, block_time: 1700000042,
                ledger_hash: 'lh', actions_hash: 'ah', contract_hash: 'ch'
            });

            let { ws, messages } = await connectWs('bitcoin', 'mainnet');
            try {
                await waitForMessages(messages, 1);
                assert.strictEqual(messages[0].type, 'status');
                assert.strictEqual(messages[0].block_height, 42);
            } finally {
                ws.close();
            }
        });
    });

    describe('per-IP connection limit', function() {
        it('closes connection when limit exceeded', async function() {
            let connections = [];
            try {
                for (let i = 0; i < 3; i++) {
                    connections.push(await connectWs('bitcoin', 'mainnet'));
                }

                // 4th connection should be closed
                let ws4 = new WebSocket('ws://127.0.0.1:' + WS_PORT + '/subscribe/indexer/bitcoin/mainnet');
                let closed = await new Promise((resolve) => {
                    ws4.on('close', (code) => resolve(code));
                    ws4.on('open', () => {
                        // May get closed shortly after open
                        setTimeout(() => resolve(null), 1000);
                    });
                });
                assert.strictEqual(closed, 1008);
            } finally {
                for (let c of connections) c.ws.close();
            }
        });
    });

    describe('invalid subscribe path', function() {
        it('destroys socket for unrecognized path', async function() {
            let ws = new WebSocket('ws://127.0.0.1:' + WS_PORT + '/invalid/path');
            let result = await new Promise((resolve) => {
                ws.on('error', () => resolve('error'));
                ws.on('close', () => resolve('closed'));
                setTimeout(() => resolve('timeout'), 2000);
            });
            assert.ok(result === 'error' || result === 'closed');
        });
    });

    describe('multiple chains', function() {
        it('only delivers events to matching chain subscribers', async function() {
            broadcaster.updateStatus('bitcoin', 'mainnet', { block_height: 1 });

            let btcConn = await connectWs('bitcoin', 'mainnet');
            // litecoin subscriber, no status data for it
            let ltcWs = new WebSocket('ws://127.0.0.1:' + WS_PORT + '/subscribe/indexer/litecoin/mainnet');
            await new Promise(resolve => ltcWs.on('open', resolve));
            let ltcMessages = [];
            ltcWs.on('message', (data) => ltcMessages.push(JSON.parse(data.toString())));

            try {
                // Broadcast to bitcoin only
                broadcaster.broadcast('bitcoin', 'mainnet', { type: 'block', block_index: 99 });

                await waitForMessages(btcConn.messages, 1);
                // btc got initial status; filter for block
                await new Promise(resolve => setTimeout(resolve, 200));

                let btcBlock = btcConn.messages.find(m => m.type === 'block');
                assert.ok(btcBlock);
                assert.strictEqual(ltcMessages.filter(m => m.type === 'block').length, 0);
            } finally {
                btcConn.ws.close();
                ltcWs.close();
            }
        });
    });
});
