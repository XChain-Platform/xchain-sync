// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// Subscriber sockets only ever send a tiny heartbeat, so the server must close
// an oversize inbound frame with 1009 instead of buffering and parsing it.

const assert    = require('assert');
const http      = require('http');
const WebSocket = require('ws');
const { createSubscriberWss } = require('../../src/api');

describe('subscriber WebSocket inbound frame cap', function () {
  this.timeout(10000);
  it('closes an oversize frame with 1009 and keeps normal heartbeats', async function () {
    const wss = createSubscriberWss();
    const received = [];
    let resolveHeartbeat;
    const heartbeatReceived = new Promise((resolve) => { resolveHeartbeat = resolve; });
    const server = http.createServer();
    server.on('upgrade', (req, socket, head) => {
        wss.handleUpgrade(req, socket, head, (ws) => {
            ws.on('error', () => {});
            ws.on('message', (d) => {
                received.push(d.length);
                resolveHeartbeat();
            });
        });
    });
    await new Promise((r) => server.listen(0, '127.0.0.1', r));
    const url = 'ws://127.0.0.1:' + server.address().port + '/subscribe/indexer/litecoin/regtest';

    try {
        const client = new WebSocket(url);
        await new Promise((res, rej) => { client.once('open', res); client.once('error', rej); });
        client.send(JSON.stringify({ type: 'heartbeat', appliedBlock: 1 }));
        await heartbeatReceived;
        assert.strictEqual(received.length, 1, 'normal heartbeat delivered');
        assert.strictEqual(client.readyState, WebSocket.OPEN, 'connection stays open after heartbeat');

        const closed = new Promise((res) => client.once('close', (code) => res(code)));
        client.send(JSON.stringify({ type: 'heartbeat', appliedBlock: 1, pad: 'x'.repeat(2 * 1024 * 1024) }));
        const code = await closed;
        assert.strictEqual(code, 1009, 'oversize frame closes with 1009');
        assert.strictEqual(received.length, 1, 'oversize frame never reaches the message handler');
    } finally {
        for (const c of wss.clients) c.terminate();
        wss.close();
        await new Promise((r) => server.close(r));
    }
  });
});
