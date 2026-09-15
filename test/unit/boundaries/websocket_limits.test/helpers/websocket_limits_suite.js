// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// Covers socket fixtures and hooks. One part of websocket_limits.test.js.
const sinon  = require('sinon');
const WebSocket = require('ws');

function mockWs(){
    return {
        readyState: WebSocket.OPEN,
        bufferedAmount: 0,
        _syncBuffered: 0,
        _syncChain: null, _syncNetwork: null, _syncIp: null,
        send: sinon.stub(), close: sinon.stub(), on: sinon.stub()
    };
}

function mockReq(ip){
    return { headers: {}, socket: { remoteAddress: ip || '127.0.0.1' } };
}

function registerHooks(){
    afterEach(function(){ sinon.restore(); });
}

module.exports = { WebSocket, mockWs, mockReq, registerHooks };
