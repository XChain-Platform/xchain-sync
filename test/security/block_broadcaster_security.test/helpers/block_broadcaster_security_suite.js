// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// Shared fixtures and hook registration. One part of block_broadcaster_security.test.js.
const assert = require('assert');
const sinon = require('sinon');
const BlockBroadcaster = require('../../../../src/server/block_broadcaster');

function createMockWs(ip){
    return {
        readyState: 1, // WebSocket.OPEN
        close: sinon.stub(),
        send: sinon.stub(),
        on: sinon.stub(),
        _syncChain: null,
        _syncNetwork: null,
        _syncIp: null,
        _syncBuffered: 0,
        bufferedAmount: 0
    };
}

function createMockReq(socketIp, forwardedFor){
    let req = {
        headers: {},
        socket: { remoteAddress: socketIp }
    };
    if(forwardedFor)
        req.headers['x-forwarded-for'] = forwardedFor;
    return req;
}

function registerHooks(beforeHook, afterHook) {
    beforeEach(beforeHook);
    afterEach(afterHook);
}

module.exports = {
    assert,
    BlockBroadcaster,
    createMockReq,
    createMockWs,
    registerHooks,
    sinon
};
