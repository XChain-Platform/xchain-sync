// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// Shared fixtures and hook registration. One part of api_security.test.js.
const assert = require('assert');
const sinon = require('sinon');
const { createApiKeyMiddleware, safeEqual } = require('../../../../src/http/middleware');

function createMockReq(authHeader){
    let req = { headers: {} };
    if(authHeader !== undefined)
        req.headers['authorization'] = authHeader;
    return req;
}

function createMockRes(){
    let res = {
        _statusCode: null,
        _body: null,
        status: function(code){
            res._statusCode = code;
            return res;
        },
        json: function(body){
            res._body = body;
            return res;
        }
    };
    return res;
}

function registerHooks(afterHook) {
    afterEach(afterHook);
}

module.exports = {
    assert,
    createApiKeyMiddleware,
    createMockReq,
    createMockRes,
    registerHooks,
    safeEqual,
    sinon
};
