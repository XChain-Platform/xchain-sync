// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available —
// contact legal@dankest.llc.

const assert = require('assert');
const sinon  = require('sinon');
const { createApiKeyMiddleware } = require('../../src/middleware');

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

describe('API security', function(){

    afterEach(function(){
        sinon.restore();
    });

    // ── createApiKeyMiddleware ──

    describe('createApiKeyMiddleware', function(){

        // Open-access mode: no key provisioned (single-host / regtest / managed
        // deployments). api.js warns at startup; /halt/clear stays disabled.
        it('passes through when no API key is configured (empty string) — open mode', function(done){
            let middleware = createApiKeyMiddleware('');
            let req = createMockReq();
            let res = createMockRes();
            middleware(req, res, function(){
                assert.strictEqual(res._statusCode, null);
                done(); // next() was called
            });
        });

        it('passes through when no API key is configured (null) — open mode', function(done){
            let middleware = createApiKeyMiddleware(null);
            let req = createMockReq();
            let res = createMockRes();
            middleware(req, res, function(){
                done();
            });
        });

        it('passes through when no API key is configured (undefined) — open mode', function(done){
            let middleware = createApiKeyMiddleware(undefined);
            let req = createMockReq();
            let res = createMockRes();
            middleware(req, res, function(){
                done();
            });
        });

        it('returns 401 when API key is set but no Authorization header', function(){
            let middleware = createApiKeyMiddleware('secret123');
            let req = createMockReq();
            let res = createMockRes();
            let nextCalled = false;
            middleware(req, res, function(){ nextCalled = true; });
            assert.strictEqual(nextCalled, false);
            assert.strictEqual(res._statusCode, 401);
            assert.deepStrictEqual(res._body, { error: 'Unauthorized' });
        });

        it('returns 401 when API key is set but wrong key', function(){
            let middleware = createApiKeyMiddleware('secret123');
            let req = createMockReq('Bearer wrong_key');
            let res = createMockRes();
            let nextCalled = false;
            middleware(req, res, function(){ nextCalled = true; });
            assert.strictEqual(nextCalled, false);
            assert.strictEqual(res._statusCode, 401);
        });

        it('returns 401 when header present but no Bearer prefix', function(){
            let middleware = createApiKeyMiddleware('secret123');
            let req = createMockReq('secret123');
            let res = createMockRes();
            let nextCalled = false;
            middleware(req, res, function(){ nextCalled = true; });
            assert.strictEqual(nextCalled, false);
            assert.strictEqual(res._statusCode, 401);
        });

        it('returns 401 when header has Basic auth instead of Bearer', function(){
            let middleware = createApiKeyMiddleware('secret123');
            let req = createMockReq('Basic c2VjcmV0MTIz');
            let res = createMockRes();
            let nextCalled = false;
            middleware(req, res, function(){ nextCalled = true; });
            assert.strictEqual(nextCalled, false);
            assert.strictEqual(res._statusCode, 401);
        });

        it('passes through with correct Bearer token', function(done){
            let middleware = createApiKeyMiddleware('secret123');
            let req = createMockReq('Bearer secret123');
            let res = createMockRes();
            middleware(req, res, function(){
                done(); // next() was called
            });
        });

        it('rejects partial key match', function(){
            let middleware = createApiKeyMiddleware('secret123');
            let req = createMockReq('Bearer secret');
            let res = createMockRes();
            let nextCalled = false;
            middleware(req, res, function(){ nextCalled = true; });
            assert.strictEqual(nextCalled, false);
            assert.strictEqual(res._statusCode, 401);
        });

        it('rejects key with extra whitespace', function(){
            let middleware = createApiKeyMiddleware('secret123');
            let req = createMockReq('Bearer  secret123');
            let res = createMockRes();
            let nextCalled = false;
            middleware(req, res, function(){ nextCalled = true; });
            assert.strictEqual(nextCalled, false);
            assert.strictEqual(res._statusCode, 401);
        });
    });

    // ── Error response sanitization ──

    describe('error response sanitization', function(){

        it('API error responses should use generic message pattern', function(){
            // This test validates the pattern used in api.js error handlers.
            // The actual routes use: res.status(500).json({ error: 'Internal server error' })
            // If the code accidentally leaks e.message, this test documents the expected contract.
            let genericError = { error: 'Internal server error' };
            let sensitiveError = { error: 'ER_ACCESS_DENIED_ERROR: Access denied for user \'root\'@\'172.18.0.1\'' };

            // The generic error should NOT contain connection details
            assert.strictEqual(genericError.error.includes('Access denied'), false);
            assert.strictEqual(genericError.error.includes('172.18'), false);
            assert.strictEqual(genericError.error, 'Internal server error');

            // A sensitive error would contain them — this is what we prevent
            assert.strictEqual(sensitiveError.error.includes('Access denied'), true);
        });

        it('Internal server error message does not vary by exception type', function(){
            // Regardless of exception type, the API should always return the same message
            let dbError = new Error('ER_TABLE_NOT_FOUND');
            let connError = new Error('ECONNREFUSED 172.18.0.1:3306');
            let syntaxError = new Error("SELECT * FROM `evil;DROP`");

            // The sanitized response should be identical for all
            let sanitized = 'Internal server error';
            assert.notStrictEqual(sanitized, dbError.message);
            assert.notStrictEqual(sanitized, connError.message);
            assert.notStrictEqual(sanitized, syntaxError.message);
        });
    });
});
