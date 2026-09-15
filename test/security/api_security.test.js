// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

const {
    assert,
    createApiKeyMiddleware,
    createMockReq,
    createMockRes,
    registerHooks,
    safeEqual,
    sinon
} = require('./api_security.test/helpers/api_security_suite');

function afterHook() {
    sinon.restore();
}

describe('API security', function(){
    registerHooks(afterHook);

    // ── createApiKeyMiddleware ──

    describe('createApiKeyMiddleware', function(){

        // Open-access mode: no key provisioned (single-host / regtest / managed
        // deployments). api.js warns at startup; /halt/clear stays disabled.
        it('passes through when no API key is configured (empty string, open mode)', function(done){
            let middleware = createApiKeyMiddleware('');
            let req = createMockReq();
            let res = createMockRes();
            middleware(req, res, function(){
                assert.strictEqual(res._statusCode, null);
                done(); // next() was called
            });
        });

        it('passes through when no API key is configured (null, open mode)', function(done){
            let middleware = createApiKeyMiddleware(null);
            let req = createMockReq();
            let res = createMockRes();
            middleware(req, res, function(){
                done();
            });
        });

        it('passes through when no API key is configured (undefined, open mode)', function(done){
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

    });
});

describe('API security', function(){
    registerHooks(afterHook);

    describe('createApiKeyMiddleware', function(){

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

});
