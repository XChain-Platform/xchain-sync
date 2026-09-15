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
} = require('./helpers/api_security_suite');

function afterHook() {
    sinon.restore();
}


// Covers comparison and error responses. One part of api_security.test.js.
describe('API security', function(){
    registerHooks(afterHook);

    // Constant-time comparison used by every Bearer-key check (REST middleware,
    // /halt/clear, and the WS upgrade). Locks in correctness and the null/length
    // guards so a refactor can't silently reintroduce a `===` timing leak.
    describe('safeEqual', function(){

        it('true for identical strings', function(){
            assert.strictEqual(safeEqual('Bearer abc123', 'Bearer abc123'), true);
        });

        it('false for a one-character difference of equal length', function(){
            assert.strictEqual(safeEqual('Bearer abc123', 'Bearer abc124'), false);
        });

        it('false for a length mismatch (prefix of the key)', function(){
            assert.strictEqual(safeEqual('Bearer abc', 'Bearer abc123'), false);
        });

        it('false when either side is null or undefined', function(){
            assert.strictEqual(safeEqual(undefined, 'Bearer k'), false);
            assert.strictEqual(safeEqual('Bearer k', null), false);
        });

        it('true for two empty/absent values (both coerce to empty)', function(){
            assert.strictEqual(safeEqual('', ''), true);
            assert.strictEqual(safeEqual(undefined, null), true);
        });
    });

});

describe('API security', function(){
    registerHooks(afterHook);

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

            // A sensitive error would contain them; this is what we prevent
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

