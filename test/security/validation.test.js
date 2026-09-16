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
const validation = require('../../src/util/validation');

describe('validation', function(){

    // ── validateIdentifier ──

    describe('validateIdentifier', function(){

        it('accepts single lowercase word', function(){
            let result = validation.validateIdentifier('blocks');
            assert.strictEqual(result.valid, true);
        });

        it('accepts mixed case with underscores', function(){
            let result = validation.validateIdentifier('index_Transactions');
            assert.strictEqual(result.valid, true);
        });

        it('accepts single character', function(){
            let result = validation.validateIdentifier('a');
            assert.strictEqual(result.valid, true);
        });

        it('accepts digits and underscores', function(){
            let result = validation.validateIdentifier('table_123_name');
            assert.strictEqual(result.valid, true);
        });

        it('accepts exactly 64 characters', function(){
            let name = 'a'.repeat(64);
            let result = validation.validateIdentifier(name);
            assert.strictEqual(result.valid, true);
        });

        it('rejects 65-character string', function(){
            let name = 'a'.repeat(65);
            let result = validation.validateIdentifier(name);
            assert.strictEqual(result.valid, false);
            assert.ok(result.reason.includes('64'));
        });
    });
});

describe('validation', function(){

    describe('validateIdentifier', function(){

        it('rejects empty string', function(){
            let result = validation.validateIdentifier('');
            assert.strictEqual(result.valid, false);
        });

        it('rejects null', function(){
            let result = validation.validateIdentifier(null);
            assert.strictEqual(result.valid, false);
        });

        it('rejects undefined', function(){
            let result = validation.validateIdentifier(undefined);
            assert.strictEqual(result.valid, false);
        });

        it('rejects non-string (number)', function(){
            let result = validation.validateIdentifier(42);
            assert.strictEqual(result.valid, false);
        });

        it('rejects backtick injection', function(){
            let result = validation.validateIdentifier('valid`DROP TABLE blocks;--');
            assert.strictEqual(result.valid, false);
        });

        it('rejects semicolons', function(){
            let result = validation.validateIdentifier('blocks;DROP');
            assert.strictEqual(result.valid, false);
        });
    });
});

describe('validation', function(){

    describe('validateIdentifier', function(){

        it('rejects spaces', function(){
            let result = validation.validateIdentifier('block s');
            assert.strictEqual(result.valid, false);
        });

        it('rejects dash', function(){
            let result = validation.validateIdentifier('block-index');
            assert.strictEqual(result.valid, false);
        });

        it('rejects SQL injection attempt', function(){
            let result = validation.validateIdentifier('1 OR 1=1');
            assert.strictEqual(result.valid, false);
        });

        it('rejects dot notation', function(){
            let result = validation.validateIdentifier('db.table');
            assert.strictEqual(result.valid, false);
        });

        it('rejects unicode characters', function(){
            let result = validation.validateIdentifier('t\u00e0ble');
            assert.strictEqual(result.valid, false);
        });

        it('rejects null byte', function(){
            let result = validation.validateIdentifier('bloc\x00ks');
            assert.strictEqual(result.valid, false);
        });

        it('rejects path traversal', function(){
            let result = validation.validateIdentifier('../etc/passwd');
            assert.strictEqual(result.valid, false);
        });
    });
});
