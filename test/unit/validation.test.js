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
const {
    validateIdentifier,
    validateDdl,
    validateWsEvent,
    extractColumnNames,
    extractColumnDefinition
} = require('../../src/validation');

describe('validation', function(){

    describe('validateIdentifier', function(){
        it('accepts a plain identifier', function(){
            assert.deepStrictEqual(validateIdentifier('my_table_1'), { valid: true });
        });
        it('accepts a 64-char identifier (boundary)', function(){
            assert.strictEqual(validateIdentifier('a'.repeat(64)).valid, true);
        });
        it('rejects null', function(){
            const r = validateIdentifier(null);
            assert.strictEqual(r.valid, false);
            assert.match(r.reason, /null or undefined/);
        });
        it('rejects undefined', function(){
            assert.strictEqual(validateIdentifier(undefined).valid, false);
        });
        it('rejects a non-string', function(){
            const r = validateIdentifier(123);
            assert.strictEqual(r.valid, false);
            assert.match(r.reason, /not a string/);
        });
        it('rejects an empty string', function(){
            const r = validateIdentifier('');
            assert.strictEqual(r.valid, false);
            assert.match(r.reason, /empty/);
        });
        it('rejects > 64 chars', function(){
            const r = validateIdentifier('a'.repeat(65));
            assert.strictEqual(r.valid, false);
            assert.match(r.reason, /exceeds 64/);
        });
        it('rejects invalid characters', function(){
            const r = validateIdentifier('bad-name;');
            assert.strictEqual(r.valid, false);
            assert.match(r.reason, /invalid characters/);
        });
    });

    describe('validateDdl', function(){
        it('accepts a simple CREATE TABLE', function(){
            assert.deepStrictEqual(validateDdl('CREATE TABLE t (id INT)'), { valid: true });
        });
        it('accepts CREATE TABLE with leading whitespace', function(){
            assert.strictEqual(validateDdl('   CREATE TABLE t (id INT)').valid, true);
        });
        it('rejects null', function(){
            assert.match(validateDdl(null).reason, /null or undefined/);
        });
        it('rejects undefined', function(){
            assert.strictEqual(validateDdl(undefined).valid, false);
        });
        it('rejects a non-string', function(){
            assert.match(validateDdl(42).reason, /not a string/);
        });
        it('rejects whitespace-only DDL', function(){
            assert.match(validateDdl('   \n  ').reason, /empty/);
        });
        it('rejects DDL not starting with CREATE TABLE', function(){
            assert.match(validateDdl('SELECT * FROM t').reason, /does not start with CREATE TABLE/);
        });
        it('rejects a disallowed statement type (CREATE TRIGGER)', function(){
            // does not match DDL_START_RE (CREATE TABLE), so rejected at the start check
            assert.match(validateDdl('CREATE TRIGGER x ...').reason, /does not start with CREATE TABLE/);
        });
        it('rejects multi-statement injection after a semicolon (DROP)', function(){
            const r = validateDdl('CREATE TABLE t (id INT); DROP TABLE users');
            assert.strictEqual(r.valid, false);
            assert.match(r.reason, /disallowed statement after semicolon/);
        });
        it('rejects CREATE PROCEDURE smuggled after a semicolon', function(){
            const r = validateDdl('CREATE TABLE t (id INT); CREATE PROCEDURE p() BEGIN END');
            assert.strictEqual(r.valid, false);
            assert.match(r.reason, /disallowed statement after semicolon/);
        });
        it('triggers the wrong-type guard when CREATE TABLE prefixes a banned type oddly', function(){
            // DDL_WRONG_TYPE_RE matches "CREATE VIEW" at the start; craft input that
            // passes DDL_START_RE only if it begins "CREATE TABLE". To reach the
            // DDL_WRONG_TYPE_RE branch we need a string that starts with CREATE TABLE
            // but the wrong-type regex... it cannot both start with CREATE TABLE and
            // CREATE VIEW, so this guard is defensive. Confirm a real CREATE VIEW is
            // rejected by the earlier start guard instead.
            assert.match(validateDdl('CREATE VIEW v AS SELECT 1').reason, /does not start with CREATE TABLE/);
        });
    });

    describe('extractColumnNames', function(){
        const ddl = [
            'CREATE TABLE `blocks` (',
            '  `block_index` int NOT NULL,',
            '  `block_hash` varchar(64) DEFAULT NULL,',
            '  PRIMARY KEY (`block_index`),',
            '  KEY `idx_hash` (`block_hash`)',
            ') ENGINE=InnoDB'
        ].join('\n');

        it('extracts only the backtick-led column lines', function(){
            assert.deepStrictEqual(extractColumnNames(ddl), ['block_index', 'block_hash']);
        });
        it('returns [] for a non-string', function(){
            assert.deepStrictEqual(extractColumnNames(null), []);
            assert.deepStrictEqual(extractColumnNames(123), []);
        });
        it('returns [] when no parseable columns exist', function(){
            assert.deepStrictEqual(extractColumnNames('CREATE TABLE t ()'), []);
        });
        it('skips a line with an unterminated backtick', function(){
            assert.deepStrictEqual(extractColumnNames('`broken'), []);
        });
    });

    describe('extractColumnDefinition', function(){
        const ddl = [
            'CREATE TABLE `t` (',
            '  `id` int NOT NULL,',
            '  `name` varchar(255) DEFAULT NULL,',
            '  PRIMARY KEY (`id`)',
            ')'
        ].join('\n');

        it('extracts a column definition with trailing comma stripped', function(){
            assert.strictEqual(extractColumnDefinition(ddl, 'name'), '`name` varchar(255) DEFAULT NULL');
        });
        it('extracts the last column (no trailing comma)', function(){
            const d = 'CREATE TABLE `t` (\n  `only` int\n)';
            assert.strictEqual(extractColumnDefinition(d, 'only'), '`only` int');
        });
        it('returns null for a non-string ddl', function(){
            assert.strictEqual(extractColumnDefinition(null, 'x'), null);
        });
        it('returns null for a non-string columnName', function(){
            assert.strictEqual(extractColumnDefinition(ddl, 42), null);
        });
        it('returns null when the column is absent', function(){
            assert.strictEqual(extractColumnDefinition(ddl, 'missing'), null);
        });
        it('returns null when the matched line smuggles a semicolon', function(){
            const hostile = 'CREATE TABLE `t` (\n  `evil` int; DROP TABLE x\n)';
            assert.strictEqual(extractColumnDefinition(hostile, 'evil'), null);
        });
        it('skips lines whose backtick is unterminated', function(){
            const d = '`id int NOT NULL,\n  `name` varchar(10)';
            assert.strictEqual(extractColumnDefinition(d, 'name'), '`name` varchar(10)');
        });

        // Stress-sweep 2026-07-08: the depth scanner must be quote-aware, or a hostile
        // server hides a smuggled second ALTER action behind a '(' inside a string.
        it('rejects a smuggled multi-action ALTER hidden by a "(" inside a COMMENT', function(){
            const hostile = "CREATE TABLE `orders` (\n  `evil` int COMMENT '(' , DROP COLUMN `give_ownership`\n)";
            assert.strictEqual(extractColumnDefinition(hostile, 'evil'), null);
        });
        it('still rejects a bare depth-0 comma with no quotes (regression)', function(){
            const hostile = 'CREATE TABLE `t` (\n  `evil` int , DROP COLUMN `victim`\n)';
            assert.strictEqual(extractColumnDefinition(hostile, 'evil'), null);
        });
        it('keeps a legit comma inside a COMMENT string', function(){
            const d = "CREATE TABLE `t` (\n  `c` int COMMENT 'a, b, c',\n  `d` int\n)";
            assert.strictEqual(extractColumnDefinition(d, 'c'), "`c` int COMMENT 'a, b, c'");
        });
        it('keeps a legit enum(...) with internal commas', function(){
            const d = "CREATE TABLE `t` (\n  `c` enum('a','b','c') DEFAULT 'a',\n  `d` int\n)";
            assert.strictEqual(extractColumnDefinition(d, 'c'), "`c` enum('a','b','c') DEFAULT 'a'");
        });
        it('handles a doubled-quote escape inside a string literal', function(){
            const d = "CREATE TABLE `t` (\n  `c` varchar(10) DEFAULT 'a''(''b',\n  `d` int\n)";
            assert.strictEqual(extractColumnDefinition(d, 'c'), "`c` varchar(10) DEFAULT 'a''(''b'");
        });
    });

    describe('validateWsEvent', function(){
        it('accepts a block event with a non-negative integer index', function(){
            assert.deepStrictEqual(validateWsEvent({ type: 'block', block_index: 5 }),
                { valid: true, type: 'block' });
        });
        it('accepts a reorg event', function(){
            assert.strictEqual(validateWsEvent({ type: 'reorg', block_index: 0 }).valid, true);
        });
        it('accepts a status event with numeric block_height', function(){
            assert.strictEqual(validateWsEvent({ type: 'status', block_height: 100 }).valid, true);
        });
        it('accepts a status event with null block_height', function(){
            assert.strictEqual(validateWsEvent({ type: 'status', block_height: null }).valid, true);
        });
        it('accepts a status event with no block_height', function(){
            assert.strictEqual(validateWsEvent({ type: 'status' }).valid, true);
        });
        it('rejects null', function(){
            assert.match(validateWsEvent(null).reason, /null or undefined/);
        });
        it('rejects undefined', function(){
            assert.strictEqual(validateWsEvent(undefined).valid, false);
        });
        it('rejects an array', function(){
            assert.match(validateWsEvent([]).reason, /not an object/);
        });
        it('rejects a non-object', function(){
            assert.match(validateWsEvent('x').reason, /not an object/);
        });
        it('rejects a non-string type', function(){
            assert.match(validateWsEvent({ type: 1 }).reason, /type is not a string/);
        });
        it('rejects an unknown type', function(){
            assert.match(validateWsEvent({ type: 'bogus' }).reason, /Unknown event type: bogus/);
        });
        it('rejects a block event missing block_index', function(){
            assert.match(validateWsEvent({ type: 'block' }).reason, /missing block_index/);
        });
        it('rejects a non-integer (fractional) block_index', function(){
            assert.match(validateWsEvent({ type: 'reorg', block_index: 5.5 }).reason, /not an integer/);
            assert.strictEqual(validateWsEvent({ type: 'block', block_index: 5.5 }).valid, false);
        });
        it('rejects a block event with null block_index', function(){
            assert.match(validateWsEvent({ type: 'block', block_index: null }).reason, /missing block_index/);
        });
        it('rejects a non-finite block_index', function(){
            assert.match(validateWsEvent({ type: 'block', block_index: NaN }).reason, /not a finite number/);
            assert.match(validateWsEvent({ type: 'block', block_index: 'x' }).reason, /not a finite number/);
        });
        it('rejects a negative block_index', function(){
            assert.match(validateWsEvent({ type: 'block', block_index: -1 }).reason, /is negative/);
        });
        it('rejects a non-finite status block_height', function(){
            assert.match(validateWsEvent({ type: 'status', block_height: NaN }).reason, /not a finite number/);
            assert.match(validateWsEvent({ type: 'status', block_height: 'x' }).reason, /not a finite number/);
        });
    });
});
