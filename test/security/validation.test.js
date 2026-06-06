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
const validation = require('../../src/validation');

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

    // ── validateDdl ──

    describe('validateDdl', function(){

        it('accepts canonical CREATE TABLE', function(){
            let ddl = "CREATE TABLE blocks (block_index INT PRIMARY KEY, block_time INT)";
            let result = validation.validateDdl(ddl);
            assert.strictEqual(result.valid, true);
        });

        it('accepts CREATE TABLE IF NOT EXISTS with backticks', function(){
            let ddl = "CREATE TABLE IF NOT EXISTS `blocks` (`block_index` INT PRIMARY KEY)";
            let result = validation.validateDdl(ddl);
            assert.strictEqual(result.valid, true);
        });

        it('accepts multiline DDL with indexes and ENGINE', function(){
            let ddl = "CREATE TABLE `actions` (\n  `action_index` INT NOT NULL,\n  PRIMARY KEY (`action_index`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";
            let result = validation.validateDdl(ddl);
            assert.strictEqual(result.valid, true);
        });

        it('accepts DDL with leading whitespace', function(){
            let ddl = "  \n  CREATE TABLE blocks (id INT)";
            let result = validation.validateDdl(ddl);
            assert.strictEqual(result.valid, true);
        });

        it('accepts lowercase create table', function(){
            let ddl = "create table blocks (id int)";
            let result = validation.validateDdl(ddl);
            assert.strictEqual(result.valid, true);
        });

        it('rejects DROP TABLE', function(){
            let ddl = "DROP TABLE blocks";
            let result = validation.validateDdl(ddl);
            assert.strictEqual(result.valid, false);
        });

        it('rejects CREATE TRIGGER', function(){
            let ddl = "CREATE TRIGGER trg_blocks AFTER INSERT ON blocks FOR EACH ROW BEGIN END";
            let result = validation.validateDdl(ddl);
            assert.strictEqual(result.valid, false);
        });

        it('rejects CREATE PROCEDURE', function(){
            let ddl = "CREATE PROCEDURE sp_evil() BEGIN DROP TABLE blocks; END";
            let result = validation.validateDdl(ddl);
            assert.strictEqual(result.valid, false);
        });

        it('rejects CREATE FUNCTION', function(){
            let ddl = "CREATE FUNCTION fn_evil() RETURNS INT BEGIN RETURN 1; END";
            let result = validation.validateDdl(ddl);
            assert.strictEqual(result.valid, false);
        });

        it('rejects CREATE EVENT', function(){
            let ddl = "CREATE EVENT ev_evil ON SCHEDULE EVERY 1 SECOND DO DELETE FROM blocks";
            let result = validation.validateDdl(ddl);
            assert.strictEqual(result.valid, false);
        });

        it('rejects CREATE VIEW', function(){
            let ddl = "CREATE VIEW vw_evil AS SELECT * FROM blocks";
            let result = validation.validateDdl(ddl);
            assert.strictEqual(result.valid, false);
        });

        it('rejects multi-statement injection with DROP after semicolon', function(){
            let ddl = "CREATE TABLE blocks (id INT); DROP TABLE blocks;";
            let result = validation.validateDdl(ddl);
            assert.strictEqual(result.valid, false);
            assert.ok(result.reason.includes('semicolon'));
        });

        it('rejects multi-statement injection with CREATE TRIGGER after semicolon', function(){
            let ddl = "CREATE TABLE blocks (id INT); CREATE TRIGGER trg AFTER INSERT ON blocks FOR EACH ROW BEGIN END";
            let result = validation.validateDdl(ddl);
            assert.strictEqual(result.valid, false);
        });

        it('rejects EXEC injection after semicolon', function(){
            let ddl = "CREATE TABLE blocks (id INT); EXEC xp_cmdshell('whoami')";
            let result = validation.validateDdl(ddl);
            assert.strictEqual(result.valid, false);
        });

        it('rejects case-insensitive banned keywords', function(){
            let ddl = "Create Trigger trg AFTER INSERT ON blocks FOR EACH ROW BEGIN END";
            let result = validation.validateDdl(ddl);
            assert.strictEqual(result.valid, false);
        });

        it('rejects empty string', function(){
            let result = validation.validateDdl('');
            assert.strictEqual(result.valid, false);
        });

        it('rejects null', function(){
            let result = validation.validateDdl(null);
            assert.strictEqual(result.valid, false);
        });

        it('rejects undefined', function(){
            let result = validation.validateDdl(undefined);
            assert.strictEqual(result.valid, false);
        });

        it('rejects non-string (object)', function(){
            let result = validation.validateDdl({ sql: 'CREATE TABLE x (id INT)' });
            assert.strictEqual(result.valid, false);
        });

        it('rejects non-string (array)', function(){
            let result = validation.validateDdl(['CREATE TABLE x (id INT)']);
            assert.strictEqual(result.valid, false);
        });
    });

    // ── validateWsEvent ──

    describe('validateWsEvent', function(){

        it('accepts valid block event', function(){
            let result = validation.validateWsEvent({ type: 'block', block_index: 100 });
            assert.strictEqual(result.valid, true);
            assert.strictEqual(result.type, 'block');
        });

        it('accepts valid reorg event', function(){
            let result = validation.validateWsEvent({ type: 'reorg', block_index: 50 });
            assert.strictEqual(result.valid, true);
            assert.strictEqual(result.type, 'reorg');
        });

        it('accepts valid status event with block_height', function(){
            let result = validation.validateWsEvent({ type: 'status', block_height: 100 });
            assert.strictEqual(result.valid, true);
            assert.strictEqual(result.type, 'status');
        });

        it('accepts status event with null block_height', function(){
            let result = validation.validateWsEvent({ type: 'status', block_height: null });
            assert.strictEqual(result.valid, true);
        });

        it('accepts status event without block_height', function(){
            let result = validation.validateWsEvent({ type: 'status' });
            assert.strictEqual(result.valid, true);
        });

        it('accepts block event with block_index 0', function(){
            let result = validation.validateWsEvent({ type: 'block', block_index: 0 });
            assert.strictEqual(result.valid, true);
        });

        it('rejects null event', function(){
            let result = validation.validateWsEvent(null);
            assert.strictEqual(result.valid, false);
        });

        it('rejects undefined event', function(){
            let result = validation.validateWsEvent(undefined);
            assert.strictEqual(result.valid, false);
        });

        it('rejects string event', function(){
            let result = validation.validateWsEvent('block');
            assert.strictEqual(result.valid, false);
        });

        it('rejects array event', function(){
            let result = validation.validateWsEvent([{ type: 'block' }]);
            assert.strictEqual(result.valid, false);
        });

        it('rejects missing type field', function(){
            let result = validation.validateWsEvent({ block_index: 100 });
            assert.strictEqual(result.valid, false);
        });

        it('rejects unknown event type', function(){
            let result = validation.validateWsEvent({ type: 'inject' });
            assert.strictEqual(result.valid, false);
            assert.ok(result.reason.includes('inject'));
        });

        it('rejects block event with non-numeric block_index', function(){
            let result = validation.validateWsEvent({ type: 'block', block_index: 'abc' });
            assert.strictEqual(result.valid, false);
        });

        it('rejects block event with missing block_index', function(){
            let result = validation.validateWsEvent({ type: 'block' });
            assert.strictEqual(result.valid, false);
        });

        it('rejects block event with negative block_index', function(){
            let result = validation.validateWsEvent({ type: 'block', block_index: -1 });
            assert.strictEqual(result.valid, false);
        });

        it('rejects reorg event with missing block_index', function(){
            let result = validation.validateWsEvent({ type: 'reorg' });
            assert.strictEqual(result.valid, false);
        });

        it('rejects block event with NaN block_index', function(){
            let result = validation.validateWsEvent({ type: 'block', block_index: NaN });
            assert.strictEqual(result.valid, false);
        });

        it('rejects block event with Infinity block_index', function(){
            let result = validation.validateWsEvent({ type: 'block', block_index: Infinity });
            assert.strictEqual(result.valid, false);
        });

        it('rejects status event with non-numeric block_height', function(){
            let result = validation.validateWsEvent({ type: 'status', block_height: 'abc' });
            assert.strictEqual(result.valid, false);
        });
    });

    // ── extractColumnNames ──

    const SAMPLE_DDL = [
        'CREATE TABLE `balances` (',
        '  `id` int(11) NOT NULL AUTO_INCREMENT,',
        '  `address` varchar(255) NOT NULL,',
        "  `amount` decimal(30,8) NOT NULL DEFAULT '0',",
        "  `kind` enum('a','b','c') DEFAULT NULL,",
        '  `block_index` int(11) NOT NULL,',
        '  PRIMARY KEY (`id`),',
        '  UNIQUE KEY `uniq_addr` (`address`),',
        '  KEY `idx_block` (`block_index`)',
        ') ENGINE=InnoDB DEFAULT CHARSET=utf8mb4'
    ].join('\n');

    describe('extractColumnNames', function(){

        it('extracts every column name in order', function(){
            let cols = validation.extractColumnNames(SAMPLE_DDL);
            assert.deepStrictEqual(cols, ['id', 'address', 'amount', 'kind', 'block_index']);
        });

        it('does not pick up the table name or constraint identifiers', function(){
            let cols = validation.extractColumnNames(SAMPLE_DDL);
            assert.ok(!cols.includes('balances'));
            assert.ok(!cols.includes('uniq_addr'));
            assert.ok(!cols.includes('idx_block'));
        });

        it('returns [] for non-string input', function(){
            assert.deepStrictEqual(validation.extractColumnNames(null), []);
            assert.deepStrictEqual(validation.extractColumnNames(undefined), []);
            assert.deepStrictEqual(validation.extractColumnNames(42), []);
        });
    });

    // ── extractColumnDefinition ──

    describe('extractColumnDefinition', function(){

        it('returns the backtick-quoted name plus definition, comma stripped', function(){
            let def = validation.extractColumnDefinition(SAMPLE_DDL, 'address');
            assert.strictEqual(def, '`address` varchar(255) NOT NULL');
        });

        it('preserves commas inside the type definition', function(){
            let def = validation.extractColumnDefinition(SAMPLE_DDL, 'amount');
            assert.strictEqual(def, "`amount` decimal(30,8) NOT NULL DEFAULT '0'");
        });

        it('handles enum definitions with embedded commas and quotes', function(){
            let def = validation.extractColumnDefinition(SAMPLE_DDL, 'kind');
            assert.strictEqual(def, "`kind` enum('a','b','c') DEFAULT NULL");
        });

        it('returns null for a column that is not present', function(){
            assert.strictEqual(validation.extractColumnDefinition(SAMPLE_DDL, 'missing'), null);
        });

        it('does not match a constraint identifier as a column', function(){
            assert.strictEqual(validation.extractColumnDefinition(SAMPLE_DDL, 'uniq_addr'), null);
            assert.strictEqual(validation.extractColumnDefinition(SAMPLE_DDL, 'idx_block'), null);
        });

        it('rejects a line carrying a smuggled second statement', function(){
            let hostile = [
                'CREATE TABLE `t` (',
                '  `x` int(11) NOT NULL; DROP TABLE users,',
                '  PRIMARY KEY (`x`)',
                ') ENGINE=InnoDB'
            ].join('\n');
            assert.strictEqual(validation.extractColumnDefinition(hostile, 'x'), null);
        });

        it('returns null for non-string input', function(){
            assert.strictEqual(validation.extractColumnDefinition(null, 'x'), null);
            assert.strictEqual(validation.extractColumnDefinition(SAMPLE_DDL, null), null);
        });
    });
});
