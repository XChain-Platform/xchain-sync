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
const validation = require('../../../src/util/validation');

describe('validation', function(){

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
    });
});

describe('validation', function(){

    describe('validateDdl', function(){

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
    });
});

describe('validation', function(){

    describe('validateDdl', function(){

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
    });
});

describe('validation', function(){

    describe('validateDdl', function(){

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
});
