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

describe('validation', function(){

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
    });
});

describe('validation', function(){

    describe('extractColumnDefinition', function(){

        it('rejects a line carrying a smuggled second statement', function(){
            let hostile = [
                'CREATE TABLE `t` (',
                '  `x` int(11) NOT NULL; DROP TABLE users,',
                '  PRIMARY KEY (`x`)',
                ') ENGINE=InnoDB'
            ].join('\n');
            assert.strictEqual(validation.extractColumnDefinition(hostile, 'x'), null);
        });

        it('rejects a multi-action ALTER smuggled via a bare comma (DROP COLUMN)', function(){
            let hostile = [
                'CREATE TABLE `t` (',
                '  `evil` int DEFAULT 0, DROP COLUMN balance,',
                '  PRIMARY KEY (`id`)',
                ') ENGINE=InnoDB'
            ].join('\n');
            assert.strictEqual(validation.extractColumnDefinition(hostile, 'evil'), null);
        });

        it('rejects a bare comma smuggling ADD COLUMN', function(){
            let hostile = [
                'CREATE TABLE `t` (',
                '  `evil` int DEFAULT 0, ADD COLUMN extra INT,',
                '  PRIMARY KEY (`id`)',
                ') ENGINE=InnoDB'
            ].join('\n');
            assert.strictEqual(validation.extractColumnDefinition(hostile, 'evil'), null);
        });
    });
});

describe('validation', function(){

    describe('extractColumnDefinition', function(){

        it('rejects a bare comma smuggling RENAME COLUMN', function(){
            let hostile = [
                'CREATE TABLE `t` (',
                '  `evil` int DEFAULT 0, RENAME COLUMN x TO y,',
                '  PRIMARY KEY (`id`)',
                ') ENGINE=InnoDB'
            ].join('\n');
            assert.strictEqual(validation.extractColumnDefinition(hostile, 'evil'), null);
        });

        it('still accepts a precision type whose only commas are inside parens', function(){
            let ddl = [
                'CREATE TABLE `t` (',
                "  `amount` decimal(18,8) NOT NULL DEFAULT '0',",
                '  PRIMARY KEY (`id`)',
                ') ENGINE=InnoDB'
            ].join('\n');
            assert.strictEqual(
                validation.extractColumnDefinition(ddl, 'amount'),
                "`amount` decimal(18,8) NOT NULL DEFAULT '0'"
            );
        });

        it('returns null for non-string input', function(){
            assert.strictEqual(validation.extractColumnDefinition(null, 'x'), null);
            assert.strictEqual(validation.extractColumnDefinition(SAMPLE_DDL, null), null);
        });
    });
});
