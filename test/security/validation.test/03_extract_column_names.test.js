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

    // ── extractColumnNames ──

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
});
