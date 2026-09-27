/*********************************************************************
 *
 * Copyright © 2025-2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC - https://dankest.llc
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This file is part of XChain Platform. Licensed under the GNU Affero
 * General Public License v3.0 or later; see LICENSE.md. A commercial
 * license (without AGPL source-disclosure terms) is available -
 * contact legal@dankest.llc.
 *
 *********************************************************************/

'use strict';

const assert = require('assert');
const { retypeAlterSql } = require('../../src/db/datetime_retype_alter');
const { DATETIME_COLUMNS, modifyClause } = require('../../src/schema/datetime_columns');

const entry = DATETIME_COLUMNS[0];

describe('DATETIME retype ALTER builder', function(){
    it('builds one ALTER for a live TIMESTAMP column', function(){
        const result = retypeAlterSql(entry.table, [entry], [
            { COLUMN_NAME: entry.column, DATA_TYPE: 'timestamp' },
        ]);

        assert.deepStrictEqual(result, {
            sql: 'ALTER TABLE `' + entry.table + '` ' + modifyClause(entry),
            columns: [entry.column],
        });
        assert.match(result.sql, /ALTER TABLE/);
        assert.match(result.sql, /MODIFY COLUMN/);
        assert.match(result.sql, new RegExp('`' + entry.column + '`'));
        assert.match(result.sql, /DATETIME/);
    });

    it('returns null for a live DATETIME column', function(){
        assert.strictEqual(retypeAlterSql(entry.table, [entry], [
            { COLUMN_NAME: entry.column, DATA_TYPE: 'datetime' },
        ]), null);
    });

    it('returns null when the table has no live rows', function(){
        assert.strictEqual(retypeAlterSql(entry.table, [entry], []), null);
    });

    it('accepts lower-case metadata keys and matches column names case-insensitively', function(){
        const result = retypeAlterSql(entry.table, [entry], [
            { column_name: entry.column.toUpperCase(), data_type: 'timestamp' },
        ]);

        assert.deepStrictEqual(result.columns, [entry.column]);
    });

    it('ignores a live row for an unrelated column', function(){
        assert.strictEqual(retypeAlterSql(entry.table, [entry], [
            { COLUMN_NAME: 'unrelated_column', DATA_TYPE: 'timestamp' },
        ]), null);
    });

    it('includes only TIMESTAMP columns from a multi-column table', function(){
        const entries = [
            { ...entry, column: 'first_seen' },
            { ...entry, column: 'last_seen' },
        ];
        const result = retypeAlterSql('events', entries, [
            { COLUMN_NAME: 'first_seen', DATA_TYPE: 'timestamp' },
            { COLUMN_NAME: 'last_seen', DATA_TYPE: 'datetime' },
        ]);

        assert.deepStrictEqual(result, {
            sql: 'ALTER TABLE `events` ' + modifyClause(entries[0]),
            columns: ['first_seen'],
        });
        assert.doesNotMatch(result.sql, /last_seen/);
    });
});
