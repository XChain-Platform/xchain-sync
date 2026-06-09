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
const { stripSqlLineComments, splitSqlStatements } = require('../../src/sqlUtil');

describe('sqlUtil', function(){

    describe('stripSqlLineComments', function(){
        it('removes a trailing -- line comment, leaving a newline', function(){
            assert.strictEqual(stripSqlLineComments('SELECT 1; -- a comment\nSELECT 2;'),
                'SELECT 1; \nSELECT 2;');
        });

        it('removes a comment that runs to the end of input (no trailing newline)', function(){
            assert.strictEqual(stripSqlLineComments('SELECT 1; -- trailing'), 'SELECT 1; ');
        });

        it('does NOT split on a ; that lives inside a -- comment', function(){
            const out = stripSqlLineComments('CREATE TABLE t (id INT); -- has ; inside\n');
            assert.ok(out.indexOf('has ; inside') === -1, 'comment body removed');
            // only one real statement-terminating ; survives
            assert.strictEqual((out.match(/;/g) || []).length, 1);
        });

        it('preserves a ; inside a single-quoted string', function(){
            const sql = "INSERT INTO t VALUES ('a;b');";
            assert.strictEqual(stripSqlLineComments(sql), sql);
        });

        it('preserves -- inside a single-quoted string (not a comment)', function(){
            const sql = "INSERT INTO t VALUES ('-- not a comment');";
            assert.strictEqual(stripSqlLineComments(sql), sql);
        });

        it('preserves content inside double-quoted identifiers', function(){
            const sql = 'SELECT "col -- weird" FROM t;';
            assert.strictEqual(stripSqlLineComments(sql), sql);
        });

        it('preserves content inside backtick identifiers', function(){
            const sql = 'SELECT `col -- weird` FROM t;';
            assert.strictEqual(stripSqlLineComments(sql), sql);
        });

        it('handles a doubled single-quote escape inside a string', function(){
            const sql = "INSERT INTO t VALUES ('it''s -- fine');";
            assert.strictEqual(stripSqlLineComments(sql), sql);
        });

        it('handles a doubled backtick escape inside an identifier', function(){
            const sql = 'SELECT `a``b -- x` FROM t;';
            assert.strictEqual(stripSqlLineComments(sql), sql);
        });

        it('closes a quote on the matching delimiter then resumes comment stripping', function(){
            const out = stripSqlLineComments("SELECT 'x' -- comment\nSELECT 2;");
            assert.strictEqual(out, "SELECT 'x' \nSELECT 2;");
        });

        it('returns empty string for empty input', function(){
            assert.strictEqual(stripSqlLineComments(''), '');
        });

        it('leaves a lone dash (not a comment) untouched', function(){
            assert.strictEqual(stripSqlLineComments('SELECT 1 - 2;'), 'SELECT 1 - 2;');
        });
    });

    describe('splitSqlStatements', function(){
        it('splits multiple statements and trims them', function(){
            assert.deepStrictEqual(
                splitSqlStatements('SELECT 1;\n  SELECT 2 ;\nSELECT 3'),
                ['SELECT 1', 'SELECT 2', 'SELECT 3']);
        });

        it('drops empty statements produced by trailing/duplicate semicolons', function(){
            assert.deepStrictEqual(splitSqlStatements('SELECT 1;;\n\n;'), ['SELECT 1']);
        });

        it('does not split on a semicolon inside a comment', function(){
            assert.deepStrictEqual(
                splitSqlStatements('CREATE TABLE t (id INT) -- note; with semicolon\n;'),
                ['CREATE TABLE t (id INT)']);
        });

        it('splits naively on ; even inside a quoted string (quotes are only honored by comment-stripping, not the splitter)', function(){
            // Documents actual behavior: splitSqlStatements does a plain `.split(';')`
            // after stripping comments, so a ; inside a string literal IS a split point.
            // Schema .sql files avoid string-literal semicolons, so this is benign here.
            assert.deepStrictEqual(
                splitSqlStatements("INSERT INTO t VALUES ('a;b;c');"),
                ["INSERT INTO t VALUES ('a", "b", "c')"]);
        });

        it('returns [] for empty / whitespace-only input', function(){
            assert.deepStrictEqual(splitSqlStatements(''), []);
            assert.deepStrictEqual(splitSqlStatements('   \n  '), []);
        });
    });
});
