/********************************************************************
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
 ********************************************************************
 * test/unit/generatedColumns.test.js
 *
 * Drift guard for src/generatedColumns.js. The applier carries a FROZEN map of
 * database-generated columns rather than probing information_schema, because its
 * insert path is hot and its query sequence is pinned by other unit tests. That trade
 * is only safe with this guard: the map is derived here from the indexer's own DDL and
 * compared, so a table that GAINS a generated column fails the build instead of
 * failing a follower's block apply with errno 1906 under STRICT_TRANS_TABLES.
 *
 * The failure this protects against is silent in the worst way: on a permissive server
 * naming a generated column is only a warning, so the same code that halts one
 * follower works on another.
 */

'use strict';

const assert = require('assert');
const fs     = require('fs');
const path   = require('path');

const { GENERATED_COLUMNS, generatedColumns } = require('../../src/generatedColumns');
const lifecycle = require('../../src/tableLifecycle');

// Sibling indexer schema, resolved the way the DB-backed suites do. Absent by default
// in a standalone checkout; XCHAIN_REQUIRE_SIBLINGS=1 makes green-by-skip fail instead.
const SQL_DIR = process.env.XCHAIN_INDEXER_SQL_PATH
    || path.resolve(__dirname, '..', '..', '..', 'xchain-indexer', 'src', 'sql');
const SIBLING_REQUIRED = process.env.XCHAIN_REQUIRE_SIBLINGS === '1';

// Every `<col> ... GENERATED ALWAYS AS (...)` in a CREATE TABLE, keyed by table.
function deriveFromDdl(sqlDir){
    const found = {};
    for(const file of fs.readdirSync(sqlDir).filter(f => f.endsWith('.sql'))){
        const text = fs.readFileSync(path.join(sqlDir, file), 'utf8');
        // Strip line comments first: the DDL documents generated columns in prose, and
        // a comment mentioning the phrase must not be mistaken for a declaration.
        const body = text.replace(/^\s*--.*$/gm, '');
        const table = (body.match(/CREATE TABLE(?:\s+IF NOT EXISTS)?\s+`?(\w+)`?/i) || [])[1];
        if(!table) continue;
        // [^,] deliberately spans newlines: the declaration wraps, with the column name
        // on one line and GENERATED ALWAYS AS on the next. The comma is the boundary
        // that stops a match from running into the following column.
        for(const m of body.matchAll(/^[ \t]*`?(\w+)`?[^,]*?GENERATED\s+ALWAYS\s+AS/gim)){
            (found[table] = found[table] || []).push(m[1]);
        }
    }
    for(const t of Object.keys(found)) found[t].sort();
    return found;
}

describe('generatedColumns: the frozen map matches the indexer DDL @regression', function(){

    it('lists exactly the generated columns the schema declares', function(){
        if(!fs.existsSync(SQL_DIR)){
            if(SIBLING_REQUIRED)
                throw new Error('XCHAIN_REQUIRE_SIBLINGS=1 but the indexer schema is absent: ' + SQL_DIR);
            this.skip();
            return;
        }
        const derived = deriveFromDdl(SQL_DIR);

        // The derivation must actually find something, or a regex that stopped matching
        // would make this test pass by finding nothing on both sides.
        assert.ok(Object.keys(derived).length > 0,
                  'derived no generated columns at all from ' + SQL_DIR + '; the DDL scan is broken');

        const frozen = {};
        for(const t of Object.keys(GENERATED_COLUMNS)) frozen[t] = [...GENERATED_COLUMNS[t]].sort();

        assert.deepStrictEqual(derived, frozen,
            'src/generatedColumns.js has drifted from the indexer DDL. A table that gained a ' +
            'generated column will make every follower fail its block apply with errno 1906 ' +
            'under STRICT_TRANS_TABLES; one that lost it wastes a column on every insert.');
    });

    it('covers the tables that are actually replicated, which is where it matters', function(){
        if(!fs.existsSync(SQL_DIR)){ this.skip(); return; }
        // A generated column only causes the 1906 failure on a table the follower
        // WRITES. If one ever appears on a table nobody replicates the map still lists
        // it (harmless), but the reverse is the bug, so this pins the direction.
        const replicated = new Set(lifecycle.TABLES
            .filter(e => e.replication && e.replication !== 'none')
            .map(e => e.table));
        for(const table of Object.keys(GENERATED_COLUMNS))
            assert.ok(replicated.has(table),
                      table + ' has a generated column but is not replicated; confirm that is intended');
    });

    it('returns an empty set for a table with no generated columns', function(){
        assert.strictEqual(generatedColumns('credits').size, 0);
        assert.strictEqual(generatedColumns('escrow_leaf_journal').size, 0);
    });

    it('returns the generated column for contract_state, and the same Set each call', function(){
        const first = generatedColumns('contract_state');
        assert.deepStrictEqual([...first], ['state_key_bin']);
        assert.strictEqual(generatedColumns('contract_state'), first, 'the per-table Set must be cached');
    });
});
