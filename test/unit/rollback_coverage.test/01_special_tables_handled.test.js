/*********************************************************************
 *
 * Copyright © 2025–2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC – https://dankest.llc
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This file is part of XChain Platform. Licensed under the GNU Affero
 * General Public License v3.0 or later; see LICENSE.md. A commercial
 * license (without AGPL source-disclosure terms) is available -
 * contact legal@dankest.llc.
 *
 **********************************************************************/

// test/unit/rollback_coverage.test/01_special_tables_handled.test.js
//
// ../rollback_coverage.test.js counts a registry 'special' or 'recomputed' table as covered
// by its label. This suite requires ClientRollback to actually touch each one, and requires
// every table the registry scopes by a non-default blockKey to sit in a delete loop on it.

'use strict';

const assert    = require('assert');
const fs        = require('fs');
const path      = require('path');
const lifecycle = require('../../../src/table_lifecycle');

const ROLLBACK_JS = path.resolve(__dirname, '../../../src/client/rollback.js');

// Tables rebuilt wholesale by a helper rather than by a statement naming them.
const HELPER_COVERED = { balances: /balanceHelpers\.rebuildBalances\(this\.db\)/ };

// Code-only text: comments cut, quotes blanked, `+` joins and whitespace collapsed.
function normalizedSource(src){
    const code = src.replace(/\/\*[\s\S]*?\*\//g, ' ').split('\n').map(line => {
        let q = null;
        for(let i = 0; i < line.length; i++){
            const ch = line[i];
            if(q){ if(ch === q && line[i - 1] !== '\\') q = null; continue; }
            if(ch === "'" || ch === '"' || ch === '`'){ q = ch; continue; }
            if(ch === '/' && line[i + 1] === '/') return line.slice(0, i);
        }
        return line;
    }).join('\n');
    return code.replace(/[`"']/g, ' ').replace(/\s+\+\s+/g, ' ').replace(/\s+/g, ' ');
}

// Every `for(const table of [..])` loop deleting by `<column> >= ?`, as table -> column.
function keyedDeleteLoops(norm){
    const out = new Map();
    const re = /for ?\(const table of \[([^\]]*)\]\) ?\{ try \{ await this\.db\.doQuery\( DELETE FROM table WHERE (\w+) >= \?/g;
    let m;
    while((m = re.exec(norm)) !== null)
        for(const t of m[1].split(',').map(s => s.trim()).filter(Boolean)) out.set(t, m[2]);
    return out;
}

// Registry rows a replica must delete by a non-default block column, as table -> column.
function registryKeyedTables(entries){
    const out = new Map();
    for(const e of entries)
        if(e.replicaRollback === 'special' && e.blockKey && e.blockKey !== 'block_index') out.set(e.table, e.blockKey);
    return out;
}

// Problems with the keyed loops: a registry row missing or on the wrong column, or an extra row.
function keyedLoopProblems(registry, loops){
    const problems = [];
    for(const [t, col] of registry)
        if(loops.get(t) !== col) problems.push(t + ' is scoped by ' + col + ' but no rollback loop deletes it by ' + col);
    for(const [t, col] of loops)
        if(registry.get(t) !== col) problems.push(t + ' is deleted by ' + col + ' but the registry does not scope it by that column');
    return problems;
}

// Tables no DELETE / UPDATE statement, keyed loop or named helper in the source touches.
function unhandledTables(tables, norm, loops){
    return tables.filter(t => {
        if(loops.has(t)) return false;
        if(new RegExp('(?:DELETE (?:\\w+ )?FROM|UPDATE) ' + t + '\\b').test(norm)) return false;
        return !(HELPER_COVERED[t] && HELPER_COVERED[t].test(norm));
    });
}

describe('Rollback coverage guard @regression', function(){
    const norm  = normalizedSource(fs.readFileSync(ROLLBACK_JS, 'utf8'));
    const loops = keyedDeleteLoops(norm);

    it('every registry row with a non-default blockKey is deleted by that column on a replica reorg', function(){
        const registry = registryKeyedTables(lifecycle.allTables());
        assert.ok(registry.size > 0 && loops.size > 0, 'no keyed rows or loops found; the extraction regex no longer matches');
        assert.deepStrictEqual(keyedLoopProblems(registry, loops), [],
            'build the delete list in src/client/rollback.js from the registry blockKey, or reclassify the registry row');
    });

    it('every special and recomputed table is touched by ClientRollback, not covered by its label alone', function(){
        const { SPECIAL_CASE, RECOMPUTED } = lifecycle.replicaRollbackBuckets();
        const missing = unhandledTables([...SPECIAL_CASE, ...RECOMPUTED], norm, loops);
        assert.deepStrictEqual(missing, [],
            'ClientRollback never deletes, updates or rebuilds: ' + missing.join(', ') +
            '; add the reorg handling in src/client/rollback.js, add a HELPER_COVERED entry, or reclassify the registry row');
    });

    // Negative controls: both checks must be able to report, or a green run proves nothing.
    it('flags a made-up special table and a made-up close_block row the source never handles', function(){
        assert.deepStrictEqual(unhandledTables(['synthetic_special_table'], norm, loops), ['synthetic_special_table']);
        const registry = registryKeyedTables([
            ...lifecycle.allTables(),
            { table: 'synthetic_close_table', replicaRollback: 'special', blockKey: 'close_block' },
        ]);
        assert.deepStrictEqual(keyedLoopProblems(registry, loops),
            ['synthetic_close_table is scoped by close_block but no rollback loop deletes it by close_block']);
    });
});
