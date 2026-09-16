// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.
//
// db.ensureReplicaSecondaryIndexes is the ONLY delivery path for a secondary index
// onto a snapshot-bootstrapped replica: such a replica never runs the indexer's
// migration runner, so an index added there reaches it by nothing else. The list is
// hand-maintained, which is exactly the shape that silently falls behind, so it is
// pinned against the schema definitions here rather than only in review.
//
// Static source gate rather than a driven call: the function's other half issues DDL
// that needs a live MariaDB, and the failure this guards is a list entry, not a query.

const assert = require('assert');
const fs     = require('fs');
const path   = require('path');

const DB_SRC  = path.join(__dirname, '..', '..', 'src', 'db', 'index.js');
const SQL_DIR = path.join(__dirname, '..', '..', 'src', 'sql');

function ensureIndexList(){
    const src = fs.readFileSync(DB_SRC, 'utf8');
    const at = src.indexOf('async ensureReplicaSecondaryIndexes()');
    assert.ok(at > 0, 'ensureReplicaSecondaryIndexes moved; re-anchor this gate');
    const open = src.indexOf('let ensureIndexes = [', at);
    assert.ok(open > at, 'the ensureIndexes array literal moved; re-anchor this gate');
    const close = src.indexOf('];', open);
    const body  = src.slice(open, close);
    const rows  = [];
    for(const m of body.matchAll(
        /table:\s*'(\w+)'\s*,\s*indexName:\s*'(\w+)'\s*,\s*columns:\s*'\(([^)]*)\)'/g))
        rows.push({ table: m[1], indexName: m[2], columns: m[3] });
    return rows;
}

describe('replica secondary-index ensure list', function(){

    it('sanity: the gate actually parses entries (it cannot pass vacuously)', function(){
        assert.ok(ensureIndexList().length >= 2,
            'parsed no ensureIndexes entries out of the database class; the anchors above have gone stale ' +
            'and every case in this file would pass over an empty list');
    });

    it('every ensured index is declared in that table\'s schema definition', function(){
        const missing = [];
        let checked = 0;
        for(const { table, indexName, columns } of ensureIndexList()){
            const file = path.join(SQL_DIR, table + '.sql');
            // index_tickers / index_addresses carry no per-table .sql in this repo (the
            // indexer owns their DDL), so there is nothing here to compare them against.
            if(!fs.existsSync(file)) continue;
            checked++;
            const sql = fs.readFileSync(file, 'utf8');
            const declared = new RegExp(
                'CREATE\\s+INDEX\\s+`?' + indexName + '`?\\s+ON\\s+`?' + table + '`?\\s*\\(\\s*' +
                columns.replace(/\s+/g, '') + '\\s*\\)', 'i').test(sql.replace(/\s+/g, ' '));
            if(!declared)
                missing.push(`  ${table}.${indexName} (${columns}) is ensured on replicas but not declared ` +
                             `in src/sql/${table}.sql`);
        }
        assert.ok(checked > 0,
            'no ensured table had a src/sql/<table>.sql to compare against, so this case asserted nothing');
        assert.deepStrictEqual(missing, [],
            'A replica would be given an index the schema does not declare, so a fresh install and a ' +
            'bootstrapped replica end up with different shapes:\n' + missing.join('\n'));
    });

    it('carries state_tree_roots.block_index, which ClientRollback\'s range delete needs', function(){
        // DELETE FROM state_tree_roots WHERE block_index >= ? on a replica. The table's two
        // other keys both lead with (chain, network), so without this entry a bootstrapped
        // replica scans the whole root history on every reorg, forever.
        const row = ensureIndexList().find(r => r.table === 'state_tree_roots');
        assert.ok(row, 'state_tree_roots is missing from the ensure list');
        assert.strictEqual(row.indexName, 'block_index');
        assert.strictEqual(row.columns, 'block_index');
    });
});
