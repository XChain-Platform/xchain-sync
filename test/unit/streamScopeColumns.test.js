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
 * test/unit/streamScopeColumns.test.js
 *
 * Binds every streaming declaration in src/tableLifecycle.js to the DDL of the
 * table it declares. A 'stream:block' entry must own its blockKey column, a
 * 'stream:action' entry must own action_index, and a 'stream:index' entry must
 * own the id cursor replicatedTables.lookupCursorColumn pages by.
 *
 * Why a guard rather than a runtime check: the readers build the scope column
 * into their SQL, so a column the registry names and the schema lacks raises
 * MariaDB errno 1054, and EVERY forward channel classifies 1054 as "the source
 * runs an older schema" and drops the table from the payload without a log line
 * (ServerPoller.isSchemaGapError, SnapshotBuilder's per-table catch,
 * BlockHasher.computeTableContentChecksums). The table then stays in the
 * /status completeness count, in the content-parity plan and in both rollback
 * sets while none of its rows ever move. That is what happened to rollcalls and
 * rollcall_absences, which are keyed by close_block: declared, counted, deleted
 * on reorg, and never delivered on any forward channel.
 *
 * This guard is what makes that class fail at build time instead.
 */

'use strict';

const assert = require('assert');
const fs     = require('fs');
const path   = require('path');

const lifecycle        = require('../../src/tableLifecycle');
const replicatedTables = require('../../src/replicatedTables');

// The registry declares indexer-schema tables plus the handful xchain-sync owns,
// so those are the two DDL trees to scan. The decoder topology is declared
// literally in replicatedTables.js and is not generated from this registry.
//
// The indexer half is a sibling checkout, resolved the way generatedColumns.test.js
// resolves it: absent in a standalone checkout, and XCHAIN_REQUIRE_SIBLINGS=1 turns
// green-by-skip into a failure.
const INDEXER_SQL = process.env.XCHAIN_INDEXER_SQL_PATH
    || path.resolve(__dirname, '..', '..', '..', 'xchain-indexer', 'src', 'sql');
const SYNC_SQL    = path.resolve(__dirname, '..', '..', 'src', 'sql');
const SIBLING_REQUIRED = process.env.XCHAIN_REQUIRE_SIBLINGS === '1';

// The scope column each replication class reads by. null means the class carries
// no scope column of its own (stream:special rides the completeness count only).
function scopeColumnFor(entry){
    if(entry.replication === 'stream:block')  return lifecycle.blockKey(entry.table);
    if(entry.replication === 'stream:action') return 'action_index';
    if(entry.replication === 'stream:index')  return replicatedTables.lookupCursorColumn(entry.table);
    return null;
}

// Column names of every CREATE TABLE in a DDL directory, keyed by table.
//
// Line comments are stripped first: the DDL documents its keys in prose and a
// comment naming a column must not be read as declaring one. The body is cut at
// the closing paren of the column list, and index clauses (PRIMARY KEY, KEY, ...)
// are dropped so a table that merely INDEXES a column is not credited with having it.
const INDEX_CLAUSE = /^(PRIMARY|UNIQUE|KEY|INDEX|CONSTRAINT|FOREIGN|FULLTEXT|SPATIAL|CHECK)$/i;

function deriveColumns(sqlDir){
    const found = {};
    for(const file of fs.readdirSync(sqlDir).filter(f => f.endsWith('.sql'))){
        const body = fs.readFileSync(path.join(sqlDir, file), 'utf8').replace(/^\s*--.*$/gm, '');
        const m = body.match(/CREATE TABLE(?:\s+IF NOT EXISTS)?\s+`?(\w+)`?\s*\(([\s\S]*?)\n\)/i);
        if(!m) continue;
        if(found[m[1]]) continue;                     // first definition wins, as the loader sees it
        const columns = [];
        for(const line of m[2].split('\n')){
            const c = line.match(/^\s+`?([a-z_][a-z0-9_]*)`?\s+[A-Za-z]/);
            if(c && !INDEX_CLAUSE.test(c[1])) columns.push(c[1]);
        }
        found[m[1]] = columns;
    }
    return found;
}

describe('streamScopeColumns: every streamed table owns the column it is scoped by @regression', function(){

    it('declares a scope column the DDL actually has', function(){
        if(!fs.existsSync(INDEXER_SQL)){
            if(SIBLING_REQUIRED)
                throw new Error('XCHAIN_REQUIRE_SIBLINGS=1 but the indexer schema is absent: ' + INDEXER_SQL);
            this.skip();
            return;
        }
        const columns = Object.assign({}, deriveColumns(INDEXER_SQL), deriveColumns(SYNC_SQL));

        // The scan must actually find tables, or a CREATE TABLE regex that stopped
        // matching would make every assertion below vacuous and this test green.
        assert.ok(Object.keys(columns).length > 50,
                  'derived only ' + Object.keys(columns).length + ' tables from ' + INDEXER_SQL +
                  ' and ' + SYNC_SQL + '; the DDL scan is broken');

        let checked = 0;
        for(const entry of lifecycle.TABLES){
            const need = scopeColumnFor(entry);
            if(need === null) continue;
            const cols = columns[entry.table];
            assert.ok(cols, entry.table + ' is declared ' + entry.replication +
                            ' but no CREATE TABLE for it was found in the indexer or sync DDL');
            assert.ok(cols.indexOf(need) !== -1,
                      entry.table + ' is declared ' + entry.replication + ' and scoped by `' + need +
                      '`, which its DDL does not declare (columns: ' + cols.join(', ') + '). ' +
                      'The reader would raise errno 1054 and every forward channel swallows that as an ' +
                      'older source schema, so the table would ship un-replicated while still counted complete.');
            checked++;
        }

        // Pin the coverage too: if the registry stopped classifying tables as streamed,
        // the loop above would assert nothing and still pass.
        assert.ok(checked > 80, 'only ' + checked + ' streamed entries were checked; expected the whole registry');
    });

    it('resolves the block scope column from the registry, defaulting to block_index', function(){
        // The default is what makes the field a no-op for the other block-scoped
        // tables, so it is pinned here rather than left implied.
        assert.strictEqual(lifecycle.blockKey('blocks'), 'block_index');
        assert.strictEqual(lifecycle.blockKey('contract_state'), 'block_index');
        assert.strictEqual(lifecycle.blockKey('sync_meta'), 'block_index');
        assert.strictEqual(lifecycle.blockKey('not_a_table'), 'block_index');
        // The two the class name lied about.
        assert.strictEqual(lifecycle.blockKey('rollcalls'), 'close_block');
        assert.strictEqual(lifecycle.blockKey('rollcall_absences'), 'close_block');
    });

    it('reads the two close_block tables by close_block on every forward channel', function(){
        // Source-text guard, in the tableContentParity.test.js style: these three
        // readers cannot be exercised without a live DB, and each one hard-coding
        // `block_index` is exactly how the tables shipped un-replicated.
        const read = (p) => fs.readFileSync(path.resolve(__dirname, '..', '..', p), 'utf8');

        const db = read('src/db.js');
        const blockScoped = db.slice(db.indexOf('async getBlockScopedRows('), db.indexOf('async getActionScopedRows('));
        assert.ok(/lifecycle\.blockKey\(table\)/.test(blockScoped),
                  'getBlockScopedRows must scope by the registry column; the literal block_index ' +
                  'raised 1054 on rollcalls/rollcall_absences and ServerPoller swallowed it');
        assert.ok(!/WHERE block_index = \?/.test(blockScoped),
                  'getBlockScopedRows still carries the hard-coded block_index predicate');

        const window = db.slice(db.indexOf('async getContentWindowRows('), db.indexOf('async getMaxRowId('));
        assert.ok(/lifecycle\.blockKey\(table\)/.test(window),
                  'the content-parity block window must use the registry scope column, or the two ' +
                  'tables it cannot read stay reported as covered');

        // Only the INDEXER branch: the decoder topology is declared literally in
        // replicatedTables.js, not generated from this registry, so its block_index
        // range is correct as written and must not be swept up by this assertion.
        const snapshot = read('src/SnapshotBuilder.js');
        const start    = snapshot.indexOf('if(indexerBlockScoped.has(table)){');
        assert.ok(start !== -1, 'the incremental catch-up indexer branch moved; this guard cannot see it');
        const branch   = snapshot.slice(start, snapshot.indexOf('indexerFullDump.has(table)', start));
        assert.ok(/tableLifecycle\.blockKey\(table\)/.test(branch),
                  'the incremental catch-up branch must use the registry scope column');
        assert.ok(!/WHERE block_index >= \?/.test(branch),
                  'the incremental catch-up branch still carries the hard-coded block_index range');
    });
});
