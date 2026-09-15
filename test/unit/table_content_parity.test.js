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
// Advisory table-content parity (TABLE_CONTENT_PARITY_CHECK).
//
// The gap this closes: getReplicatedTables('indexer') returns 107 per-block
// replicated tables, while the block hashes commit the ledger/actions/contract
// projections, state_hash commits the in-place mutation classes, the state
// commitment covers balances + stakes, computeIndexMapChecksum covers
// index_addresses, and verifyTableCounts compares CARDINALITY only (and only
// flags remote > local). An equal-count content substitution in any other
// replicated table therefore passed every check a follower ran.
//
// Acceptance cases, modelled on test/integration/index_map_parity.test.js:
//   1. every replicated table is committed by something: covered here, or
//      knowingly excluded by one of the two declared classes;
//   2. a faithful replica matches the source, digest-for-digest;
//   3. equal row count + substituted content DIVERGES (the case counts miss);
//   4. row ORDER is not content: a reordered table still matches;
//   5. a by-design column difference (the stripped blocks.id surrogate) does
//      NOT raise a false alarm;
//   6. a row-count difference is SKIPPED, not reported as divergence;
//   7. the check never halts, and one unreadable table cannot sink the pass.

const assert = require('assert');

const lifecycle       = require('../../src/table_lifecycle');
const replicatedTables = require('../../src/schema/replicated_tables');

describe('Advisory table-content parity', function(){

    describe('coverage contract', function(){

        // The finding in one assertion: no replicated table may be silently
        // uncommitted. Every table in the per-block replicated set is either in the
        // content-parity plan or carries a declared reason for being out.
        for(const dbType of ['indexer', 'decoder']){
            it('every replicated ' + dbType + ' table is either covered or declared excluded', function(){
                let replicated = replicatedTables.getReplicatedTables(dbType);
                let covered    = new Set(replicatedTables.contentParityPlan(dbType).map(p => p.table));
                let excluded   = replicatedTables.contentParityExclusions(dbType);

                let orphans = replicated.filter(t => !covered.has(t) && excluded[t] === undefined);
                assert.deepStrictEqual(orphans, [],
                    'replicated tables with NO content commitment and NO declared exclusion: ' + orphans.join(', ') +
                    '. Add the table to the content-parity plan or declare why it cannot be bounded ' +
                    '(src/table_lifecycle.js CONTENT_PARITY_*).');

                // ...and nothing is claimed twice, which would hide an exclusion behind
                // a check that never runs (or vice versa).
                let both = replicated.filter(t => covered.has(t) && excluded[t] !== undefined);
                assert.deepStrictEqual(both, [], 'tables both covered and excluded: ' + both.join(', '));
            });
        }

        it('the operator carve-outs are exactly markets (indexer) and dispensers (decoder)', function(){
            // Pinned to the 2026-08-11 ruling. Widening this set is an operator
            // decision, not a code change: each carve-out is a replicated table left
            // with no content commitment at all.
            assert.deepStrictEqual(
                lifecycle.CONTENT_PARITY_CARVE_OUTS.map(c => c.dbType + ':' + c.table).sort(),
                ['decoder:dispensers', 'indexer:markets']);
            for(let c of lifecycle.CONTENT_PARITY_CARVE_OUTS)
                assert.ok(c.reason && c.reason.length > 20, c.table + ' carve-out must carry its reason');
        });

        it('the indexer dispensers table is covered even though the decoder one is carved out', function(){
            // Same table name, different schema and different lifecycle: the carve-out
            // is dbType-scoped, and treating it as a bare name would silently drop 1 of
            // the 93 covered indexer tables.
            assert.strictEqual(lifecycle.contentParityCarveOut('dispensers', 'decoder') !== null, true);
            assert.strictEqual(lifecycle.contentParityCarveOut('dispensers', 'indexer'), null);
            assert.ok(replicatedTables.contentParityPlan('indexer').some(p => p.table === 'dispensers'));
            assert.ok(!replicatedTables.contentParityPlan('decoder').some(p => p.table === 'dispensers'));
        });

        it('the second exclusion class is exactly the state_hash (in-place mutated) tables', function(){
            // These are excluded because they are ALREADY committed by the enforced
            // state_hash, and because an in-place edit in a later block moves content
            // inside an already-published window. Derived from the hash declarations,
            // so a new mutation class cannot join one without the other.
            let mutable  = new Set(lifecycle.contentParityMutableTables());
            let excluded = replicatedTables.contentParityExclusions('indexer');
            let carved   = new Set(lifecycle.CONTENT_PARITY_CARVE_OUTS.map(c => c.table));
            for(let table of Object.keys(excluded)){
                if(carved.has(table)) continue;
                assert.ok(mutable.has(table),
                    table + ' is excluded from content parity but declares no state_hash class; ' +
                    'an uncommitted table must not be silently dropped');
            }
            for(let table of ['stakes', 'bets', 'tokens', 'credits'])
                assert.ok(excluded[table], table + ' mutates in place and must ride state_hash, not the window checksum');
        });

        it('bounds match the scope each table actually replicates through', function(){
            let byTable = {};
            for(let p of replicatedTables.contentParityPlan('indexer')) byTable[p.table] = p.bound;
            assert.strictEqual(byTable['blocks'], 'block');
            assert.strictEqual(byTable['sends'], 'action');
            assert.strictEqual(byTable['sync_meta'], 'block');
            // contract_emissions carries a NULL action_index for internal emissions, so
            // it must NOT ride the generic action join (which would drop them).
            assert.strictEqual(byTable['contract_emissions'], 'emission');
            // The two reorg-scoped lookups carry a block stamp; the inert ones do not.
            assert.strictEqual(byTable['index_addresses'], 'block');
            assert.strictEqual(byTable['index_tickers'], 'block');
            assert.strictEqual(byTable['index_statuses'], 'id');

            let decoder = {};
            for(let p of replicatedTables.contentParityPlan('decoder')) decoder[p.table] = p.bound;
            assert.strictEqual(decoder['transaction_outputs'], 'tx');
            // The decoder schema stamps no block on its lookups, so index_addresses is
            // id-bounded there even though the indexer's copy is block-bounded.
            assert.strictEqual(decoder['index_addresses'], 'id');
        });

        it('the block-bounded read excludes NULL-block rows, as the index-map checksum does', function(){
            // Rows assigned outside a consensus block tx (recovery pre-seed, API
            // read-path createAddress) are benign source-local drift. Hashing them
            // would make an honest replica fail forever.
            const src = require('fs').readFileSync(require('path').join(__dirname, '../../src/db/tables.js'), 'utf8');
            const body = src.slice(src.indexOf('async getContentWindowRows('), src.indexOf('async getMaxRowId('));
            // The scope column is now the registry's (lifecycle.blockKey), so the guard
            // pins the PROPERTY (the same column, NULL-excluded, then range-bounded)
            // rather than the literal 'block_index' a hard-coded query would assume.
            assert.ok(/lifecycle\.blockKey\(table\)/.test(body),
                'getContentWindowRows must window by the registry scope column; the literal block_index ' +
                'raised errno 1054 on the close_block-keyed tables and the caller swallowed it');
            assert.ok(/" \+ key \+ " IS NOT NULL AND " \+ key \+ " BETWEEN/.test(body),
                'getContentWindowRows lost its NULL-block exclusion; benign out-of-band rows would false-alarm');
            assert.ok(/doQueryStrict/.test(body),
                'getContentWindowRows must read strictly: a fail-soft [] would read as "table has no rows" on both sides');
        });
    });

});
