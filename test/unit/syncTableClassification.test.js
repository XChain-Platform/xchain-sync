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
// Registry-exhaustiveness guard for the sync-owned table set (item #2283).
//
// Every table created from xchain-sync/src/sql/*.sql exists on every
// indexer-shaped DB (db.verifySyncTables applies each file), so every one of
// them MUST be deliberately classified for the replication seam, in exactly
// one place:
//
//   - a tableLifecycle registry entry (participates in replication artifacts:
//     stream topology / snapshot / replica rollback buckets), or
//   - SnapshotBuilder.OPERATOR_LOCAL_TABLES (node-local state, clear-PROTECTED
//     on full-snapshot apply), or
//   - SnapshotBuilder.SOURCE_UNSTREAMED_TABLES (never streamed, but still
//     CLEARED on full-snapshot apply so previously-imported foreign copies
//     heal).
//
// merkle_reorgs fell through every set until 2026-07: full snapshots shipped
// the source node's reorg audit trail to every bootstrapping replica while the
// incremental path's errno 1054 was silently swallowed. mempool_transactions
// and state_tree_roots were earlier instances of the same retro-fix. This
// test makes "the registries are exhaustive" structurally true so the next
// sync-owned table is classified on the day it is added.

const assert = require('assert');
const fs = require('fs');
const path = require('path');
const lifecycle = require('../../src/tableLifecycle');
const { OPERATOR_LOCAL_TABLES, SOURCE_UNSTREAMED_TABLES } = require('../../src/SnapshotBuilder');

describe('sync-owned table classification (registry exhaustiveness)', function(){
    const sqlDir = path.join(__dirname, '..', '..', 'src', 'sql');
    const sqlTables = fs.readdirSync(sqlDir)
        .filter(f => f.endsWith('.sql'))
        .map(f => path.basename(f, '.sql'));
    const lifecycleTables = new Set(lifecycle.allTables().map(t => t.table));

    it('classifies every src/sql table in at least one registry', function(){
        assert.ok(sqlTables.length >= 6, 'sql dir enumeration looks broken: ' + sqlTables.join(','));
        for(const table of sqlTables){
            const memberships = [
                lifecycleTables.has(table) ? 'tableLifecycle' : null,
                OPERATOR_LOCAL_TABLES.has(table) ? 'OPERATOR_LOCAL_TABLES' : null,
                SOURCE_UNSTREAMED_TABLES.has(table) ? 'SOURCE_UNSTREAMED_TABLES' : null,
            ].filter(Boolean);
            // A lifecycle entry may legitimately pair with OPERATOR_LOCAL_TABLES
            // (state_tree_roots: the entry itself documents the exclusion), so the
            // invariant is at-least-one, not exactly-one.
            assert.ok(memberships.length >= 1,
                table + ' is classified in NO registry - it rides full snapshots by ' +
                'accident and is silently swallowed out of incrementals (the ' +
                'merkle_reorgs failure mode). Add a tableLifecycle entry, or put it in ' +
                'OPERATOR_LOCAL_TABLES / SOURCE_UNSTREAMED_TABLES deliberately.');
        }
    });

    it('never puts a table in both snapshot-builder sets (contradictory clear semantics)', function(){
        for(const table of SOURCE_UNSTREAMED_TABLES){
            assert.ok(!OPERATOR_LOCAL_TABLES.has(table),
                table + ' is in both OPERATOR_LOCAL_TABLES (clear-protected) and ' +
                'SOURCE_UNSTREAMED_TABLES (deliberately clearable) - pick one');
        }
    });

    it('classifies sync_state (created ad hoc by db.js, no .sql file)', function(){
        assert.ok(OPERATOR_LOCAL_TABLES.has('sync_state'),
            'sync_state is a replica-local durable control table and must stay clear-protected');
    });

    it('keeps merkle_reorgs out of the streamed set but in the clearable set', function(){
        assert.ok(SOURCE_UNSTREAMED_TABLES.has('merkle_reorgs'));
        assert.ok(!OPERATOR_LOCAL_TABLES.has('merkle_reorgs'),
            'merkle_reorgs must NOT be clear-protected: replicas that imported a ' +
            'foreign copy before the exclusion heal via the full-snapshot clear loop');
    });
});
