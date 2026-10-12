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
// Source-side snapshot classification guards. A table the builder does not
// classify reaches the incremental action_index query, raises errno 1054 and is
// dropped as though the source schema were older: silent, and the failure mode
// that once froze pubkeys and merkle_reorgs on incrementally caught-up followers.

const assert = require('assert');
const fs = require('fs');
const path = require('path');
const sinon = require('sinon');
const lifecycle = require('../../src/table_lifecycle');
const SnapshotBuilder = require('../../src/server/snapshot_builder');
const {
    indexerBlockScopedSet,
    indexerFullDumpSet,
} = require('../../src/server/snapshot_builder/table_sets');
const Utility = require('../../src/util');

const {
    OPERATOR_LOCAL_TABLES,
    SOURCE_UNSTREAMED_TABLES,
    REPLICA_LEDGER_TABLES,
    INDEXER_INCREMENTAL_SKIP,
} = SnapshotBuilder;

function selectCtx(firstActionIndex){
    return {
        sinceBlock: 100, conn: null, skipLookups: false, lookupSet: new Set(),
        firstActionIndex, indexerBlockScoped: indexerBlockScopedSet(),
        indexerFullDump: indexerFullDumpSet(),
    };
}

describe('indexer incremental-snapshot buckets (snapshot-class exhaustiveness) @regression', function(){
    const snapshotTables = lifecycle.tablesWhere(t => t.replication === 'snapshot');

    it('places every snapshot-class registry table in exactly one incremental bucket', function(){
        assert.ok(snapshotTables.length >= 6,
            'registry snapshot-class enumeration looks broken: ' + snapshotTables.join(','));
        const buckets = [indexerBlockScopedSet(), indexerFullDumpSet(), INDEXER_INCREMENTAL_SKIP];
        const misplaced = snapshotTables.filter(t => buckets.filter(b => b.has(t)).length !== 1);
        assert.deepStrictEqual(misplaced, [],
            'snapshot-class table(s) not in exactly one of indexerBlockScopedSet, indexerFullDumpSet ' +
            'or INDEXER_INCREMENTAL_SKIP: ' + misplaced.join(', ') + '. An unclassified table falls ' +
            'to the action_index query, hits errno 1054, is dropped as an older-schema gap and ' +
            'freezes at bootstrap height on every incrementally caught-up follower.');
    });

    it('keeps the skip set to snapshot-class tables that every snapshot still streams', function(){
        for(const table of INDEXER_INCREMENTAL_SKIP){
            assert.ok(snapshotTables.includes(table),
                table + ' is in INDEXER_INCREMENTAL_SKIP but its registry entry is not ' +
                "replication 'snapshot'; remove it from the skip set");
            for(const [name, set] of [['OPERATOR_LOCAL_TABLES', OPERATOR_LOCAL_TABLES],
                                      ['SOURCE_UNSTREAMED_TABLES', SOURCE_UNSTREAMED_TABLES],
                                      ['REPLICA_LEDGER_TABLES', REPLICA_LEDGER_TABLES]])
                assert.ok(!set.has(table), table + ' is in both INDEXER_INCREMENTAL_SKIP and ' + name);
        }
    });

    it('returns no rows for a skipped table without running the action_index query', async function(){
        const builder = new SnapshotBuilder(new Utility());
        const db = { findRowsFromActionIndex: sinon.stub().rejects(new Error('action_index query reached')) };
        for(const table of INDEXER_INCREMENTAL_SKIP){
            assert.strictEqual(await builder.selectIndexerRows(db, table, selectCtx(7)), null, table);
            assert.strictEqual(await builder.selectIndexerRows(db, table, selectCtx(null)), null, table);
        }
        assert.strictEqual(db.findRowsFromActionIndex.callCount, 0);
    });

    it('keeps push_generations node-local and outside every snapshot bucket', async function(){
        assert.strictEqual(lifecycle.entry('push_generations').replication, 'local');
        assert.ok(OPERATOR_LOCAL_TABLES.has('push_generations'));
        assert.ok(!snapshotTables.includes('push_generations'));
        for(const bucket of [indexerBlockScopedSet(), indexerFullDumpSet(), INDEXER_INCREMENTAL_SKIP])
            assert.ok(!bucket.has('push_generations'));

        const builder = new SnapshotBuilder(new Utility());
        const db = { findStreamableTableNames: sinon.stub().resolves([
            { table_name: 'blocks' }, { table_name: 'push_generations' },
        ]) };
        assert.deepStrictEqual(await builder.getOrderedTables(db), ['blocks']);
    });
});

describe('replica-owned migration ledger classification', function(){
    it('classifies schema_migrations as the replica ledger, apart from the other exclusion sets', function(){
        // The src/sql exhaustiveness scan cannot see this table (the migration runner
        // creates it), so it is pinned by name.
        assert.ok(REPLICA_LEDGER_TABLES.has('schema_migrations'));
        for(const table of REPLICA_LEDGER_TABLES){
            assert.ok(!OPERATOR_LOCAL_TABLES.has(table), table + ' is also in OPERATOR_LOCAL_TABLES');
            assert.ok(!SOURCE_UNSTREAMED_TABLES.has(table),
                table + ' is also in SOURCE_UNSTREAMED_TABLES, whose full-snapshot clear would erase it');
        }
    });

    it('keeps the source ledger out of every snapshot the builder streams', async function(){
        const builder = new SnapshotBuilder(new Utility());
        const db = { findStreamableTableNames: sinon.stub().resolves([
            { table_name: 'blocks' }, { table_name: 'schema_migrations' }, { table_name: 'merkle_reorgs' },
        ]) };
        assert.deepStrictEqual(await builder.getOrderedTables(db), ['blocks']);
    });

    it('defines the replica ledger set once, for the builder and the applier alike', function(){
        const applier = fs.readFileSync(path.join(__dirname, '../../src/client/applier.js'), 'utf8');
        assert.ok(!/new Set\(\[\s*'schema_migrations'/.test(applier),
            'ClientApplier must read REPLICA_LEDGER_TABLES from the snapshot table sets, not redefine it');
        assert.ok(/REPLICA_LEDGER_TABLES[^\n]*require\('\.\.\/server\/snapshot_builder'\)/.test(applier));
    });

    it('stamps a schema version that clients without the applier ledger guard refuse', function(){
        // Clients below indexer 18 / decoder 5 lack the guard and would clear their own ledger
        // on a full apply; the version gate refuses this builder's snapshots there first.
        const { SCHEMA_VERSION } = require('../../src/schema/version');
        assert.ok(SCHEMA_VERSION.indexer >= 18, 'indexer SCHEMA_VERSION ' + SCHEMA_VERSION.indexer);
        assert.ok(SCHEMA_VERSION.decoder >= 5, 'decoder SCHEMA_VERSION ' + SCHEMA_VERSION.decoder);
    });

    it('refuses a full snapshot from another schema version before clearing any table', async function(){
        const { SCHEMA_VERSION } = require('../../src/schema/version');
        const ClientApplier = require('../../src/client/applier');
        const db = { dbType: 'indexer', deleteAllRows: sinon.stub().resolves(),
            beginTransaction: sinon.stub().resolves(), rollbackTransaction: sinon.stub().resolves() };
        const applier = new ClientApplier(db, new Utility());
        await assert.rejects(applier.applyFullSnapshot({
            schema_version: SCHEMA_VERSION.indexer + 1, block_height: 10, tables: { blocks: [] },
        }), /Schema version mismatch/);
        assert.strictEqual(db.deleteAllRows.callCount, 0);
        assert.strictEqual(db.beginTransaction.callCount, 0);
    });
});
