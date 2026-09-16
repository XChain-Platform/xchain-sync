// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

const {
    assert,
    sinon,
    PassThrough,
    Readable,
    EventEmitter,
    zlib,
    SnapshotBuilder,
    SnapshotStreamWriter,
    Utility,
    poolSizing,
    createMockDb,
    createMockRes
} = require('./snapshot_builder.test/helpers/support');

let builder;

function prepareSnapshotBuilder(){
    const util = new Utility();
    builder = new SnapshotBuilder(util);
    sinon.stub(console, 'error');
}

function restoreSnapshotBuilder(){
    sinon.restore();
}

function getOrderedTablesTests(){
    it('orders priority tables first, trailing tables last, middle alphabetically', async function(){
        let db = createMockDb();
        db.doQuery.resolves([
            { table_name: 'balances' },      // trailing
            { table_name: 'zebra' },          // middle
            { table_name: 'actions' },        // priority
            { table_name: 'blocks' },         // priority
            { table_name: 'apple' },          // middle
            { table_name: 'sync_meta' },      // trailing
            { table_name: 'index_actions' }   // priority (first)
        ]);

        let ordered = await builder.getOrderedTables(db);

        // Priority tables come first in defined order
        assert.strictEqual(ordered[0], 'index_actions');
        assert.ok(ordered.indexOf('blocks') < ordered.indexOf('actions'));
        assert.ok(ordered.indexOf('actions') < ordered.indexOf('apple'));

        // Middle tables alphabetically
        assert.ok(ordered.indexOf('apple') < ordered.indexOf('zebra'));

        // Trailing tables last
        assert.ok(ordered.indexOf('zebra') < ordered.indexOf('balances'));
        assert.strictEqual(ordered[ordered.length - 1], 'sync_meta');
    });

    it('skips priority/trailing tables not in DB', async function(){
        let db = createMockDb();
        db.doQuery.resolves([
            { table_name: 'blocks' },
            { table_name: 'custom_table' }
        ]);
        let ordered = await builder.getOrderedTables(db);
        assert.strictEqual(ordered.length, 2);
        assert.strictEqual(ordered[0], 'blocks');
        assert.strictEqual(ordered[1], 'custom_table');
    });
}

function getOrderedTablesExclusionTests(){
    it('excludes mempool_transactions (node-local, non-deterministic) like every other channel', async function(){
        // The per-block stream, the incremental decoderSkip, and the /status
        // completeness count all exclude mempool_transactions; the FULL snapshot
        // must too, or full-bootstrap replicas freeze the source's mempool forever.
        let db = createMockDb();
        db.doQuery.resolves([
            { table_name: 'blocks' },
            { table_name: 'mempool_transactions' },
            { table_name: 'oracle_prices' } // existing operator-local exclusion, sanity check
        ]);
        let ordered = await builder.getOrderedTables(db);
        assert.deepStrictEqual(ordered, ['blocks']);
    });
}

function snapshotBuilderTests(){
    beforeEach(prepareSnapshotBuilder);
    afterEach(restoreSnapshotBuilder);
    describe('getOrderedTables', getOrderedTablesTests);
    describe('getOrderedTables', getOrderedTablesExclusionTests);
}

describe('SnapshotBuilder', snapshotBuilderTests);
