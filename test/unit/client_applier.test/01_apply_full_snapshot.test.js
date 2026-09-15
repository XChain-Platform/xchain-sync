// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

const assert = require('assert');
const sinon  = require('sinon');
const ClientApplier = require('../../../src/client/applier');
const Utility = require('../../../src/util');
const { SCHEMA_VERSION } = require('../../../src/schema/version');
const balanceHelpers = require('../../../src/db/balance_helpers');
const { withDbMixins } = require('../../helpers/db_mixins.js');

function createMockDb(){
    return withDbMixins({
        doQuery: sinon.stub().resolves([]),
        getBlockHashRow: sinon.stub().resolves(null),
        beginTransaction: sinon.stub().resolves(),
        commitTransaction: sinon.stub().resolves(),
        rollbackTransaction: sinon.stub().resolves(),
        truncateTable: sinon.stub().resolves()
    });
}

let applier, db, util;

function registerHooks(){
    beforeEach(function(){
        db = createMockDb();
        util = new Utility();
        applier = new ClientApplier(db, util);
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });

    afterEach(function(){
        sinon.restore();
    });
}

function registerFullSnapshotCases1(){
    it('skips null snapshot', async function(){
        await applier.applyFullSnapshot(null);
        assert.strictEqual(db.beginTransaction.called, false);
    });
    it('skips snapshot without tables', async function(){
        await applier.applyFullSnapshot({ block_height: 10 });
        assert.strictEqual(db.beginTransaction.called, false);
    });
    it('clears tables in reverse order and inserts in forward order', async function(){
        let snapshot = {
            schema_version: SCHEMA_VERSION.indexer,
            block_height: 10,
            tables: {
                tableA: [{ id: 1 }],
                tableB: [{ id: 2 }]
            }
        };
        await applier.applyFullSnapshot(snapshot);
        assert.strictEqual(db.beginTransaction.calledOnce, true);
        // Tables are cleared child-before-parent (reverse of declared order)
        // via DELETE, not TRUNCATE: MariaDB rejects TRUNCATE on FK-referenced
        // tables, so the bootstrap uses FK-safe row-by-row DELETEs.
        let deletes = db.doQuery.getCalls()
            .map(c => c.args[0])
            .filter(q => /^DELETE FROM `/.test(q)); // generic whole-table clears only, not the scoped state_tree_roots delete
        assert.deepStrictEqual(deletes, ['DELETE FROM `tableB`', 'DELETE FROM `tableA`']);
        assert.strictEqual(db.commitTransaction.calledOnce, true);
    });
    it('clears a source-empty local table absent from the payload (re-bootstrap staleness)', async function(){
        // The builder omits zero-row source tables from the payload; a re-bootstrap
        // over a populated replica must still clear them (union with the local set).
        db.doQuery.withArgs(sinon.match(/information_schema\.tables/)).resolves([
            { table_name: 'blocks' },
            { table_name: 'transactions' }
        ]);
        let snapshot = {
            schema_version: SCHEMA_VERSION.indexer,
            block_height: 10,
            tables: { blocks: [{ block_index: 1 }] } // transactions empty on source, omitted
        };
        await applier.applyFullSnapshot(snapshot);
        let deletes = db.doQuery.getCalls().map(c => c.args[0]).filter(q => /^DELETE FROM `/.test(q)); // generic whole-table clears only
        // Union of payload + local snapshot-eligible tables, reverse dependency order.
        assert.deepStrictEqual(deletes, ['DELETE FROM `transactions`', 'DELETE FROM `blocks`']);
        assert.strictEqual(db.commitTransaction.calledOnce, true);
    });
}

function registerFullSnapshotCases2(){
    it('does NOT clear replica-local control tables sync_halt / sync_state on full-snapshot apply @regression', async function(){
        // These durable control tables are never shipped in a snapshot; the clear
        // loop must leave them untouched (they hold the halt audit record and the
        // bootstrap-base + index-map mismatch state). Regression for the crash-loop-
        // adjacent state-loss defect (source-parity monitor).
        db.doQuery.withArgs(sinon.match(/information_schema\.tables/)).resolves([
            { table_name: 'blocks' },
            { table_name: 'sync_halt' },
            { table_name: 'sync_state' }
        ]);
        let snapshot = {
            schema_version: SCHEMA_VERSION.indexer,
            block_height: 10,
            tables: { blocks: [{ block_index: 1 }] }
        };
        await applier.applyFullSnapshot(snapshot);
        let touched = db.doQuery.getCalls().map(c => c.args[0])
            .filter(q => /sync_halt|sync_state/.test(q));
        assert.deepStrictEqual(touched, [], 'sync_halt / sync_state must never be cleared by the snapshot apply');
    });
    it('scoped-clears state_tree_roots at/above the snapshot height before seeding @regression', async function(){
        // state_tree_roots is clear-protected (OPERATOR_LOCAL / follower-derived), so the
        // generic clear loop leaves it untouched while its backing state_tree_nodes store is
        // wiped and re-imported. Without a scoped delete, future-dated orphaned-fork roots
        // (heights above the snapshot after a deep reorg + oversized-incremental fallback)
        // survive and the follower serves them as authoritative SPV commitments. Regression
        // for the sync-snapshot-source-parity finding: the applier must delete
        // block_index >= snapshot height, mirroring ClientRollback's predicate.
        let snapshot = {
            schema_version: SCHEMA_VERSION.indexer,
            block_height: 10,
            tables: { blocks: [{ block_index: 1 }] }
        };
        await applier.applyFullSnapshot(snapshot);
        let scoped = db.doQuery.getCalls().find(c =>
            /DELETE FROM state_tree_roots/.test(c.args[0]) && /block_index >= \?/.test(c.args[0]));
        assert.ok(scoped, 'a scoped DELETE FROM state_tree_roots ... block_index >= ? must be issued');
        assert.strictEqual(scoped.args[1][scoped.args[1].length - 1], 10,
            'the delete must be bounded at the snapshot height');
        // It must NOT be swept up by the generic clear loop (that would prove it was not
        // clear-protected); the generic loop uses backtick-quoted whole-table deletes.
        let genericWipe = db.doQuery.getCalls()
            .map(c => c.args[0])
            .some(q => /^DELETE FROM `state_tree_roots`/.test(q));
        assert.strictEqual(genericWipe, false, 'state_tree_roots must not be whole-table wiped by the clear loop');
    });
}

function registerFullSnapshotCases3(){
    it('binds the scoped state_tree_roots clear to the TICKER, not the full coin name @regression', async function(){
        // SyncService constructs ClientApplier with cfg.coin, the hub's full lowercase
        // name ('bitcoin'), while every state_tree_roots writer is called with
        // this.coinTicker, so the rows carry 'BTC'. Binding this.chain made the scoped
        // clear match zero rows on every production chain: a permanent silent no-op that
        // left future-dated orphan roots for the Explorer to serve as commitments. The
        // enclosing suite's applier is built with NO chain, so both fields are null there
        // and cannot tell the two apart; this case supplies the full name on purpose.
        let localDb = createMockDb();
        let fullNameApplier = new ClientApplier(localDb, new Utility(), 'bitcoin', 'mainnet');
        assert.strictEqual(fullNameApplier.chain, 'bitcoin');
        assert.strictEqual(fullNameApplier.coinTicker, 'BTC', 'the two identities must actually differ here');

        await fullNameApplier.applyFullSnapshot({
            schema_version: SCHEMA_VERSION.indexer,
            block_height: 10,
            tables: { blocks: [{ block_index: 1 }] }
        });

        let scoped = localDb.doQuery.getCalls().find(c =>
            /DELETE FROM state_tree_roots/.test(c.args[0]) && /block_index >= \?/.test(c.args[0]));
        assert.ok(scoped, 'the scoped delete must still be issued');
        assert.strictEqual(scoped.args[1][0], 'BTC',
            'the chain predicate must bind the ticker the rows are written with');
        assert.notStrictEqual(scoped.args[1][0], 'bitcoin',
            'binding the full coin name makes the cleanup match zero rows');
    });
    it('ignores node-local tables (mempool_transactions) shipped by an older source', async function(){
        let snapshot = {
            schema_version: SCHEMA_VERSION.indexer,
            block_height: 10,
            tables: {
                blocks: [{ block_index: 1 }],
                mempool_transactions: [{ tx_hash: 'aa' }]
            }
        };
        await applier.applyFullSnapshot(snapshot);
        let touched = db.doQuery.getCalls().map(c => c.args[0]).filter(q => /mempool_transactions/.test(q));
        assert.deepStrictEqual(touched, []);
    });
    it('rolls back on error', async function(){
        db.doQuery.rejects(new Error('truncate fail'));
        let snapshot = { schema_version: SCHEMA_VERSION.indexer, block_height: 10, tables: { t: [{ id: 1 }] } };
        await assert.rejects(() => applier.applyFullSnapshot(snapshot));
        assert.strictEqual(db.rollbackTransaction.calledOnce, true);
    });
}

function registerFullSnapshotCases4(){
    it('aborts the bootstrap (no commit) when local table enumeration fails with a non-schema-gap error', async function(){
        // Fail closed: an enumeration failure other than a schema gap (1146/1054)
        // must propagate so the surrounding catch rolls the transaction back
        // instead of committing a bootstrap with localTables=[], which would
        // silently retain stale rows in tables omitted from the payload.
        let enumErr = new Error('connection lost');
        enumErr.errno = 2013;
        db.doQuery.withArgs(sinon.match(/information_schema\.tables/)).rejects(enumErr);
        let snapshot = {
            schema_version: SCHEMA_VERSION.indexer,
            block_height: 10,
            tables: { blocks: [{ block_index: 1 }] }
        };
        await assert.rejects(() => applier.applyFullSnapshot(snapshot), /connection lost/);
        assert.strictEqual(db.commitTransaction.called, false);
        assert.strictEqual(db.rollbackTransaction.calledOnce, true);
    });
    it('tolerates a genuine schema-gap error (1146) on local table enumeration', async function(){
        // A missing information_schema view/table on a thin/older replica is the
        // one enumeration failure safe to swallow; the apply proceeds with
        // localTables=[] (payload tables only) and still commits.
        let schemaGapErr = new Error('table does not exist');
        schemaGapErr.errno = 1146;
        db.doQuery.withArgs(sinon.match(/information_schema\.tables/)).rejects(schemaGapErr);
        let snapshot = {
            schema_version: SCHEMA_VERSION.indexer,
            block_height: 10,
            tables: { blocks: [{ block_index: 1 }] }
        };
        await applier.applyFullSnapshot(snapshot);
        assert.strictEqual(db.commitTransaction.calledOnce, true);
    });
    it('throws on a schema-version mismatch before opening a transaction', async function(){
        let snapshot = { schema_version: 'v0-wrong', block_height: 10, tables: { t: [{ id: 1 }] } };
        await assert.rejects(() => applier.applyFullSnapshot(snapshot), /Schema version mismatch/);
        assert.strictEqual(db.beginTransaction.called, false);
    });
    it('fails closed on an invalid table name (rejects rather than silently dropping its rows)', async function(){
        let snapshot = {
            schema_version: SCHEMA_VERSION.indexer,
            block_height: 10,
            tables: { 'bad-name;drop': [{ id: 1 }], good: [{ id: 2 }] }
        };
        // Fail closed: an invalid identifier must abort the apply (transaction rolls
        // back) rather than silently drop the table's rows and commit a short replica.
        await assert.rejects(() => applier.applyFullSnapshot(snapshot), /Rejected table name/);
    });
}
describe('ClientApplier', function(){
    registerHooks();

    describe('applyFullSnapshot', function(){
        registerFullSnapshotCases1();
        registerFullSnapshotCases2();
        registerFullSnapshotCases3();
        registerFullSnapshotCases4();
    });
});
