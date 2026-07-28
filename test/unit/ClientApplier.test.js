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
const ClientApplier = require('../../src/ClientApplier');
const Utility = require('../../src/utility');
const { SCHEMA_VERSION } = require('../../src/schema-version');
const balanceHelpers = require('../../src/balance-helpers');

function createMockDb(){
    return {
        doQuery: sinon.stub().resolves([]),
        getBlockHashRow: sinon.stub().resolves(null),
        beginTransaction: sinon.stub().resolves(),
        commitTransaction: sinon.stub().resolves(),
        rollbackTransaction: sinon.stub().resolves(),
        truncateTable: sinon.stub().resolves()
    };
}

describe('ClientApplier', function(){

    let applier, db, util;

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

    describe('applyBlock', function(){
        it('skips null payload', async function(){
            await applier.applyBlock(null);
            assert.strictEqual(db.beginTransaction.called, false);
        });

        it('skips payload without data', async function(){
            await applier.applyBlock({ block_index: 1 });
            assert.strictEqual(db.beginTransaction.called, false);
        });

        it('skips payload without block_index', async function(){
            await applier.applyBlock({ data: {} });
            assert.strictEqual(db.beginTransaction.called, false);
        });

        it('applies the genesis block (block_index 0) instead of silently dropping it', async function(){
            await applier.applyBlock({
                block_index: 0,
                data: { blocks: [{ block_index: 0, block_time: 0 }] }
            });
            assert.strictEqual(db.beginTransaction.calledOnce, true);
            assert.strictEqual(db.commitTransaction.calledOnce, true);
        });

        it('skips existing block (duplicate detection)', async function(){
            db.getBlockHashRow.resolves({ block_index: 1, ledger_hash: 'abc' });
            await applier.applyBlock({ block_index: 1, data: { blocks: [{ block_index: 1 }] } });
            assert.strictEqual(db.beginTransaction.called, false);
        });

        it('applies block in a transaction', async function(){
            let payload = {
                block_index: 5,
                data: {
                    blocks: [{ block_index: 5, block_time: 100 }],
                    transactions: [{ tx_index: 1, block_index: 5 }]
                }
            };
            await applier.applyBlock(payload);
            assert.strictEqual(db.beginTransaction.calledOnce, true);
            assert.strictEqual(db.commitTransaction.calledOnce, true);
            assert.ok(db.doQuery.called);
        });

        it('rejects a live block payload with a mismatched schema_version', async function(){
            let payload = {
                block_index: 5,
                schema_version: 9999,
                data: { blocks: [{ block_index: 5 }] }
            };
            await assert.rejects(() => applier.applyBlock(payload), /Schema version mismatch/);
            assert.strictEqual(db.beginTransaction.called, false);
        });

        it('accepts a live block payload with a matching schema_version', async function(){
            let payload = {
                block_index: 5,
                schema_version: SCHEMA_VERSION[db.dbType || 'indexer'],
                data: { blocks: [{ block_index: 5 }] }
            };
            await applier.applyBlock(payload);
            assert.strictEqual(db.beginTransaction.calledOnce, true);
        });

        it('accepts a live block payload without schema_version (pre-5250 server)', async function(){
            let payload = {
                block_index: 5,
                data: { blocks: [{ block_index: 5 }] }
            };
            await applier.applyBlock(payload); // must not throw
            assert.strictEqual(db.beginTransaction.calledOnce, true);
        });

        it('rolls back on error', async function(){
            db.doQuery.rejects(new Error('insert fail'));
            let payload = {
                block_index: 5,
                data: { blocks: [{ block_index: 5 }] }
            };
            await assert.rejects(() => applier.applyBlock(payload), { message: 'insert fail' });
            assert.strictEqual(db.rollbackTransaction.calledOnce, true);
        });

        it('skips empty table arrays', async function(){
            let payload = {
                block_index: 5,
                data: { blocks: [], transactions: [{ tx_index: 1 }] }
            };
            await applier.applyBlock(payload);
            // Only 1 INSERT for transactions (blocks is empty)
            assert.strictEqual(db.doQuery.callCount, 1);
        });

        it('rebuilds balances when an indexer payload touches credits/debits', async function(){
            db.dbType = 'indexer';
            let rb = sinon.stub(balanceHelpers, 'rebuildBalances').resolves();
            await applier.applyBlock({ block_index: 5, data: { credits: [{ id: 1 }] } });
            assert.strictEqual(rb.calledOnce, true);
        });

        it('does NOT rebuild balances on a decoder replica', async function(){
            db.dbType = 'decoder';
            let rb = sinon.stub(balanceHelpers, 'rebuildBalances').resolves();
            await applier.applyBlock({ block_index: 5, data: { credits: [{ id: 1 }] } });
            assert.strictEqual(rb.called, false);
        });
    });

    describe('_rebuildBalances error handling', function(){
        it('swallows a 1146 (table-missing) error on rebuildBalances', async function(){
            sinon.stub(balanceHelpers, 'rebuildBalances').rejects(Object.assign(new Error('no table'), { errno: 1146 }));
            await applier._rebuildBalances(); // must not throw
        });

        it('rethrows a non-1146 error on rebuildBalances', async function(){
            sinon.stub(balanceHelpers, 'rebuildBalances').rejects(Object.assign(new Error('real'), { errno: 1234 }));
            await assert.rejects(() => applier._rebuildBalances(), { message: 'real' });
        });
    });

    describe('scoped balance rebuilds', function(){
        beforeEach(function(){ db.dbType = 'indexer'; });

        it('passes the distinct touched (address_id, tick_id) ids to rebuildBalances', async function(){
            let rb = sinon.stub(balanceHelpers, 'rebuildBalances').resolves();
            await applier.applyBlock({ block_index: 5, data: {
                credits: [{ address_id: 7, tick_id: 3, amount: '1' }, { address_id: 7, tick_id: 3, amount: '2' }],
                debits:  [{ address_id: 9, tick_id: 3, amount: '1' }]
            }});
            assert.strictEqual(rb.calledOnce, true);
            assert.deepStrictEqual(rb.firstCall.args[1], { addressIds: [7, 9], tickIds: [3] });
        });

        it('falls back to the FULL rebuild when a row is missing its ids', async function(){
            let rb = sinon.stub(balanceHelpers, 'rebuildBalances').resolves();
            await applier.applyBlock({ block_index: 5, data: {
                credits: [{ address_id: 7, tick_id: 3 }, { address_id: null, tick_id: 3 }]
            }});
            assert.strictEqual(rb.calledOnce, true);
            assert.strictEqual(rb.firstCall.args[1], undefined);
        });

        it('falls back to the FULL rebuild when the touched-id set exceeds the IN-list cap', async function(){
            let rb = sinon.stub(balanceHelpers, 'rebuildBalances').resolves();
            let credits = [];
            for(let i = 0; i < 1001; i++) credits.push({ address_id: i + 1, tick_id: 1, amount: '1' });
            await applier.applyBlock({ block_index: 5, data: { credits } });
            assert.strictEqual(rb.calledOnce, true);
            assert.strictEqual(rb.firstCall.args[1], undefined);
        });

        it('skips the rebuild entirely when the touched tables are empty arrays', async function(){
            let rb  = sinon.stub(balanceHelpers, 'rebuildBalances').resolves();
            await applier.applyBlock({ block_index: 5, data: { credits: [], deposits: [], blocks: [{ block_index: 5 }] } });
            assert.strictEqual(rb.called, false);
        });

        it('scopes the incremental catch-up rebuild the same way', async function(){
            let rb = sinon.stub(balanceHelpers, 'rebuildBalances').resolves();
            await applier.applyIncrementalSnapshot({
                schema_version: SCHEMA_VERSION.indexer,
                since_block: 10,
                tables: { debits: [{ address_id: 12, tick_id: 5, amount: '3' }] }
            });
            assert.strictEqual(rb.calledOnce, true);
            assert.deepStrictEqual(rb.firstCall.args[1], { addressIds: [12], tickIds: [5] });
        });
    });

    describe('applyFullSnapshot', function(){
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
    });

    describe('applyIncrementalSnapshot', function(){
        it('inserts rows without truncation', async function(){
            let snapshot = {
                schema_version: SCHEMA_VERSION.indexer,
                block_height: 20,
                since_block: 10,
                tables: { blocks: [{ block_index: 11 }] }
            };
            await applier.applyIncrementalSnapshot(snapshot);
            assert.strictEqual(db.truncateTable.called, false);
            assert.strictEqual(db.beginTransaction.calledOnce, true);
            assert.strictEqual(db.commitTransaction.calledOnce, true);
        });

        it('skips null snapshot', async function(){
            await applier.applyIncrementalSnapshot(null);
            assert.strictEqual(db.beginTransaction.called, false);
        });

        it('skips a snapshot without tables', async function(){
            await applier.applyIncrementalSnapshot({ since_block: 1 });
            assert.strictEqual(db.beginTransaction.called, false);
        });

        it('throws on a schema-version mismatch', async function(){
            await assert.rejects(
                () => applier.applyIncrementalSnapshot({ schema_version: 'wrong', tables: { t: [{ id: 1 }] } }),
                /Schema version mismatch/);
            assert.strictEqual(db.beginTransaction.called, false);
        });

        it('rebuilds balances when the catch-up touches credits/debits', async function(){
            db.dbType = 'indexer';
            let rb  = sinon.stub(balanceHelpers, 'rebuildBalances').resolves();
            await applier.applyIncrementalSnapshot({
                schema_version: SCHEMA_VERSION.indexer,
                since_block: 10,
                tables: { debits: [{ id: 1 }] }
            });
            assert.strictEqual(rb.calledOnce, true);
        });

        it('rolls back on error', async function(){
            db.doQuery.rejects(new Error('inc fail'));
            // Carries block_index because `blocks` rows are now identified by it; a
            // bare {id} row is refused before any query runs, which would mask the
            // DB failure this case exists to check.
            await assert.rejects(() => applier.applyIncrementalSnapshot({
                schema_version: SCHEMA_VERSION.indexer, since_block: 1, tables: { blocks: [{ id: 1, block_index: 1 }] }
            }), { message: 'inc fail' });
            assert.strictEqual(db.rollbackTransaction.calledOnce, true);
        });
    });

    describe('_insertRows', function(){
        it('does nothing for empty rows', async function(){
            await applier._insertRows('blocks', []);
            assert.strictEqual(db.doQuery.called, false);
        });

        it('does nothing for null rows', async function(){
            await applier._insertRows('blocks', null);
            assert.strictEqual(db.doQuery.called, false);
        });

        it('uses INSERT IGNORE for index tables', async function(){
            await applier._insertRows('index_actions', [{ id: 1, name: 'test' }]);
            let query = db.doQuery.firstCall.args[0];
            assert.ok(query.startsWith('INSERT IGNORE'));
        });

        it('uses INSERT for non-index tables', async function(){
            await applier._insertRows('blocks', [{ block_index: 1 }]);
            let query = db.doQuery.firstCall.args[0];
            assert.ok(query.startsWith('INSERT INTO'));
            assert.ok(!query.includes('IGNORE'));
        });

        // #4771: c793db4 added merkle_epochs (INSERT IGNORE, append-only) and the
        // mutable-aggregate full-dump tables markets / attest_validator_stats
        // (INSERT ... ON DUPLICATE KEY UPDATE so a re-dump refreshes stale values
        // instead of skipping them). Pin both so a future edit can't silently drop
        // either mode (the F-2 divergence class) and pass CI.
        it('uses INSERT IGNORE for append-only merkle_epochs', async function(){
            await applier._insertRows('merkle_epochs', [{ epoch: 1, root: 'aa' }]);
            let query = db.doQuery.firstCall.args[0];
            assert.ok(query.startsWith('INSERT IGNORE'), 'merkle_epochs must be INSERT IGNORE');
            assert.ok(!query.includes('ON DUPLICATE KEY UPDATE'));
        });

        for(const table of ['markets', 'attest_validator_stats']){
            it('upserts ' + table + ' with ON DUPLICATE KEY UPDATE covering every column', async function(){
                await applier._insertRows(table, [{ id: 1, a: 'x', b: 'y' }]);
                let query = db.doQuery.firstCall.args[0];
                assert.ok(query.startsWith('INSERT INTO'), table + ' upsert starts as INSERT (not IGNORE)');
                assert.ok(!query.startsWith('INSERT IGNORE'), table + ' must not be INSERT IGNORE');
                assert.ok(query.includes('ON DUPLICATE KEY UPDATE'), table + ' must upsert');
                for(const col of ['id', 'a', 'b']){
                    assert.ok(query.includes('`' + col + '` = VALUES(`' + col + '`)'),
                        table + ' upsert must refresh column ' + col);
                }
            });
        }

        // These three cover generic column/batch handling and used `blocks` only as an
        // arbitrary plain-INSERT table, with synthetic {id} rows a real blocks row never
        // has (it always carries block_index). `blocks` is now a localSurrogateIdTables
        // member whose id is stripped, so it is no longer a neutral stand-in; retargeted
        // to `actions`, which takes the plain-INSERT path. blocks' own behaviour is
        // pinned separately below.
        it('batches inserts in groups of 100', async function(){
            let rows = [];
            for(let i = 0; i < 250; i++) rows.push({ id: i });
            await applier._insertRows('actions', rows);
            assert.strictEqual(db.doQuery.callCount, 3); // 100 + 100 + 50
        });

        it('handles null column values', async function(){
            await applier._insertRows('actions', [{ id: 1, name: null }]);
            let args = db.doQuery.firstCall.args[1];
            assert.strictEqual(args[1], null);
        });

        it('handles undefined column values as null', async function(){
            await applier._insertRows('actions', [{ id: 1, name: undefined }]);
            let args = db.doQuery.firstCall.args[1];
            assert.strictEqual(args[1], null);
        });

        // Item 808: litecoin/mainnet/indexer froze for days, reporting halted:false,
        // because the source streamed a `blocks` row carrying its own surrogate id that
        // the replica had long ago assigned to a different block, so every apply died on
        // ER_DUP_ENTRY and rolled back the whole transaction. The replica must not
        // inherit that id at all.
        describe('blocks surrogate id (item 808)', function(){
            it('strips the source id so the replica assigns its own', async function(){
                await applier._insertRows('blocks', [{ id: 27681, block_index: 3147670, block_time: 5 }]);
                let insert = db.doQuery.getCalls().map(c => c.args[0]).find(q => /^INSERT/.test(q));
                assert.ok(!insert.includes('`id`'), 'the source surrogate id must not be replicated');
                assert.ok(insert.includes('`block_index`') && insert.includes('`block_time`'),
                    'every other column must still be written');
            });

            it('deletes the existing row for that block_index first, so a re-send is idempotent', async function(){
                await applier._insertRows('blocks', [{ id: 27681, block_index: 3147670 }]);
                let calls = db.doQuery.getCalls().map(c => c.args[0]);
                let delIdx = calls.findIndex(q => /^DELETE FROM `blocks`/.test(q));
                let insIdx = calls.findIndex(q => /^INSERT/.test(q));
                assert.ok(delIdx !== -1, 'must clear the natural key before inserting');
                assert.ok(delIdx < insIdx, 'the DELETE must precede the INSERT');
                assert.ok(calls[delIdx].includes('`block_index` IN'), 'the DELETE must be scoped to block_index');
                assert.deepStrictEqual(db.doQuery.getCall(delIdx).args[1], [3147670],
                    'the DELETE must be scoped to exactly the block_index being applied');
            });

            it('scopes the delete to the applied blocks only, never the whole table', async function(){
                await applier._insertRows('blocks', [
                    { id: 1, block_index: 10 },
                    { id: 2, block_index: 11 }
                ]);
                let del = db.doQuery.getCalls().find(c => /^DELETE FROM `blocks`/.test(c.args[0]));
                assert.deepStrictEqual(del.args[1], [10, 11]);
                assert.ok(!/DELETE FROM `blocks`\s*$/.test(del.args[0]), 'must never be an unscoped wipe');
            });

            it('does not use IGNORE or UPSERT, which would drop or overwrite a block', async function(){
                await applier._insertRows('blocks', [{ id: 1, block_index: 10 }]);
                let insert = db.doQuery.getCalls().map(c => c.args[0]).find(q => /^INSERT/.test(q));
                assert.ok(!insert.startsWith('INSERT IGNORE'), 'IGNORE would silently skip the block');
                assert.ok(!insert.includes('ON DUPLICATE KEY UPDATE'),
                    'UPSERT would overwrite whichever unrelated block holds that id');
            });

            it('fails closed on a row with no block_index rather than appending a duplicate', async function(){
                // block_index is a plain INDEX, not UNIQUE, so an unscoped insert cannot
                // be de-duplicated afterwards; refuse instead of corrupting the table.
                await assert.rejects(
                    () => applier._insertRows('blocks', [{ id: 1, block_time: 5 }]),
                    /missing its natural key block_index/);
            });

            it('leaves a legacy row that carries no id untouched', async function(){
                await applier._insertRows('blocks', [{ block_index: 10, block_time: 5 }]);
                let calls = db.doQuery.getCalls().map(c => c.args[0]);
                assert.ok(!calls.some(q => /^DELETE/.test(q)), 'no id to strip means no delete is needed');
                assert.ok(calls.some(q => /^INSERT INTO `blocks`/.test(q)));
            });
        });

        it('backtick-wraps column names', async function(){
            await applier._insertRows('blocks', [{ 'block_index': 1 }]);
            let query = db.doQuery.firstCall.args[0];
            assert.ok(query.includes('`block_index`'));
        });

        it('throws on an invalid table name without querying (fail closed)', async function(){
            await assert.rejects(() => applier._insertRows('bad;name', [{ id: 1 }]), /Rejected table name/);
            assert.strictEqual(db.doQuery.called, false);
        });

        it('throws on an invalid column name without querying (fail closed)', async function(){
            await assert.rejects(() => applier._insertRows('blocks', [{ 'bad-col': 1 }]), /Rejected column name/);
            assert.strictEqual(db.doQuery.called, false);
        });
    });

    describe('applyDispensersReplace', function(){
        it('is a no-op on a non-decoder DB', async function(){
            // createMockDb has no dbType (indexer-shaped); the guard short-circuits.
            await applier.applyDispensersReplace([{ tx_index: 1, address_id: 2 }]);
            assert.strictEqual(db.beginTransaction.called, false);
            assert.strictEqual(db.doQuery.called, false);
        });

        it('replaces atomically: DELETE then INSERT inside one transaction (decoder)', async function(){
            db.dbType = 'decoder';
            await applier.applyDispensersReplace([
                { tx_index: 5, address_id: 9, expiration: 1700000000, expired_block_index: null },
                { tx_index: 7, address_id: 2, expiration: 1700000001, expired_block_index: 42 }
            ]);
            assert.ok(db.beginTransaction.calledOnce, 'opened a transaction');
            assert.ok(db.commitTransaction.calledOnce, 'committed');
            assert.strictEqual(db.rollbackTransaction.called, false);
            let calls = db.doQuery.getCalls().map(c => c.args[0]);
            assert.ok(/^DELETE FROM `dispensers`/.test(calls[0]), 'DELETE runs first');
            assert.ok(calls.some(q => /^INSERT INTO `dispensers`/.test(q)), 'rows re-inserted with plain INSERT (not IGNORE)');
        });

        it('clears the table even when the new set is empty (decoder)', async function(){
            db.dbType = 'decoder';
            await applier.applyDispensersReplace([]);
            assert.ok(db.beginTransaction.calledOnce);
            assert.ok(db.commitTransaction.calledOnce);
            let calls = db.doQuery.getCalls().map(c => c.args[0]);
            assert.ok(/^DELETE FROM `dispensers`/.test(calls[0]));
            assert.ok(!calls.some(q => /^INSERT/.test(q)), 'no INSERT for an empty set');
        });

        it('rolls back and rethrows if a write fails (decoder, table left intact)', async function(){
            db.dbType = 'decoder';
            db.doQuery.rejects(new Error('boom'));
            let threw = false;
            try { await applier.applyDispensersReplace([{ tx_index: 1, address_id: 2 }]); }
            catch(e){ threw = true; }
            assert.ok(threw, 'error propagates so the caller can leave the table intact');
            assert.ok(db.rollbackTransaction.calledOnce, 'transaction rolled back');
            assert.strictEqual(db.commitTransaction.called, false);
        });
    });
});
