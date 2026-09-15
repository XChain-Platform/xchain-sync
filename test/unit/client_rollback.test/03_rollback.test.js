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
const ClientRollback = require('../../../src/client/rollback');
const Utility = require('../../../src/util');

function createMockDb(){
    return {
        doQuery: sinon.stub().resolves([]),
        getFirstActionIndex: sinon.stub().resolves(500),
        getStatusId: sinon.stub().resolves(null),
        beginTransaction: sinon.stub().resolves(),
        commitTransaction: sinon.stub().resolves(),
        rollbackTransaction: sinon.stub().resolves()
    };
}

let rollback, db, util;

function registerHooks(){
    beforeEach(function(){
        db = createMockDb();
        util = new Utility();
        rollback = new ClientRollback(db, util, undefined, 'regtest');
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });

    afterEach(function(){
        sinon.restore();
    });
}

// ATTEST v5 batch head: an orphaned v6 continuation that COMPLETED the batch stamped the
// failure verdict in place on the surviving head. Deleting the continuation cannot undo
// that, and the head then goes missing from its own chunk set (the reader accepts 'valid'
// only), so the window is permanently dead on this replica while the source restores it
// (xchain-indexer/src/rollback.js). The forward channel ships the stamp to every follower,
// so without this reset every follower that reorgs across a completing chunk diverges.
const ATTEST_HEAD_RE = /UPDATE attests p .*batch_chunk_index = 0/;

describe('ClientRollback', function(){

    describe('rollback', function(){
        registerHooks();
        it('restores a stamped ATTEST v5 batch head before the action-scoped deletes', async function(){
            await rollback.rollback(100);
            let calls = db.doQuery.getCalls();
            let reset = calls.find(c => ATTEST_HEAD_RE.test(c.args[0]));
            assert.ok(reset, 'expected the ATTEST batch-head status restore');
            let sql = reset.args[0];
            // ONLY a marked stamp is restored: a head terminal AT WRITE TIME (duplicate head,
            // foreign network, single-chunk quorum failure) must never be revived.
            assert.ok(/JOIN index_statuses ps ON ps\.id = p\.status_id AND ps\.status LIKE \?/.test(sql),
                'head-status join must match the completion stamp, not any non-valid status');
            assert.strictEqual(reset.args[1][0], '% (stamped on batch completion)');
            // Same request, a v6 continuation inside the orphaned range, chunk-indexed and valid.
            assert.ok(/JOIN attests c ON c\.request_id = p\.request_id/.test(sql));
            assert.ok(sql.includes('c.version = 6'));
            assert.ok(sql.includes('c.batch_chunk_index IS NOT NULL'));
            assert.ok(/JOIN index_statuses cs ON cs\.id = c\.status_id AND cs\.status = 'valid'/.test(sql));
            // Publisher scope on the action source ids (fail-closed on an unresolvable author).
            assert.ok(/JOIN actions ca ON ca\.action_index = c\.action_index AND ca\.source_id = pa\.source_id/.test(sql));
            // Restore target is 'valid', the one value the flip could have overwritten.
            assert.ok(/JOIN index_statuses vs ON vs\.status = 'valid' SET p\.status_id = vs\.id/.test(sql));
            assert.ok(sql.includes('p.version = 5'));
            assert.deepStrictEqual(reset.args[1], ['% (stamped on batch completion)', 500, 500]);
            // Both rows must still exist when it runs.
            let resetIdx  = calls.indexOf(reset);
            let deleteIdx = calls.findIndex(c => c.args[0].includes('DELETE FROM `attests` WHERE action_index >= ?'));
            assert.ok(deleteIdx >= 0, 'expected the action-scoped attests delete');
            assert.ok(resetIdx < deleteIdx, 'the head restore must precede the delete that removes the continuation');
        });

        it('skips the ATTEST batch-head restore when firstActionIndex is null (no orphaned continuation)', async function(){
            db.getFirstActionIndex.resolves(null);
            await rollback.rollback(100);
            assert.ok(!db.doQuery.getCalls().some(c => ATTEST_HEAD_RE.test(c.args[0])));
        });

        it('swallows a schema gap on the ATTEST batch-head restore but aborts on a transient fault', async function(){
            let gap = new Error('no such column'); gap.errno = 1054;
            db.doQuery.withArgs(sinon.match(ATTEST_HEAD_RE)).rejects(gap);
            await rollback.rollback(100); // must not throw on a pre-batch-rail replica schema

            let deadlock = new Error('deadlock'); deadlock.errno = 1213;
            db.doQuery.withArgs(sinon.match(ATTEST_HEAD_RE)).rejects(deadlock);
            await assert.rejects(rollback.rollback(100), /deadlock/,
                'a transient fault must abort the reorg rather than commit a partial rollback');
        });

    });
});

describe('ClientRollback', function(){

    describe('rollback', function(){
        registerHooks();
        // A derived anchor reward is EARNED at the checkpoint's snapshot_block but MATERIALIZED
        // at a later BTC block, so the block_index loop above cannot reach it for a reorg landing
        // between the two heights. The replica must drop exactly what the source drops or the
        // two disagree on SUM(validator_rewards).
        it('deletes validator_rewards by derive_block_index as well, mirroring the source', async function(){
            await rollback.rollback(100);
            let calls = db.doQuery.getCalls();
            let del = calls.find(c => /DELETE FROM validator_rewards WHERE derive_block_index\s*>=\s*\?/.test(c.args[0]));
            assert.ok(del, 'expected a validator_rewards delete scoped on the materialization block');
            assert.deepStrictEqual(del.args[1], [100]);
            let delIdx   = calls.indexOf(del);
            let indexIdx = calls.findIndex(c => c.args[0].includes('DELETE FROM `index_addresses`'));
            assert.ok(indexIdx >= 0, 'expected the index_addresses rollback delete');
            assert.ok(delIdx < indexIdx, 'the derive-block delete must precede the index-lookup deletes');
        });

        it('rolls back transaction on error and rethrows', async function(){
            db.commitTransaction.rejects(new Error('commit fail'));
            await assert.rejects(() => rollback.rollback(100), { message: 'commit fail' });
            assert.strictEqual(db.rollbackTransaction.calledOnce, true);
        });

        it('handles per-table errors gracefully (table may not exist)', async function(){
            // Simulate a MariaDB "table not found" error (errno 1146) on the 3rd query.
            // All bespoke optional-delete paths check e.errno 1146/1054 and swallow it.
            let callCount = 0;
            db.doQuery.callsFake(async (query) => {
                callCount++;
                if(callCount === 3) throw Object.assign(new Error('Table does not exist'), { errno: 1146 });
                return [];
            });
            // Should not throw; individual table errors are caught
            await rollback.rollback(100);
            assert.strictEqual(db.commitTransaction.calledOnce, true);
        });

    });
});

describe('ClientRollback', function(){

    describe('rollback', function(){
        registerHooks();
        it('swallows missing-table errors on every optional delete + the generic loops', async function(){
            // Make each optional delete throw a genuine schema gap (errno 1146); rollback
            // must still complete. Every optional-delete sweep (price_snapshots, sync_meta,
            // merkle_epochs, attest_validator_stats, markets/pubkeys, oracle_prices, the
            // cross-chain-mirror pair) now discriminates on errno 1146/1054 (a real MariaDB
            // "no such table" always carries it), as do the generic per-table loops.
            db.doQuery.callsFake(async (query) => {
                if(/price_snapshots|sync_meta|merkle_epochs|attest_validator_stats/.test(query))
                    throw Object.assign(new Error('Table does not exist'), { errno: 1146 });
                if(/contract_emissions|icons WHERE token_id NOT IN/.test(query))
                    throw Object.assign(new Error('Table does not exist'), { errno: 1146 });
                if(/DELETE FROM `blocks`/.test(query))
                    throw Object.assign(new Error('Table does not exist'), { errno: 1146 }); // blockTables loop
                return [];
            });
            await rollback.rollback(100);
            assert.strictEqual(db.commitTransaction.calledOnce, true);
        });

        // The sync_meta / merkle_epochs / mirror-table reorg deletes abort on any error
        // rather than swallowing it: a swallowed error commits a PARTIAL rollback, and for
        // merkle_epochs that silently reinstates the stale UNIQUE root the delete exists
        // to purge (the corrected re-dump is INSERT IGNORE and collides).
        it('aborts (fail-closed) on a transient error in the merkle_epochs reorg delete', async function(){
            db.doQuery.callsFake(async (query) => {
                if(/merkle_epochs/.test(query))
                    throw Object.assign(new Error('Lock wait timeout'), { errno: 1205 });
                return [];
            });
            await assert.rejects(() => rollback.rollback(100), { errno: 1205 });
            assert.strictEqual(db.rollbackTransaction.calledOnce, true, 'txn rolled back, not partially committed');
            assert.strictEqual(db.commitTransaction.called, false, 'a partial rollback must never commit');
        });

        it('aborts (fail-closed) on a transient error in the sync_meta reorg delete', async function(){
            db.doQuery.callsFake(async (query) => {
                if(/sync_meta/.test(query))
                    throw Object.assign(new Error('Deadlock found'), { errno: 1213 });
                return [];
            });
            await assert.rejects(() => rollback.rollback(100), { errno: 1213 });
            assert.strictEqual(db.rollbackTransaction.calledOnce, true);
            assert.strictEqual(db.commitTransaction.called, false);
        });

    });
});

describe('ClientRollback', function(){

    describe('rollback', function(){
        registerHooks();
        // The contract_emissions delete (consensus table) and the icons orphan-sweep
        // swallow ONLY errno 1146 (a missing-table schema gap) and abort on any other
        // error, so a transient fault cannot commit a partial rollback.
        it('aborts (fail-closed) on a transient error in the contract_emissions delete', async function(){
            db.doQuery.callsFake(async (query) => {
                if(/contract_emissions/.test(query))
                    throw Object.assign(new Error('Lock wait timeout'), { errno: 1205 });
                return [];
            });
            await assert.rejects(() => rollback.rollback(100), { errno: 1205 });
            assert.strictEqual(db.rollbackTransaction.calledOnce, true, 'txn rolled back, not partially committed');
            assert.strictEqual(db.commitTransaction.called, false, 'a partial rollback must never commit');
        });

        it('aborts (fail-closed) on a transient error in the icons orphan-sweep', async function(){
            db.doQuery.callsFake(async (query) => {
                if(/icons WHERE token_id NOT IN/.test(query))
                    throw Object.assign(new Error('Deadlock found'), { errno: 1213 });
                return [];
            });
            await assert.rejects(() => rollback.rollback(100), { errno: 1213 });
            assert.strictEqual(db.rollbackTransaction.calledOnce, true);
            assert.strictEqual(db.commitTransaction.called, false);
        });

    });
});

describe('ClientRollback', function(){

    describe('rollback', function(){
        registerHooks();
        // The generic dataTables/blockTables/indexTables DELETE loops abort the whole
        // rollback (fail closed) on any non-schema-gap error: swallowing it commits a
        // PARTIAL rollback that only the next block's VERIFY_STATE_COMMITMENT recompute
        // catches, and that recompute is off for truncated replicas.
        it('aborts the rollback (fail-closed) on a transient error in a generic DELETE loop', async function(){
            db.doQuery.callsFake(async (query) => {
                // A deadlock (errno 1213) on a consensus data-table delete: not a schema gap.
                if(/DELETE FROM `credits`/.test(query))
                    throw Object.assign(new Error('Deadlock found'), { errno: 1213 });
                return [];
            });
            await assert.rejects(() => rollback.rollback(100), { errno: 1213 });
            assert.strictEqual(db.rollbackTransaction.calledOnce, true, 'txn rolled back, not partially committed');
            assert.strictEqual(db.commitTransaction.called, false, 'a partial rollback must never commit');
        });

        it('aborts the rollback on a transient error in the blockTables loop', async function(){
            db.doQuery.callsFake(async (query) => {
                if(/DELETE FROM `blocks`/.test(query))
                    throw Object.assign(new Error('Lock wait timeout'), { errno: 1205 });
                return [];
            });
            await assert.rejects(() => rollback.rollback(100), { errno: 1205 });
            assert.strictEqual(db.rollbackTransaction.calledOnce, true);
        });
    });
});
