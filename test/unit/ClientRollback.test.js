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
const ClientRollback = require('../../src/ClientRollback');
const Utility = require('../../src/utility');
const balanceHelpers = require('../../src/balance-helpers');

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

describe('ClientRollback', function(){

    let rollback, db, util;

    beforeEach(function(){
        db = createMockDb();
        util = new Utility();
        rollback = new ClientRollback(db, util);
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });

    afterEach(function(){
        sinon.restore();
    });

    describe('table lists', function(){
        it('has 12 block-scoped tables', function(){
            assert.strictEqual(rollback.blockTables.length, 12);
            assert.ok(rollback.blockTables.includes('blocks'));
            assert.ok(rollback.blockTables.includes('transactions'));
            assert.ok(rollback.blockTables.includes('slash_events'));
            assert.ok(rollback.blockTables.includes('contract_slash_debits'));
            // Pre-rotation signing keys for the DELEGATE v1 materialization sweep (#4366),
            // block-scoped for the same reason as the slash-debit log: the reorg restore
            // reads it before the generic delete drops the orphaned rows.
            assert.ok(rollback.blockTables.includes('contract_delegation_rotations'));
            // RB-ANCHOR: pre-image log of reconcile-deleted validator_rewards losers.
            assert.ok(rollback.blockTables.includes('anchor_reward_reconcile_log'));
            // WI-2 bump 2 capability-stake equivocation slashing (committed 8e95482).
            assert.ok(rollback.blockTables.includes('capability_slash_events'));
            assert.ok(rollback.blockTables.includes('capability_slash_debits'));
            // Light-client per-block SMT roots (SPV spec sec.4), block-scoped so
            // orphaned-fork roots drop on reorg; state_tree_nodes stays (COW/immutable).
            assert.ok(rollback.blockTables.includes('state_tree_roots'));
            // Per-(address,tick) locked totals (SPV sub-tree spec Stage B). It MUST be
            // block-scoped: the escrow leaf derivation threads from the surviving row
            // after a reorg, so a journal row left behind by an orphaned block would
            // commit a locked amount for a lock that no longer exists.
            assert.ok(rollback.blockTables.includes('escrow_leaf_journal'));
        });

        it('has action-scoped data tables', function(){
            assert.ok(rollback.dataTables.length > 40);
            assert.ok(rollback.dataTables.includes('actions'));
            assert.ok(rollback.dataTables.includes('credits'));
            assert.ok(rollback.dataTables.includes('debits'));
            assert.ok(rollback.dataTables.includes('attests'));
            assert.ok(rollback.dataTables.includes('balances') === false); // balances are recalculated, not in dataTables
        });
    });

    describe('rollback', function(){
        it('wraps everything in a transaction', async function(){
            await rollback.rollback(100);
            assert.strictEqual(db.beginTransaction.calledOnce, true);
            assert.strictEqual(db.commitTransaction.calledOnce, true);
            assert.ok(db.beginTransaction.calledBefore(db.commitTransaction));
        });

        it('gets first action index for the block', async function(){
            await rollback.rollback(100);
            assert.strictEqual(db.getFirstActionIndex.calledOnce, true);
            assert.strictEqual(db.getFirstActionIndex.firstCall.args[0], 100);
        });

        it('deletes contract_emissions first', async function(){
            await rollback.rollback(100);
            let firstDelete = db.doQuery.getCalls().find(c => c.args[0].includes('DELETE'));
            assert.ok(firstDelete.args[0].includes('contract_emissions'));
        });

        it('deletes from action-scoped tables with action_index', async function(){
            await rollback.rollback(100);
            let actionDeletes = db.doQuery.getCalls().filter(c =>
                c.args[0].includes('DELETE FROM') && c.args[0].includes('action_index >=') &&
                !c.args[0].includes('contract_emissions') && !c.args[0].includes('oracle_prices') &&
                !c.args[0].includes('cross_chain_calls') && !c.args[0].includes('cross_chain_matches')
            );
            // Should have one delete per dataTable. oracle_prices and the two cross_chain
            // mirrors (cross_chain_calls / cross_chain_matches) are bespoke source_chain-
            // qualified deletes, excluded above (their source_action_index / a_action_index
            // predicates also contain the 'action_index >=' substring this filter keys on).
            assert.strictEqual(actionDeletes.length, rollback.dataTables.length);
            // Each should use firstActionIndex = 500
            for(let call of actionDeletes){
                assert.deepStrictEqual(call.args[1], [500]);
            }
        });

        it('deletes from block-scoped tables with block_index', async function(){
            await rollback.rollback(100);
            let blockDeletes = db.doQuery.getCalls().filter(c =>
                c.args[0].includes('DELETE FROM') && c.args[0].includes('block_index >=') && !c.args[0].includes('sync_meta')
            );
            // Block-scoped deletes cover both blockTables and the index id tables
            // (index_addresses / index_tickers), which are also pruned by block_index >=, plus
            // the one validator_rewards delete keyed on derive_block_index, whose
            // column name ends in the same substring this filter matches on.
            assert.strictEqual(blockDeletes.length,
                rollback.blockTables.length + rollback.indexTables.length + 1);
            for(let call of blockDeletes){
                assert.deepStrictEqual(call.args[1], [100]);
            }
        });

        it('deletes from sync_meta', async function(){
            await rollback.rollback(100);
            let syncMetaDelete = db.doQuery.getCalls().find(c =>
                c.args[0].includes('sync_meta') && c.args[0].includes('DELETE')
            );
            assert.ok(syncMetaDelete);
            assert.deepStrictEqual(syncMetaDelete.args[1], [100]);
        });

        it('deletes from merkle_epochs by end_block, mirroring the server pruneFrom (item 4770)', async function(){
            await rollback.rollback(100);
            // merkle_epochs is sync-owned and snapshot-ride-along only (applied
            // INSERT IGNORE), so without this delete a reorg that re-roots a closed
            // epoch leaves the follower serving the stale root forever. It is keyed
            // by end_block, not block_index.
            let merkleDelete = db.doQuery.getCalls().find(c =>
                c.args[0].includes('merkle_epochs') && c.args[0].includes('DELETE')
            );
            assert.ok(merkleDelete, 'expected a DELETE FROM merkle_epochs on reorg');
            assert.ok(/end_block\s*>=\s*\?/.test(merkleDelete.args[0]),
                'merkle_epochs delete must be scoped by end_block >= ?');
            assert.deepStrictEqual(merkleDelete.args[1], [100]);
        });

        it('recalculates balances from credits/debits', async function(){
            await rollback.rollback(100);
            let balanceDelete = db.doQuery.getCalls().find(c =>
                c.args[0] === 'DELETE FROM balances'
            );
            assert.ok(balanceDelete);
            let balanceInsert = db.doQuery.getCalls().find(c =>
                c.args[0].includes('INSERT INTO balances') && c.args[0].includes('credits')
            );
            assert.ok(balanceInsert);
        });

        it('skips action-scoped deletes when firstActionIndex is null', async function(){
            db.getFirstActionIndex.resolves(null);
            await rollback.rollback(100);
            let actionDeletes = db.doQuery.getCalls().filter(c =>
                c.args[0].includes('action_index >=')
            );
            assert.strictEqual(actionDeletes.length, 0);
        });

        // tokens.escrow_action_index is RE-DERIVED after the dataTables delete (mirror of
        // xchain-indexer rollback.js, TP-03 #4017): set to the surviving open GIVE_OWNERSHIP
        // offer's action_index, else NULL. Collapses both directions and matches the source
        // byte-for-byte.
        const AFFECTED_RE  = /escrow_action_index IS NOT NULL/i;
        const OPENOFFER_RE = /SELECT o\.action_index FROM orders/i;
        const REDERIVE_RE  = /UPDATE tokens SET escrow_action_index=\?\s+WHERE tick_id=\(SELECT id FROM index_tickers/i;

        it('re-stamps escrow to a surviving open offer (orphaned release / CLEAR direction)', async function(){
            db.doQuery.withArgs(sinon.match(AFFECTED_RE)).resolves([{ tick: 'FOO' }]);
            db.doQuery.withArgs(sinon.match(OPENOFFER_RE)).resolves([{ action_index: 30 }]);
            await rollback.rollback(100);
            let rederive = db.doQuery.getCalls().find(c => REDERIVE_RE.test(c.args[0]));
            assert.ok(rederive, 'expected a re-derive UPDATE on tokens.escrow_action_index');
            assert.deepStrictEqual(rederive.args[1], [30, 'FOO']);
            // old SET-only reset must be gone
            assert.ok(!db.doQuery.getCalls().some(c => /escrow_action_index\s*>=\s*\?/i.test(c.args[0])),
                'the old SET-only `escrow_action_index >= ?` reset must no longer be issued');
        });

        it('clears escrow when no offer survives (orphaned offer / SET direction)', async function(){
            db.doQuery.withArgs(sinon.match(AFFECTED_RE)).resolves([{ tick: 'BAR' }]);
            db.doQuery.withArgs(sinon.match(OPENOFFER_RE)).resolves([]);
            await rollback.rollback(100);
            let rederive = db.doQuery.getCalls().find(c => REDERIVE_RE.test(c.args[0]));
            assert.ok(rederive, 'expected a re-derive UPDATE');
            assert.deepStrictEqual(rederive.args[1], [null, 'BAR']);
        });

        it('re-derives escrow AFTER the action-scoped deletes', async function(){
            db.doQuery.withArgs(sinon.match(AFFECTED_RE)).resolves([{ tick: 'FOO' }]);
            await rollback.rollback(100);
            let calls = db.doQuery.getCalls();
            let tokenDeleteIdx = calls.findIndex(c => c.args[0].includes('DELETE FROM `tokens`'));
            let affectedIdx    = calls.findIndex(c => AFFECTED_RE.test(c.args[0]));
            assert.ok(tokenDeleteIdx >= 0 && affectedIdx >= 0);
            assert.ok(affectedIdx > tokenDeleteIdx, 'escrow re-derive must run AFTER the tokens delete');
        });

        it('does not throw if the escrow re-derive tables are missing (older replica schema)', async function(){
            // Simulate a MariaDB "table not found" error with errno 1146 (the errno the
            // schema-gap catch checks for). A plain Error without errno would be rethrown.
            db.doQuery.withArgs(sinon.match(AFFECTED_RE))
                .rejects(Object.assign(new Error('Table does not exist'), { errno: 1146 }));
            await rollback.rollback(100);
            assert.strictEqual(db.commitTransaction.calledOnce, true, 'rollback still commits');
        });

        it('replays the attests and xcalls request_status resets (block_index-keyed)', async function(){
            await rollback.rollback(100);
            let attestReset = db.doQuery.getCalls().find(c =>
                c.args[0].includes('UPDATE attests') && c.args[0].includes("request_status = 'pending'")
            );
            assert.ok(attestReset, 'expected an attests request_status reset');
            assert.deepStrictEqual(attestReset.args[1], [100]); // block_index
            let xcallReset = db.doQuery.getCalls().find(c =>
                c.args[0].includes('UPDATE xcalls') && c.args[0].includes("request_status = 'pending'")
            );
            assert.ok(xcallReset, 'expected an xcalls request_status reset');
            assert.deepStrictEqual(xcallReset.args[1], [100]); // block_index
        });

        it('runs the in-place resets before the action-scoped deletes', async function(){
            await rollback.rollback(100);
            let calls = db.doQuery.getCalls();
            // attests/xcalls request_status resets are in-place on surviving rows and must
            // precede the deletes (escrow is the exception: it is re-derived AFTER, tested above).
            let attestIdx = calls.findIndex(c => c.args[0].includes('UPDATE attests') && c.args[0].includes("request_status = 'pending'"));
            let tokenDeleteIdx = calls.findIndex(c => c.args[0].includes('DELETE FROM `tokens`'));
            assert.ok(attestIdx >= 0 && tokenDeleteIdx >= 0);
            assert.ok(attestIdx < tokenDeleteIdx, 'attests reset must precede the deletes');
        });

        it('skips the in-place resets and the escrow re-derive when firstActionIndex is null', async function(){
            db.getFirstActionIndex.resolves(null);
            await rollback.rollback(100);
            let touched = db.doQuery.getCalls().filter(c =>
                AFFECTED_RE.test(c.args[0]) ||
                REDERIVE_RE.test(c.args[0]) ||
                (c.args[0].includes("request_status = 'pending'"))
            );
            assert.strictEqual(touched.length, 0);
        });

        it('replays the slash-debit restore for both stake tables (block_index-keyed, before deletes)', async function(){
            await rollback.rollback(100);
            let calls = db.doQuery.getCalls();
            let stakeRestore = calls.find(c => c.args[0].includes('UPDATE contract_stakes') && c.args[0].includes('contract_slash_debits') && c.args[0].includes('SET t.amount = d.prev_amount'));
            let unstakeRestore = calls.find(c => c.args[0].includes('UPDATE contract_unstakes') && c.args[0].includes('contract_slash_debits'));
            assert.ok(stakeRestore, 'expected a contract_stakes slash restore');
            assert.ok(unstakeRestore, 'expected a contract_unstakes slash restore');
            assert.deepStrictEqual(stakeRestore.args[1], ['contract_stakes', 100, 100]);
            assert.deepStrictEqual(unstakeRestore.args[1], ['contract_unstakes', 100, 100]);
            let restoreIdx = calls.indexOf(stakeRestore);
            let deleteIdx = calls.findIndex(c => c.args[0].includes('DELETE FROM `contract_stakes`'));
            assert.ok(restoreIdx >= 0 && deleteIdx >= 0 && restoreIdx < deleteIdx, 'slash restore must precede the contract_stakes delete');
        });

        it('contract slash-restore tiebreaks on (execution_index, slash_position), byte-matching the source (not AUTO_INCREMENT id)', async function(){
            // Must mirror xchain-indexer rollback.js exactly. If the replica tiebreaks on the
            // AUTO_INCREMENT `id` (assigned in physical insert order, differs source-vs-replica),
            // a reorg retracting a block with >=2 contract slashes on one stake_action_index
            // restores a divergent prev_amount on the replica -> stake-weight / quorum fork.
            await rollback.rollback(100);
            let calls = db.doQuery.getCalls();
            let restores = calls.filter(c =>
                /UPDATE contract_(?:un)?stakes/.test(c.args[0]) &&
                c.args[0].includes('contract_slash_debits') &&
                c.args[0].includes('d.prev_amount'));
            assert.strictEqual(restores.length, 2, 'expected contract_stakes + contract_unstakes slash restores');
            for(let r of restores){
                let sql = r.args[0];
                assert.ok(/e\.execution_index\s*<\s*d\.execution_index/.test(sql),
                    'restore must order by execution_index (deterministic, replay-stable)');
                assert.ok(/e\.slash_position\s*<\s*d\.slash_position/.test(sql),
                    'restore must use slash_position as the within-EXECUTE secondary tiebreak');
                assert.ok(!/e\.id\s*<\s*d\.id/.test(sql),
                    'restore must NOT tiebreak on the non-deterministic AUTO_INCREMENT id');
            }
        });

        it('replays the delegation-rotation key restore into contract_stakes (block_index-keyed, before deletes)', async function(){
            // Forward twin: updatedRows carries a materialized DELEGATE v1 rotation to the
            // replica. On a reorg the replica must copy prev_signing_pubkey_id back exactly as
            // the source does, or it keeps a key the source has reverted and hands contracts a
            // different staker set (#4366).
            await rollback.rollback(100);
            let calls = db.doQuery.getCalls();
            let restores = calls.filter(c =>
                /UPDATE contract_(?:un)?stakes/.test(c.args[0]) &&
                c.args[0].includes('contract_delegation_rotations') &&
                c.args[0].includes('SET t.signing_pubkey_id = r.prev_signing_pubkey_id'));
            assert.strictEqual(restores.length, 2,
                'both stake-ledger tables rotate, so both must restore (cooldown rows are slashable)');
            let restore = restores[0];
            assert.deepStrictEqual(restore.args[1], ['contract_stakes', 100, 100]);
            assert.deepStrictEqual(restores[1].args[1], ['contract_unstakes', 100, 100]);
            assert.ok(/e\.delegation_action_index\s*<\s*r\.delegation_action_index/.test(restore.args[0]),
                'restore must tiebreak on delegation_action_index (replay-stable), byte-matching the source');
            assert.ok(!/e\.id\s*<\s*r\.id/.test(restore.args[0]),
                'restore must NOT tiebreak on the non-deterministic AUTO_INCREMENT id');
            let restoreIdx = calls.indexOf(restore);
            let deleteIdx  = calls.findIndex(c => c.args[0].includes('DELETE FROM `contract_stakes`'));
            assert.ok(restoreIdx >= 0 && deleteIdx >= 0 && restoreIdx < deleteIdx,
                'key restore must precede the contract_stakes delete');
        });

        it('replays the anchor reconcile-log restore into validator_rewards (block_index-keyed, before deletes) (RB-ANCHOR)', async function(){
            await rollback.rollback(100);
            let calls = db.doQuery.getCalls();
            let restore = calls.find(c =>
                c.args[0].includes('INSERT IGNORE INTO validator_rewards') &&
                c.args[0].includes('anchor_reward_reconcile_log'));
            assert.ok(restore, 'expected an anchor_reward_reconcile_log restore into validator_rewards');
            // Scoped to reconciles being orphaned (block_index >= reorg) that deleted losers whose
            // ORIGINAL earn-block SURVIVES the reorg (reward_block_index < reorg); byte-mirrors the source.
            assert.ok(/d\.block_index\s*>=\s*\?/.test(restore.args[0]));
            assert.ok(/d\.reward_block_index\s*<\s*\?/.test(restore.args[0]));
            // A loser MATERIALIZED inside the orphaned range must stay deleted, or the
            // replica restores an orphan the source (and a from-genesis replay) does not have.
            assert.ok(/d\.reward_derive_block_index IS NULL OR d\.reward_derive_block_index\s*<\s*\?/.test(restore.args[0]),
                'restore must also require the loser materialization block to survive the reorg');
            assert.ok(/derive_block_index\)/.test(restore.args[0]),
                'restore must carry derive_block_index back onto the restored row');
            assert.deepStrictEqual(restore.args[1], [100, 100, 100]);
            let restoreIdx = calls.indexOf(restore);
            let deleteIdx = calls.findIndex(c => c.args[0].includes('DELETE FROM `validator_rewards`'));
            assert.ok(restoreIdx >= 0 && deleteIdx >= 0 && restoreIdx < deleteIdx, 'reconcile restore must precede the validator_rewards delete');
        });

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

        // Item 1848: the sync_meta / merkle_epochs / mirror-table reorg deletes used to
        // swallow EVERY error, committing a PARTIAL rollback on a transient fault. For
        // merkle_epochs this silently reinstates the stale UNIQUE root the delete exists
        // to purge (the corrected re-dump is INSERT IGNORE and collides). They now abort.
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

        // Operator-approved fail-closed errno-1146 gate (run-4): the contract_emissions
        // delete (consensus table) and the icons orphan-sweep previously swallowed EVERY
        // error, so a transient fault committed a partial rollback. They now swallow ONLY
        // errno 1146 and abort on anything else.
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

        // Residual hardening (2026-07-08 re-sweep): the generic dataTables/blockTables/
        // indexTables DELETE loops used to swallow EVERY error, so a transient deadlock/
        // lock-wait committed a PARTIAL rollback - caught only by the next block's
        // VERIFY_STATE_COMMITMENT recompute, which is off for truncated replicas. A
        // non-schema-gap error must now abort the whole rollback (fail closed).
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

    // The replica used to run ONLY the dangling-tick markets sweep, so a market whose
    // pair kept both ticks but lost its only order/trade to the reorg was deleted on
    // the source and retained here. markets rides the snapshot as an UPSERT-only full
    // dump, so no later replication could remove the stale zeroed OHLCV row that
    // xchain-explorer then served.
    describe('pair-scoped markets rollback (IDX-2 mirror)', function(){
        // Answer the affected-pair collection with one pair, everything else empty, so
        // the survival probes find no surviving orders/order_matches for it.
        function orphanOnePair(pair){
            db.doQuery.callsFake(async (query) => {
                if(/FROM orders\b[\s\S]*action_index >= \?/.test(query) && /UNION/.test(query))
                    return [{ tick1_id: pair[0], tick2_id: pair[1] }];
                return [];
            });
        }

        it('deletes a market whose pair kept no surviving order or match', async function(){
            orphanOnePair([7, 9]);
            await rollback.rollback(100);
            let del = db.doQuery.getCalls().find(c =>
                /DELETE FROM markets WHERE \(tick1_id=\? AND tick2_id=\?\)/.test(c.args[0]));
            assert.ok(del, 'pair-scoped markets delete must run for an orphaned pair');
            assert.deepStrictEqual(del.args[1], [7, 9, 9, 7], 'both pair orientations must be deleted');
            assert.strictEqual(db.commitTransaction.calledOnce, true);
        });

        it('collects the affected pairs BEFORE the action-scoped delete removes them', async function(){
            orphanOnePair([7, 9]);
            await rollback.rollback(100);
            let calls = db.doQuery.getCalls();
            let collectIdx = calls.findIndex(c => /UNION/.test(c.args[0]) && /order_matches/.test(c.args[0]));
            let ordersDelIdx = calls.findIndex(c => /DELETE FROM `orders` WHERE action_index >= \?/.test(c.args[0]));
            assert.ok(collectIdx >= 0 && ordersDelIdx >= 0);
            assert.ok(collectIdx < ordersDelIdx,
                'the pair collection must read orders before the dataTables delete empties the orphaned range');
        });

        it('keeps the market when a surviving order still references the pair', async function(){
            db.doQuery.callsFake(async (query) => {
                if(/FROM orders\b[\s\S]*action_index >= \?/.test(query) && /UNION/.test(query))
                    return [{ tick1_id: 7, tick2_id: 9 }];
                if(/SELECT 1 FROM orders WHERE \(give_tick_id=\?/.test(query)) return [{ 1: 1 }];
                return [];
            });
            await rollback.rollback(100);
            assert.ok(!db.doQuery.getCalls().some(c =>
                /DELETE FROM markets WHERE \(tick1_id=\?/.test(c.args[0])),
                'a pair with a surviving order must keep its market row');
        });

        it('keeps the market when only an order_match survives', async function(){
            db.doQuery.callsFake(async (query) => {
                if(/FROM orders\b[\s\S]*action_index >= \?/.test(query) && /UNION/.test(query))
                    return [{ tick1_id: 7, tick2_id: 9 }];
                if(/SELECT 1 FROM order_matches WHERE \(give_tick_id=\?/.test(query)) return [{ 1: 1 }];
                return [];
            });
            await rollback.rollback(100);
            assert.ok(!db.doQuery.getCalls().some(c =>
                /DELETE FROM markets WHERE \(tick1_id=\?/.test(c.args[0])),
                'a pair with a surviving match must keep its market row');
        });

        // A truncated replica holds only [base..tip] of orders/order_matches, so "no
        // surviving reference" there can simply mean "traded below my join floor". The
        // sweep is skipped rather than deleting a market the source still keeps.
        it('skips the sweep on a truncated replica', async function(){
            db.getSyncState = sinon.stub().resolves('900');
            orphanOnePair([7, 9]);
            await rollback.rollback(100);
            assert.ok(!db.doQuery.getCalls().some(c => /UNION/.test(c.args[0]) && /order_matches/.test(c.args[0])),
                'no pair collection on a truncated replica');
            assert.ok(!db.doQuery.getCalls().some(c => /DELETE FROM markets WHERE \(tick1_id=\?/.test(c.args[0])),
                'no pair-scoped markets delete on a truncated replica');
        });

        it('aborts (fail-closed) on a transient error in the pair-scoped sweep', async function(){
            db.doQuery.callsFake(async (query) => {
                if(/FROM orders\b[\s\S]*action_index >= \?/.test(query) && /UNION/.test(query))
                    return [{ tick1_id: 7, tick2_id: 9 }];
                if(/SELECT 1 FROM orders WHERE \(give_tick_id=\?/.test(query))
                    throw Object.assign(new Error('Lock wait timeout'), { errno: 1205 });
                return [];
            });
            await assert.rejects(() => rollback.rollback(100), { errno: 1205 });
            assert.strictEqual(db.commitTransaction.called, false, 'a partial rollback must never commit');
        });

        it('swallows a schema gap in the pair-scoped sweep', async function(){
            db.doQuery.callsFake(async (query) => {
                if(/FROM orders\b[\s\S]*action_index >= \?/.test(query) && /UNION/.test(query))
                    throw Object.assign(new Error('Table does not exist'), { errno: 1146 });
                return [];
            });
            await rollback.rollback(100);
            assert.strictEqual(db.commitTransaction.calledOnce, true);
        });
    });

    describe('balance-rebuild error handling', function(){
        it('logs (does not rethrow) a 1146 error from rebuildBalances', async function(){
            sinon.stub(balanceHelpers, 'rebuildBalances')
                .rejects(Object.assign(new Error('no table'), { errno: 1146 }));
            await rollback.rollback(100); // must not throw
            assert.strictEqual(db.commitTransaction.calledOnce, true);
        });

        it('rethrows a non-1146 error from rebuildBalances', async function(){
            sinon.stub(balanceHelpers, 'rebuildBalances')
                .rejects(Object.assign(new Error('real db error'), { errno: 2002 }));
            await assert.rejects(() => rollback.rollback(100), { message: 'real db error' });
            assert.strictEqual(db.rollbackTransaction.calledOnce, true);
        });

        // Fix #5 (MED): a reorg must recompute tokens.supply, not just balances -
        // an orphaned MINT mutates supply in place, so the deleted credit alone leaves
        // it inflated. recomputeTokenSupplies runs after rebuildBalances, before commit.
        it('recomputes token supplies after rebuilding balances (before commit)', async function(){
            let rebuild = sinon.stub(balanceHelpers, 'rebuildBalances').resolves();
            let supplies = sinon.stub(balanceHelpers, 'recomputeTokenSupplies').resolves();
            await rollback.rollback(100);
            assert.ok(supplies.calledOnce, 'tokens.supply is recomputed on reorg');
            assert.ok(rebuild.calledBefore(supplies), 'balances first, then supply (supply reads the rebuilt ledger)');
            assert.ok(supplies.calledBefore(db.commitTransaction), 'supply recompute lands inside the rollback txn');
        });

        it('logs (does not rethrow) a 1146 error from recomputeTokenSupplies', async function(){
            sinon.stub(balanceHelpers, 'recomputeTokenSupplies')
                .rejects(Object.assign(new Error('no tokens table'), { errno: 1146 }));
            await rollback.rollback(100); // must not throw (older/decoder-shaped schema)
            assert.strictEqual(db.commitTransaction.calledOnce, true);
        });

        it('rethrows a non-1146 error from recomputeTokenSupplies', async function(){
            sinon.stub(balanceHelpers, 'recomputeTokenSupplies')
                .rejects(Object.assign(new Error('real supply db error'), { errno: 2002 }));
            await assert.rejects(() => rollback.rollback(100), { message: 'real supply db error' });
            assert.strictEqual(db.rollbackTransaction.calledOnce, true);
        });
    });

    describe('_rollbackDecoder', function(){
        let decoderDb, decoderRollback;

        beforeEach(function(){
            decoderDb = createMockDb();
            decoderDb.dbType = 'decoder';
            decoderRollback = new ClientRollback(decoderDb, util);
        });

        it('routes a decoder DB through _rollbackDecoder', async function(){
            let spy = sinon.spy(decoderRollback, '_rollbackDecoder');
            await decoderRollback.rollback(50);
            assert.strictEqual(spy.calledOnceWith(50), true);
        });

        it('deletes tx-scoped tables by tx_index, then block-scoped tables by block_index', async function(){
            decoderDb.doQuery.withArgs(sinon.match(/SELECT tx_index/))
                .resolves([{ tx_index: 7 }, { tx_index: 9 }]);
            await decoderRollback.rollback(50);

            let txDelete = decoderDb.doQuery.getCalls().find(c => /DELETE FROM `transaction_outputs`/.test(c.args[0]));
            assert.ok(txDelete, 'deletes the tx-scoped table');
            assert.ok(/tx_index IN \(\?,\?\)/.test(txDelete.args[0]));
            assert.deepStrictEqual(txDelete.args[1], [7, 9]);

            assert.ok(decoderDb.doQuery.getCalls().some(c =>
                /DELETE FROM `transactions` WHERE block_index >= /.test(c.args[0]) && c.args[1][0] === 50));
            assert.ok(decoderDb.doQuery.getCalls().some(c =>
                /DELETE FROM `blocks` WHERE block_index >= /.test(c.args[0]) && c.args[1][0] === 50));
            assert.strictEqual(decoderDb.commitTransaction.calledOnce, true);
        });

        it('skips tx-scoped deletes when no transactions are in range', async function(){
            decoderDb.doQuery.withArgs(sinon.match(/SELECT tx_index/)).resolves([]);
            await decoderRollback.rollback(50);
            assert.ok(!decoderDb.doQuery.getCalls().some(c => /transaction_outputs/.test(c.args[0])),
                'no tx-scoped delete when nothing is in range');
            assert.strictEqual(decoderDb.commitTransaction.calledOnce, true);
        });

        it('swallows a missing tx-scoped table error (schema gap) and still completes', async function(){
            decoderDb.doQuery.withArgs(sinon.match(/SELECT tx_index/)).resolves([{ tx_index: 1 }]);
            decoderDb.doQuery.withArgs(sinon.match(/transaction_outputs/))
                .rejects(Object.assign(new Error('no table'), { errno: 1146 }));
            await decoderRollback.rollback(50);
            assert.strictEqual(decoderDb.commitTransaction.calledOnce, true);
        });

        it('aborts (fail-closed) on a transient error in a tx-scoped delete (item 1848)', async function(){
            decoderDb.doQuery.withArgs(sinon.match(/SELECT tx_index/)).resolves([{ tx_index: 1 }]);
            decoderDb.doQuery.withArgs(sinon.match(/transaction_outputs/))
                .rejects(Object.assign(new Error('Deadlock found'), { errno: 1213 }));
            await assert.rejects(() => decoderRollback.rollback(50), { errno: 1213 });
            assert.strictEqual(decoderDb.rollbackTransaction.calledOnce, true);
            assert.strictEqual(decoderDb.commitTransaction.called, false);
        });

        it('rolls back and rethrows when a block-scoped delete fails', async function(){
            decoderDb.doQuery.withArgs(sinon.match(/SELECT tx_index/)).resolves([]);
            decoderDb.doQuery.withArgs(sinon.match(/DELETE FROM `transactions`/)).rejects(new Error('decoder boom'));
            await assert.rejects(() => decoderRollback.rollback(50), { message: 'decoder boom' });
            assert.strictEqual(decoderDb.rollbackTransaction.calledOnce, true);
        });
    });
});
