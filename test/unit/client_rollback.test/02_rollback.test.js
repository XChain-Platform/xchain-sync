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

// tokens.escrow_action_index is RE-DERIVED after the dataTables delete (mirror of
// xchain-indexer rollback.js, TP-03 #4017): set to the surviving open GIVE_OWNERSHIP
// offer's action_index, else NULL. Collapses both directions and matches the source
// byte-for-byte.
const AFFECTED_RE  = /escrow_action_index IS NOT NULL/i;
const OPENOFFER_RE = /SELECT o\.action_index FROM orders/i;
const REDERIVE_RE  = /UPDATE tokens SET escrow_action_index=\?\s+WHERE tick_id=\(SELECT id FROM index_tickers/i;

describe('ClientRollback', function(){

    describe('rollback', function(){
        registerHooks();
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

    });
});

describe('ClientRollback', function(){

    describe('rollback', function(){
        registerHooks();
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

        it('replays BOTH polls resets: the re-open (callback_due_block re-NULLed) and the timelock re-fire reset', async function(){
            // Source twin: xchain-indexer/src/rollback.js polls re-open + timelock reset.
            // Omitting callback_due_block from the replica's re-open, with no
            // timelock reset at all (#5607), leaves a reorged replica holding a stale
            // F+delay stamp / an orphaned EXECUTE's action_index the source cleared.
            await rollback.rollback(100);
            let calls = db.doQuery.getCalls();
            let reopen = calls.find(c => c.args[0].includes("UPDATE polls SET poll_status = 'open'"));
            assert.ok(reopen, 'expected the polls re-open reset');
            assert.ok(reopen.args[0].includes('callback_execute_action_index = NULL, callback_due_block = NULL'),
                're-open must re-NULL callback_due_block like the source');
            assert.ok(reopen.args[0].includes("WHERE poll_status IN ('finalized', 'failed_quorum') AND resolved_block >= ?"));
            assert.deepStrictEqual(reopen.args[1], [100]);
            let timelock = calls.find(c => c.args[0].includes('UPDATE polls SET callback_execute_action_index = NULL'));
            assert.ok(timelock, 'expected the polls timelock re-fire reset');
            assert.ok(timelock.args[0].includes('AND callback_due_block >= ? AND callback_execute_action_index IS NOT NULL'));
            assert.ok(!timelock.args[0].includes('callback_due_block = NULL'), 'the surviving finalization keeps its derived due block');
            assert.deepStrictEqual(timelock.args[1], [100]);
            // Both are in-place resets on surviving rows and precede the action-scoped deletes.
            let tokenDeleteIdx = calls.findIndex(c => c.args[0].includes('DELETE FROM `tokens`'));
            assert.ok(calls.indexOf(reopen) < tokenDeleteIdx && calls.indexOf(timelock) < tokenDeleteIdx);
        });

    });
});

describe('ClientRollback', function(){

    describe('rollback', function(){
        registerHooks();
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

    });
});

describe('ClientRollback', function(){

    describe('rollback', function(){
        registerHooks();
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
                // The pick follows the debit chain's own values (highest orphaned prev_amount =
                // the amount before the first orphaned debit). The position columns invert under
                // a re-entrant nested EXECUTE and serve only as the tiebreak for equal amounts.
                assert.ok(/CAST\(e\.prev_amount AS DECIMAL\(60,18\)\)\s*>\s*CAST\(d\.prev_amount AS DECIMAL\(60,18\)\)/.test(sql),
                    'restore must pick the highest orphaned prev_amount, not the lowest position key');
                assert.ok(/e\.execution_index\s*<\s*d\.execution_index/.test(sql),
                    'restore must order by execution_index (deterministic, replay-stable)');
                assert.ok(/e\.slash_position\s*<\s*d\.slash_position/.test(sql),
                    'restore must use slash_position as the within-EXECUTE secondary tiebreak');
                assert.ok(!/e\.id\s*<\s*d\.id/.test(sql),
                    'restore must NOT tiebreak on the non-deterministic AUTO_INCREMENT id');
            }
        });

    });
});

describe('ClientRollback', function(){

    describe('rollback', function(){
        registerHooks();
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

    });
});

describe('ClientRollback', function(){

    describe('rollback', function(){
        registerHooks();
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
            // round_qualifier is part of reward_unique (2026-08-24 indexer migration) and the
            // pre-image log carries it, so both the column list and the projection must name
            // it. Dropped, the loser comes back under the schema default 0, a DIFFERENT row
            // from the one the reconcile deleted: INSERT IGNORE then either swallows it
            // against a legacy qualifier-0 row or lands a wrong-identity duplicate, and the
            // replica forks SUM(validator_rewards) either way. The source twin at
            // xchain-indexer/src/rollback.js already carries both halves.
            assert.ok(/round_reference, round_qualifier,/.test(restore.args[0]),
                'restore column list must name round_qualifier');
            assert.ok(/d\.round_qualifier/.test(restore.args[0]),
                'restore projection must select d.round_qualifier rather than fall back to the schema default 0');
            assert.deepStrictEqual(restore.args[1], [100, 100, 100]);
            let restoreIdx = calls.indexOf(restore);
            let deleteIdx = calls.findIndex(c => c.args[0].includes('DELETE FROM `validator_rewards`'));
            assert.ok(restoreIdx >= 0 && deleteIdx >= 0 && restoreIdx < deleteIdx, 'reconcile restore must precede the validator_rewards delete');
        });

    });
});

describe('ClientRollback', function(){

    describe('rollback', function(){
        registerHooks();
        // RB-ANCHOR-NULL. The reconcile has two callers and only the DOGE ANCHOR handler mints an
        // actions row; the BTC-side derive (anchor_reward_derive.js) reconciles with a NULL action
        // index. So an orphaned range whose only reward work was a derive-side reconcile leaves
        // firstActionIndex null, while the block-scoped deletes below still drop both the log rows
        // and the replacement winner. Gated on firstActionIndex the restore would be skipped and
        // the earlier winner deleted for good, forking SUM(validator_rewards) from the source,
        // which runs this statement outside its own guard (xchain-indexer/src/rollback.js).
        it('still replays the anchor reconcile-log restore when firstActionIndex is null (RB-ANCHOR-NULL)', async function(){
            db.getFirstActionIndex.resolves(null);
            await rollback.rollback(100);
            let calls = db.doQuery.getCalls();
            let restore = calls.find(c =>
                c.args[0].includes('INSERT IGNORE INTO validator_rewards') &&
                c.args[0].includes('anchor_reward_reconcile_log'));
            assert.ok(restore, 'the RB-ANCHOR restore must run on an action-empty reorg');
            assert.deepStrictEqual(restore.args[1], [100, 100, 100]);
            // Still ahead of both deletes that would otherwise destroy the evidence and the reward.
            let restoreIdx = calls.indexOf(restore);
            let logDelIdx  = calls.findIndex(c => c.args[0].includes('DELETE FROM `anchor_reward_reconcile_log`'));
            let rewDelIdx  = calls.findIndex(c => /DELETE FROM validator_rewards WHERE derive_block_index\s*>=\s*\?/.test(c.args[0]));
            assert.ok(logDelIdx >= 0, 'expected the block-scoped anchor_reward_reconcile_log delete');
            assert.ok(rewDelIdx >= 0, 'expected the derive-block validator_rewards delete');
            assert.ok(restoreIdx < logDelIdx, 'restore must precede the pre-image log delete');
            assert.ok(restoreIdx < rewDelIdx, 'restore must precede the derive-block reward delete');
            // The restore carries no action_index term, so it does not violate the
            // action-empty contract the sibling test pins.
            assert.ok(!restore.args[0].includes('action_index'), 'the restore is keyed purely on block heights');
        });

    });
});
