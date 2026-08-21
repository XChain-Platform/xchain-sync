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
// Coverage for the in-place "updated rows" channel that closes the forward
// in-place-mutation replication gap (no UPDATE path on the replica). Mirrors,
// in the forward direction, the reorg-reset predicates ClientRollback runs.

const assert = require('assert');
const sinon  = require('sinon');
const { collectUpdatedRows } = require('../../src/updatedRows');
const ClientApplier = require('../../src/ClientApplier');
const ClientRollback = require('../../src/ClientRollback');
const Utility = require('../../src/utility');

// A doQuery stub that branches on a substring of the SQL so each in-place class
// can be given canned rows independently. Records every (sql, args) pair.
function fakeDb(routes){
    let calls = [];
    return {
        calls,
        dbType: 'indexer',
        doQuery: sinon.stub().callsFake(async (sql, args) => {
            calls.push({ sql, args });
            for(let r of routes || []){
                if(sql.indexOf(r.match) !== -1) return r.rows;
            }
            return [];
        }),
        beginTransaction: sinon.stub().resolves(),
        commitTransaction: sinon.stub().resolves(),
        rollbackTransaction: sinon.stub().resolves(),
        getBlockHashRow: sinon.stub().resolves(null)
    };
}

describe('updatedRows.collectUpdatedRows', function(){

    afterEach(() => sinon.restore());

    it('skips the deactivation_block class entirely when activationDelay is null', async function(){
        let db = fakeDb([]);
        await collectUpdatedRows(db, 100, 100, null);
        // No query should reference deactivation_block (delay unknown, so skip).
        let hitDeactivation = db.calls.some(c => c.sql.indexOf('deactivation_block') !== -1);
        assert.strictEqual(hitDeactivation, false);
        // The slash + delegation-rotation + request_status + poll-finalize + cooldown-status
        // + bet-status + anchor_invalid + tokens-supply classes still run, none of which
        // depend on the activation delay (4 slash + 2 rotation + 2 request + 1 poll
        // + 2 cooldown-status + 2 bet-status + 1 anchor + 1 tokens = 15).
        assert.strictEqual(db.calls.length, 15);
        // And the cooldown status flip is keyed by cooldown_end_block, not the delay.
        let hitCooldown = db.calls.some(c => c.sql.indexOf('cooldown_end_block') !== -1);
        assert.strictEqual(hitCooldown, true);
    });

    it('detects deactivation stamps by value-threshold [from+delay, to+delay]', async function(){
        let db = fakeDb([
            { match: 'FROM `stakes` WHERE deactivation_block', rows: [{ action_index: 7, deactivation_block: 106 }] }
        ]);
        let out = await collectUpdatedRows(db, 100, 100, 6); // BTC delay = 6
        // stakes deactivation query must use 106..106 (100+6).
        let q = db.calls.find(c => c.sql.indexOf('FROM `stakes` WHERE deactivation_block') !== -1);
        assert.deepStrictEqual(q.args, [106, 106]);
        assert.ok(out.stakes && out.stakes.length === 1);
        assert.strictEqual(out.stakes[0].action_index, 7);
    });

    it('detects SLASH amount cuts via the debit log join and v0 request_status flips', async function(){
        let db = fakeDb([
            { match: 'JOIN `contract_slash_debits`',  rows: [{ action_index: 11, amount: '5' }] },
            { match: 'FROM `attests` WHERE version = 0', rows: [{ action_index: 21, version: 0, request_status: 'fulfilled' }] },
            { match: 'FROM `xcalls` WHERE version = 0',  rows: [{ action_index: 22, version: 0, request_status: 'completed' }] }
        ]);
        let out = await collectUpdatedRows(db, 50, 50, 6);
        assert.ok(out.contract_stakes && out.contract_stakes[0].action_index === 11);
        assert.ok(out.attests && out.attests[0].request_status === 'fulfilled');
        assert.ok(out.xcalls && out.xcalls[0].request_status === 'completed');
        // request_status keyed on resolved_block window (50..50).
        let aq = db.calls.find(c => c.sql.indexOf('FROM `attests` WHERE version = 0') !== -1);
        assert.deepStrictEqual(aq.args, [50, 50]);
    });

    it('keys the VOTE poll class on resolved_block OR a fired deferred-callback due block (one scan)', async function(){
        // A binding poll with callback_delay_blocks > 0 finalizes at F (resolved_block = F)
        // and fires at D = F + delay, where the sweep UPDATEs callback_execute_action_index
        // IN PLACE on the surviving row. At block D resolved_block is below the window, so
        // keying on resolved_block alone never carried the fire stamp (#5606).
        let db = fakeDb([
            { match: 'FROM `polls` WHERE', rows: [{ action_index: 41, resolved_block: 60, callback_due_block: 70, callback_execute_action_index: 905 }] }
        ]);
        let out = await collectUpdatedRows(db, 70, 70, 6);
        assert.ok(out.polls && out.polls.length === 1);
        assert.strictEqual(out.polls[0].callback_execute_action_index, 905);
        let pq = db.calls.filter(c => c.sql.indexOf('FROM `polls` WHERE') !== -1);
        assert.strictEqual(pq.length, 1, 'both keys ride ONE polls scan');
        assert.match(pq[0].sql, /WHERE resolved_block BETWEEN \? AND \? OR \(callback_due_block BETWEEN \? AND \? AND callback_execute_action_index IS NOT NULL\)/);
        assert.deepStrictEqual(pq[0].args, [70, 70, 70, 70]);
    });

    it('carries a DELEGATE v1 signing-key rotation on surviving stake AND cooldown rows', async function(){
        // The rotated row's action_index is below the window (the STAKE happened earlier), so
        // only its contract_delegation_rotations entry pins the change to this block. Without
        // this class the follower keeps the pre-rotation key and hands contracts a different
        // staker set than the source, and its Pass-2 slash lookup misses the cooldown-locked
        // tokens the source can still debit (#4366).
        let db = fakeDb([
            { match: 'JOIN `contract_delegation_rotations`', rows: [{ action_index: 31, signing_pubkey_id: 77 }] }
        ]);
        let out = await collectUpdatedRows(db, 60, 60, 6);
        assert.ok(out.contract_stakes && out.contract_stakes.length === 1);
        assert.strictEqual(out.contract_stakes[0].signing_pubkey_id, 77);
        assert.ok(out.contract_unstakes && out.contract_unstakes.length === 1);
        let queries = db.calls.filter(c => c.sql.indexOf('JOIN `contract_delegation_rotations`') !== -1);
        assert.strictEqual(queries.length, 2, 'one per rotated stake-ledger table');
        assert.deepStrictEqual(queries.map(q => q.args), [
            ['contract_stakes', 60, 60],
            ['contract_unstakes', 60, 60]
        ], 'scoped by target_table and keyed on the journal row block window');
    });

    it('dedups a row reached by two classes (deactivated AND slashed) by action_index', async function(){
        let db = fakeDb([
            { match: 'FROM `stakes` WHERE deactivation_block', rows: [{ action_index: 9, deactivation_block: 106 }] },
            { match: 'JOIN `capability_slash_debits`',          rows: [{ action_index: 9, amount: '3' }] }
        ]);
        let out = await collectUpdatedRows(db, 100, 100, 6);
        // stakes appears in both the deactivation and capability-slash paths but
        // must be emitted once (same UNIQUE action_index).
        assert.strictEqual(out.stakes.length, 1);
        assert.strictEqual(out.stakes[0].action_index, 9);
    });

    it('refreshes surviving tokens rows for ticks touched by ledger changes in the window', async function(){
        let db = fakeDb([
            { match: 'FROM `tokens` t WHERE t.tick_id IN', rows: [{ id: 5, tick_id: 42, action_index: 100, last_action_index: 100, supply: '1000' }] }
        ]);
        let out = await collectUpdatedRows(db, 200, 200, 6);
        // The tokens refresh query joins credits/debits/escrows to actions on the
        // [from, to] block window (200..200) and carries the full current row. Each
        // ledger table is its own UNION branch (so the block-range predicate pushes
        // down per table instead of materialising a UNION ALL derived table), so the
        // [from, to] pair is bound once per branch -> three (200, 200) pairs.
        let tq = db.calls.find(c => c.sql.indexOf('FROM `tokens` t WHERE t.tick_id IN') !== -1);
        assert.ok(tq, 'expected the tokens-supply refresh query');
        assert.deepStrictEqual(tq.args, [200, 200, 200, 200, 200, 200]);
        assert.ok(tq.sql.indexOf('FROM credits') !== -1);
        assert.ok(tq.sql.indexOf('FROM debits') !== -1);
        assert.ok(tq.sql.indexOf('FROM escrows') !== -1);
        // SELECT t.* carries the source `id` so the follower's upsert lands on the PK.
        assert.ok(tq.sql.indexOf('SELECT t.*') !== -1);
        assert.ok(out.tokens && out.tokens.length === 1);
        assert.strictEqual(out.tokens[0].supply, '1000');
    });

    it('uses target_table to separate contract_stakes vs contract_unstakes', async function(){
        let db = fakeDb([]);
        await collectUpdatedRows(db, 1, 1, 6);
        let csCall = db.calls.find(c => c.sql.indexOf('FROM `contract_stakes` t') !== -1);
        let cuCall = db.calls.find(c => c.sql.indexOf('FROM `contract_unstakes` t') !== -1);
        assert.strictEqual(csCall.args[0], 'contract_stakes');
        assert.strictEqual(cuCall.args[0], 'contract_unstakes');
    });
});

describe('ClientApplier in-place updated-rows apply', function(){

    let db, applier;
    beforeEach(function(){
        db = fakeDb([]);
        applier = new ClientApplier(db, new Utility());
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });
    afterEach(() => sinon.restore());

    it('_upsertRows emits INSERT ... ON DUPLICATE KEY UPDATE writing every column', async function(){
        await applier._upsertRows('stakes', [{ action_index: 3, deactivation_block: 50 }]);
        let q = db.calls.find(c => c.sql.indexOf('ON DUPLICATE KEY UPDATE') !== -1);
        assert.ok(q, 'expected an upsert query');
        assert.ok(q.sql.indexOf('INSERT INTO `stakes`') === 0);
        assert.ok(q.sql.indexOf('`deactivation_block` = VALUES(`deactivation_block`)') !== -1);
        assert.ok(q.sql.indexOf('`action_index` = VALUES(`action_index`)') !== -1);
    });

    it('_upsertRows throws on an invalid table identifier without querying (fail closed)', async function(){
        await assert.rejects(() => applier._upsertRows('stakes; DROP TABLE x', [{ action_index: 1 }]), /Rejected table name/);
        assert.strictEqual(db.calls.length, 0);
    });

    it('applyBlock UPSERTs payload.updated_rows for surviving rows', async function(){
        let payload = {
            block_index: 9,
            data: { blocks: [{ block_index: 9 }] },
            updated_rows: { stakes: [{ action_index: 4, deactivation_block: 80 }] }
        };
        await applier.applyBlock(payload);
        let upsert = db.calls.find(c => c.sql.indexOf('ON DUPLICATE KEY UPDATE') !== -1 && c.sql.indexOf('`stakes`') !== -1);
        assert.ok(upsert, 'surviving stakes row should be UPSERTed');
        assert.strictEqual(db.commitTransaction.calledOnce, true);
    });

    it('_maybeRederiveEscrow runs the escrow re-derive only when an escrow-relevant table is present', async function(){
        // The re-derive's first query is the affected-tickers SELECT (escrow_action_index
        // IS NOT NULL ...). Observe it directly rather than stubbing the captured fn ref.
        let isEscrowQuery = (c) => c.sql.indexOf('escrow_action_index IS NOT NULL') !== -1;

        await applier._maybeRederiveEscrow({ sends: [{}] });          // not escrow-relevant
        assert.strictEqual(db.calls.some(isEscrowQuery), false);

        await applier._maybeRederiveEscrow({ order_statuses: [{}] }); // escrow-relevant
        assert.strictEqual(db.calls.some(isEscrowQuery), true);
    });
});
