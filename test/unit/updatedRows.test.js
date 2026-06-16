// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available —
// contact legal@dankest.llc.
//
// Coverage for the in-place "updated rows" channel that closes the forward
// in-place-mutation replication gap (no UPDATE path on the replica). Mirrors —
// in the forward direction — the reorg-reset predicates ClientRollback runs.

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
        // No query should reference deactivation_block (delay unknown → skip).
        let hitDeactivation = db.calls.some(c => c.sql.indexOf('deactivation_block') !== -1);
        assert.strictEqual(hitDeactivation, false);
        // The slash + request_status classes still run (4 slash + 2 request = 6).
        assert.strictEqual(db.calls.length, 6);
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

    it('_upsertRows emits INSERT … ON DUPLICATE KEY UPDATE writing every column', async function(){
        await applier._upsertRows('stakes', [{ action_index: 3, deactivation_block: 50 }]);
        let q = db.calls.find(c => c.sql.indexOf('ON DUPLICATE KEY UPDATE') !== -1);
        assert.ok(q, 'expected an upsert query');
        assert.ok(q.sql.indexOf('INSERT INTO `stakes`') === 0);
        assert.ok(q.sql.indexOf('`deactivation_block` = VALUES(`deactivation_block`)') !== -1);
        assert.ok(q.sql.indexOf('`action_index` = VALUES(`action_index`)') !== -1);
    });

    it('_upsertRows rejects an invalid table identifier without querying', async function(){
        await applier._upsertRows('stakes; DROP TABLE x', [{ action_index: 1 }]);
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
        // IS NOT NULL …). Observe it directly rather than stubbing the captured fn ref.
        let isEscrowQuery = (c) => c.sql.indexOf('escrow_action_index IS NOT NULL') !== -1;

        await applier._maybeRederiveEscrow({ sends: [{}] });          // not escrow-relevant
        assert.strictEqual(db.calls.some(isEscrowQuery), false);

        await applier._maybeRederiveEscrow({ order_statuses: [{}] }); // escrow-relevant
        assert.strictEqual(db.calls.some(isEscrowQuery), true);
    });
});
