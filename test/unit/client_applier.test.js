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
const ClientApplier = require('../../src/client/applier');
const Utility = require('../../src/util');
const { SCHEMA_VERSION } = require('../../src/schema/version');
const balanceHelpers = require('../../src/db/balance_helpers');
const { withDbMixins } = require('../helpers/db_mixins.js');

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

function registerApplyBlockCases1(){
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
    // A fail-soft read answers "block absent" for a DB blip too, and that answer
    // re-INSERTs the block's credits/debits/escrows (plain INSERT) and rebuilds
    // balances over the duplicates.
    it('reads the duplicate guard fail-CLOSED (opts.rethrow)', async function(){
        await applier.applyBlock({ block_index: 1, data: { blocks: [{ block_index: 1 }] } });
        assert.deepStrictEqual(db.getBlockHashRow.firstCall.args[2], { rethrow: true });
    });
    it('never opens a transaction when the duplicate guard read faults', async function(){
        let err = new Error('deadlock found'); err.errno = 1213;
        db.getBlockHashRow.rejects(err);
        await assert.rejects(
            () => applier.applyBlock({ block_index: 1, data: { blocks: [{ block_index: 1 }] } }),
            /deadlock found/
        );
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
}

function registerApplyBlockCases2(){
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
}

function registerApplyBlockCases3(){
    it('mirrors the anchor-reward winner collapse from this block\'s reconcile-log pre-images (keyed delete, after inserts, in-txn)', async function(){
        // The source pre-images each loser validator_rewards row into
        // anchor_reward_reconcile_log and DELETEs it; the log replicated, the DELETE
        // never did, so a replica holding the loser stayed AHEAD forever (#5605/#5610).
        db.dbType = 'indexer';
        await applier.applyBlock({
            block_index: 961700,
            data: {
                blocks: [{ block_index: 961700 }],
                anchor_reward_reconcile_log: [{ id: 1, reward_type: 'anchor_BTC', round_reference: 961500,
                    source_id: 7, signing_pubkey_id: 3, amount: '1.00000000', reward_block_index: 961500,
                    reward_derive_block_index: 961700, block_index: 961700 }]
            }
        });
        let calls = db.doQuery.getCalls();
        let del = calls.find(c => /^DELETE vr FROM validator_rewards vr JOIN anchor_reward_reconcile_log d/.test(c.args[0]));
        assert.ok(del, 'expected the loser-row delete mirror');
        assert.ok(/d\.source_id = vr\.source_id AND d\.signing_pubkey_id = vr\.signing_pubkey_id/.test(del.args[0]));
        assert.ok(/d\.reward_type = vr\.reward_type AND d\.round_reference <=> vr\.round_reference/.test(del.args[0]),
            'NULL-safe round_reference match (the UNIQUE key component is nullable)');
        // round_qualifier joined reward_unique in the 2026-08-24 indexer migration and
        // anchor_reward_reconcile_log pre-images it. Without this predicate the keyed
        // delete ALSO matches the other archive snapshot's reward whenever a hub rebase
        // reissued the MATCH_BATCH_SEQ round_reference, destroying a row the source still
        // holds.
        assert.ok(/AND d\.round_qualifier <=> vr\.round_qualifier/.test(del.args[0]),
            'the mirror delete must carry the full five-column reward identity');
        assert.ok(/WHERE d\.block_index = \?$/.test(del.args[0]), 'scoped to THIS block\'s reconcile rows');
        assert.deepStrictEqual(del.args[1], [961700]);
        let logInsert = calls.findIndex(c => /anchor_reward_reconcile_log/.test(c.args[0]) && /^INSERT/.test(c.args[0]));
        assert.ok(logInsert >= 0 && logInsert < calls.indexOf(del), 'the log rows are inserted before the mirror reads them');
        assert.strictEqual(db.commitTransaction.calledOnce, true);
    });
    it('does not issue the reconcile delete mirror when the block carries no reconcile-log rows', async function(){
        db.dbType = 'indexer';
        await applier.applyBlock({ block_index: 5, data: { blocks: [{ block_index: 5 }], validator_rewards: [{ id: 1, source_id: 1, signing_pubkey_id: 2, reward_type: 'anchor_BTC', round_reference: 1, amount: '1', block_index: 5 }] } });
        assert.ok(!db.doQuery.getCalls().some(c => /^DELETE vr FROM validator_rewards/.test(c.args[0])));
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
}

function registerApplyBlockCases4(){
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
}

function registerBalanceErrorCases1(){
    it('swallows a 1146 (table-missing) error on rebuildBalances', async function(){
        sinon.stub(balanceHelpers, 'rebuildBalances').rejects(Object.assign(new Error('no table'), { errno: 1146 }));
        await applier.rebuildBalances(); // must not throw
    });
    it('rethrows a non-1146 error on rebuildBalances', async function(){
        sinon.stub(balanceHelpers, 'rebuildBalances').rejects(Object.assign(new Error('real'), { errno: 1234 }));
        await assert.rejects(() => applier.rebuildBalances(), { message: 'real' });
    });
}

function registerScopedBalanceCases1(){
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
}
describe('ClientApplier', function(){
    registerHooks();

    describe('applyBlock', function(){
        registerApplyBlockCases1();
        registerApplyBlockCases2();
        registerApplyBlockCases3();
        registerApplyBlockCases4();
    });

    describe('rebuildBalances error handling', function(){
        registerBalanceErrorCases1();
    });

    describe('scoped balance rebuilds', function(){
        registerScopedBalanceCases1();
    });
});
