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
// Guards the sync replica's deactivation_block re-NULL mirror (#3976 / #4110):
// on a reorg the source indexer re-NULLs deactivation_block stamps that orphaned
// UNSTAKE / DELEGATE-revoke actions wrote in place on surviving parent rows
// (xchain-indexer/src/rollback.js). The thin replica must mirror those four
// resets or it keeps stale deactivation_block values and diverges from the
// source on active-set membership. The resets key on the frozen per-chain
// ACTIVATION_DELAY_BLOCKS (src/consensus-constants.js), now held by the replica.

const assert = require('assert');
const sinon  = require('sinon');
const ClientRollback = require('../../src/ClientRollback');
const Utility = require('../../src/utility');
const { ACTIVATION_DELAY_BLOCKS_BY_COIN } = require('../../src/consensus-constants');

function createMockDb(){
    return {
        dbType: 'indexer',
        doQuery: sinon.stub().resolves([]),
        getFirstActionIndex: sinon.stub().resolves(500),
        getStatusId: sinon.stub().resolves(null),
        beginTransaction: sinon.stub().resolves(),
        commitTransaction: sinon.stub().resolves(),
        rollbackTransaction: sinon.stub().resolves()
    };
}

// The four deactivation_block re-NULL statements, identified by the SET clause.
function deactivationResets(db){
    return db.doQuery.getCalls().filter(c =>
        typeof c.args[0] === 'string' && c.args[0].includes('deactivation_block = NULL'));
}

describe('deactivation_block sync-mirror (#3976/#4110)', function(){

    afterEach(function(){ sinon.restore(); });

    describe('frozen per-chain ACTIVATION_DELAY_BLOCKS', function(){
        it('matches the indexer golden (BTC 6 / LTC 24 / DOGE 60)', function(){
            // Mirrors xchain-indexer/test/unit/consensus-params.test.js GOLDEN_STAKING_PER_CHAIN.
            assert.deepStrictEqual(ACTIVATION_DELAY_BLOCKS_BY_COIN, { BTC: 6, LTC: 24, DOGE: 60 });
        });

        it('a supplied-but-unknown coin is a hard error (never a silent wrong delay)', function(){
            assert.throws(() => new ClientRollback(createMockDb(), new Utility(), 'XRP'),
                /unrecognized coin "XRP"/);
        });

        it('an omitted coin leaves activationDelay null (legacy path)', function(){
            const r = new ClientRollback(createMockDb(), new Utility());
            assert.strictEqual(r.activationDelay, null);
        });

        it('resolves both the ticker and the full coin name (any case), since cfg.coin varies', function(){
            // Production cfg.coin is a ticker ('BTC'); some fixtures/callers use the full
            // name ('bitcoin'). Both must resolve to the same frozen delay.
            for(const c of ['BTC', 'bitcoin', 'Bitcoin'])  assert.strictEqual(new ClientRollback(createMockDb(), new Utility(), c).activationDelay, 6, c);
            for(const c of ['LTC', 'litecoin'])            assert.strictEqual(new ClientRollback(createMockDb(), new Utility(), c).activationDelay, 24, c);
            for(const c of ['DOGE', 'dogecoin'])           assert.strictEqual(new ClientRollback(createMockDb(), new Utility(), c).activationDelay, 60, c);
        });
    });

    describe('rollback emits the four re-NULL resets when a coin is set', function(){
        let db, rollback;
        beforeEach(async function(){
            sinon.stub(console, 'log');
            sinon.stub(console, 'warn');
            db = createMockDb();
            rollback = new ClientRollback(db, new Utility(), 'BTC');
            await rollback.rollback(100); // block_index=100, activationDelay=6
        });

        it('emits exactly four deactivation_block resets', function(){
            assert.strictEqual(deactivationResets(db).length, 4);
        });

        it('stakes reset joins unstakes and matches the EXACT stamped value (precise, not blanket)', function(){
            const q = deactivationResets(db).find(c => c.args[0].includes('UPDATE stakes s'));
            assert.ok(q, 'stakes reset present');
            assert.ok(q.args[0].includes('JOIN unstakes u'));
            // precise: deactivation_block = u.block_index + ? (not a blanket >= block_index)
            assert.ok(q.args[0].includes('s.deactivation_block = u.block_index + ?'));
            assert.deepStrictEqual(q.args[1], [100, 6]);
        });

        it('delegations reset self-joins on source + signing pubkey', function(){
            const q = deactivationResets(db).find(c => c.args[0].includes('UPDATE delegations p'));
            assert.ok(q);
            assert.ok(q.args[0].includes('JOIN delegations r ON r.source_id = p.source_id'));
            assert.ok(q.args[0].includes('p.deactivation_block = r.block_index + ?'));
            assert.deepStrictEqual(q.args[1], [100, 6]);
        });

        it('contract_stakes reset joins contract_unstakes on pubkey+contract+tick', function(){
            const q = deactivationResets(db).find(c => c.args[0].includes('UPDATE contract_stakes cs'));
            assert.ok(q);
            assert.ok(q.args[0].includes('JOIN contract_unstakes cu'));
            assert.ok(q.args[0].includes('cs.deactivation_block = cu.block_index + ?'));
            assert.deepStrictEqual(q.args[1], [100, 6]);
        });

        it('contract_delegations reset is the value-threshold form (no child row) at block+delay', function(){
            const q = deactivationResets(db).find(c => c.args[0].includes('UPDATE contract_delegations'));
            assert.ok(q);
            assert.ok(!q.args[0].includes('JOIN'));
            assert.ok(q.args[0].includes('deactivation_block >= ?'));
            assert.deepStrictEqual(q.args[1], [106]); // 100 + 6
        });
    });

    describe('without a coin the mirror is skipped (and warns, not silent)', function(){
        it('emits zero resets and warns', async function(){
            sinon.stub(console, 'log');
            const warn = sinon.stub(console, 'warn');
            const db = createMockDb();
            const rollback = new ClientRollback(db, new Utility()); // no coin
            await rollback.rollback(100);
            assert.strictEqual(deactivationResets(db).length, 0);
            assert.ok(warn.getCalls().some(c => String(c.args[0]).includes('deactivation_block re-NULL mirror skipped')));
        });
    });
});
