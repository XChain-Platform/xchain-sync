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
const sinon = require('sinon');
const ClientRollback = require('../../../src/client/rollback');
const Utility = require('../../../src/util');
const gateRegistry = require('../../../src/consensus/gate_registry');

const GATE_KEY = 'cooldown_maturity_escrow_reversal_activation.COOLDOWN_MATURITY_ESCROW_REVERSAL_ACTIVATION';

function createMockDb(firstActionIndex = 500){
    const db = {
        doQuery: sinon.stub().resolves([]),
        getFirstActionIndex: sinon.stub().resolves(firstActionIndex),
        getStatusId: sinon.stub(),
        beginTransaction: sinon.stub().resolves(),
        commitTransaction: sinon.stub().resolves(),
        rollbackTransaction: sinon.stub().resolves()
    };
    db.getStatusId.withArgs('completed').resolves(8);
    db.getStatusId.withArgs('valid').resolves(3);
    return db;
}

function maturityReleaseDeletes(db){
    return db.doQuery.getCalls().filter(c => /^DELETE e FROM escrows AS e/.test(c.args[0]));
}

function maturityRefundDeletes(db){
    return db.doQuery.getCalls().filter(c => /^DELETE c FROM credits c/.test(c.args[0]) && /cooldown_end_block/.test(c.args[0]));
}

function maturityStatusResets(db){
    return db.doQuery.getCalls().filter(c => /^UPDATE (contract_)?unstakes SET status_id = \? WHERE status_id = \? AND cooldown_end_block/.test(c.args[0]));
}

describe('ClientRollback cooldown-maturity escrow-release reversal: activation', function(){
    afterEach(function(){ sinon.restore(); });

    it('keeps the releases on an unarmed network and still deletes the credits and resets the status', async function(){
        for(const network of ['mainnet', 'testnet']){
            for(const firstActionIndex of [500, null]){
                const db = createMockDb(firstActionIndex);
                const rollback = new ClientRollback(db, new Utility(), 'BTC', network);

                await rollback.rollback(100);

                assert.strictEqual(maturityReleaseDeletes(db).length, 0, network + ': the release must survive below the activation');
                assert.strictEqual(maturityRefundDeletes(db).length, 2, network + ': both refund credits are still deleted');
                assert.strictEqual(maturityStatusResets(db).length, 2, network + ': both completed flips are still reset');
            }
        }
    });

    it('reads the switch at the rollback target block, for this network and coin', async function(){
        const activeAt = sinon.stub(gateRegistry, 'activeAt').callThrough();
        activeAt.withArgs(GATE_KEY).callsFake((key, network, coin, height) => height >= 500);

        const below = createMockDb();
        await new ClientRollback(below, new Utility(), 'BTC', 'testnet').rollback(499);
        assert.strictEqual(maturityReleaseDeletes(below).length, 0, 'one block below the height keeps the releases');
        assert.strictEqual(maturityRefundDeletes(below).length, 2);

        const at = createMockDb();
        await new ClientRollback(at, new Utility(), 'BTC', 'testnet').rollback(500);
        assert.strictEqual(maturityReleaseDeletes(at).length, 2, 'the height itself deletes them');
        assert.strictEqual(maturityRefundDeletes(at).length, 2);

        const reads = activeAt.getCalls().filter(c => c.args[0] === GATE_KEY).map(c => c.args.slice(1));
        assert.deepStrictEqual(reads, [['testnet', 'BTC', 499, null], ['testnet', 'BTC', 500, null]]);
    });

    it('the row is unarmed on mainnet, armed on every testnet chain by the v0.23.0 cut, and active from regtest genesis', function(){
        for(const coin of ['BTC', 'LTC', 'DOGE', null]){
            assert.strictEqual(gateRegistry.activeAt(GATE_KEY, 'mainnet', coin, 1e9, null), false);
            assert.strictEqual(gateRegistry.activeAt(GATE_KEY, 'regtest', coin, 0, null), true);
        }
        for(const coin of ['BTC', 'LTC', 'DOGE']){
            assert.strictEqual(gateRegistry.activeAt(GATE_KEY, 'testnet', coin, 1e9, null), true);
            assert.strictEqual(gateRegistry.activeAt(GATE_KEY, 'testnet', coin, 1, null), false);
        }
    });
});

describe('ClientRollback cooldown-maturity escrow-release reversal', function(){
    afterEach(function(){ sinon.restore(); });
    it('deletes both backdated releases with the same keys as their refund credits', async function(){
        const db = createMockDb();
        const rollback = new ClientRollback(db, new Utility(), undefined, 'regtest');

        await rollback.rollback(100);

        const deletes = maturityReleaseDeletes(db);
        assert.strictEqual(deletes.length, 2, 'expected capability and contract escrow-release deletes');

        assert.match(deletes[0].args[0], /JOIN unstakes u ON u\.action_index = e\.action_index AND u\.source_id = e\.address_id/);
        assert.match(deletes[0].args[0], /JOIN index_tickers g ON g\.id = e\.tick_id AND g\.tick = \?/);
        assert.match(deletes[0].args[0], /WHERE u\.status_id = \? AND u\.cooldown_end_block >= \? AND u\.block_index < \?/);
        assert.deepStrictEqual(deletes[0].args[1], ['XCHAIN', 8, 100, 100]);

        assert.match(deletes[1].args[0], /JOIN contract_unstakes cu ON cu\.action_index = e\.action_index/);
        assert.match(deletes[1].args[0], /AND cu\.source_id = e\.address_id AND cu\.tick_id = e\.tick_id/);
        assert.match(deletes[1].args[0], /WHERE cu\.status_id = \? AND cu\.cooldown_end_block >= \? AND cu\.block_index < \?/);
        assert.deepStrictEqual(deletes[1].args[1], [8, 100, 100]);
    });

    it('limits release deletes to negative amounts so positive slash escrows survive', async function(){
        const db = createMockDb();
        const rollback = new ClientRollback(db, new Utility(), undefined, 'regtest');

        await rollback.rollback(100);

        const deletes = maturityReleaseDeletes(db);
        assert.strictEqual(deletes.length, 2, 'expected capability and contract escrow-release deletes');
        for(const releaseDelete of deletes){
            assert.match(releaseDelete.args[0], /CAST\(e\.amount AS DECIMAL\(60,18\)\) < 0/);
        }
    });

    it('keeps a positive escrows row on the unstake\'s action and deletes only the negative release, as the indexer does', async function(){
        // Escrows rows sharing the matured unstake's action keys: the negative release the
        // maturity wrote and a positive escrows row (a slash or re-lock) on the same action.
        const rows = [
            { action_index: 40, address_id: 7, tick_id: 2, amount: '-12.5' },
            { action_index: 40, address_id: 7, tick_id: 2, amount: '3.000000000000000001' },
            { action_index: 40, address_id: 7, tick_id: 2, amount: '0' }
        ];
        const db = createMockDb();
        db.doQuery.withArgs(sinon.match(/^DELETE e FROM escrows AS e/)).callsFake(async (sql) => {
            const signFiltered = /CAST\(e\.amount AS DECIMAL\(60,18\)\) < 0/.test(sql);
            for(let i = rows.length - 1; i >= 0; i--){
                if(!signFiltered || Number(rows[i].amount) < 0) rows.splice(i, 1);
            }
            return [];
        });
        const rollback = new ClientRollback(db, new Utility(), undefined, 'regtest');

        await rollback.rollback(100);

        assert.strictEqual(maturityReleaseDeletes(db).length, 2, 'expected capability and contract escrow-release deletes');
        assert.deepStrictEqual(rows.map(r => r.amount), ['3.000000000000000001', '0'],
            'only the negative release is removed; the positive and zero escrows rows survive');
    });
});

describe('ClientRollback cooldown-maturity escrow-release transaction handling', function(){
    afterEach(function(){ sinon.restore(); });

    it('deletes the releases before resetting status and before generic escrow deletion', async function(){
        const db = createMockDb();
        const rollback = new ClientRollback(db, new Utility(), undefined, 'regtest');

        await rollback.rollback(100);

        const calls = db.doQuery.getCalls();
        const deletes = maturityReleaseDeletes(db);
        const statusReset = calls.find(c => /^UPDATE unstakes SET status_id/.test(c.args[0]));
        const genericDelete = calls.find(c => /DELETE FROM `escrows` WHERE action_index >= \?/.test(c.args[0]));
        assert.strictEqual(deletes.length, 2, 'expected capability and contract escrow-release deletes');
        assert.ok(statusReset, 'expected the completed-to-valid status reset');
        assert.ok(genericDelete, 'expected the generic action-range escrow delete');
        assert.ok(calls.indexOf(deletes[1]) < calls.indexOf(statusReset), 'release deletes must precede the status reset');
        assert.ok(calls.indexOf(deletes[1]) < calls.indexOf(genericDelete), 'release deletes must precede the generic delete');
    });

    it('deletes releases for an action-empty maturity range', async function(){
        const db = createMockDb(null);
        const rollback = new ClientRollback(db, new Utility(), undefined, 'regtest');

        await rollback.rollback(100);

        assert.strictEqual(maturityReleaseDeletes(db).length, 2,
            'legacy maturities have no action in their maturity block, so reversal must be unconditional');
    });

    it('rolls back the transaction when a release delete faults', async function(){
        const db = createMockDb();
        const deadlock = Object.assign(new Error('deadlock deleting escrow release'), { errno: 1213 });
        db.doQuery.withArgs(sinon.match(/^DELETE e FROM escrows AS e/)).rejects(deadlock);
        const rollback = new ClientRollback(db, new Utility(), undefined, 'regtest');

        await assert.rejects(() => rollback.rollback(100), /deadlock deleting escrow release/);
        assert.strictEqual(db.commitTransaction.called, false);
        assert.strictEqual(db.rollbackTransaction.calledOnce, true);
    });
});
