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

let decoderDb, decoderRollback;

function registerDecoderHooks(){
    beforeEach(function(){
        decoderDb = createMockDb();
        decoderDb.dbType = 'decoder';
        decoderRollback = new ClientRollback(decoderDb, util, undefined, 'regtest');
    });
}

describe('ClientRollback', function(){

    describe('rollbackDecoder', function(){
        registerHooks();
        registerDecoderHooks();
        it('routes a decoder DB through rollbackDecoder', async function(){
            let spy = sinon.spy(decoderRollback, 'rollbackDecoder');
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

    });
});

describe('ClientRollback', function(){

    describe('rollbackDecoder', function(){
        registerHooks();
        registerDecoderHooks();
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
