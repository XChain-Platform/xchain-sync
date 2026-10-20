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
const { archiveHeadPickPredicate, foldArchiveHeadFloor } = require('../../../src/consensus/archive_head_pick');

const NETWORKS = ['regtest', 'testnet', 'mainnet'];
const ARCHIVE_RESET = /^UPDATE anchor_actions p /;

function createMockDb(firstActionIndex, archiveErrno){
    return {
        doQuery: sinon.stub().callsFake(async (sql) => {
            if(archiveErrno && typeof sql === 'string' && ARCHIVE_RESET.test(sql)){
                const error = new Error('archive reset error ' + archiveErrno);
                error.errno = archiveErrno;
                throw error;
            }
            return [];
        }),
        getFirstActionIndex: sinon.stub().resolves(firstActionIndex),
        getStatusId: sinon.stub().resolves(null),
        beginTransaction: sinon.stub().resolves(),
        commitTransaction: sinon.stub().resolves(),
        rollbackTransaction: sinon.stub().resolves()
    };
}

function archiveResetCalls(db){
    return db.doQuery.getCalls().filter(call =>
        typeof call.args[0] === 'string' && ARCHIVE_RESET.test(call.args[0]));
}

describe('ClientRollback invalid_archive parent reset @regression', function(){
    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'warn');
        sinon.stub(console, 'error');
    });

    afterEach(function(){
        sinon.restore();
    });

    it('resets one surviving parent from each network using the first orphaned action index', async function(){
        for(const network of NETWORKS){
            const db = createMockDb(500);
            await new ClientRollback(db, new Utility(), 'BTC', network).rollback(100);

            const resets = archiveResetCalls(db);
            assert.strictEqual(resets.length, 1, network + ' must issue one archive reset');
            const sql = resets[0].args[0];
            assert.ok(sql.includes("ps.status = 'invalid_archive'"));
            assert.ok(sql.includes('c.version = 2'));
            assert.ok(sql.includes('c.action_index >= ?'));
            assert.ok(sql.includes("cs.status = 'valid'"));
            assert.ok(sql.includes("us.status = 'unverified'"));
            assert.ok(sql.includes('p.action_index < ?'));
            assert.ok(sql.includes(archiveHeadPickPredicate('p')));
            assert.deepStrictEqual(resets[0].args[1], [500, foldArchiveHeadFloor(network, 'DOGE'), 500]);
        }
    });

    it('skips the reset when getFirstActionIndex resolves null', async function(){
        for(const network of NETWORKS){
            const db = createMockDb(null);
            await new ClientRollback(db, new Utility(), 'BTC', network).rollback(100);
            assert.strictEqual(archiveResetCalls(db).length, 0);
        }
    });

    it('swallows missing-table and missing-column errors from the reset', async function(){
        for(const network of NETWORKS){
            for(const errno of [1146, 1054]){
                const db = createMockDb(500, errno);
                await new ClientRollback(db, new Utility(), 'BTC', network).rollback(100);
                assert.strictEqual(db.commitTransaction.calledOnce, true);
                assert.strictEqual(db.rollbackTransaction.called, false);
            }
        }
    });

    it('rethrows any other reset error and rolls back the transaction', async function(){
        for(const network of NETWORKS){
            const db = createMockDb(500, 1213);
            await assert.rejects(
                () => new ClientRollback(db, new Utility(), 'BTC', network).rollback(100),
                { errno: 1213 }
            );
            assert.strictEqual(db.commitTransaction.called, false);
            assert.strictEqual(db.rollbackTransaction.calledOnce, true);
        }
    });
});
