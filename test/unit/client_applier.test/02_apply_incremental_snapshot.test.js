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
const ClientApplier = require('../../../src/client/applier');
const Utility = require('../../../src/util');
const { SCHEMA_VERSION } = require('../../../src/schema/version');
const balanceHelpers = require('../../../src/db/balance_helpers');
const { withDbMixins } = require('../../helpers/db_mixins.js');

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

function registerIncrementalSnapshotCases1(){
    it('inserts rows without truncation', async function(){
        const snapshot = {
            schema_version: SCHEMA_VERSION.indexer,
            block_height: 20,
            since_block: 10,
            tables: { blocks: [{ block_index: 11 }] }
        };
        await applier.applyIncrementalSnapshot(snapshot);
        assert.strictEqual(db.truncateTable.called, false);
        assert.strictEqual(db.beginTransaction.calledOnce, true);
        assert.strictEqual(db.commitTransaction.calledOnce, true);
    });
    registerRecommittedEpochCase();
    it('skips null snapshot', async function(){
        await applier.applyIncrementalSnapshot(null);
        assert.strictEqual(db.beginTransaction.called, false);
    });
    it('skips a snapshot without tables', async function(){
        await applier.applyIncrementalSnapshot({ since_block: 1 });
        assert.strictEqual(db.beginTransaction.called, false);
    });
    it('throws on a schema-version mismatch', async function(){
        await assert.rejects(
            () => applier.applyIncrementalSnapshot({ schema_version: 'wrong', tables: { t: [{ id: 1 }] } }),
            /Schema version mismatch/);
        assert.strictEqual(db.beginTransaction.called, false);
    });
    it('mirrors the anchor-reward winner collapses the catch-up window carried (reconcile-log rows at/above since_block)', async function(){
        db.dbType = 'indexer';
        await applier.applyIncrementalSnapshot({
            schema_version: SCHEMA_VERSION.indexer,
            block_height: 961800,
            since_block: 961600,
            tables: { anchor_reward_reconcile_log: [{ id: 1, reward_type: 'anchor_BTC', round_reference: 961500,
                source_id: 7, signing_pubkey_id: 3, amount: '1.00000000', reward_block_index: 961500,
                reward_derive_block_index: 961700, block_index: 961700 }] }
        });
        const del = db.doQuery.getCalls().find(c => /^DELETE vr FROM validator_rewards vr JOIN anchor_reward_reconcile_log d/.test(c.args[0]));
        assert.ok(del, 'expected the loser-row delete mirror on the incremental path');
        assert.ok(/WHERE d\.block_index >= \?$/.test(del.args[0]));
        assert.deepStrictEqual(del.args[1], [961600]);
    });
    it('rebuilds balances when the catch-up touches credits/debits', async function(){
        db.dbType = 'indexer';
        const rb  = sinon.stub(balanceHelpers, 'rebuildBalances').resolves();
        await applier.applyIncrementalSnapshot({
            schema_version: SCHEMA_VERSION.indexer,
            since_block: 10,
            tables: { debits: [{ id: 1 }] }
        });
        assert.strictEqual(rb.calledOnce, true);
    });
}

function registerRecommittedEpochCase(){
    it('authoritatively refreshes recommitted epoch roots without carrying the source id', async function(){
        await applier.applyIncrementalSnapshot({
            schema_version: SCHEMA_VERSION.indexer,
            block_height: 20,
            since_block: 10,
            tables: { merkle_epochs: [{ id: 91, epoch: 2, start_block: 10, end_block: 19,
                merkle_root: 'corrected', leaf_count: 10 }] }
        });
        const query = db.doQuery.getCalls().map(c => c.args[0])
            .find(sql => /^INSERT INTO `merkle_epochs`/.test(sql));
        assert.ok(query, 'the authoritative epoch set must use a plain INSERT upsert');
        assert.ok(query.includes('ON DUPLICATE KEY UPDATE'), 'a duplicate epoch must refresh its root');
        assert.ok(!query.includes('`id`'), 'the source-local surrogate id must not reach the replica');
        assert.ok(query.includes('`merkle_root` = VALUES(`merkle_root`)'));
        assert.ok(query.includes('`leaf_count` = VALUES(`leaf_count`)'));
    });
}

function registerIncrementalSnapshotRepairCases(){
    it('never derives sync_meta leaves from local blocks on an incremental indexer snapshot', async function(){
        db.dbType = 'indexer';
        await applier.applyIncrementalSnapshot({
            schema_version: SCHEMA_VERSION.indexer,
            block_height: 20,
            since_block: 10,
            tables: { blocks: [{ block_index: 11 }] }
        });
        const derived = db.doQuery.getCalls().filter(c => /INTO sync_meta[\s\S]*FROM blocks/.test(c.args[0]));
        assert.deepStrictEqual(derived, [],
            'a source that was down records no leaves for those blocks, so the replica must not invent them');
    });
    it('writes no sync_meta leaves on decoder snapshots', async function(){
        db.dbType = 'decoder';
        await applier.applyIncrementalSnapshot({
            schema_version: SCHEMA_VERSION.decoder,
            block_height: 20,
            since_block: 10,
            tables: { blocks: [{ block_index: 11 }] }
        });
        assert.ok(!db.doQuery.getCalls().some(c => /INSERT IGNORE INTO sync_meta/.test(c.args[0])));
    });
}

function registerIncrementalSnapshotCases2(){
    it('rolls back on error', async function(){
        db.doQuery.rejects(new Error('inc fail'));
        // Carries block_index because `blocks` rows are now identified by it; a
        // bare {id} row is refused before any query runs, which would mask the
        // DB failure this case exists to check.
        await assert.rejects(() => applier.applyIncrementalSnapshot({
            schema_version: SCHEMA_VERSION.indexer, since_block: 1, tables: { blocks: [{ id: 1, block_index: 1 }] }
        }), { message: 'inc fail' });
        assert.strictEqual(db.rollbackTransaction.calledOnce, true);
    });
}
describe('ClientApplier', function(){
    registerHooks();

    describe('applyIncrementalSnapshot', function(){
        registerIncrementalSnapshotCases1();
        registerIncrementalSnapshotRepairCases();
        registerIncrementalSnapshotCases2();
    });
});
