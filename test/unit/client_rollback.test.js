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
const ClientRollback = require('../../src/client/rollback');
const Utility = require('../../src/util');
const balanceHelpers = require('../../src/db/balance_helpers');

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
        rollback = new ClientRollback(db, util, undefined, 'regtest');
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
});
