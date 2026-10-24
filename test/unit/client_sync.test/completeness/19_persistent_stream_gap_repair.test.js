// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// A dogecoin testnet replica sat 2 mappings_actions rows short of its source (1188 vs
// 1186) for good: the repair pass re-pages only the append-only index tables, and a
// stream:action table has no id cursor to re-page from. A table the registry marks
// replicaRepair 'replace' is now re-read from the source and replaced once its gap
// outlasts one sweep, and a one-sweep lag is reported at warn, never at error.
//
// The fake replica below holds real rows per table; getTableCount reads them and the
// stubbed repair snapshot and insertRows write them, so the sweeps see the repair land.

const {
    assert, sinon, axios, ClientSync, registerClientSyncHooks
} = require('../support');
const tableLifecycle = require('../../../../src/table_lifecycle');

const SOURCE = 'http://source1:3006';
const SOURCE_ROWS = Array.from({ length: 1188 }, (_, i) => ({ action_index: i + 1 }));

let sync, db, applier, config;
let local, snapshot;

function assignState(state){
    ({ sync, db, applier, config } = state);
}

const lines = (stub) => stub.getCalls().map(c => String(c.args[0])).join('\n');

// One completed equal-height sweep against a source holding `counts`.
async function sweep(counts){
    axios.get.resolves({ data: { block_height: 100, table_counts: counts } });
    sync._lastCompletenessSweepAt = 0;
    await sync.maybeVerifyCompleteness(SOURCE, 100);
}

const sourceCounts = () => ({ mappings_actions: SOURCE_ROWS.length, mappings_files: 5 });

function registerFakeReplica(){
    beforeEach(function(){
        config.COMPLETENESS_CHECK_INTERVAL = 60000;
        sync.lastAppliedBlock = 100;
        local = { mappings_actions: SOURCE_ROWS.slice(0, 1186), mappings_files: [1, 2, 3, 4] };
        snapshot = { block_height: 100, tables: { mappings_actions: SOURCE_ROWS.slice() } };
        db.getTableCount = sinon.stub().callsFake(async (table) => (local[table] || []).length);
        db.setSyncState = sinon.stub().resolves();
        db.getSyncState = sinon.stub().resolves(null);
        db.beginTransaction = sinon.stub().resolves();
        db.commitTransaction = sinon.stub().resolves(true);
        db.rollbackTransaction = sinon.stub().resolves();
        db.deleteAllRows = sinon.stub().callsFake(async (table) => { local[table] = []; });
        applier.insertRows = sinon.stub().callsFake(async (table, rows) => { local[table].push(...rows); });
        sinon.stub(sync, 'downloadIncrementalCatchUp').callsFake(async () => ({ snapshotData: snapshot }));
        sinon.stub(axios, 'get');
        sinon.stub(console, 'warn');
    });
}

function registerRepairTests(){
    it('re-reads a persistent mappings_actions gap from the source, replaces it, and the gap closes', async function(){
        await sweep(sourceCounts());
        assert.strictEqual(sync.downloadIncrementalCatchUp.called, false, 'a first sighting is never repaired');

        await sweep(sourceCounts());
        assert.strictEqual(sync.downloadIncrementalCatchUp.callCount, 1);
        assert.deepStrictEqual(sync.downloadIncrementalCatchUp.firstCall.args, [SOURCE, 0]);
        assert.strictEqual(db.deleteAllRows.calledOnceWith('mappings_actions'), true);
        assert.strictEqual(local.mappings_actions.length, 1188, 'the replica now holds every source row');
        assert.strictEqual(db.commitTransaction.calledOnce, true);

        await sweep(sourceCounts());
        assert.ok(!sync.getReplicaGaps().some(g => g.table === 'mappings_actions'), 'the gap closed');
        assert.match(lines(console.warn), /REPLICA_GAP_CLOSED: .* table mappings_actions now matches the source/);
        assert.strictEqual(sync.downloadIncrementalCatchUp.callCount, 1, 'a closed gap is not repaired again');
    });

    it('logs a one-sweep lag at warn and never at error', async function(){
        local.mappings_files = [1, 2, 3, 4, 5];
        await sweep(sourceCounts());
        local.mappings_actions = SOURCE_ROWS.slice();
        await sweep(sourceCounts());

        assert.match(lines(console.warn), /TABLE_COUNT_MISMATCH at block 100/);
        assert.doesNotMatch(lines(console.error), /TABLE_COUNT_MISMATCH|REPLICA_GAP_PERSISTENT/);
        assert.strictEqual(sync.downloadIncrementalCatchUp.called, false);
    });

    it('never replaces a stream table the registry does not mark repairable', async function(){
        local.mappings_actions = SOURCE_ROWS.slice();
        for(let i = 0; i < 3; i++) await sweep(sourceCounts());

        assert.strictEqual(sync.downloadIncrementalCatchUp.called, false);
        assert.strictEqual(local.mappings_files.length, 4);
        assert.match(lines(console.error), /REPLICA_GAP_PERSISTENT: .*mappings_files short 1 row/);
    });
}

function registerGuardTests(){
    it('skips the replace when the source moved during the repair read', async function(){
        snapshot.block_height = 101;
        await sweep(sourceCounts());
        await sweep(sourceCounts());

        assert.strictEqual(db.deleteAllRows.called, false);
        assert.match(lines(console.warn), /Stream-table repair skipped: source moved from block 100 to 101/);
    });

    it('refuses a snapshot whose row count is not the one the sweep saw, and keeps the replica rows', async function(){
        snapshot.tables.mappings_actions = SOURCE_ROWS.slice(0, 1187);
        await sweep(sourceCounts());
        await sweep(sourceCounts());

        assert.strictEqual(db.deleteAllRows.called, false);
        assert.strictEqual(local.mappings_actions.length, 1186);
        assert.match(lines(console.error), /Stream-table repair pass failed .*held 1187 row\(s\), expected 1188/);
    });

    it('rolls the swap back when the insert fails', async function(){
        applier.insertRows = sinon.stub().rejects(new Error('insert failed'));
        await sweep(sourceCounts());
        await sweep(sourceCounts());

        assert.strictEqual(db.rollbackTransaction.calledOnce, true);
        assert.strictEqual(db.commitTransaction.called, false);
    });

    it('stops after three failed attempts at one gap', async function(){
        snapshot.block_height = 101;
        for(let i = 0; i < 6; i++) await sweep(sourceCounts());
        assert.strictEqual(sync.downloadIncrementalCatchUp.callCount, 3);
    });

    it('never repairs on a truncated replica, whose window cannot match the full-history count', async function(){
        sinon.stub(sync, 'isTruncated').returns(true);
        for(let i = 0; i < 3; i++) await sweep(sourceCounts());
        assert.strictEqual(sync.downloadIncrementalCatchUp.called, false);
    });
}

function registerRegistryTests(){
    it('marks only stream:action mirror tables replaceable, mappings_actions among them', function(){
        const marked = tableLifecycle.tablesWhere(t => t.replicaRepair != null);
        assert.ok(marked.includes('mappings_actions'));
        for(const table of marked){
            const entry = tableLifecycle.TABLES.find(t => t.table === table);
            assert.strictEqual(entry.replicaRepair, 'replace', table);
            assert.strictEqual(entry.replication, 'stream:action', table);
            assert.strictEqual(entry.replicaRollback, 'mirror', table);
        }
    });
}

describe('ClientSync', function(){
    registerClientSyncHooks(assignState);
    describe('persistent stream-table gap repair @regression', function(){
        registerFakeReplica();
        registerRepairTests();
        registerGuardTests();
        registerRegistryTests();
    });
});
