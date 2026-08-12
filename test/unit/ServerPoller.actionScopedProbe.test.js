// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

/*********************************************************************
 * ServerPoller: the action-scoped non-empty-table probe ().
 *
 * _buildBlockPayload used to call getActionScopedRows once per table in the
 * lifecycle registry (86 today), empty ones included, so the per-block
 * round-trip count grew with every replicated table added. It now asks
 * getNonEmptyActionScopedTables once and fetches only the tables that answer.
 *
 * The property under test is not "fewer queries" but "fewer queries AND the
 * same bytes": payload.data feeds a consensus hash followers recompute, so
 * every case below compares the probed payload against the same build with the
 * probe absent, byte for byte.
 *********************************************************************/

const assert = require('assert');
const sinon  = require('sinon');
const ServerPoller = require('../../src/ServerPoller');
const Utility = require('../../src/utility');

const HASH_ROW = {
    block_index: 7, block_time: 1700,
    ledger_hash: 'l7', actions_hash: 'a7', contract_hash: 'c7'
};

// Rows for the two tables this block actually touches. Deliberately two, so a
// probe that returned an empty Set could not pass by accident.
const SEND_ROWS   = [{ action_index: 11, tick_id: 3, quantity: '500' }];
const CREDIT_ROWS = [{ action_index: 11, address_id: 4, tick_id: 3, amount: '500' }];

function createMockDb(){
    return {
        getLastBlock: sinon.stub().resolves(null),
        getBlockHashRow: sinon.stub().resolves(HASH_ROW),
        getBlockScopedRows: sinon.stub().resolves([]),
        getTxScopedRows: sinon.stub().resolves([]),
        getActionScopedRows: sinon.stub().callsFake(async (table) => {
            if(table === 'sends')   return SEND_ROWS;
            if(table === 'credits') return CREDIT_ROWS;
            return [];
        }),
        getEmissionRowsForBlock: sinon.stub().resolves([]),
        getStateRootsRow: sinon.stub().resolves({
            balances_root: 'br', block_merkle_root: 'bmr', state_root: 'sr'
        }),
        getTransactions: sinon.stub().resolves([]),
        getActions: sinon.stub().resolves([{ action_index: 11, action_id: 1, status_id: 1 }]),
        getStatusId: sinon.stub().resolves(null),
        doQuery: sinon.stub().resolves([]),
        beginReadSnapshot: sinon.stub().resolves({ mockSnapshotConn: true }),
        commitReadSnapshot: sinon.stub().resolves(),
        rollbackReadSnapshot: sinon.stub().resolves()
    };
}

function createPoller(db){
    return new ServerPoller('bitcoin', 'mainnet', db,
        { broadcast: sinon.stub(), updateStatus: sinon.stub(), getSubscriberCount: sinon.stub().returns(0) },
        { recordBlock: sinon.stub().resolves(), pruneFrom: sinon.stub().resolves() },
        { BLOCK_POLL_INTERVAL: 3000 }, new Utility());
}

describe('ServerPoller action-scoped non-empty-table probe', function(){

    let db, poller;

    beforeEach(function(){
        db = createMockDb();
        poller = createPoller(db);
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });

    afterEach(function(){
        sinon.restore();
    });

    it('emits a payload byte-identical to the unprobed build while querying only non-empty tables', async function(){
        // Baseline: no probe on the db at all, which is the pre-fix path verbatim.
        let baseline = await poller._buildBlockPayload(7);
        let baselineFetches = db.getActionScopedRows.getCalls().length;

        let probedDb = createMockDb();
        probedDb.getNonEmptyActionScopedTables =
            sinon.stub().resolves(new Set(['sends', 'credits']));
        let probedPoller = createPoller(probedDb);
        let probed = await probedPoller._buildBlockPayload(7);

        assert.strictEqual(JSON.stringify(probed), JSON.stringify(baseline),
            'probed payload must be byte-identical to the unprobed build');

        // The point of the change: the loop stops scaling with the registry.
        assert.strictEqual(probedDb.getNonEmptyActionScopedTables.callCount, 1,
            'exactly one discovery round-trip per block');
        assert.deepStrictEqual(
            probedDb.getActionScopedRows.getCalls().map(c => c.args[0]).sort(),
            ['credits', 'sends'],
            'only the tables the probe reported non-empty are fetched');
        assert.ok(baselineFetches > 40,
            'baseline really did query the whole registry (' + baselineFetches + ')');
    });

    it('passes the snapshot connection to the probe and excludes actions/contract_emissions from it', async function(){
        db.getNonEmptyActionScopedTables = sinon.stub().resolves(new Set());
        const snap = { mockSnapshotConn: true };
        db.beginReadSnapshot.resolves(snap);
        poller.lastPolledBlock = 6;
        db.getLastBlock.resolves(7);

        await poller._poll();

        const call = db.getNonEmptyActionScopedTables.firstCall;
        assert.ok(call, 'probe must run');
        assert.strictEqual(call.args[1], 7, 'probe is scoped to the block being built');
        assert.strictEqual(call.args[2], snap, 'probe must read the pinned snapshot connection');
        assert.ok(!call.args[0].includes('actions'),
            'actions is handled by getActions and must not be probed');
        assert.ok(!call.args[0].includes('contract_emissions'),
            'contract_emissions streams via execution_index and must never be probe-gated');
    });

    it('still streams contract_emissions when the probe reports nothing', async function(){
        // The regression this guards: contract_emissions has NULL-action_index rows the
        // probe's action join cannot see, so gating it on the probe would drop them from
        // the payload while the consensus hash still counts them (follower HALT).
        db.getNonEmptyActionScopedTables = sinon.stub().resolves(new Set());
        db.getEmissionRowsForBlock.resolves([
            { execution_index: 11, emitted_action: 'SLASH', action_index: null, position: 0 }
        ]);

        let payload = await poller._buildBlockPayload(7);

        assert.strictEqual(db.getEmissionRowsForBlock.callCount, 1);
        assert.deepStrictEqual(payload.data['contract_emissions'], [
            { execution_index: 11, emitted_action: 'SLASH', action_index: null, position: 0 }
        ]);
        assert.strictEqual(db.getActionScopedRows.getCalls().length, 0,
            'an empty probe means no per-table fetches at all');
    });

    it('falls back to querying every table when the probe throws', async function(){
        let baseline = await poller._buildBlockPayload(7);
        let baselineFetches = db.getActionScopedRows.getCalls().length;

        let failingDb = createMockDb();
        failingDb.getNonEmptyActionScopedTables =
            sinon.stub().rejects(Object.assign(new Error('lock wait timeout'), { errno: 1205 }));
        let failingPoller = createPoller(failingDb);

        let payload = await failingPoller._buildBlockPayload(7);

        assert.strictEqual(JSON.stringify(payload), JSON.stringify(baseline),
            'a probe fault must cost round-trips, never rows');
        assert.strictEqual(failingDb.getActionScopedRows.getCalls().length, baselineFetches,
            'fallback queries the whole registry exactly as before');
    });

    it('falls back when the probe returns something that is not a Set', async function(){
        let baseline = await poller._buildBlockPayload(7);
        let baselineFetches = db.getActionScopedRows.getCalls().length;

        let oddDb = createMockDb();
        oddDb.getNonEmptyActionScopedTables = sinon.stub().resolves(null);
        let oddPoller = createPoller(oddDb);

        let payload = await oddPoller._buildBlockPayload(7);

        assert.strictEqual(JSON.stringify(payload), JSON.stringify(baseline));
        assert.strictEqual(oddDb.getActionScopedRows.getCalls().length, baselineFetches);
    });

    it('reports probe_queries in the metric so a silent regression to the N+1 is visible', async function(){
        process.env.SYNC_QUERY_METRIC_INTERVAL_MS = '1';
        try {
            db.getNonEmptyActionScopedTables = sinon.stub().resolves(new Set(['sends', 'credits']));
            await poller._buildBlockPayload(7);

            let line = console.log.getCalls()
                .map(c => String(c.args[0]))
                .find(s => s.startsWith('[METRIC] ') && s.includes('sync_action_scoped_queries_per_block'));
            assert.ok(line, 'the metric line must be emitted');
            let metric = JSON.parse(line.slice('[METRIC] '.length));
            assert.strictEqual(metric.probe_queries, 1);
            // sends + credits + the contract_emissions special case.
            assert.strictEqual(metric.queries, 3);
            assert.strictEqual(metric.non_empty_tables, 2);
            assert.ok(metric.candidate_tables > 40,
                'candidate_tables still records the registry size the loop would have walked');
        } finally {
            delete process.env.SYNC_QUERY_METRIC_INTERVAL_MS;
        }
    });
});
