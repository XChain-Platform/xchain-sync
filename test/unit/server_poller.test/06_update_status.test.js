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
const ServerPoller = require('../../../src/server/poller');
const Utility = require('../../../src/util');
const { withDbMixins } = require('../../helpers/db_mixins.js');

function createMockDb(){
    // Queries read through named Database methods. The real ones are installed for
    // any this fake does not stub, so they still reach doQuery below and every
    // doQuery call count these suites assert keeps counting them.
    return withDbMixins({
        getLastBlock: sinon.stub().resolves(null),
        getBlockHashRow: sinon.stub().resolves(null),
        getBlockScopedRows: sinon.stub().resolves([]),
        getTxScopedRows: sinon.stub().resolves([]),
        getActionScopedRows: sinon.stub().resolves([]),
        getEmissionRowsForBlock: sinon.stub().resolves([]),
        getStateRootsRow: sinon.stub().resolves({
            balances_root: 'br', block_merkle_root: 'bmr', state_root: 'sr'
        }),
        getTransactions: sinon.stub().resolves([]),
        getActions: sinon.stub().resolves([]),
        // Used by collectMaturedCooldownCredits; null short-circuits it to no credits.
        getStatusId: sinon.stub().resolves(null),
        doQuery: sinon.stub().resolves([]),
        // The forward batch is pinned to a REPEATABLE READ snapshot (H-P2).
        beginReadSnapshot: sinon.stub().resolves({ mockSnapshotConn: true }),
        commitReadSnapshot: sinon.stub().resolves(),
        rollbackReadSnapshot: sinon.stub().resolves()
    });
}

function createMockBroadcaster(){
    return {
        broadcast: sinon.stub(),
        updateStatus: sinon.stub(),
        getSubscribers: sinon.stub().returns([]),
        getSubscriberCount: sinon.stub().returns(0)
    };
}

function createMockLog(){
    return {
        epochSize:        100,
        recordBlock:      sinon.stub().resolves(),
        pruneFrom:        sinon.stub().resolves(),
        getHighWaterMark: sinon.stub().resolves(null),
        getRecordedHash:  sinon.stub().resolves(null),
        findGaps:         sinon.stub().resolves([]),
        recommitEpoch:    sinon.stub().resolves()
    };
}

describe('ServerPoller', function(){

    let poller, db, broadcaster, log, config, util;

    beforeEach(function(){
        db = createMockDb();
        broadcaster = createMockBroadcaster();
        log = createMockLog();
        config = { BLOCK_POLL_INTERVAL: 3000 };
        util = new Utility();
        poller = new ServerPoller('bitcoin', 'mainnet', db, broadcaster, log, config, util);
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });

    afterEach(function(){
        sinon.restore();
    });

    describe('updateStatus', function(){
        it('calls broadcaster.updateStatus with correct shape', async function(){
            poller.lastPolledBlock = 50;
            db.getBlockHashRow.resolves({
                block_index: 50, block_time: 1700000000,
                ledger_hash: 'lh', actions_hash: 'ah', contract_hash: 'ch'
            });

            await poller.updateStatus();

            assert.strictEqual(broadcaster.updateStatus.calledOnce, true);
            let args = broadcaster.updateStatus.firstCall.args;
            assert.strictEqual(args[0], 'bitcoin');
            assert.strictEqual(args[1], 'mainnet');
            assert.strictEqual(args[2].block_height, 50);
            assert.strictEqual(args[2].ledger_hash, 'lh');
        });

        it('handles null lastPolledBlock', async function(){
            poller.lastPolledBlock = null;
            await poller.updateStatus();
            let status = broadcaster.updateStatus.firstCall.args[2];
            assert.strictEqual(status.block_height, null);
            assert.strictEqual(status.block_time, null);
        });

        // An undated status is indistinguishable from a stale one, which is what let a
        // cached healthy object keep certifying freshness after the poll started failing.
        it('stamps measured_at on a successful measurement, and publishes nothing on a failed one', async function(){
            poller.lastPolledBlock = 50;
            db.getBlockHashRow.resolves({ block_index: 50, block_time: 1700000000,
                ledger_hash: 'lh', actions_hash: 'ah', contract_hash: 'ch' });

            let before = Date.now();
            await poller.updateStatus();
            let status = broadcaster.updateStatus.firstCall.args[2];
            assert.ok(typeof status.measured_at === 'number' && status.measured_at >= before,
                'a successful poll dates its own observation');

            // The confirmed fault path: the block-hash read rejects, so nothing is
            // published and the previous healthy object survives in the cache untouched.
            broadcaster.updateStatus.resetHistory();
            db.getBlockHashRow.rejects(new Error('replica read failed'));
            await assert.rejects(() => poller.updateStatus());
            assert.strictEqual(broadcaster.updateStatus.called, false,
                'a failed measurement publishes no status, so only its AGE can expose it');
        });
    });

    describe('updateStatus', function(){
        // Replication freshness. source_block_height falls back to
        // db.getLastBlock(), a MAX(block_index) against the SERVED database, so on
        // a node fronting a native SQL replica both heights freeze together when
        // replication stops applying and the derived lag reads 0. These pin the
        // fail-closed rules: only a confirmed primary or a confirmed in-window
        // replica may report fresh.
        describe('replication freshness', function(){
            function statusAfter(){ return broadcaster.updateStatus.firstCall.args[2]; }

            it('reports fresh on a primary (not a replica at all)', async function(){
                db.getReplicaStatus = sinon.stub().resolves({ isReplica: false, running: null, secondsBehind: null });
                await poller.updateStatus(100);
                assert.strictEqual(statusAfter().replica_stale, false);
                assert.strictEqual(statusAfter().replica_seconds_behind, null);
            });

            it('reports fresh on a replica inside the lag ceiling', async function(){
                config.SYNC_REPLICA_MAX_LAG_S = 120;
                db.getReplicaStatus = sinon.stub().resolves({ isReplica: true, running: true, secondsBehind: 5 });
                await poller.updateStatus(100);
                assert.strictEqual(statusAfter().replica_stale, false);
                assert.strictEqual(statusAfter().replica_seconds_behind, 5);
            });

            it('reports stale past the lag ceiling', async function(){
                config.SYNC_REPLICA_MAX_LAG_S = 120;
                db.getReplicaStatus = sinon.stub().resolves({ isReplica: true, running: true, secondsBehind: 900 });
                await poller.updateStatus(100);
                assert.strictEqual(statusAfter().replica_stale, true);
            });

            it('reports stale when the SQL thread stopped (Seconds_Behind NULL)', async function(){
                db.getReplicaStatus = sinon.stub().resolves({ isReplica: true, running: false, secondsBehind: null });
                await poller.updateStatus(100);
                assert.strictEqual(statusAfter().replica_stale, true,
                    'a stopped applier is unbounded lag; this is the failure that published lag 0');
            });

            it('fails closed when the replication status is unreadable', async function(){
                db.getReplicaStatus = sinon.stub().resolves({ isReplica: null, running: null, secondsBehind: null });
                await poller.updateStatus(100);
                assert.strictEqual(statusAfter().replica_stale, true);
            });

            it('fails closed when the read throws', async function(){
                db.getReplicaStatus = sinon.stub().rejects(new Error('boom'));
                await poller.updateStatus(100);
                assert.strictEqual(statusAfter().replica_stale, true);
            });
        });
    });
});
