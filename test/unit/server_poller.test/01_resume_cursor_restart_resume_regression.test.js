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

    describe('resumeCursor (restart resume) @regression', function(){
        it('indexer resumes from the transparency-log high-water mark, not the source tip', async function(){
            // sync_meta recorded up to 100; source DB has since advanced to 250 (e.g.
            // the indexer ran on while the sync server was down).
            log.getHighWaterMark.resolves(100);
            db.getLastBlock.resolves(250);

            let cursor = await poller.resumeCursor();

            assert.strictEqual(cursor, 100, 'must resume from sync_meta high-water mark');
            assert.strictEqual(db.getLastBlock.called, false, 'must not seed from the source tip');
        });

        it('indexer resume is null on a fresh node (empty sync_meta)', async function(){
            log.getHighWaterMark.resolves(null);
            let cursor = await poller.resumeCursor();
            assert.strictEqual(cursor, null, 'null lets poll initialise from the current tip');
        });

        it('decoder resumes from the source tip (no transparency log)', async function(){
            let decoderDb = createMockDb();
            decoderDb.dbType = 'decoder';
            decoderDb.getLastBlock.resolves(777);
            let decoderPoller = new ServerPoller('bitcoin', 'mainnet', decoderDb, broadcaster, null, config, util);

            let cursor = await decoderPoller.resumeCursor();
            assert.strictEqual(cursor, 777);
        });

        it('does not skip blocks advanced during downtime when polling resumes', async function(){
            // Restart-mid-advance: recorded through block 100, source now at 105.
            // After seeding the cursor from the high-water mark, poll must record
            // every block in [101, 105]: none may be skipped.
            log.getHighWaterMark.resolves(100);
            db.getLastBlock.resolves(105);
            db.getBlockHashRow.callsFake(async (idx) => ({
                block_index: idx, block_time: idx * 10,
                ledger_hash: 'l' + idx, actions_hash: 'a' + idx, contract_hash: 'c' + idx
            }));

            poller.lastPolledBlock = await poller.resumeCursor();
            await poller.poll();

            let recorded = log.recordBlock.getCalls().map(c => c.args[0]);
            assert.deepStrictEqual(recorded, [101, 102, 103, 104, 105],
                'every downtime block must be recorded in the transparency log');
            assert.strictEqual(poller.lastPolledBlock, 105);
        });
    });
});
