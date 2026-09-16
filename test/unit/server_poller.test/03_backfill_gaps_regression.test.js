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

    describe('backfillGaps @regression', function(){
        it('is a no-op for the decoder (no transparency log)', async function(){
            let decoderDb = createMockDb();
            decoderDb.dbType = 'decoder';
            let decoderPoller = new ServerPoller('bitcoin', 'mainnet', decoderDb, broadcaster, null, config, util);
            let n = await decoderPoller.backfillGaps();
            assert.strictEqual(n, 0);
        });

        it('is a no-op when the transparency log reports no gaps', async function(){
            log.findGaps.resolves([]);
            let n = await poller.backfillGaps();
            assert.strictEqual(n, 0);
            assert.strictEqual(log.recordBlock.called, false);
            assert.strictEqual(log.recommitEpoch.called, false);
        });

        it('replays recordBlock for each missing block and recomputes affected epochs', async function(){
            // Interior holes at 150 and 201 (epochs 2 and 3 with epochSize 100).
            log.findGaps.resolves([150, 201]);
            db.getBlockHashRow.callsFake(async (idx) => ({
                block_index: idx, block_time: idx * 10,
                ledger_hash: 'l' + idx, actions_hash: 'a' + idx, contract_hash: 'c' + idx
            }));

            let n = await poller.backfillGaps();

            assert.strictEqual(n, 2);
            let recorded = log.recordBlock.getCalls().map(c => c.args[0]);
            assert.deepStrictEqual(recorded, [150, 201]);
            let recomputed = log.recommitEpoch.getCalls().map(c => c.args[0]).sort();
            assert.deepStrictEqual(recomputed, [2, 3], 'epochs spanning the holes are rebuilt');
        });

        it('skips a gap whose source block has vanished (reorg) without recording it', async function(){
            log.findGaps.resolves([150]);
            db.getBlockHashRow.resolves(null);  // block no longer in source

            let n = await poller.backfillGaps();

            assert.strictEqual(n, 1);  // counted as detected
            assert.strictEqual(log.recordBlock.called, false);
            assert.strictEqual(log.recommitEpoch.called, false);
        });
    });
});
