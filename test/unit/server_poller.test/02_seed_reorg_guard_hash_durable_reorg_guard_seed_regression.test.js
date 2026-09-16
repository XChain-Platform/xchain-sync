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

    // Fix #4 (MED): on restart the reorg guard seeded lastPolledBlockHash from a LIVE
    // source read, so a reorg that completed entirely during downtime was masked
    // (live == live) and never detected - stale sync_meta/merkle_epochs served
    // chain-wrong proofs forever. Seed from the DURABLE recorded hash instead so the
    // first poll compares recorded(pre-reorg) vs live(post-reorg) and fires.
    describe('seedReorgGuardHash (durable reorg-guard seed) @regression', function(){
        it('indexer seeds from the recorded (pre-reorg) hash, NOT a live source read', async function(){
            log.getRecordedHash.withArgs(100).resolves('recorded-pre-reorg');
            // The live source at 100 is already post-reorg; seeding from it would mask
            // the reorg. getBlockHashRow (the live read) must not be consulted.
            db.getBlockHashRow.withArgs(100).resolves({ ledger_hash: 'live-post-reorg' });

            let seed = await poller.seedReorgGuardHash(100);

            assert.strictEqual(seed, 'recorded-pre-reorg', 'reorg guard seeds from the durable record');
            assert.ok(log.getRecordedHash.calledWith(100));
            assert.strictEqual(db.getBlockHashRow.called, false, 'must not seed from the live (post-reorg) source');
        });

        it('detects a during-downtime reorg on the first poll after restart', async function(){
            // Recorded up through 100 (pre-reorg ledger hash), no advance in height, but
            // the chain forked at 100 while the server was down: live hash differs.
            // Drive the seed + first poll directly (start() would enter its live poll
            // loop); this mirrors the resumeCursor regression above.
            log.getHighWaterMark.resolves(100);
            log.getRecordedHash.withArgs(100).resolves('l100-pre');
            db.getLastBlock.resolves(100);
            db.getBlockHashRow.withArgs(100).resolves({ ledger_hash: 'l100-post' });

            poller.lastPolledBlock = await poller.resumeCursor();
            poller.lastPolledBlockHash = await poller.seedReorgGuardHash(poller.lastPolledBlock);
            assert.strictEqual(poller.lastPolledBlockHash, 'l100-pre', 'seeded from the recorded pre-reorg hash');

            await poller.poll();

            assert.ok(broadcaster.broadcast.calledWith('bitcoin', 'mainnet', sinon.match({ type: 'reorg', block_index: 100 })),
                'the during-downtime reorg is detected and broadcast');
            assert.ok(log.pruneFrom.calledWith(100), 'transparency log pruned from the fork point');
        });

        it('falls back to the live read for a fresh node (no recorded hash) and null cursor', async function(){
            assert.strictEqual(await poller.seedReorgGuardHash(null), null, 'null cursor -> no seed');
            log.getRecordedHash.withArgs(42).resolves(null);       // never recorded
            db.getBlockHashRow.withArgs(42).resolves({ ledger_hash: 'live-42' });
            assert.strictEqual(await poller.seedReorgGuardHash(42), 'live-42', 'live fallback on a recorded miss');
        });

        it('decoder (no transparency log) seeds from the live source read', async function(){
            let decoderDb = createMockDb();
            decoderDb.dbType = 'decoder';
            decoderDb.getBlockHashRow.withArgs(7).resolves({ block_hash: 'bh7' });
            let decoderPoller = new ServerPoller('bitcoin', 'mainnet', decoderDb, broadcaster, null, config, util);
            assert.strictEqual(await decoderPoller.seedReorgGuardHash(7), 'bh7');
        });
    });
});
