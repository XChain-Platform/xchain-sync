// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// Read-only transparency log (REPLICA_DB_READONLY): the posture the sync.xchain.io
// web tier runs in, where sync_meta / merkle_epochs arrive over MariaDB binlog
// replication and the box itself is read_only=1. Every write entry point must be a
// no-op there; every read path must keep working, because the tier still serves
// /transparency to the public.

const assert = require('assert');
const sinon  = require('sinon');
const TransparencyLog = require('../../src/TransparencyLog');
const ServerPoller    = require('../../src/ServerPoller');

describe('TransparencyLog (read-only replica)', function(){

    let db, log;

    beforeEach(function(){
        db  = { doQuery: sinon.stub().resolves([]) };
        log = new TransparencyLog(db, 100, true);
    });

    afterEach(function(){
        sinon.restore();
    });

    describe('the flag itself', function(){
        it('defaults to false when the third argument is omitted', function(){
            assert.strictEqual(new TransparencyLog(db, 100).readOnly, false);
        });

        it('is true only for a real boolean true, never a truthy string', function(){
            assert.strictEqual(new TransparencyLog(db, 100, true).readOnly, true);
            assert.strictEqual(new TransparencyLog(db, 100, 'false').readOnly, false);
            assert.strictEqual(new TransparencyLog(db, 100, 1).readOnly, false);
        });
    });

    describe('write entry points are no-ops', function(){
        it('recordBlock writes nothing', async function(){
            await log.recordBlock(101, 1700000000, 'lhash', 'ahash', 'chash');
            assert.strictEqual(db.doQuery.called, false);
        });

        it('recordBlock does not commit an epoch at a boundary block', async function(){
            let commit = sinon.spy(log, 'commitEpoch');
            await log.recordBlock(100, 1700000000, 'l', 'a', 'c');
            assert.strictEqual(commit.called, false);
            assert.strictEqual(db.doQuery.called, false);
        });

        it('commitEpoch writes nothing', async function(){
            await log.commitEpoch(1);
            assert.strictEqual(db.doQuery.called, false);
        });

        it('pruneFrom writes nothing on a reorg', async function(){
            await log.pruneFrom(500);
            assert.strictEqual(db.doQuery.called, false);
        });

        it('recommitEpoch writes nothing', async function(){
            await log.recommitEpoch(1, 100000);
            assert.strictEqual(db.doQuery.called, false);
        });
    });

    describe('read paths still work', function(){
        it('getHighWaterMark still queries and returns the replicated tip', async function(){
            db.doQuery.resolves([{ tip: 961197 }]);
            assert.strictEqual(await log.getHighWaterMark(), 961197);
            assert.strictEqual(db.doQuery.calledOnce, true);
        });

        it('getRecordedHash still queries', async function(){
            db.doQuery.resolves([{ ledger_hash: 'abc' }]);
            assert.strictEqual(await log.getRecordedHash(961197), 'abc');
            assert.strictEqual(db.doQuery.calledOnce, true);
        });

        it('getLatestRoot still queries', async function(){
            db.doQuery.resolves([{ epoch: 9611, merkle_root: 'root' }]);
            let row = await log.getLatestRoot();
            assert.strictEqual(row.merkle_root, 'root');
        });

        it('getPage still queries', async function(){
            db.doQuery.onFirstCall().resolves([{ total: 42 }]);
            db.doQuery.onSecondCall().resolves([{ block_index: 1 }]);
            let page = await log.getPage(0, 10);
            assert.strictEqual(page.total, 42);
            assert.strictEqual(page.results.length, 1);
        });

        it('getProof still builds a verifiable proof from replicated rows', async function(){
            db.doQuery.onFirstCall().resolves([
                { epoch: 1, start_block: 1, end_block: 2, merkle_root: null }
            ]);
            db.doQuery.onSecondCall().resolves([
                { block_index: 1, ledger_hash: 'l1', actions_hash: 'a1', contract_hash: 'c1' },
                { block_index: 2, ledger_hash: 'l2', actions_hash: 'a2', contract_hash: 'c2' }
            ]);
            let proof = await log.getProof(1);
            assert.strictEqual(proof.blockIndex, 1);
            assert.strictEqual(proof.epoch, 1);
            assert.ok(Array.isArray(proof.proof));
        });

        it('findGaps still queries (it is a scan, not a repair)', async function(){
            db.doQuery.onFirstCall().resolves([{ lo: 1, hi: 100 }]);
            db.doQuery.onSecondCall().resolves([{ block_index: 50 }]);
            assert.deepStrictEqual(await log.findGaps(), [50]);
        });
    });

    describe('a writable log is unaffected', function(){
        it('recordBlock still writes when the flag is off', async function(){
            let writable = new TransparencyLog(db, 100, false);
            await writable.recordBlock(101, 1700000000, 'l', 'a', 'c');
            assert.strictEqual(db.doQuery.calledOnce, true);
            assert.ok(db.doQuery.firstCall.args[0].includes('INSERT IGNORE'));
        });
    });
});

describe('ServerPoller.backfillGaps (read-only replica)', function(){

    afterEach(function(){
        sinon.restore();
    });

    // backfillGaps replays recordBlock for every gap and recommits the affected
    // epochs, all writes. On a replicated log it must not even scan: findGaps could
    // legitimately report a gap mid-replication, and the repair loop would then log
    // a success it did not perform.
    function pollerWithLog(log){
        return Object.create(ServerPoller.prototype, {
            transparencyLog: { value: log, writable: true },
            db:              { value: { getBlockHashRow: sinon.stub().resolves(null) }, writable: true },
            chain:           { value: 'bitcoin', writable: true },
            network:         { value: 'mainnet', writable: true },
            dbType:          { value: 'indexer', writable: true }
        });
    }

    it('returns 0 without scanning when the log is read-only', async function(){
        let log = {
            readOnly: true,
            epochSize: 100,
            findGaps: sinon.stub().resolves([50]),
            recordBlock: sinon.stub().resolves(),
            recommitEpoch: sinon.stub().resolves(),
            getHighWaterMark: sinon.stub().resolves(100)
        };
        assert.strictEqual(await pollerWithLog(log).backfillGaps(), 0);
        assert.strictEqual(log.findGaps.called, false);
        assert.strictEqual(log.recordBlock.called, false);
        assert.strictEqual(log.recommitEpoch.called, false);
    });

    it('still scans and repairs when the log is writable', async function(){
        let log = {
            readOnly: false,
            epochSize: 100,
            findGaps: sinon.stub().resolves([50]),
            recordBlock: sinon.stub().resolves(),
            recommitEpoch: sinon.stub().resolves(),
            getHighWaterMark: sinon.stub().resolves(100)
        };
        sinon.stub(console, 'log');
        let poller = pollerWithLog(log);
        poller.db.getBlockHashRow = sinon.stub().resolves({
            block_index: 50, block_time: 1, ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c'
        });
        assert.strictEqual(await poller.backfillGaps(), 1);
        assert.strictEqual(log.findGaps.calledOnce, true);
        assert.strictEqual(log.recordBlock.calledOnce, true);
        assert.strictEqual(log.recommitEpoch.calledOnce, true);
    });

    it('returns 0 when there is no transparency log at all (decoder)', async function(){
        assert.strictEqual(await pollerWithLog(null).backfillGaps(), 0);
    });
});
