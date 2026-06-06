// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available —
// contact legal@dankest.llc.

const assert = require('assert');
const sinon  = require('sinon');
const TransparencyLog = require('../../src/TransparencyLog');

describe('TransparencyLog', function(){

    let log, db;

    beforeEach(function(){
        db = { doQuery: sinon.stub().resolves([]) };
        log = new TransparencyLog(db);
    });

    describe('recordBlock', function(){
        it('calls doQuery with INSERT IGNORE and correct params', async function(){
            // Use a non-epoch-boundary block (epochSize=100) so recordBlock makes exactly
            // one doQuery; block 100 would also trigger a commitEpoch insert.
            await log.recordBlock(101, 1700000000, 'lhash', 'ahash', 'chash');
            assert.strictEqual(db.doQuery.calledOnce, true);
            let query = db.doQuery.firstCall.args[0];
            assert.ok(query.includes('INSERT IGNORE'));
            assert.ok(query.includes('sync_meta'));
            let args = db.doQuery.firstCall.args[1];
            assert.deepStrictEqual(args, [101, 1700000000, 'lhash', 'ahash', 'chash']);
        });
    });

    describe('getPage', function(){
        it('returns paginated results', async function(){
            let rows = [{ block_index: 100, block_time: 123, ledger_hash: 'a', actions_hash: 'b', contract_hash: 'c', logged_at: '2025-01-01' }];
            db.doQuery.onFirstCall().resolves([{ total: 50n }]);
            db.doQuery.onSecondCall().resolves(rows);

            let result = await log.getPage(0, 10);
            assert.strictEqual(result.page, 0);
            assert.strictEqual(result.limit, 10);
            assert.strictEqual(result.total, 50);
            assert.deepStrictEqual(result.results, rows);
        });

        it('clamps negative page to 0', async function(){
            db.doQuery.onFirstCall().resolves([{ total: 0n }]);
            db.doQuery.onSecondCall().resolves([]);
            let result = await log.getPage(-5, 10);
            assert.strictEqual(result.page, 0);
        });

        it('falls back to default 100 for limit of 0 (parseInt(0) || 100)', async function(){
            db.doQuery.onFirstCall().resolves([{ total: 0n }]);
            db.doQuery.onSecondCall().resolves([]);
            let result = await log.getPage(0, 0);
            assert.strictEqual(result.limit, 100);
        });

        it('clamps limit below 1 to 1 for negative values', async function(){
            db.doQuery.onFirstCall().resolves([{ total: 0n }]);
            db.doQuery.onSecondCall().resolves([]);
            let result = await log.getPage(0, -5);
            assert.strictEqual(result.limit, 1);
        });

        it('clamps limit above 1000 to 1000', async function(){
            db.doQuery.onFirstCall().resolves([{ total: 0n }]);
            db.doQuery.onSecondCall().resolves([]);
            let result = await log.getPage(0, 5000);
            assert.strictEqual(result.limit, 1000);
        });

        it('calculates correct offset', async function(){
            db.doQuery.onFirstCall().resolves([{ total: 100n }]);
            db.doQuery.onSecondCall().resolves([]);
            await log.getPage(3, 25);
            let args = db.doQuery.secondCall.args[1];
            assert.strictEqual(args[1], 75); // page 3 * limit 25
        });

        it('handles non-numeric page gracefully', async function(){
            db.doQuery.onFirstCall().resolves([{ total: 0n }]);
            db.doQuery.onSecondCall().resolves([]);
            let result = await log.getPage('abc', 10);
            assert.strictEqual(result.page, 0);
        });
    });

    describe('pruneFrom', function(){
        it('records an audit marker and prunes sync_meta + merkle_epochs for an invalidated epoch', async function(){
            db.doQuery.withArgs(sinon.match(/SELECT epoch, start_block/)).resolves([
                { epoch: 3, start_block: 201, end_block: 300, merkle_root: 'OLDROOT' }
            ]);
            // No pending marker yet → the guard SELECT returns empty.
            db.doQuery.withArgs(sinon.match(/SELECT id FROM merkle_reorgs/)).resolves([]);

            await log.pruneFrom(250);

            // Marker captures the pre-reorg root before the epoch is deleted.
            let ins = db.doQuery.getCalls().find(c => /INSERT INTO merkle_reorgs/.test(c.args[0]));
            assert.ok(ins, 'inserts a merkle_reorgs audit marker');
            assert.deepStrictEqual(ins.args[1], [250, 3, 201, 300, 'OLDROOT']);

            // Both tables pruned from the reorg point.
            assert.ok(db.doQuery.getCalls().some(c =>
                /DELETE FROM merkle_epochs WHERE end_block >= /.test(c.args[0]) && c.args[1][0] === 250));
            assert.ok(db.doQuery.getCalls().some(c =>
                /DELETE FROM sync_meta WHERE block_index >= /.test(c.args[0]) && c.args[1][0] === 250));
        });

        it('does not duplicate the audit marker when one is already pending (retry-safe)', async function(){
            db.doQuery.withArgs(sinon.match(/SELECT epoch, start_block/)).resolves([
                { epoch: 3, start_block: 201, end_block: 300, merkle_root: 'OLDROOT' }
            ]);
            // A pending marker already exists from a prior (faulted) attempt.
            db.doQuery.withArgs(sinon.match(/SELECT id FROM merkle_reorgs/)).resolves([{ id: 1 }]);

            await log.pruneFrom(250);

            assert.ok(!db.doQuery.getCalls().some(c => /INSERT INTO merkle_reorgs/.test(c.args[0])),
                'no duplicate marker insert when one is already pending');
            // Prune still proceeds.
            assert.ok(db.doQuery.getCalls().some(c => /DELETE FROM merkle_epochs/.test(c.args[0])));
            assert.ok(db.doQuery.getCalls().some(c => /DELETE FROM sync_meta/.test(c.args[0])));
        });

        it('still prunes sync_meta when no committed epoch is invalidated', async function(){
            db.doQuery.withArgs(sinon.match(/SELECT epoch, start_block/)).resolves([]);

            await log.pruneFrom(42);

            assert.ok(!db.doQuery.getCalls().some(c => /INSERT INTO merkle_reorgs/.test(c.args[0])),
                'no audit marker when nothing committed is invalidated');
            assert.ok(db.doQuery.getCalls().some(c => /DELETE FROM sync_meta/.test(c.args[0])));
        });

        it('propagates a DB error so the poll loop retries (no transaction state to corrupt)', async function(){
            db.doQuery.withArgs(sinon.match(/SELECT epoch, start_block/)).rejects(new Error('boom'));

            await assert.rejects(() => log.pruneFrom(10), /boom/);
        });
    });

    describe('commitEpoch reorg-marker backfill', function(){
        it('backfills a pending reorg marker new_root when the epoch re-commits', async function(){
            // Not yet committed.
            db.doQuery.withArgs(sinon.match(/SELECT id FROM merkle_epochs/)).resolves([]);
            // Epoch blocks present in sync_meta (re-synced canonical chain).
            db.doQuery.withArgs(sinon.match(/FROM sync_meta/)).resolves([
                { block_index: 1, ledger_hash: 'a', actions_hash: 'b', contract_hash: 'c' },
                { block_index: 2, ledger_hash: 'd', actions_hash: 'e', contract_hash: 'f' }
            ]);

            await log.commitEpoch(1);

            let upd = db.doQuery.getCalls().find(c => /UPDATE merkle_reorgs SET new_root/.test(c.args[0]));
            assert.ok(upd, 'updates merkle_reorgs with the recomputed root');
            assert.strictEqual(upd.args[1][1], 1); // epoch param
            assert.ok(/AND new_root IS NULL/.test(upd.args[0]), 'only backfills the pending marker');
        });
    });
});
