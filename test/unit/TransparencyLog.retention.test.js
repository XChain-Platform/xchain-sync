// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// Opt-in sync_meta retention. The default posture is UNCHANGED: with no
// SYNC_META_RETENTION_BLOCKS the log keeps full history and pruneSyncMeta deletes
// nothing, so every historical inclusion proof stays serveable. When an operator
// does arm a window, the prune must land on a COMMITTED epoch boundary, because a
// half-pruned epoch would make getProof rebuild a tree that no longer hashes to the
// committed root.

const assert = require('assert');
const sinon  = require('sinon');
const TransparencyLog = require('../../src/TransparencyLog');

// db double whose SELECTs are keyed by query shape. hwm = MAX(block_index) in
// sync_meta, boundary = MAX(end_block) of a committed epoch at/below the cutoff.
function createDb({ hwm, boundary, straddling } = {}){
    let db = { doQuery: sinon.stub() };
    db.doQuery.withArgs(sinon.match(/MAX\(block_index\) AS tip FROM sync_meta/))
        .resolves([{ tip: (hwm === undefined ? null : hwm) }]);
    db.doQuery.withArgs(sinon.match(/MAX\(end_block\) AS eb FROM merkle_epochs/))
        .resolves([{ eb: (boundary === undefined ? null : boundary) }]);
    db.doQuery.withArgs(sinon.match(/COUNT\(\*\) AS c FROM merkle_epochs/))
        .resolves([{ c: straddling ? 1 : 0 }]);
    db.doQuery.withArgs(sinon.match(/DELETE FROM sync_meta WHERE block_index <= /))
        .resolves({ affectedRows: 900 });
    db.doQuery.resolves([]);
    return db;
}

function deleteCalls(db){
    return db.doQuery.getCalls().filter(c => /DELETE FROM sync_meta/.test(c.args[0]));
}

describe('TransparencyLog sync_meta retention', function(){

    afterEach(function(){ sinon.restore(); });

    describe('default-off', function(){
        it('retentionBlocks is 0 when the fourth constructor argument is omitted', function(){
            assert.strictEqual(new TransparencyLog(createDb(), 100).retentionBlocks, 0);
        });

        it('treats 0, negative, non-numeric and null windows as off', function(){
            let db = createDb();
            for(let value of [0, -1, -100000, 'lots', '', null, undefined, NaN])
                assert.strictEqual(new TransparencyLog(db, 100, false, value).retentionBlocks, 0,
                    'value: ' + String(value));
        });

        it('accepts a positive window, including a numeric string from the env', function(){
            let db = createDb();
            assert.strictEqual(new TransparencyLog(db, 100, false, 50000).retentionBlocks, 50000);
            assert.strictEqual(new TransparencyLog(db, 100, false, '50000').retentionBlocks, 50000);
        });

        it('pruneSyncMeta is an inert no-op with no window: nothing is even read', async function(){
            let db  = createDb({ hwm: 1000000, boundary: 900000 });
            let log = new TransparencyLog(db, 100);
            let result = await log.pruneSyncMeta();
            assert.deepStrictEqual(result, { enabled: false, deleted: 0 });
            assert.strictEqual(db.doQuery.called, false, 'no query at all when retention is off');
        });

        it('an explicit zero argument is a no-op even when the instance has a window', async function(){
            let db  = createDb({ hwm: 1000000, boundary: 900000 });
            let log = new TransparencyLog(db, 100, false, 50000);
            let result = await log.pruneSyncMeta(0);
            assert.deepStrictEqual(result, { enabled: false, deleted: 0 });
            assert.strictEqual(db.doQuery.called, false);
        });

        it('recordBlock never prunes at an epoch boundary when retention is off', async function(){
            let db  = createDb({ hwm: 1000000, boundary: 900000 });
            let log = new TransparencyLog(db, 100);
            sinon.stub(log, 'commitEpoch').resolves();
            let prune = sinon.spy(log, 'pruneSyncMeta');
            await log.recordBlock(1000, 1700000000, 'l', 'a', 'c');
            assert.strictEqual(prune.called, false);
            assert.strictEqual(deleteCalls(db).length, 0);
        });
    });

    describe('armed window', function(){
        it('deletes at the committed epoch boundary below the cutoff', async function(){
            // tip 1,000,000 with a 50,000-block window: cutoff 950,000, and the highest
            // committed epoch ending at or below it ends at 950,000.
            let db  = createDb({ hwm: 1000000, boundary: 950000 });
            let log = new TransparencyLog(db, 100, false, 50000);
            sinon.stub(console, 'log');

            let result = await log.pruneSyncMeta();

            assert.strictEqual(result.enabled, true);
            assert.strictEqual(result.skipped, false);
            assert.strictEqual(result.tip, 1000000);
            assert.strictEqual(result.cutoff, 950000);
            assert.strictEqual(result.prunedThrough, 950000);
            assert.strictEqual(result.deleted, 900);

            let del = deleteCalls(db);
            assert.strictEqual(del.length, 1, 'exactly one range delete');
            assert.deepStrictEqual(del[0].args[1], [950000]);
            // The boundary lookup is bounded by the cutoff, never by the tip.
            let bound = db.doQuery.getCalls().find(c => /MAX\(end_block\) AS eb/.test(c.args[0]));
            assert.deepStrictEqual(bound.args[1], [950000]);
        });

        it('an explicit argument overrides the constructor window', async function(){
            let db  = createDb({ hwm: 1000000, boundary: 200000 });
            let log = new TransparencyLog(db, 100, false, 50000);
            sinon.stub(console, 'log');
            let result = await log.pruneSyncMeta(750000);
            assert.strictEqual(result.cutoff, 250000);
            assert.strictEqual(result.prunedThrough, 200000);
        });

        it('never prunes the committed roots: merkle_epochs is only read', async function(){
            let db  = createDb({ hwm: 1000000, boundary: 950000 });
            let log = new TransparencyLog(db, 100, false, 50000);
            sinon.stub(console, 'log');
            await log.pruneSyncMeta();
            assert.ok(!db.doQuery.getCalls().some(c => /DELETE FROM merkle_epochs/.test(c.args[0])),
                'committed Merkle roots survive retention');
        });

        it('does nothing when no committed epoch lies below the cutoff', async function(){
            let db  = createDb({ hwm: 1000000, boundary: undefined });
            let log = new TransparencyLog(db, 100, false, 50000);
            let result = await log.pruneSyncMeta();
            assert.strictEqual(result.deleted, 0);
            assert.strictEqual(result.prunedThrough, null);
            assert.strictEqual(deleteCalls(db).length, 0);
        });

        it('does nothing when the window is wider than the log', async function(){
            let db  = createDb({ hwm: 500, boundary: 400 });
            let log = new TransparencyLog(db, 100, false, 50000);
            let result = await log.pruneSyncMeta();
            assert.strictEqual(result.cutoff, null);
            assert.strictEqual(result.deleted, 0);
            assert.strictEqual(deleteCalls(db).length, 0);
        });

        it('does nothing on an empty log', async function(){
            let db  = createDb({ hwm: null });
            let log = new TransparencyLog(db, 100, false, 50000);
            let result = await log.pruneSyncMeta();
            assert.strictEqual(result.tip, null);
            assert.strictEqual(result.deleted, 0);
            assert.strictEqual(deleteCalls(db).length, 0);
        });

        it('refuses to cut through a committed epoch (epoch size changed mid-life)', async function(){
            let db  = createDb({ hwm: 1000000, boundary: 950000, straddling: true });
            let log = new TransparencyLog(db, 100, false, 50000);
            sinon.stub(console, 'error');
            let result = await log.pruneSyncMeta();
            assert.strictEqual(result.skipped, true);
            assert.strictEqual(result.reason, 'straddling_epoch');
            assert.strictEqual(result.deleted, 0);
            assert.strictEqual(deleteCalls(db).length, 0, 'no partial prune');
            assert.match(console.error.firstCall.args[0], /straddles block 950000/);
        });

        it('is a no-op on a read-only replica: the DELETEs arrive over replication', async function(){
            let db  = createDb({ hwm: 1000000, boundary: 950000 });
            let log = new TransparencyLog(db, 100, true, 50000);
            let result = await log.pruneSyncMeta();
            assert.strictEqual(result.skipped, true);
            assert.strictEqual(result.reason, 'read_only');
            assert.strictEqual(db.doQuery.called, false);
        });
    });

    describe('recordBlock wiring', function(){
        it('sweeps once per epoch boundary, after the boundary commit', async function(){
            let db  = createDb({ hwm: 1000000, boundary: 950000 });
            let log = new TransparencyLog(db, 100, false, 50000);
            let commit = sinon.stub(log, 'commitEpoch').resolves();
            let prune  = sinon.stub(log, 'pruneSyncMeta').resolves({ enabled: true, deleted: 0 });

            await log.recordBlock(1000, 1700000000, 'l', 'a', 'c');

            assert.strictEqual(prune.calledOnce, true);
            assert.ok(commit.calledBefore(prune), 'the closing epoch commits before anything is pruned');
        });

        it('does not sweep on a non-boundary block', async function(){
            let db  = createDb({ hwm: 1000000, boundary: 950000 });
            let log = new TransparencyLog(db, 100, false, 50000);
            let prune = sinon.stub(log, 'pruneSyncMeta').resolves({ enabled: true, deleted: 0 });
            await log.recordBlock(1001, 1700000000, 'l', 'a', 'c');
            assert.strictEqual(prune.called, false);
        });

        it('swallows a prune failure so the poll loop keeps recording blocks', async function(){
            let db  = createDb({ hwm: 1000000, boundary: 950000 });
            let log = new TransparencyLog(db, 100, false, 50000);
            sinon.stub(log, 'commitEpoch').resolves();
            sinon.stub(log, 'pruneSyncMeta').rejects(new Error('prune boom'));
            sinon.stub(console, 'error');

            await log.recordBlock(1000, 1700000000, 'l', 'a', 'c');

            assert.match(console.error.firstCall.args[0], /Error pruning sync_meta at epoch 10/);
        });
    });

    describe('getProof after retention', function(){
        it('reports not-available (null) for an epoch whose leaves were pruned', async function(){
            let db = { doQuery: sinon.stub().resolves([]) };
            db.doQuery.withArgs(sinon.match(/SELECT \* FROM merkle_epochs WHERE epoch/))
                .resolves([{ epoch: 1, start_block: 1, end_block: 100, merkle_root: 'r', leaf_count: 100 }]);
            db.doQuery.withArgs(sinon.match(/FROM sync_meta/)).resolves([]);
            let log = new TransparencyLog(db, 100, false, 50000);
            assert.strictEqual(await log.getProof(5), null);
        });

        it('refuses a proof rather than serving one from a partial leaf set', async function(){
            let db = { doQuery: sinon.stub().resolves([]) };
            db.doQuery.withArgs(sinon.match(/SELECT \* FROM merkle_epochs WHERE epoch/))
                .resolves([{ epoch: 1, start_block: 1, end_block: 100, merkle_root: 'r', leaf_count: 100 }]);
            db.doQuery.withArgs(sinon.match(/FROM sync_meta/)).resolves([
                { block_index: 1, ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c' },
                { block_index: 2, ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c' }
            ]);
            let log = new TransparencyLog(db, 100, false, 50000);
            assert.deepStrictEqual(await log.getProof(1), { error: 'epoch leaves incomplete' });
        });

        it('still serves a full epoch (leaf_count matches the surviving rows)', async function(){
            let MerkleTree = require('../../src/MerkleTree');
            let rows = [1, 2, 3, 4].map(i => ({
                block_index: i, ledger_hash: 'l' + i, actions_hash: 'a' + i, contract_hash: 'c' + i
            }));
            let leaves = rows.map(r => MerkleTree.computeLeaf(r.ledger_hash, r.actions_hash, r.contract_hash));
            let tree = MerkleTree.buildTree(leaves);

            let db = { doQuery: sinon.stub().resolves([]) };
            db.doQuery.withArgs(sinon.match(/SELECT \* FROM merkle_epochs WHERE epoch/))
                .resolves([{ epoch: 1, start_block: 1, end_block: 4, merkle_root: tree.root, leaf_count: 4 }]);
            db.doQuery.withArgs(sinon.match(/FROM sync_meta/)).resolves(rows);

            let log = new TransparencyLog(db, 100, false, 50000);
            let proof = await log.getProof(2);
            assert.strictEqual(proof.verified, true);
        });
    });
});
