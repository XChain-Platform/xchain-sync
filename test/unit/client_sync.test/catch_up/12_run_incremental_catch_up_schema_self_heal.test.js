// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

const {
    assert, sinon, axios, ClientSync, createMockDb, registerClientSyncHooks
} = require('../support');

let sync, db, applier, rollback, hashVerifier, config, util;

function assignState(state){
    ({ sync, db, applier, rollback, hashVerifier, config, util } = state);
}

function registerRunIncrementalCatchUpSchemaSelfHealTests(){
    describe('runIncrementalCatchUp schema self-heal', function(){
        it('heals and retries ONCE when the catch-up apply hits a missing table', async function(){
            db.getLastBlock.resolves(5);
            let snapshot = { schema_version: 'x', block_height: 9, tables: {} };
            let gz = require('zlib').gzipSync(JSON.stringify(snapshot));
            sinon.stub(axios, 'get').resolves({ data: gz });
            // First apply fails on the schema gap, the post-heal retry succeeds.
            applier.applyIncrementalSnapshot
                .onFirstCall().rejects(Object.assign(new Error('no table'), { errno: 1146 }))
                .onSecondCall().resolves();
            let heal = sinon.stub(sync, 'fetchAndApplySchema').resolves();

            await sync.runIncrementalCatchUp();

            assert.strictEqual(heal.calledOnce, true);
            assert.strictEqual(applier.applyIncrementalSnapshot.callCount, 2);
            assert.strictEqual(sync.lastAppliedBlock, 9, 'retry applied the snapshot');
        });

        it('does not retry when the retry would hit the heal debounce', async function(){
            db.getLastBlock.resolves(5);
            let snapshot = { schema_version: 'x', block_height: 9, tables: {} };
            let gz = require('zlib').gzipSync(JSON.stringify(snapshot));
            sinon.stub(axios, 'get').resolves({ data: gz });
            // Persistent schema-gap failure (e.g. the DDL was rejected): the
            // first failure heals + retries, the second failure is debounced:
            // exactly two apply attempts, no spin.
            applier.applyIncrementalSnapshot.rejects(Object.assign(new Error('no table'), { errno: 1146 }));
            sinon.stub(sync, 'fetchAndApplySchema').resolves();

            await sync.runIncrementalCatchUp();

            assert.strictEqual(applier.applyIncrementalSnapshot.callCount, 2);
        });
    });
}

// A decoder catch-up window whose first block names a parent other than the committed
// tip is a reorg missed while disconnected: rewind the tip, never land the window on it.
function registerRunIncrementalCatchUpDecoderForkTests(){
    describe('runIncrementalCatchUp decoder join-block link', function(){
        function decoderCatchUp(parentHash){
            let decoderDb = createMockDb();
            decoderDb.dbType = 'decoder';
            decoderDb.getLastBlock.resolves(100);
            decoderDb.getBlockHashRow.resolves({ block_index: 100, block_hash: 'hash100' });
            let s = new ClientSync('bitcoin', 'mainnet', decoderDb, applier, rollback, hashVerifier, config, util);
            s.lastAppliedBlock = 100;
            s.lastHashes = { block_hash: 'hash100' };
            sinon.stub(s, 'verifyDecoderCompleteness').resolves();
            sinon.stub(s, 'shouldReconcileDispensers').returns(false);
            let snapshot = { schema_version: 'x', since_block: 101, block_height: 103, tables: {
                blocks: [{ block_index: 101, block_hash_id: 8, previous_block_hash_id: 7 }],
                index_transactions: [{ id: 7, hash: parentHash }] } };
            sinon.stub(axios, 'get').resolves({ data: require('zlib').gzipSync(JSON.stringify(snapshot)) });
            return s;
        }

        it('rewinds the tip and flags a re-run instead of applying a window built on another parent', async function(){
            let s = decoderCatchUp('FORKED100');

            await s.runIncrementalCatchUp();

            assert.strictEqual(applier.applyIncrementalSnapshot.called, false, 'the window must not land on the orphan');
            assert.strictEqual(rollback.rollback.calledOnceWith(100), true, 'the orphaned tip is unwound one block deep');
            assert.strictEqual(s.lastAppliedBlock, 99);
            assert.strictEqual(s._catchUpPending, true, 'the coalescing loop re-runs from the rewound tip');
        });

        it('applies a window whose first block builds on the committed tip', async function(){
            let s = decoderCatchUp('hash100');

            await s.runIncrementalCatchUp();

            assert.strictEqual(rollback.rollback.called, false);
            assert.strictEqual(applier.applyIncrementalSnapshot.calledOnce, true);
            assert.strictEqual(s.lastAppliedBlock, 103);
        });
    });
}

describe('ClientSync', function(){
    registerClientSyncHooks(assignState);
    registerRunIncrementalCatchUpSchemaSelfHealTests();
    registerRunIncrementalCatchUpDecoderForkTests();
});
