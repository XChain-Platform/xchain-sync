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
} = require('./support');

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

describe('ClientSync', function(){
    registerClientSyncHooks(assignState);
    registerRunIncrementalCatchUpSchemaSelfHealTests();
});
