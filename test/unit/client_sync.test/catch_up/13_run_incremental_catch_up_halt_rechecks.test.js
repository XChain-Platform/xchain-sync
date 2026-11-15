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
    assert, sinon, axios, registerClientSyncHooks
} = require('../support');

let sync, db, applier;

function assignState(state){
    ({ sync, db, applier } = state);
}

// Schema self-heal: a halt or strict hold raised while healing blocks the retry download.
function schemaHealHoldCases(){
    it('does not retry after schema healing halts the client @regression', async function(){
        db.getLastBlock.resolves(5);
        let snapshot = { schema_version: 'x', block_height: 9, tables: {} };
        let gz = require('zlib').gzipSync(JSON.stringify(snapshot));
        sinon.stub(axios, 'get').resolves({ data: gz });
        applier.applyIncrementalSnapshot.onFirstCall()
            .rejects(Object.assign(new Error('no table'), { errno: 1146 }));
        sinon.stub(sync, 'fetchAndApplySchema').callsFake(async function(){
            sync._halted = { blockIndex: 5, reason: 'schema-apply-failed' };
        });

        await sync.runIncrementalCatchUp();

        assert.strictEqual(axios.get.callCount, 1, 'a durable halt must prevent a retry download');
        assert.strictEqual(applier.applyIncrementalSnapshot.callCount, 1);
        assert.notStrictEqual(sync.lastAppliedBlock, 9);
    });

    it('does not retry after schema healing records a strict hold @regression', async function(){
        db.getLastBlock.resolves(5);
        let snapshot = { schema_version: 'x', block_height: 9, tables: {} };
        let gz = require('zlib').gzipSync(JSON.stringify(snapshot));
        sinon.stub(axios, 'get').resolves({ data: gz });
        applier.applyIncrementalSnapshot.onFirstCall()
            .rejects(Object.assign(new Error('no table'), { errno: 1146 }));
        sinon.stub(sync, 'fetchAndApplySchema').callsFake(async function(){
            sync._strictConfirmPending.add(7);
        });

        await sync.runIncrementalCatchUp();

        assert.strictEqual(axios.get.callCount, 1, 'a strict hold must prevent a retry download');
        assert.strictEqual(applier.applyIncrementalSnapshot.callCount, 1);
        assert.notStrictEqual(sync.lastAppliedBlock, 9);
    });
}

// Halts and holds raised mid-download or under the lock are re-checked before the snapshot applies.
function midDownloadRecheckCases(){
    it('re-checks a mid-download halt before applying the snapshot @regression', async function(){
        db.getLastBlock.resolves(5);
        let snapshot = { schema_version: 'x', block_height: 9, tables: {} };
        sinon.stub(axios, 'get').callsFake(async function(){
            sync._halted = { blockIndex: 6, reason: 'no-source-quorum' };
            return { data: Buffer.from(JSON.stringify(snapshot)) };
        });

        await sync.runIncrementalCatchUp();

        assert.strictEqual(applier.applyIncrementalSnapshot.called, false);
        assert.notStrictEqual(sync.lastAppliedBlock, 9);
    });

    it('re-checks a mid-download strict hold before applying the snapshot @regression', async function(){
        db.getLastBlock.resolves(5);
        let snapshot = { schema_version: 'x', block_height: 9, tables: {} };
        sinon.stub(axios, 'get').callsFake(async function(){
            sync._strictReorgPending.add(6);
            return { data: Buffer.from(JSON.stringify(snapshot)) };
        });

        await sync.runIncrementalCatchUp();

        assert.strictEqual(applier.applyIncrementalSnapshot.called, false);
        assert.notStrictEqual(sync.lastAppliedBlock, 9);
    });

    it('re-checks a halt raised by the under-lock tip read @regression', async function(){
        db.getLastBlock.resolves(5);
        db.getLastBlock.onSecondCall().callsFake(async function(){
            sync['_halted'] = { blockIndex: 6, reason: 'no-source-quorum' };
            return 5;
        });
        const snapshot = { schema_version: 'x', block_height: 9, tables: {} };
        sinon.stub(axios, 'get').resolves({ data: Buffer.from(JSON.stringify(snapshot)) });

        await sync.runIncrementalCatchUp();

        assert.strictEqual(applier.applyIncrementalSnapshot.called, false);
        assert.notStrictEqual(sync.lastAppliedBlock, 9);
    });
}

function registerRunIncrementalCatchUpHaltRecheckTests(){
    describe('runIncrementalCatchUp schema self-heal', function(){
        schemaHealHoldCases();

        midDownloadRecheckCases();
    });
}

describe('ClientSync', function(){
    registerClientSyncHooks(assignState);
    registerRunIncrementalCatchUpHaltRecheckTests();
});
