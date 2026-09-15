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

function registerApplyBlockEventTests(){
    describe('applyBlockEvent', function(){
        it('calls applier.applyBlock', async function(){
            let event = { block_index: 10, ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c' };
            await sync.applyBlockEvent(event);
            assert.strictEqual(applier.applyBlock.calledOnce, true);
            assert.strictEqual(applier.applyBlock.firstCall.args[0], event);
        });

        it('updates lastAppliedBlock and lastHashes', async function(){
            await sync.applyBlockEvent({ block_index: 10, ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c' });
            assert.strictEqual(sync.lastAppliedBlock, 10);
            assert.strictEqual(sync.lastHashes.ledger_hash, 'l');
        });

        it('cleans up old pendingHashes entries', async function(){
            sync.pendingHashes.set(5, { 0: {} });
            sync.pendingHashes.set(15, { 0: {} });
            await sync.applyBlockEvent({ block_index: 10, ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c' });
            assert.strictEqual(sync.pendingHashes.has(5), false);
            assert.strictEqual(sync.pendingHashes.has(15), true);
        });

        it('handles apply error gracefully', async function(){
            applier.applyBlock.rejects(new Error('apply fail'));
            await sync.applyBlockEvent({ block_index: 10, ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c' });
            // Should not throw; error is caught and logged
            assert.strictEqual(console.error.called, true);
        });

        it('re-applies the source schema when the apply hits a missing table', async function(){
            applier.applyBlock.rejects(Object.assign(new Error('no table'), { errno: 1146 }));
            let heal = sinon.stub(sync, 'fetchAndApplySchema').resolves();
            await sync.applyBlockEvent({ block_index: 10, ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c' });
            assert.strictEqual(heal.calledOnce, true);
            assert.strictEqual(heal.firstCall.args[0], 'http://source1:3006');
        });
    });
}

describe('ClientSync', function(){
    registerClientSyncHooks(assignState);
    registerApplyBlockEventTests();
});
