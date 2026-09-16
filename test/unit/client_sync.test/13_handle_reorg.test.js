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

function registerHandleReorgTests(){
    describe('handleReorg', function(){
        // A live reorg only ever arrives once the replica holds a committed tip (the WS
        // is opened after start()'s non-null guard), so these exercise the reachable
        // below-tip rollback path with a real tip.
        beforeEach(function(){ sync.lastAppliedBlock = 100; });

        it('calls rollback with the event block_index', async function(){
            let event = { type: 'reorg', chain: 'bitcoin', network: 'mainnet', block_index: 50 };
            await sync.handleReorg(event);
            assert.strictEqual(rollback.rollback.calledOnce, true);
            assert.strictEqual(rollback.rollback.firstCall.args[0], 50);
        });

        it('resets lastAppliedBlock to block_index - 1', async function(){
            db.getBlockHashRow.resolves({ ledger_hash: 'l49', actions_hash: 'a49', contract_hash: 'c49' });
            await sync.handleReorg({ block_index: 50 });
            assert.strictEqual(sync.lastAppliedBlock, 49);
        });

        it('loads new lastHashes from DB', async function(){
            let hashes = { ledger_hash: 'l49', actions_hash: 'a49', contract_hash: 'c49' };
            db.getBlockHashRow.resolves(hashes);
            await sync.handleReorg({ block_index: 50 });
            assert.strictEqual(sync.lastHashes, hashes);
        });

        it('sets lastHashes to null when rolling back to block 0', async function(){
            await sync.handleReorg({ block_index: 0 });
            assert.strictEqual(sync.lastHashes, null);
        });

        it('handles rollback error gracefully', async function(){
            rollback.rollback.rejects(new Error('rollback fail'));
            await sync.handleReorg({ block_index: 50 });
            assert.strictEqual(console.error.called, true);
        });

        // Defense-in-depth (2026-07-08 re-sweep): a reorg with NO committed tip must be
        // a no-op, never a cursor advance. With a null tip the DB is empty, so there is
        // nothing to roll back, and setting lastAppliedBlock = block_index - 1 from
        // server-supplied data would inflate the tip past an empty DB and wedge the
        // replica (the same shape as the above-tip case). Unreachable on the live path
        // but guarded so a future WS-ordering change cannot re-open it.
        it('null tip: ignores the reorg entirely (no rollback, no cursor advance)', async function(){
            sync.lastAppliedBlock = null;
            await sync.handleReorg({ block_index: 5000 });
            assert.strictEqual(rollback.rollback.called, false, 'nothing to roll back with no committed tip');
            assert.strictEqual(sync.lastAppliedBlock, null, 'the cursor must NOT be inflated from server data');
            assert.strictEqual(sync.isHalted(), false, 'a null-tip reorg is a benign no-op, not a halt');
        });
    });
}

describe('ClientSync', function(){
    registerClientSyncHooks(assignState);
    registerHandleReorgTests();
});
