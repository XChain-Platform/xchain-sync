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

function registerBootstrapFailureGatingEmptyReplicaDefectTests(){
    describe('bootstrap-failure gating (empty-replica defect)', function(){
        // The defect: a swallowed bootstrap failure let start() proceed into
        // live-follow with lastAppliedBlock=null, applying the first WS block onto an
        // empty DB with every continuity/fork/duplicate guard (all gated on
        // lastAppliedBlock !== null) disabled: risking a durable halt or silent block loss.

        it('start() refuses live-follow when bootstrap leaves the replica empty', async function(){
            db.getLastBlock.resolves(null);
            // Bootstrap returns without committing a tip, matching a swallowed failure.
            sinon.stub(sync, 'bootstrapFromSnapshot').resolves();
            let connect = sinon.stub(sync, 'connectWebSockets');

            await assert.rejects(() => sync.start(), /Refusing to enter live-follow/);
            assert.strictEqual(connect.called, false, 'must not open WebSockets onto an empty replica');
        });

        it('start() propagates a permanent bootstrap failure without live-following', async function(){
            db.getLastBlock.resolves(null);
            sinon.stub(sync, 'bootstrapFromSnapshot').rejects(new Error('all sync sources exhausted'));
            let connect = sinon.stub(sync, 'connectWebSockets');

            await assert.rejects(() => sync.start(), /all sync sources exhausted/);
            assert.strictEqual(connect.called, false);
        });

        it('bootstrapFromSnapshot rejects with BootstrapExhaustedError once all retry rounds exhaust', async function(){
            // The typed error is what lets the live WS event chain distinguish
            // permanent exhaustion (escalate to process.exit) from transient
            // handler errors (log and continue); pin the type at the throw site.
            config.BOOTSTRAP_MAX_RETRIES   = 0;
            config.BOOTSTRAP_RETRY_BASE_MS = 1;
            config.BOOTSTRAP_RETRY_MAX_MS  = 1;
            sinon.stub(sync, 'bootstrapRotateSources').resolves(false);

            await assert.rejects(() => sync.bootstrapFromSnapshot(),
                e => e instanceof ClientSync.BootstrapExhaustedError && /sources exhausted/.test(e.message));
        });

        it('handleBlock refuses to apply a non-genesis block onto an empty replica', async function(){
            sync.lastAppliedBlock = null;
            sinon.stub(sync, 'incrementalCatchUp').resolves();

            await sync.handleBlock(
                { type: 'block', block_index: 5, ledger_hash: 'l', actions_hash: 'a', contract_hash: 'c' }, 0);

            assert.strictEqual(applier.applyBlock.called, false, 'must not apply onto an empty DB');
            assert.strictEqual(sync.incrementalCatchUp.calledOnce, true);
        });
    });
}

describe('ClientSync', function(){
    registerClientSyncHooks(assignState);
    registerBootstrapFailureGatingEmptyReplicaDefectTests();
});
