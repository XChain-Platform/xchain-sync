// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact the copyright holder.

const assert = require('assert');
const sinon = require('sinon');
const { buildClient, hashesAt, KEY } = require('../../../helpers/verified_tip_client');

function block(height, suffix){
    const hashes = suffix ? {
        ledger_hash: 'L' + suffix,
        actions_hash: 'A' + suffix,
        contract_hash: 'C' + suffix
    } : hashesAt(height);
    return Object.assign({ type: 'block', block_index: height, block_time: 1 }, hashes);
}

function heldClient(){
    const ctx = buildClient({ tip: 199 });
    ctx.state.set(KEY, '199');
    ctx.sync.lastAppliedBlock = 199;
    ctx.sync.haltOnDivergence = sinon.stub().resolves();
    ctx.sync.blockHasher.computeBlockHashes.onFirstCall().rejects(new Error('transient read'));
    ctx.applier.applyBlock = sinon.stub().callsFake(async () => { ctx.db.tip = 200; return true; });
    ctx.rollback.rollback = sinon.stub().callsFake(async height => { ctx.db.tip = height - 1; });
    return ctx;
}

async function holdTip(ctx){
    await ctx.sync.applyBlockEvent(block(200));
    assert.strictEqual(ctx.db.tip, 200);
    assert.strictEqual(ctx.sync.lastAppliedBlock, 199);
}

function registerRollbackAtTipTests(){
    it('rolls back a direct reorg at the stored tip and applies its replacement', async function(){
        const ctx = heldClient();
        await holdTip(ctx);
        const generation = ctx.sync._reorgGeneration;

        await ctx.sync.handleReorg({ type: 'reorg', block_index: 200 });

        assert.strictEqual(ctx.rollback.rollback.calledOnceWith(200), true);
        assert.strictEqual(ctx.sync._reorgGeneration, generation + 1);
        assert.strictEqual(ctx.sync.rollbackGuard.peak, 200);
        const replacement = block(200, 'replacement');
        ctx.sync.blockHasher.computeBlockHashes.onSecondCall().resolves({
            ledger_hash: replacement.ledger_hash,
            actions_hash: replacement.actions_hash,
            contract_hash: replacement.contract_hash
        });
        await ctx.sync.applyBlockEvent(replacement);
        assert.strictEqual(ctx.sync.lastAppliedBlock, 200);
        assert.strictEqual(ctx.sync.haltOnDivergence.called, false);
    });

    it('rolls back a one-source reorg at the stored tip', async function(){
        const ctx = heldClient();
        await holdTip(ctx);

        await ctx.sync.handleSourceReorg({ type: 'reorg', block_index: 200 }, 0);

        assert.strictEqual(ctx.rollback.rollback.calledOnceWith(200), true);
        assert.strictEqual(ctx.sync.lastAppliedBlock, 199);
    });
}

function registerHeldTipFailureTests(){
    it('halts instead of dropping a direct reorg when the stored tip read fails', async function(){
        const ctx = heldClient();
        await holdTip(ctx);
        ctx.db.getLastBlock = sinon.stub().rejects(new Error('tip read fault'));
        const generation = ctx.sync._reorgGeneration;

        await ctx.sync.handleReorg({ type: 'reorg', block_index: 200 });

        assert.strictEqual(ctx.rollback.rollback.called, false);
        assert.strictEqual(ctx.sync._reorgGeneration, generation);
        assert.strictEqual(ctx.sync.haltOnDivergence.calledOnce, true);
        assert.strictEqual(ctx.sync.haltOnDivergence.firstCall.args[3], 'reorg-rollback-failed');
    });

    it('halts instead of dropping a source reorg when the stored tip read fails', async function(){
        const ctx = heldClient();
        await holdTip(ctx);
        ctx.db.getLastBlock = sinon.stub().rejects(new Error('tip read fault'));

        await ctx.sync.handleSourceReorg({ type: 'reorg', block_index: 200 }, 0);

        assert.strictEqual(ctx.rollback.rollback.called, false);
        assert.strictEqual(ctx.sync.haltOnDivergence.calledOnce, true);
        assert.strictEqual(ctx.sync.haltOnDivergence.firstCall.args[3], 'reorg-rollback-failed');
    });

    it('still ignores a reorg above both the cursor and stored tip', async function(){
        const ctx = heldClient();
        await holdTip(ctx);

        await ctx.sync.handleReorg({ type: 'reorg', block_index: 201 });

        assert.strictEqual(ctx.rollback.rollback.called, false);
        assert.strictEqual(ctx.sync.lastAppliedBlock, 199);
        assert.strictEqual(ctx.db.tip, 200);
    });
}

describe('ClientSync reorg at a committed but held tip @regression', function(){
    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
        sinon.stub(console, 'warn');
    });
    afterEach(function(){ sinon.restore(); });

    registerRollbackAtTipTests();
    registerHeldTipFailureTests();
});
