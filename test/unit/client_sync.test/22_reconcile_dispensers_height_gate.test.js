// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

const assert = require('assert');
const sinon = require('sinon');
const axios = require('axios');
const ClientSync = require('../../../src/client/sync');
const { SCHEMA_VERSION } = require('../../../src/schema/version');

function pinnedBody(height, hash){
    return Buffer.from(JSON.stringify({ schema_version: SCHEMA_VERSION.decoder,
        block_height: height, block_hash: hash, has_more: false,
        rows: [{ tx_index: 1, address_id: 2 }] }));
}

function context(overrides){
    let ctx = {
        dbType: 'decoder', chain: 'bitcoin', network: 'mainnet',
        config: { SNAPSHOT_MAX_CONTENT: 1024 * 1024 },
        lastAppliedBlock: 100, lastHashes: { block_hash: 'hash100' },
        db: { getBlockHashRow: sinon.stub().resolves({ block_hash: 'hash100' }) },
        applier: { applyDispensersReplace: sinon.stub().resolves() },
        withApplyLock: async fn => fn(), healSchemaIfStale: sinon.stub().resolves()
    };
    return Object.assign(ctx, overrides || {});
}

async function reconcile(ctx){
    return ClientSync.prototype.reconcileDispensers.call(ctx, 'http://source:3006');
}

describe('ClientSync reconcileDispensers source block gate', function(){
    beforeEach(function(){ sinon.stub(console, 'log'); sinon.stub(console, 'error'); });
    afterEach(function(){ sinon.restore(); });

    it('skips and re-arms when the source height is ahead', async function(){
        sinon.stub(axios, 'get').resolves({ data: pinnedBody(101, 'hash101') });
        let ctx = context();
        assert.strictEqual(await reconcile(ctx), false);
        assert.strictEqual(ctx.applier.applyDispensersReplace.called, false);
        assert.strictEqual(ctx._dispenserReconcilePending, true);
        assert.strictEqual(ctx._lastDispenserReconcileAttemptAt, undefined);
    });

    it('skips an equal-height dump from another branch', async function(){
        sinon.stub(axios, 'get').resolves({ data: pinnedBody(100, 'other-hash') });
        let ctx = context();
        assert.strictEqual(await reconcile(ctx), false);
        assert.strictEqual(ctx.applier.applyDispensersReplace.called, false);
        assert.strictEqual(ctx._dispenserReconcilePending, true);
    });

    it('rechecks the replica height under the apply lock', async function(){
        sinon.stub(axios, 'get').resolves({ data: pinnedBody(100, 'hash100') });
        let ctx = context({ withApplyLock: async fn => { ctx.lastAppliedBlock = 101; return fn(); } });
        assert.strictEqual(await reconcile(ctx), false);
        assert.strictEqual(ctx.applier.applyDispensersReplace.called, false);
        assert.strictEqual(ctx._dispenserReconcilePending, true);
    });

    it('replaces only at the same height and hash, then clears pending', async function(){
        sinon.stub(axios, 'get').resolves({ data: pinnedBody(100, 'hash100') });
        let ctx = context({ _dispenserReconcilePending: true });
        assert.strictEqual(await reconcile(ctx), true);
        assert.strictEqual(ctx.applier.applyDispensersReplace.calledOnce, true);
        assert.strictEqual(ctx.db.getBlockHashRow.calledWith(100), true);
        assert.strictEqual(ctx._dispenserReconcilePending, false);
    });

    it('rejects pages that disagree on source block identity', async function(){
        let first = JSON.parse(pinnedBody(100, 'hash100').toString());
        first.has_more = true;
        first.max_tx = 1;
        first.max_addr = 2;
        sinon.stub(axios, 'get')
            .onFirstCall().resolves({ data: Buffer.from(JSON.stringify(first)) })
            .onSecondCall().resolves({ data: pinnedBody(101, 'hash101') });
        let ctx = context();
        assert.strictEqual(await reconcile(ctx), false);
        assert.strictEqual(ctx.applier.applyDispensersReplace.called, false);
        assert.strictEqual(ctx._dispenserReconcilePending, true);
    });
});
