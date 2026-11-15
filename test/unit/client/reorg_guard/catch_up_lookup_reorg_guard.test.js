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
const ClientSync = require('../../../../src/client/sync');

describe('ClientSync catch-up lookup paging across reorgs @regression', function(){
    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
        sinon.stub(console, 'warn');
    });
    afterEach(function(){ sinon.restore(); });

    it('refuses a lookup page after the catch-up window generation was replaced', async function(){
        const sync = Object.create(ClientSync.prototype);
        sync['_reorgGeneration'] = 2;
        sync.applier = { applyIncrementalSnapshot: sinon.stub().resolves() };

        const applied = await sync.applyLookupPage('index_addresses', [{ id: 7 }], 1, false,
            { reorgGeneration: 1 });

        assert.strictEqual(applied, false);
        assert.strictEqual(sync.applier.applyIncrementalSnapshot.called, false);
    });

    it('does not restore the stale window tip when a reorg lands after paging', async function(){
        const sync = Object.create(ClientSync.prototype);
        Object.assign(sync, {
            lastAppliedBlock: 100,
            _reorgGeneration: 0,
            _catchUpPending: false,
            _catchUpTipMoved: false,
            _upsertDupKeyTarget: null,
            db: { getLastBlock: sinon.stub().resolves(99) }
        });
        sync.incrementalCatchUpBlocked = sinon.stub().returns(false);
        sync.checkTrainActivation = sinon.stub().resolves(false);
        sync.rewindIfCatchUpForked = sinon.stub().resolves(false);
        sync.applyIncrementalCatchUpUnderLock = sinon.stub().resolves({
            status: 'applied', reorgGeneration: 0
        });
        sync.syncLookupTablesPaged = sinon.stub().callsFake(async (source, opts, guard) => {
            assert.deepStrictEqual(opts, { tip: 105 });
            assert.deepStrictEqual(guard, { reorgGeneration: 0 });
            sync['_reorgGeneration'] = 1;
            return true;
        });
        sync.refreshTipHashes = sinon.stub().resolves();

        const stopped = await sync.applyIncrementalCatchUpSnapshot('http://source',
            { block_height: 105, tables: {} }, 101, 100, true);

        assert.strictEqual(stopped, true);
        assert.strictEqual(sync.lastAppliedBlock, 100);
        assert.strictEqual(sync['_catchUpPending'], true);
        assert.strictEqual(sync['_catchUpTipMoved'], true);
        assert.strictEqual(sync.refreshTipHashes.called, false);
        assert.strictEqual(sync.db.getLastBlock.called, false,
            'generation mismatch stops before a stale tip can be finalized');
    });
});
