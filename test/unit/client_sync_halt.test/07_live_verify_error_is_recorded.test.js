/*********************************************************************
 *
 * Copyright © 2025–2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC – https://dankest.llc
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This file is part of XChain Platform. Licensed under the GNU Affero
 * General Public License v3.0 or later; see LICENSE.md. A commercial
 * license (without AGPL source-disclosure terms) is available -
 * contact legal@dankest.llc.
 *
 **********************************************************************
 * ClientSync: a live recompute ERROR that advances the tip leaves a trace.
 *
 * The live consensus-hash recompute fails open on a local infrastructure fault
 * (the block advances, nothing halts), so the height is counted durably in
 * sync_state as unverified. The state_hash check is unchanged: its error still
 * holds the tip for redelivery, and a real mismatch on either still halts.
 ********************************************************************/

const assert = require('assert');
const sinon  = require('sinon');
const ClientSync = require('../../../src/client/sync');
const Utility = require('../../../src/util');
const HashVerifier = require('../../../src/client/hash_verifier');

function createMockDb(){
    const syncState = new Map();
    return {
        syncState,
        dbName: 'test_db',
        dbType: 'indexer',
        getLastBlock: sinon.stub().resolves(null),
        getBlockHashRow: sinon.stub().resolves(null),
        doQuery: sinon.stub().resolves([]),
        doQueryStrict: sinon.stub().resolves([]),
        getSyncState: sinon.stub().callsFake(async key => (syncState.has(key) ? syncState.get(key) : null)),
        setSyncState: sinon.stub().callsFake(async (key, value) => { syncState.set(key, value); }),
        recordHalt: sinon.stub().resolves({ block_index: 0 }),
        getActiveHalt: sinon.stub().resolves(null),
        clearHalt: sinon.stub().resolves(1)
    };
}

describe('ClientSync: a live recompute error is recorded durably @regression', function(){
    let sync, db, applier, config;

    function build(overrides){
        db = createMockDb();
        applier = { applyBlock: sinon.stub().resolves(), applyFullSnapshot: sinon.stub().resolves(), applyIncrementalSnapshot: sinon.stub().resolves() };
        config = Object.assign({ SYNC_SOURCES: 'http://a:3006', VERIFY_HASHES: true, HASH_CONFIRM_TIMEOUT: 5000,
            HALT_ON_DIVERGENCE: true, VERIFY_RECOMPUTE: false }, overrides);
        const util = new Utility();
        sync = new ClientSync('bitcoin', 'mainnet', db, applier, { rollback: sinon.stub().resolves() }, new HashVerifier(), config, util);
    }

    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
        sinon.stub(console, 'warn');
        build({});
    });
    afterEach(function(){ sinon.restore(); });

    it('a state_hash read error still holds the tip and records nothing', async function(){
        sync.blockHasher.computeStateHash = sinon.stub().rejects(new Error('transient DB error'));
        await sync.applyBlockEvent({ block_index: 200, block_time: 1, state_hash: 'SOURCE_STATE' });

        assert.strictEqual(sync.isHalted(), false);
        assert.strictEqual(sync.lastAppliedBlock, null, 'an unverified state_hash never advances the tip');
        assert.strictEqual(db.syncState.size, 0);
    });

    it('a clean recompute records nothing', async function(){
        build({ VERIFY_RECOMPUTE: true });
        sync.blockHasher.computeBlockHashes = sinon.stub().resolves({ ledger_hash: 'x', actions_hash: 'y', contract_hash: 'z' });
        await sync.applyBlockEvent({ block_index: 200, block_time: 1, ledger_hash: 'x', actions_hash: 'y', contract_hash: 'z' });

        assert.strictEqual(sync.lastAppliedBlock, 200);
        assert.strictEqual(db.syncState.size, 0);
    });

    it('a recompute error still advances when the durable record cannot be written', async function(){
        build({ VERIFY_RECOMPUTE: true });
        db.setSyncState = sinon.stub().rejects(new Error('sync_state down'));
        sync.blockHasher.computeBlockHashes = sinon.stub().rejects(new Error('transient DB error'));
        await sync.applyBlockEvent({ block_index: 200, block_time: 1, ledger_hash: 'x', actions_hash: 'y', contract_hash: 'z' });

        assert.strictEqual(sync.isHalted(), false);
        assert.strictEqual(sync.lastAppliedBlock, 200);
    });

    it('records a live recompute error durably and does not halt', async function(){
        build({ VERIFY_RECOMPUTE: true });
        // The production reader: doQuery swallows into [], doQueryStrict throws.
        db.doQueryStrict = sinon.stub().rejects(new Error('transient DB error'));
        await sync.applyBlockEvent({ block_index: 200, block_time: 1, ledger_hash: 'x', actions_hash: 'y', contract_hash: 'z' });

        assert.strictEqual(sync.isHalted(), false);
        assert.strictEqual(sync.lastAppliedBlock, 200);
        assert.strictEqual(db.syncState.get('unverified_recompute_count:indexer'), '1');
        assert.strictEqual(db.syncState.get('unverified_recompute_last_block:indexer'), '200');
    });
});
