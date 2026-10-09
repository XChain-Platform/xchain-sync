// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

'use strict';

const assert = require('assert');
const sinon = require('sinon');
const ClientSync = require('../../../src/client/sync');
const Utility = require('../../../src/util');
const HashVerifier = require('../../../src/client/hash_verifier');

function makeDb(syncState){
    return {
        dbName: 'test_db',
        dbType: 'indexer',
        getLastBlock: sinon.stub().resolves(null),
        getBlockHashRow: sinon.stub().resolves(null),
        getActiveHalt: sinon.stub().resolves(null),
        recordHalt: sinon.stub().resolves({ block_index: 0 }),
        clearHalt: sinon.stub().resolves(1),
        getSyncState: sinon.stub().callsFake(async key =>
            syncState.has(key) ? syncState.get(key) : null),
        setSyncState: sinon.stub().callsFake(async (key, value) => {
            syncState.set(key, value);
            return true;
        }),
        deleteSyncState: sinon.stub().callsFake(async key => {
            syncState.delete(key);
            return true;
        }),
        doQuery: sinon.stub().resolves([])
    };
}

function makeSync(db){
    const applier = {
        applyBlock: sinon.stub().resolves(),
        applyFullSnapshot: sinon.stub().resolves(),
        applyIncrementalSnapshot: sinon.stub().resolves()
    };
    const rollback = { rollback: sinon.stub().resolves() };
    const config = {
        SYNC_SOURCES: 'http://source:3006',
        MAX_ROLLBACK_DEPTH: 5,
        MAX_ROLLBACK_DEPTH_EXPLICIT: true,
        VERIFY_RECOMPUTE: false
    };
    const sync = new ClientSync('bitcoin', 'mainnet', db, applier, rollback,
        new HashVerifier(), config, new Utility());
    return { sync, applier, rollback };
}

describe('ClientSync rollback guard persistence', function(){
    let errorStub;

    beforeEach(function(){
        sinon.stub(console, 'log');
        errorStub = sinon.stub(console, 'error');
    });

    afterEach(function(){ sinon.restore(); });

    it('halts when a split rollback exceeds the limit across a restart', async function(){
        const durableState = new Map();
        const db = makeDb(durableState);
        const first = makeSync(db);
        first.sync.lastAppliedBlock = 100;

        await first.sync.handleReorg({ type: 'reorg', block_index: 98 });

        assert.strictEqual(first.rollback.rollback.calledOnceWith(98), true);
        assert.deepStrictEqual(JSON.parse(durableState.get('rollback_guard:indexer')),
            { peak: 100, low: 98 });
        assert.ok(db.setSyncState.calledBefore(first.rollback.rollback),
            'guard state must be durable before rollback begins');

        const second = makeSync(db);
        sinon.stub(second.sync, 'synchronizeStoredReplica').callsFake(async () => {
            second.sync.lastAppliedBlock = 97;
        });
        sinon.stub(second.sync, 'prepareLiveFollow').resolves();
        sinon.stub(second.sync, 'beginLiveFollow').callsFake(() => {
            second.sync.running = false;
        });

        await second.sync.start();
        await second.sync.handleReorg({ type: 'reorg', block_index: 95 });

        assert.strictEqual(second.rollback.rollback.called, false,
            'the second shallow rollback must include the pre-restart streak');
        assert.strictEqual(second.sync.getHaltInfo().reason, 'max-rollback-depth-exceeded');
        assert.strictEqual(second.sync.getHaltInfo().mismatches[0].depth, 6);
    });

    it('treats a rollback guard read failure as an empty streak', async function(){
        const db = makeDb(new Map());
        db.getSyncState.rejects(new Error('sync_state unavailable'));
        const { sync } = makeSync(db);

        await sync.loadRollbackGuardState();

        assert.deepStrictEqual(sync.rollbackGuardState(), { peak: null, low: null });
        assert.strictEqual(errorStub.called, true);
    });

    it('resets the guard after a full snapshot is applied', async function(){
        const durableState = new Map([
            ['rollback_guard:indexer', JSON.stringify({ peak: 100, low: 98 })]
        ]);
        const db = makeDb(durableState);
        const { sync } = makeSync(db);
        await sync.loadRollbackGuardState();
        sinon.stub(sync, 'checkTrainActivation').resolves(false);
        sinon.stub(sync, 'refreshTipHashes').resolves();
        sinon.stub(sync, 'clearBootstrapBase').resolves();

        assert.strictEqual(await sync.bootstrapApplySnapshot({ block_height: 200 }), true);

        assert.deepStrictEqual(sync.rollbackGuardState(), { peak: null, low: null });
        assert.strictEqual(durableState.has('rollback_guard:indexer'), false);
    });

    it('resets the guard when an operator clears a max-depth halt', async function(){
        const durableState = new Map([
            ['rollback_guard:indexer', JSON.stringify({ peak: 100, low: 98 })]
        ]);
        const db = makeDb(durableState);
        const { sync } = makeSync(db);
        await sync.loadRollbackGuardState();
        await sync.haltOnDivergence(95, [], [], 'max-rollback-depth-exceeded');

        await sync.clearHalt();

        assert.deepStrictEqual(sync.rollbackGuardState(), { peak: null, low: null });
        assert.strictEqual(durableState.has('rollback_guard:indexer'), false);
        assert.strictEqual(db.clearHalt.calledOnceWith('indexer'), true);
    });
});
