// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later

const assert = require('assert');
const sinon = require('sinon');
const ClientSync = require('../../../../src/client/sync');
const Utility = require('../../../../src/util');
const HashVerifier = require('../../../../src/client/hash_verifier');

function makeSync(configOverrides){
    const db = {
        dbName: 'test_db', dbType: 'indexer',
        getLastBlock: sinon.stub().resolves(50),
        getBlockHashRow: sinon.stub().resolves({ block_index: 50 }),
        getActiveHalt: sinon.stub().resolves(null),
        doQuery: sinon.stub().resolves([]),
        setSyncState: sinon.stub().resolves(true),
        deleteSyncState: sinon.stub().resolves(true)
    };
    const applier = {
        applyBlock: sinon.stub().resolves(),
        applyFullSnapshot: sinon.stub().resolves(),
        applyIncrementalSnapshot: sinon.stub().resolves()
    };
    const config = Object.assign({
        SYNC_SOURCES: 'http://a:3006', VERIFY_HASHES: false,
        BOOTSTRAP_MAX_RETRIES: 3, BOOTSTRAP_RETRY_BASE_MS: 1,
        BOOTSTRAP_RETRY_MAX_MS: 1, HASH_CONFIRM_TIMEOUT: 5000,
        SNAPSHOT_MAX_CONTENT: 1024, MAX_ROLLBACK_DEPTH: 10
    }, configOverrides || {});
    const sync = new ClientSync('bitcoin', 'mainnet', db, applier,
        { rollback: sinon.stub().resolves() }, new HashVerifier(), config, new Utility());
    sync.lastAppliedBlock = 50;
    return { sync, db, applier };
}

function quietConsole(){
    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'warn');
        sinon.stub(console, 'error');
    });
    afterEach(function(){ sinon.restore(); });
}

describe('ClientSync full snapshot lifecycle guard @regression', function(){
    quietConsole();

    async function assertFullSnapshotHold(pendingSet){
        const { sync, applier } = makeSync();
        sinon.stub(sync, 'fetchAndApplySchema').resolves();
        const download = sinon.stub(sync, 'bootstrapDownloadSnapshot').callsFake(async () => {
            sync[pendingSet].add(77);
            return { block_height: 200, tables: {} };
        });
        const sleep = sinon.stub(sync.util, 'sleep').resolves();
        const adopt = sinon.spy(sync, 'adoptVerifiedTip');

        assert.strictEqual(await sync.bootstrapFromSnapshot(), 'blocked');
        assert.strictEqual(download.callCount, 1, 'a strict hold must not retry another snapshot');
        assert.strictEqual(sleep.called, false, 'a strict hold must not enter retry backoff');
        assert.strictEqual(applier.applyFullSnapshot.called, false);
        assert.strictEqual(sync.lastAppliedBlock, 50);
        assert.strictEqual(adopt.called, false, 'a refused snapshot must not move the verified tip');
    }

    it('refuses a full snapshot when confirmation becomes pending during download', async function(){
        await assertFullSnapshotHold('_strictConfirmPending');
    });

    it('refuses a full snapshot when reorg confirmation becomes pending during download', async function(){
        await assertFullSnapshotHold('_strictReorgPending');
    });

    it('preserves rows when a durable halt appears during full snapshot download', async function(){
        const { sync, applier } = makeSync();
        sinon.stub(sync, 'fetchAndApplySchema').resolves();
        sinon.stub(sync, 'bootstrapDownloadSnapshot').callsFake(async () => {
            sync._halted = { blockIndex: 49, reason: 'test-halt' };
            return { block_height: 200, tables: {} };
        });

        await assert.rejects(() => sync.bootstrapFromSnapshot(), /aborted by test-halt/);

        assert.strictEqual(applier.applyFullSnapshot.called, false);
        assert.strictEqual(sync.lastAppliedBlock, 50);
    });
});

function initializeVerifiedPosture(sync){
    sync._bootstrapBase = 10;
    sync._verifiedTip = 40;
    sync._persistedVerifiedTip = 40;
}

// The forward window base write lands, then the hold appears; the restore write
// fails the way the fail-soft db helper reports it (false) or the strict one does (throw).
function heldWindowSync(restoreFailure){
    const ctx = makeSync({ VERIFY_RECOMPUTE: true });
    const { sync, db } = ctx;
    initializeVerifiedPosture(sync);
    db.setSyncState = sinon.stub().callsFake(async (key, value) => {
        if(key === 'bootstrap_base:indexer' && value === '100'){
            sync._strictConfirmPending.add(77);
            return true;
        }
        if(key === 'bootstrap_base:indexer') return restoreFailure();
        return true;
    });
    return ctx;
}

function heldWindow(sync){
    return sync.applyBootstrapWindow({ block_height: 200, since_block: 100, tables: {} }, 100, 200);
}

describe('ClientSync bootstrap window lifecycle guard @regression', function(){
    quietConsole();

    it('leaves bootstrap markers unchanged when a height window is held', async function(){
        const { sync, applier } = makeSync();
        sync._bootstrapBase = 10;
        sinon.stub(sync, 'discoverBootstrapRange').resolves({ tip: 200, base: 100 });
        sinon.stub(sync, 'fetchAndApplySchema').resolves();
        sinon.stub(sync, 'syncLookupTablesPaged').resolves();
        sinon.stub(sync, 'fetchBootstrapWindow').callsFake(async () => {
            sync._strictConfirmPending.add(77);
            return { block_height: 200, since_block: 100, tables: {} };
        });
        const persist = sinon.spy(sync, 'persistBootstrapBase');
        const adopt = sinon.spy(sync, 'adoptVerifiedTip');

        assert.strictEqual(await sync.bootstrapFromHeightRetry(100), 'blocked');
        assert.strictEqual(applier.applyIncrementalSnapshot.called, false);
        assert.strictEqual(sync._bootstrapBase, 10);
        assert.strictEqual(sync.lastAppliedBlock, 50);
        assert.strictEqual(persist.called, false);
        assert.strictEqual(adopt.called, false);
    });

    it('does not enter either size fallback while strict confirmation is pending', async function(){
        const error = new Error('maxContentLength size exceeded');
        error.code = 'ERR_FR_MAX_CONTENT_LENGTH_EXCEEDED';
        const full = makeSync();
        full.sync._strictConfirmPending.add(77);
        const fullFallback = sinon.stub(full.sync, 'bootstrapFromSnapshot').resolves();
        await full.sync.handleIncrementalCatchUpFailure('http://a:3006', 101, error, 100);

        const bounded = makeSync({ SYNC_BOOTSTRAP_DEPTH: { 'BTC:MAINNET': 100 } });
        bounded.sync._strictReorgPending.add(88);
        const boundedFallback = sinon.stub(bounded.sync, 'bootstrapFromHeightRetry').resolves();
        await bounded.sync.handleIncrementalCatchUpFailure('http://a:3006', 1, error, null);

        assert.strictEqual(fullFallback.called, false);
        assert.strictEqual(boundedFallback.called, false);
    });
});

describe('ClientSync bootstrap floor durability @regression', function(){
    quietConsole();

    async function assertForwardFloorFailure(failure){
        const { sync, db, applier } = makeSync({ VERIFY_RECOMPUTE: true });
        initializeVerifiedPosture(sync);
        db.setSyncState = sinon.stub().callsFake(async (key, value) => {
            if(key === 'bootstrap_base:indexer' && value === '100'){
                if(failure === 'throw') throw new Error('floor write lost');
                return false;
            }
            return true;
        });

        await assert.rejects(() => heldWindow(sync),
            failure === 'throw' ? /floor write lost/ : /bootstrap base write failed/);
        assert.strictEqual(applier.applyIncrementalSnapshot.called, false);
        assert.strictEqual(db.setSyncState.calledWith('verified_tip:indexer', '100'), false);
        assert.strictEqual(db.setSyncState.calledWith('bootstrap_base:indexer', '10'), true);
        assert.strictEqual(sync._bootstrapBase, 10);
        assert.strictEqual(sync._verifiedTip, 40);
    }

    it('undoes posture when the forward floor write reports failure', async function(){
        await assertForwardFloorFailure('false');
    });

    it('undoes posture when the forward floor write throws', async function(){
        await assertForwardFloorFailure('throw');
    });

    it('deletes an uncertain first floor write before failing the round', async function(){
        const { sync, db, applier } = makeSync({ VERIFY_RECOMPUTE: true });
        sync._bootstrapBase = null;
        db.setSyncState = sinon.stub().rejects(new Error('floor write lost'));

        await assert.rejects(() => heldWindow(sync), /floor write lost/);
        assert.strictEqual(applier.applyIncrementalSnapshot.called, false);
        assert.strictEqual(db.deleteSyncState.calledWith('bootstrap_base:indexer'), true);
        assert.strictEqual(sync._bootstrapBase, null);
    });
});

describe('ClientSync bootstrap window lifecycle guard @regression', function(){
    quietConsole();

    it('restores bootstrap posture when confirmation becomes pending during base persistence', async function(){
        const { sync, applier } = makeSync({ VERIFY_RECOMPUTE: true });
        initializeVerifiedPosture(sync);
        const persistBase = sync.persistBootstrapBase.bind(sync);
        const persist = sinon.stub(sync, 'persistBootstrapBase').callsFake(async (base, opts) => {
            await persistBase(base, opts);
            if(base === 100) sync._strictConfirmPending.add(77);
        });

        const result = await sync.applyBootstrapWindow(
            { block_height: 200, since_block: 100, tables: {} }, 100, 200);

        assert.strictEqual(result, 'blocked');
        assert.strictEqual(applier.applyIncrementalSnapshot.called, false);
        assert.deepStrictEqual(persist.args.map(args => args[0]), [100, 10]);
        assert.strictEqual(sync._bootstrapBase, 10);
        assert.strictEqual(sync._verifiedTip, 40);
        assert.strictEqual(sync._persistedVerifiedTip, 40);
    });

    it('restores bootstrap posture when confirmation becomes pending during marker adoption', async function(){
        const { sync, applier } = makeSync({ VERIFY_RECOMPUTE: true });
        initializeVerifiedPosture(sync);
        const adoptTip = sync.adoptVerifiedTip.bind(sync);
        sinon.stub(sync, 'adoptVerifiedTip').callsFake(async (height, opts) => {
            await adoptTip(height, opts);
            sync._strictReorgPending.add(88);
        });

        const result = await sync.applyBootstrapWindow(
            { block_height: 200, since_block: 100, tables: {} }, 100, 200);

        assert.strictEqual(result, 'blocked');
        assert.strictEqual(applier.applyIncrementalSnapshot.called, false);
        assert.strictEqual(sync._bootstrapBase, 10);
        assert.strictEqual(sync._verifiedTip, 40);
        assert.strictEqual(sync._persistedVerifiedTip, 40);
        assert.strictEqual(sync.db.setSyncState.calledWith('verified_tip:indexer', '40'), true);
    });
});

describe('ClientSync bootstrap window posture restore failure @regression', function(){
    quietConsole();

    it('escalates when the restore of the bootstrap base reports a failed write', async function(){
        const { sync, db, applier } = heldWindowSync(async () => false);

        await assert.rejects(() => heldWindow(sync),
            e => e instanceof ClientSync.BootstrapExhaustedError && /could not be restored/.test(e.message));
        assert.strictEqual(applier.applyIncrementalSnapshot.called, false);
        assert.strictEqual(db.setSyncState.calledWith('bootstrap_base:indexer', '10'), true);
        assert.strictEqual(sync._bootstrapBase, 100, 'memory follows only a restore write that landed');
    });

    it('escalates when the restore of the bootstrap base throws', async function(){
        const { sync, applier } = heldWindowSync(async () => { throw new Error('db down'); });

        await assert.rejects(() => heldWindow(sync),
            e => e instanceof ClientSync.BootstrapExhaustedError && /db down/.test(e.message));
        assert.strictEqual(applier.applyIncrementalSnapshot.called, false);
    });

    it('escalates when the restore deletes a base that did not exist and the delete fails', async function(){
        const { sync, db, applier } = heldWindowSync(async () => true);
        sync._bootstrapBase = null;
        db.deleteSyncState = sinon.stub().resolves(false);

        await assert.rejects(() => heldWindow(sync), e => e instanceof ClientSync.BootstrapExhaustedError);
        assert.strictEqual(applier.applyIncrementalSnapshot.called, false);
    });
});

describe('ClientSync bootstrap window posture restore failure @regression', function(){
    quietConsole();

    it('escalates when the restore of the verified marker fails', async function(){
        const { sync, db, applier } = heldWindowSync(async () => true);
        sync._persistedVerifiedTip = null;
        db.deleteSyncState = sinon.stub().callsFake(async key => key !== 'verified_tip:indexer');

        await assert.rejects(() => heldWindow(sync), e => e instanceof ClientSync.BootstrapExhaustedError);
        assert.strictEqual(applier.applyIncrementalSnapshot.called, false);
    });

    it('carries a failed restore out of the height retry loop instead of returning blocked', async function(){
        const { sync, applier } = heldWindowSync(async () => false);
        sinon.stub(sync, 'discoverBootstrapRange').resolves({ tip: 200, base: 100 });
        sinon.stub(sync, 'fetchAndApplySchema').resolves();
        sinon.stub(sync, 'syncLookupTablesPaged').resolves();
        const fetch = sinon.stub(sync, 'fetchBootstrapWindow')
            .resolves({ block_height: 200, since_block: 100, tables: {} });
        const sleep = sinon.stub(sync.util, 'sleep').resolves();

        await assert.rejects(() => sync.bootstrapFromHeightRetry(100),
            e => e instanceof ClientSync.BootstrapExhaustedError && /could not be restored/.test(e.message));
        assert.strictEqual(fetch.callCount, 1);
        assert.strictEqual(sleep.called, false);
        assert.strictEqual(applier.applyIncrementalSnapshot.called, false);
    });
});

describe('ClientSync bootstrap window posture restore failure @regression', function(){
    quietConsole();

    it('escalates when a hold lands during the marker write and the marker restore throws', async function(){
        const { sync, db, applier } = makeSync({ VERIFY_RECOMPUTE: true });
        initializeVerifiedPosture(sync);
        db.setSyncState = sinon.stub().callsFake(async (key, value) => {
            if(key === 'verified_tip:indexer' && value === '100') sync._strictReorgPending.add(88);
            if(key === 'verified_tip:indexer' && value === '40') throw new Error('marker write lost');
            return true;
        });
        sinon.stub(sync, 'discoverBootstrapRange').resolves({ tip: 200, base: 100 });
        sinon.stub(sync, 'fetchAndApplySchema').resolves();
        sinon.stub(sync, 'syncLookupTablesPaged').resolves();
        const fetch = sinon.stub(sync, 'fetchBootstrapWindow')
            .resolves({ block_height: 200, since_block: 100, tables: {} });
        sinon.stub(sync.util, 'sleep').resolves();

        await assert.rejects(() => sync.bootstrapFromHeightRetry(100),
            e => e instanceof ClientSync.BootstrapExhaustedError && /marker write lost/.test(e.message));
        assert.strictEqual(fetch.callCount, 1);
        assert.strictEqual(applier.applyIncrementalSnapshot.called, false);
        assert.strictEqual(db.setSyncState.calledWith('bootstrap_base:indexer', '10'), true);
        assert.strictEqual(sync._bootstrapBase, 10);
        assert.strictEqual(sync._verifiedTip, 100, 'memory follows only a restore write that landed');
        assert.strictEqual(sync._persistedVerifiedTip, 100);
    });
});
