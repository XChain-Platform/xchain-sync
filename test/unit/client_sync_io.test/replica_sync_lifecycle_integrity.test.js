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
const { withDbMixins } = require('../../helpers/db_mixins.js');

function makeSync(configOverrides, dbOverrides){
    const state = new Map();
    const db = withDbMixins(Object.assign({
        dbName: 'test_db', dbType: 'indexer',
        getLastBlock: sinon.stub().resolves(null),
        getBlockHashRow: sinon.stub().resolves(null),
        getActiveHalt: sinon.stub().resolves(null),
        getTableCount: sinon.stub().resolves(0),
        doQuery: sinon.stub().resolves([]),
        recordHalt: sinon.stub().resolves({ block_index: 0 }),
        clearHalt: sinon.stub().resolves(1),
        getSyncState: sinon.stub().callsFake(async key => state.has(key) ? state.get(key) : null),
        setSyncState: sinon.stub().callsFake(async (key, value) => { state.set(key, value); return true; }),
        deleteSyncState: sinon.stub().callsFake(async key => { state.delete(key); return true; })
    }, dbOverrides || {}));
    const applier = {
        applyBlock: sinon.stub().resolves(),
        applyFullSnapshot: sinon.stub().resolves(),
        applyIncrementalSnapshot: sinon.stub().resolves()
    };
    const config = Object.assign({
        SYNC_SOURCES: 'source', VERIFY_HASHES: false,
        VERIFY_RECOMPUTE: false, MAX_ROLLBACK_DEPTH: 10,
        SNAPSHOT_MAX_CONTENT: 1024, LOOKUP_PAGE_SIZE: 50,
        BOOTSTRAP_MAX_RETRIES: 2, BOOTSTRAP_RETRY_BASE_MS: 1,
        BOOTSTRAP_RETRY_MAX_MS: 1, HASH_CONFIRM_TIMEOUT: 5000
    }, configOverrides || {});
    const sync = new ClientSync('bitcoin', 'mainnet', db, applier,
        { rollback: sinon.stub().resolves() }, new HashVerifier(), config, new Utility());
    return { sync, db, applier, state };
}

function quietConsole(){
    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'warn');
        sinon.stub(console, 'error');
    });
    afterEach(function(){ sinon.restore(); });
}

function sizeError(){
    const error = new Error('maxContentLength exceeded');
    error.code = 'ERR_FR_MAX_CONTENT_LENGTH_EXCEEDED';
    return error;
}

describe('ClientSync full snapshot height integrity @regression', function(){
    quietConsole();

    it('rejects malformed heights before applying or changing the cursor', async function(){
        const invalid = [undefined, null, '200', NaN, 1.5, -1, Number.MAX_SAFE_INTEGER + 1];
        for(const height of invalid){
            const { sync, applier } = makeSync();
            sync.lastAppliedBlock = 50;
            await assert.rejects(() => sync.bootstrapApplySnapshot({ block_height: height }),
                /invalid block_height/);
            assert.strictEqual(applier.applyFullSnapshot.called, false);
            assert.strictEqual(sync.lastAppliedBlock, 50);
        }
    });

    it('refuses live-follow for every invalid cursor while accepting block zero', function(){
        const { sync } = makeSync();
        for(const height of [null, undefined, '5', NaN, -1, Number.MAX_SAFE_INTEGER + 1]){
            sync.lastAppliedBlock = height;
            assert.throws(() => sync.ensureLiveFollowTip(), /Refusing to enter live-follow/);
        }
        sync.lastAppliedBlock = 0;
        assert.doesNotThrow(() => sync.ensureLiveFollowTip());
    });
});

describe('ClientSync committed bootstrap retry integrity @regression', function(){
    quietConsole();

    it('retries finalization without discovering or applying a second window', async function(){
        const { sync } = makeSync();
        const discover = sinon.stub(sync, 'discoverBootstrapRange').resolves({ tip: 200, base: 100 });
        sinon.stub(sync, 'fetchAndApplySchema').resolves();
        const page = sinon.stub(sync, 'syncLookupTablesPaged');
        page.onCall(0).resolves();
        page.onCall(1).rejects(new Error('lookup unavailable'));
        page.onCall(2).resolves();
        sinon.stub(sync, 'fetchBootstrapWindow').resolves({ block_height: 200, since_block: 100 });
        const apply = sinon.stub(sync, 'applyBootstrapWindow').resolves(true);
        sinon.stub(sync, 'refreshTipHashes').resolves();
        sinon.stub(sync, 'verifyBootstrapTerminalBlock').resolves(false);
        sinon.stub(sync, 'sleepUntilStop').resolves();

        await sync.bootstrapFromHeightRetry(100);

        assert.strictEqual(discover.callCount, 1);
        assert.strictEqual(apply.callCount, 1);
        assert.strictEqual(page.callCount, 3);
    });

    it('refuses a new window when a failed pre-commit round finds stored rows', async function(){
        const { sync, db } = makeSync();
        const seed = sinon.stub(sync, 'bootstrapFromHeight').rejects(new Error('source unavailable'));
        db.getLastBlock.resolves(88);
        sinon.stub(sync, 'sleepUntilStop').resolves();

        await assert.rejects(() => sync.bootstrapFromHeightRetry(100),
            error => error instanceof ClientSync.BootstrapExhaustedError && /non-empty/.test(error.message));
        assert.strictEqual(seed.callCount, 1);
    });
});

describe('ClientSync tail lookup healing @regression', function(){
    quietConsole();

    it('pages to the committed tip before the verified-tail walk', async function(){
        const { sync, state } = makeSync({
            VERIFY_RECOMPUTE: true,
            SYNC_BOOTSTRAP_DEPTH: { 'BTC:MAINNET': 100 }
        });
        state.set('verified_tip:indexer', '100');
        const page = sinon.stub(sync, 'syncLookupTablesPaged').resolves(true);
        const walk = sinon.stub(sync, 'verifyUnverifiedTail').resolves('ok');
        sinon.stub(sync, 'syncCursorToCommittedTip').resolves('unchanged');

        assert.strictEqual(await sync.prepareCatchUpFromTip(105, 106, {}), true);
        assert.deepStrictEqual(page.firstCall.args[1], { tip: 105 });
        assert.ok(page.calledBefore(walk));
    });

    it('holds without walking when lookup healing has a transport fault', async function(){
        const { sync, state } = makeSync({
            VERIFY_RECOMPUTE: true,
            SYNC_BOOTSTRAP_DEPTH: { 'BTC:MAINNET': 100 }
        });
        state.set('verified_tip:indexer', '100');
        sinon.stub(sync, 'syncLookupTablesPaged').rejects(new Error('lookup unavailable'));
        const walk = sinon.stub(sync, 'verifyUnverifiedTail').resolves('ok');

        assert.strictEqual(await sync.prepareCatchUpFromTip(105, 106, {}), false);
        assert.strictEqual(walk.called, false);
        assert.strictEqual(sync.isHalted(), false);
    });

    it('heals a stored replica before its startup tail walk', async function(){
        const { sync, db, state } = makeSync({
            VERIFY_RECOMPUTE: true,
            SYNC_BOOTSTRAP_DEPTH: { 'BTC:MAINNET': 100 }
        });
        db.getLastBlock.resolves(105);
        state.set('verified_tip:indexer', '100');
        sinon.stub(sync, 'fetchAndApplySchema').resolves();
        const page = sinon.stub(sync, 'syncLookupTablesPaged').resolves(true);
        const walk = sinon.stub(sync, 'verifyUnverifiedTail').resolves('ok');
        sinon.stub(sync, 'incrementalCatchUp').resolves();

        await sync.synchronizeStoredReplica();

        assert.deepStrictEqual(page.firstCall.args[1], { tip: 105 });
        assert.ok(page.calledBefore(walk));
    });
});

describe('ClientSync lookup overflow routing @regression', function(){
    quietConsole();

    it('tags an overflow with its lookup page origin', async function(){
        const { sync } = makeSync();
        const error = sizeError();
        sinon.stub(sync, 'fetchLookupPage').rejects(error);

        await assert.rejects(() => sync.syncLookupTablesPaged('source'), caught => caught === error);
        assert.strictEqual(sync.isLookupPageOverflow(error), true);
        assert.ok(error.lookupPageOverflow.table);
        assert.strictEqual(error.lookupPageOverflow.pageSize, 50);
    });

    it('uses the lookup-specific halt and never starts a bootstrap fallback', async function(){
        const { sync } = makeSync({ SYNC_BOOTSTRAP_DEPTH: { 'BTC:MAINNET': 100 } });
        sync.lastAppliedBlock = 80;
        const fallback = sinon.stub(sync, 'bootstrapFromHeightRetry').resolves();
        const error = sizeError();
        error.lookupPageOverflow = { table: 'index_transactions', afterId: 40, pageSize: 50 };

        await sync.handleIncrementalCatchUpFailure('source', 81, error, null);

        assert.strictEqual(sync.getHaltInfo().reason, 'lookup-page-too-large');
        assert.strictEqual(fallback.called, false);
        const logged = console.error.getCalls().map(call => String(call.args[0])).join('\n');
        assert.ok(logged.includes('LOOKUP_PAGE_SIZE'));
        assert.strictEqual(logged.includes('wipe'), false);
    });

    it('routes a bootstrap lookup overflow away from the window-size halt', async function(){
        const { sync } = makeSync({ SYNC_BOOTSTRAP_DEPTH: { 'BTC:MAINNET': 100 } });
        const error = sizeError();
        error.lookupPageOverflow = { table: 'index_transactions', afterId: 40, pageSize: 50 };
        sinon.stub(sync, 'bootstrapFromHeight').rejects(error);

        await assert.rejects(() => sync.bootstrapFromHeightRetry(100), /lookup-page-too-large/);
        assert.strictEqual(sync.getHaltInfo().reason, 'lookup-page-too-large');
    });

    it('re-reads the tip before routing an untagged window overflow', async function(){
        const { sync, db } = makeSync({ SYNC_BOOTSTRAP_DEPTH: { 'BTC:MAINNET': 100 } });
        db.getLastBlock.resolves(120);
        const fallback = sinon.stub(sync, 'bootstrapFromHeightRetry').resolves();

        await sync.handleIncrementalCatchUpFailure('source', 1, sizeError(), null);

        assert.strictEqual(sync.getHaltInfo().reason, 'catch-up-too-large');
        assert.strictEqual(sync.getHaltInfo().blockIndex, 120);
        assert.strictEqual(fallback.called, false);
    });
});
