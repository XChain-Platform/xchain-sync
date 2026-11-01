/*********************************************************************
 *
 * Copyright © 2025–2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC – https://dankest.llc
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 ********************************************************************/

const assert = require('assert');
const sinon = require('sinon');
const ClientSync = require('../../../../src/client/sync');
const Utility = require('../../../../src/util');
const HashVerifier = require('../../../../src/client/hash_verifier');

function makeSync(){
    const db = {
        dbName: 'test_db', dbType: 'indexer',
        getLastBlock: sinon.stub().resolves(100),
        getBlockHashRow: sinon.stub().resolves(null),
        getActiveHalt: sinon.stub().resolves(null),
        doQuery: sinon.stub().resolves([])
    };
    const applier = {
        applyBlock: sinon.stub().resolves(), applyFullSnapshot: sinon.stub().resolves(),
        applyIncrementalSnapshot: sinon.stub().resolves()
    };
    const config = {
        SYNC_SOURCES: 'http://a:3006,http://b:3006,http://c:3006',
        VERIFY_HASHES: true, SOURCE_QUORUM: 2, HASH_CONFIRM_TIMEOUT: 5000,
        CLIENT_RECONNECT_DELAY: 5000, BOOTSTRAP_MAX_RETRIES: 0,
        BOOTSTRAP_RETRY_BASE_MS: 1, BOOTSTRAP_RETRY_MAX_MS: 1,
        SNAPSHOT_MAX_CONTENT: 1024, MAX_ROLLBACK_DEPTH: 10
    };
    const sync = new ClientSync('bitcoin', 'mainnet', db, applier, { rollback: sinon.stub().resolves() },
        new HashVerifier(), config, new Utility());
    return { sync, db, applier };
}

describe('ClientSync: active source selection @regression', function(){
    beforeEach(function(){
        sinon.stub(console, 'log'); sinon.stub(console, 'warn'); sinon.stub(console, 'error');
    });
    afterEach(function(){ sinon.restore(); });

    it('uses the first non-evicted source for incremental catch-up', async function(){
        const { sync } = makeSync();
        sync._evictedSources.add(0);
        sinon.stub(sync, 'verifyUnverifiedTail').resolves('ok');
        const download = sinon.stub(sync, 'downloadIncrementalCatchUp').resolves({
            snapshotData: {}, skipLookups: false
        });
        sinon.stub(sync, 'applyIncrementalCatchUpSnapshot').resolves(true);

        await sync.runIncrementalCatchUp();

        assert.strictEqual(download.firstCall.args[0], 'http://b:3006');
    });

    it('keeps source order stable and prefers the successful bootstrap source', async function(){
        const { sync } = makeSync();
        const original = sync.sources.slice();
        sinon.stub(sync, 'fetchAndApplySchema').resolves();
        const download = sinon.stub(sync, 'bootstrapDownloadSnapshot');
        download.onFirstCall().rejects(new Error('source down'));
        download.onSecondCall().resolves({ block_height: 500 });
        sinon.stub(sync, 'bootstrapApplySnapshot').resolves(true);
        const verify = sinon.stub(sync, 'bootstrapVerifySnapshot').resolves(true);

        assert.strictEqual(await sync.bootstrapRotateSources(), true);
        assert.deepStrictEqual(sync.sources, original);
        assert.strictEqual(sync.primarySource(), 'http://b:3006');
        assert.strictEqual(verify.calledOnceWithExactly(1), true);
    });

    it('does not count an evicted source in bootstrap cross-checks', async function(){
        const { sync } = makeSync();
        sync.lastAppliedBlock = 500;
        sync._evictedSources.add(0);
        const verify = sinon.stub(sync, 'verifyAgainstSource').resolves('agree');

        await sync.bootstrapVerifyIndexerQuorum(1);

        assert.strictEqual(verify.calledOnceWith('http://c:3006', 500), true);
    });
});
