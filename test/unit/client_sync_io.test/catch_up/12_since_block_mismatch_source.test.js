// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later

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
        recordHalt: sinon.stub().resolves({ block_index: 101 }),
        doQuery: sinon.stub().resolves([])
    };
    const applier = {
        applyBlock: sinon.stub().resolves(), applyFullSnapshot: sinon.stub().resolves(),
        applyIncrementalSnapshot: sinon.stub().resolves()
    };
    const sync = new ClientSync('bitcoin', 'mainnet', db, applier,
        { rollback: sinon.stub().resolves() }, new HashVerifier(), {
            SYNC_SOURCES: 'http://a:3006,http://b:3006', VERIFY_RECOMPUTE: true,
            HASH_CONFIRM_TIMEOUT: 5000, SNAPSHOT_MAX_CONTENT: 1024,
            MAX_ROLLBACK_DEPTH: 10
        }, new Utility());
    return { sync, db };
}

describe('ClientSync catch-up boundary source attribution @regression', function(){
    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'warn');
        sinon.stub(console, 'error');
    });
    afterEach(function(){ sinon.restore(); });

    it('records the source that served a mismatched since_block echo', async function(){
        const { sync, db } = makeSync();
        sync._preferredSourceIndex = 1;
        assert.strictEqual(sync.primarySource(), 'http://b:3006');
        sinon.stub(sync, 'verifyUnverifiedTail').resolves('ok');
        sinon.stub(sync, 'downloadIncrementalCatchUp').resolves({
            snapshotData: { since_block: 90, block_height: 105, tables: {} },
            skipLookups: false
        });
        sinon.stub(sync, 'applyIncrementalCatchUpSnapshot').resolves(false);
        sinon.stub(sync, 'anchorCheckpointAfterCatchUp').resolves();

        await sync.runIncrementalCatchUp();

        assert.strictEqual(sync.getHaltInfo().reason, 'catchup-since-block-mismatch');
        assert.deepStrictEqual(sync.getHaltInfo().sources, ['http://b:3006']);
        assert.deepStrictEqual(db.recordHalt.firstCall.args[4], ['http://b:3006']);
    });
});
