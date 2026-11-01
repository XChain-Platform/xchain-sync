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
const { withDbMixins } = require('../../../helpers/db_mixins.js');

function makeSync(overrides){
    const db = withDbMixins({
        dbName: 'test_db', dbType: 'indexer',
        getLastBlock: sinon.stub().resolves(null),
        getBlockHashRow: sinon.stub().resolves(null),
        getActiveHalt: sinon.stub().resolves(null),
        getTableCount: sinon.stub().resolves(0),
        addMissingColumns: sinon.stub().resolves(),
        recordHalt: sinon.stub().resolves({ block_index: 0 }),
        clearHalt: sinon.stub().resolves(1),
        doQuery: sinon.stub().resolves([])
    });
    const applier = {
        applyBlock: sinon.stub().resolves(), applyFullSnapshot: sinon.stub().resolves(),
        applyIncrementalSnapshot: sinon.stub().resolves()
    };
    const config = Object.assign({
        SYNC_SOURCES: 'http://src1:3006', VERIFY_HASHES: false,
        CLIENT_RECONNECT_DELAY: 5000, HASH_CONFIRM_TIMEOUT: 5000,
        BOOTSTRAP_MAX_RETRIES: 5, BOOTSTRAP_RETRY_BASE_MS: 1,
        BOOTSTRAP_RETRY_MAX_MS: 1, SNAPSHOT_MAX_CONTENT: 1024,
        MAX_ROLLBACK_DEPTH: 10
    }, overrides || {});
    const sync = new ClientSync('bitcoin', 'mainnet', db, applier,
        { rollback: sinon.stub().resolves() }, new HashVerifier(), config, new Utility());
    return { sync, db, applier };
}

function sizeError(){
    const error = new Error('maxContentLength size exceeded');
    error.code = 'ERR_FR_MAX_CONTENT_LENGTH_EXCEEDED';
    return error;
}

describe('ClientSync: bounded bootstrap size wall @regression', function(){
    beforeEach(function(){
        sinon.stub(console, 'log'); sinon.stub(console, 'warn'); sinon.stub(console, 'error');
    });
    afterEach(function(){ sinon.restore(); });

    it('halts after one oversized window instead of retrying', async function(){
        const { sync, db, applier } = makeSync();
        const sleep = sinon.stub(sync.util, 'sleep').resolves();
        const fromHeight = sinon.stub(sync, 'bootstrapFromHeight').rejects(sizeError());

        await assert.rejects(() => sync.bootstrapFromHeightRetry(50000), /snapshot-too-large/);

        assert.strictEqual(fromHeight.callCount, 1);
        assert.strictEqual(sleep.called, false);
        assert.strictEqual(sync.getHaltInfo().reason, 'snapshot-too-large');
        assert.strictEqual(sync.getHaltInfo().mismatches[0].bootstrap_depth, 50000);
        assert.strictEqual(db.recordHalt.calledOnce, true);
        assert.strictEqual(applier.applyIncrementalSnapshot.called, false);
        const logged = console.error.getCalls().map(call => String(call.args[0])).join('\n');
        assert.ok(logged.includes('SYNC_BOOTSTRAP_DEPTH'));
        assert.ok(logged.includes('SNAPSHOT_MAX_CONTENT'));
    });

    it('types generic exhaustion without halting', async function(){
        const { sync, db } = makeSync({ BOOTSTRAP_MAX_RETRIES: 1 });
        sinon.stub(sync.util, 'sleep').resolves();
        sinon.stub(sync, 'bootstrapFromHeight').rejects(new Error('ECONNRESET'));

        await assert.rejects(() => sync.bootstrapFromHeightRetry(50000), error =>
            error instanceof ClientSync.BootstrapExhaustedError && /exhausted after 2 round/.test(error.message));
        assert.strictEqual(sync.isHalted(), false);
        assert.strictEqual(db.recordHalt.called, false);
    });
});
