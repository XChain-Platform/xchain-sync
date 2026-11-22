/*********************************************************************
 *
 * Copyright © 2025–2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC – https://dankest.llc
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This file is part of XChain Platform. Licensed under the GNU Affero
 * General Public License v3.0 or later; see LICENSE.md.
 *
 ********************************************************************/

'use strict';

const assert = require('assert');
const sinon = require('sinon');
const ClientSync = require('../../../src/client/sync');
const Utility = require('../../../src/util');
const HashVerifier = require('../../../src/client/hash_verifier');
const { withDbMixins } = require('../../helpers/db_mixins.js');

const ENVKEY = 'CHECKPOINT_VALIDATORS_BTC_REGTEST';

function makeSync(extra){
    const state = new Map();
    const db = withDbMixins({ dbName: 'test_db', dbType: 'indexer',
        recordHalt: sinon.stub().resolves({ block_index: 0 }),
        getActiveHalt: sinon.stub().resolves(null), clearHalt: sinon.stub().resolves(1),
        getSyncState: sinon.stub().callsFake(async key => state.get(key) || null),
        setSyncState: sinon.stub().callsFake(async (key, value) => { state.set(key, value); }) });
    const config = Object.assign({ SYNC_SOURCES: 'http://a:3006', VERIFY_CHECKPOINT_QUORUM: true,
        CHECKPOINT_FRESHNESS_BLOCKS: 500, CHECKPOINT_FRESHNESS_STRICT: true }, extra || {});
    const sync = new ClientSync('BTC', 'regtest', db, {}, {}, new HashVerifier(), config, new Utility());
    sync.lastAppliedBlock = 1000;
    return sync;
}

describe('ClientSync: throwing checkpoint anchors count toward strict freshness @regression', function(){
    beforeEach(function(){
        process.env[ENVKEY] = JSON.stringify([{ pubkey: '11'.repeat(32), weight: '100', source: 'S1' }]);
        sinon.stub(console, 'error');
    });
    afterEach(function(){ sinon.restore(); delete process.env[ENVKEY]; });

    it('halts when the last verified anchor is stale and the anchor cycle throws', async function(){
        const sync = makeSync();
        sync._lastVerifiedCheckpointBlock = 100;
        sinon.stub(sync, 'anchorLatestCheckpoint').rejects(new Error('anchor database unavailable'));
        await sync.verifyCheckpointQuorum();
        assert.strictEqual(sync.getHaltInfo().reason, 'checkpoint-freshness-stale');
    });

    it('logs but does not halt when the last verified anchor is still fresh', async function(){
        const sync = makeSync();
        sync._lastVerifiedCheckpointBlock = 900;
        sinon.stub(sync, 'anchorLatestCheckpoint').rejects(new Error('anchor database unavailable'));
        await sync.verifyCheckpointQuorum();
        assert.strictEqual(sync.isHalted(), false);
        assert.ok(console.error.getCalls().some(call => /counts toward strict freshness/.test(call.args[0])));
    });

    it('logs but does not halt when strict freshness is disabled', async function(){
        const sync = makeSync({ CHECKPOINT_FRESHNESS_STRICT: false });
        sync._lastVerifiedCheckpointBlock = 100;
        sinon.stub(sync, 'anchorLatestCheckpoint').rejects(new Error('anchor database unavailable'));
        await sync.verifyCheckpointQuorum();
        assert.strictEqual(sync.isHalted(), false);
    });
});
