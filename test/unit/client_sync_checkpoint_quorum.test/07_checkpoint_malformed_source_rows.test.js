// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md.

const assert = require('assert');
const sinon = require('sinon');
const ClientSync = require('../../../src/client/sync');
const Utility = require('../../../src/util');
const HashVerifier = require('../../../src/client/hash_verifier');
const checkpoint = require('../../../src/checkpoint');
const { withDbMixins } = require('../../helpers/db_mixins.js');

function makeSync(){
    const db = withDbMixins({ dbType: 'indexer', recordHalt: sinon.stub().resolves(),
        setSyncState: sinon.stub().resolves(), doQuery: sinon.stub().resolves([]) });
    const config = { SYNC_SOURCES: 'http://source:3006', VERIFY_RECOMPUTE: true,
        CHECKPOINT_FRESHNESS_STRICT: false, CHECKPOINT_FRESHNESS_BLOCKS: 500 };
    const sync = new ClientSync('BTC', 'regtest', db, {}, {}, new HashVerifier(), config, new Utility());
    sync.lastAppliedBlock = 1000;
    return sync;
}

describe('ClientSync malformed checkpoint source rows @regression', function(){
    beforeEach(function(){ sinon.stub(console, 'warn'); sinon.stub(console, 'error'); });
    afterEach(function(){ sinon.restore(); });

    it('halts the latest anchor on a case-only network variant', async function(){
        const sync = makeSync();
        sinon.stub(sync, 'fetchLatestCheckpoint').resolves({ chain: 'BTC', network: 'Regtest' });
        const halt = sinon.stub(sync, 'haltCheckpoint').resolves('halted');
        const result = await sync.anchorLatestCheckpoint('http://source:3006', []);
        assert.strictEqual(result, 'halted');
        assert.deepStrictEqual(halt.firstCall.args[1].map(m => m.field), ['checkpoint_network']);
        assert.strictEqual(halt.firstCall.args[2], 'checkpoint-binding-mismatch');
    });

    it('applies exact network binding to rotation steps', async function(){
        const sync = makeSync();
        const result = await sync.attestRotationStep({ chain: 'BTC', network: 'Regtest' }, { block_index: 1 });
        assert.strictEqual(result.verdict, 'divergence');
        assert.strictEqual(result.haltReason, 'checkpoint-binding-mismatch');
    });

    for(const malformed of ['990', null]){
        it('warns before skipping non-number block_index ' + JSON.stringify(malformed), function(){
            const sync = makeSync();
            sinon.stub(checkpoint, 'commitmentMissing').returns(false);
            sinon.stub(checkpoint, 'commitmentActive').returns(true);
            const skipped = sync.checkpointSkipsAnchoring({ block_index: malformed,
                checkpoint_seq: 9, state_root: 'ab'.repeat(32) });
            assert.strictEqual(skipped, true);
            assert.match(console.warn.firstCall.args[0], /non-number block_index/);
        });
    }
});
