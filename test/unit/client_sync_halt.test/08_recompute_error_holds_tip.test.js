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
 * ClientSync: a live recompute error holds the tip.
 *
 * The same transient DB fault that holds the tip on a state_hash read must
 * hold it on the consensus-hash recompute, so redelivery re-verifies the block.
 ********************************************************************/

const assert = require('assert');
const sinon  = require('sinon');
const ClientSync = require('../../../src/client/sync');
const Utility = require('../../../src/util');
const HashVerifier = require('../../../src/client/hash_verifier');

describe('ClientSync: a recompute error holds the tip @regression', function(){
    let sync, db;

    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
        db = {
            dbName: 'test_db', dbType: 'indexer',
            getLastBlock: sinon.stub().resolves(null),
            getBlockHashRow: sinon.stub().resolves(null),
            doQuery: sinon.stub().resolves([]),
            doQueryStrict: sinon.stub().resolves([]),
            getSyncState: sinon.stub().resolves(null),
            setSyncState: sinon.stub().resolves(),
            recordHalt: sinon.stub().resolves({ block_index: 0 }),
            getActiveHalt: sinon.stub().resolves(null),
            clearHalt: sinon.stub().resolves(1)
        };
        const applier = { applyBlock: sinon.stub().resolves() };
        const config = { SYNC_SOURCES: 'http://a:3006', VERIFY_HASHES: true, HASH_CONFIRM_TIMEOUT: 5000,
            HALT_ON_DIVERGENCE: true, VERIFY_RECOMPUTE: true };
        sync = new ClientSync('bitcoin', 'mainnet', db, applier, { rollback: sinon.stub().resolves() }, new HashVerifier(), config, new Utility());
    });
    afterEach(function(){ sinon.restore(); });

    const event = { block_index: 200, block_time: 1, ledger_hash: 'x', actions_hash: 'y', contract_hash: 'z' };

    it('does not advance or halt on a recompute error, then advances once the recompute succeeds', async function(){
        const compute = sinon.stub(sync.blockHasher, 'computeBlockHashes');
        compute.onFirstCall().rejects(new Error('transient DB error'));
        compute.onSecondCall().resolves({ ledger_hash: 'x', actions_hash: 'y', contract_hash: 'z' });

        await sync.applyBlockEvent(event);
        assert.strictEqual(sync.isHalted(), false);
        assert.strictEqual(sync.lastAppliedBlock, null, 'a failed recompute must not advance the tip');

        await sync.applyBlockEvent(event);
        assert.strictEqual(sync.lastAppliedBlock, 200, 'redelivery verifies and advances');
    });

    it('verifyRecompute still returns null without holdTip', async function(){
        sinon.stub(sync.blockHasher, 'computeBlockHashes').rejects(new Error('transient DB error'));
        assert.strictEqual(await sync.verifyRecompute(event), null);
    });

    it('verifyRecompute rethrows with holdTip', async function(){
        sinon.stub(sync.blockHasher, 'computeBlockHashes').rejects(new Error('transient DB error'));
        await assert.rejects(sync.verifyRecompute(event, null, { holdTip: true }), /transient DB error/);
    });
});
