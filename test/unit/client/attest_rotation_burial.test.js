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
 **********************************************************************
 * ClientSync.attestRotationStep buries the signer-set lookup.
 *
 * The checkpoint declares the RAW snapshot_block; the signer resolved its set at
 * the buried height, so the light client must look the set up at
 * buriedSnapshotBlock(snapshot_block), never at the declared height.
 ********************************************************************/

const assert = require('assert');
const sinon  = require('sinon');
const ClientSync   = require('../../../src/client/sync');
const Utility      = require('../../../src/util');
const HashVerifier = require('../../../src/client/hash_verifier');
const checkpointVerifier = require('../../../src/checkpoint');
const { CANONICAL_REORG_BUFFER, buriedSnapshotBlock } = require('../../../src/consensus/snapshot_reorg_buffer');
const { withDbMixins } = require('../../helpers/db_mixins.js');

function makeSync(){
    const config = { SYNC_SOURCES: 'http://a:3006', VERIFY_RECOMPUTE: true,
        VERIFY_CHECKPOINT_QUORUM: true, CHECKPOINT_VERIFY_INTERVAL: 1 };
    const s = new ClientSync('BTC', 'regtest', withDbMixins({}), { applyBlock: sinon.stub().resolves() },
        { rollback: sinon.stub().resolves() }, new HashVerifier(), config, new Utility());
    s.lastAppliedBlock = 1000;
    return s;
}

describe('ClientSync.attestRotationStep: signer-set lookup is buried @regression', function(){
    afterEach(function(){ sinon.restore(); });

    it('looks the set up at the buried height, not the declared one', async function(){
        const sync = makeSync();
        sinon.stub(sync, 'checkpointBindingMismatches').returns(null);
        sinon.stub(checkpointVerifier, 'commitmentActive').returns(true);
        const lookup = sinon.stub(sync, 'oraclePublishSetAt').resolves(Object.assign([], { truncated: true }));
        const next = { block_index: 100, snapshot_block: 90, state_root: 'b1'.repeat(32) };

        const verdict = await sync.attestRotationStep(next, { block_index: 90 });

        assert.strictEqual(buriedSnapshotBlock(90, 'regtest'), 90 - CANONICAL_REORG_BUFFER);
        assert.ok(lookup.calledOnceWithExactly(90 - CANONICAL_REORG_BUFFER), 'lookup at the buried height');
        assert.strictEqual(verdict.verdict, 'wait');
    });

    it('clamps the buried height at zero near genesis', async function(){
        const sync = makeSync();
        sinon.stub(sync, 'checkpointBindingMismatches').returns(null);
        sinon.stub(checkpointVerifier, 'commitmentActive').returns(true);
        const lookup = sinon.stub(sync, 'oraclePublishSetAt').resolves(Object.assign([], { truncated: true }));

        await sync.attestRotationStep({ block_index: 10, snapshot_block: 2, state_root: 'b1'.repeat(32) }, { block_index: 5 });

        assert.ok(lookup.calledOnceWithExactly(0));
    });

    it('still gates on the declared height being attested by the trust root', async function(){
        const sync = makeSync();
        sinon.stub(sync, 'checkpointBindingMismatches').returns(null);
        sinon.stub(checkpointVerifier, 'commitmentActive').returns(true);
        const lookup = sinon.stub(sync, 'oraclePublishSetAt').resolves([]);

        const verdict = await sync.attestRotationStep(
            { block_index: 100, snapshot_block: 95, state_root: 'b1'.repeat(32) }, { block_index: 90 });

        assert.strictEqual(verdict.verdict, 'wait');
        assert.strictEqual(lookup.called, false);
    });
});
