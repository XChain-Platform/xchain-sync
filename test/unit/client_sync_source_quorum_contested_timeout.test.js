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
 * ClientSync: the cross-source quorum timeout applies a block only when every
 * active report agrees with the payload it holds. A height two sources already
 * contest takes the no-source-quorum path instead, even while a third source is
 * still silent.
 ********************************************************************/

const assert = require('assert');
const sinon  = require('sinon');
const ClientSync   = require('../../src/client/sync');
const Utility      = require('../../src/util');
const HashVerifier = require('../../src/client/hash_verifier');

function createMockDb(){
    return {
        dbName: 'test_db', dbType: 'indexer',
        getLastBlock: sinon.stub().resolves(null),
        getBlockHashRow: sinon.stub().resolves(null),
        getActiveHalt: sinon.stub().resolves(null),
        recordHalt: sinon.stub().resolves({ block_index: 0 }),
        doQuery: sinon.stub().resolves([])
    };
}

// A block event carrying a given hash tuple; a different h is a conflicting report.
function blockEvent(blockIndex, h){
    return { type: 'block', block_index: blockIndex,
        ledger_hash: 'lh' + h, actions_hash: 'ah' + h, contract_hash: 'ch' + h };
}

function makeSync(extraConfig){
    let applier = { applyBlock: sinon.stub().resolves(), applyFullSnapshot: sinon.stub().resolves() };
    let config = Object.assign({
        SYNC_SOURCES: 'http://a:3006,http://b:3006,http://c:3006', VERIFY_HASHES: true, HALT_ON_DIVERGENCE: true,
        HASH_CONFIRM_TIMEOUT: 5000, CLIENT_RECONNECT_DELAY: 5000,
        VERIFY_RECOMPUTE: false, VERIFY_STATE_HASH: false, VERIFY_STATE_COMMITMENT: false
    }, extraConfig || {});
    let sync = new ClientSync('bitcoin', 'mainnet', createMockDb(), applier,
        { rollback: sinon.stub().resolves() }, new HashVerifier(), config, new Utility());
    sync.lastAppliedBlock = 100;
    sync.lastHashes = { ledger_hash: 'lhX', actions_hash: 'ahX', contract_hash: 'chX' };
    return { sync, applier };
}

describe('ClientSync: contested height at the quorum timeout @regression', function(){
    let clock;
    beforeEach(function(){
        sinon.stub(console, 'log'); sinon.stub(console, 'warn'); sinon.stub(console, 'error');
        clock = sinon.useFakeTimers();
    });
    afterEach(function(){ clock.restore(); sinon.restore(); });

    async function pastTimeout(sync){ await clock.tickAsync(sync.config.HASH_CONFIRM_TIMEOUT + 100); }

    it('halts no-source-quorum, applying nothing, when two of three sources disagree and the third is silent', async function(){
        let { sync, applier } = makeSync();
        await sync.handleBlock(blockEvent(101, 'X'), 0);
        await sync.handleBlock(blockEvent(101, 'Y'), 1);
        await pastTimeout(sync);
        assert.strictEqual(applier.applyBlock.called, false, 'a contested height is never applied');
        assert.strictEqual(sync.isHalted(), true);
        assert.strictEqual(sync.getHaltInfo().reason, 'no-source-quorum');
    });

    it('refuses the contested height without halting in log-only mode', async function(){
        let { sync, applier } = makeSync({ HALT_ON_DIVERGENCE: false });
        await sync.handleBlock(blockEvent(101, 'X'), 0);
        await sync.handleBlock(blockEvent(101, 'Y'), 1);
        await pastTimeout(sync);
        assert.strictEqual(applier.applyBlock.called, false);
        assert.strictEqual(sync.isHalted(), false);
        assert.strictEqual(sync.pendingHashes.has(101), false, 'the refused height leaves no pending state');
    });

    it('still applies a single uncontested report once the other sources stay silent', async function(){
        let { sync, applier } = makeSync();
        await sync.handleBlock(blockEvent(101, 'X'), 0);
        await pastTimeout(sync);
        assert.strictEqual(applier.applyBlock.calledOnce, true, 'the silent-peer fallback is kept');
        assert.strictEqual(sync.isHalted(), false);
    });

    it('applies once, and strikes the dissenter, when the third source completes the majority in time', async function(){
        let { sync, applier } = makeSync();
        await sync.handleBlock(blockEvent(101, 'X'), 0);
        await sync.handleBlock(blockEvent(101, 'Y'), 1);
        await sync.handleBlock(blockEvent(101, 'X'), 2);
        assert.strictEqual(applier.applyBlock.calledOnce, true);
        await pastTimeout(sync);
        assert.strictEqual(applier.applyBlock.calledOnce, true, 'the timer was cancelled by the quorum');
        assert.ok(sync._sourceStrikes.get(1), 'the dissenting source is struck');
    });

    it('does not apply the held payload once its source is evicted and another report differs', async function(){
        let { sync, applier } = makeSync();
        await sync.handleBlock(blockEvent(101, 'X'), 0);
        await sync.handleBlock(blockEvent(101, 'Y'), 1);
        sync._evictedSources.add(0);
        await pastTimeout(sync);
        assert.strictEqual(applier.applyBlock.called, false);
    });

    it('does not apply the held payload once its source is evicted and no active report remains', async function(){
        let { sync, applier } = makeSync();
        await sync.handleBlock(blockEvent(101, 'X'), 0);
        sync._evictedSources.add(0);
        await pastTimeout(sync);
        assert.strictEqual(applier.applyBlock.called, false);
        assert.strictEqual(sync.pendingHashes.has(101), false);
    });
});
