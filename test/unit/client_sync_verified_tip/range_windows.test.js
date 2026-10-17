// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// The walk over unverified committed blocks runs in VERIFY_RANGE_BATCH windows: the
// marker is written after each, a failure leaves it on the last block that passed, a
// block with no resolvable committed hash holds the replica without halting, and a stop
// request ends the walk between windows.
const assert = require('assert');
const sinon  = require('sinon');
const { buildClient, KEY } = require('../../helpers/verified_tip_client');

function quiet(){
    beforeEach(function(){ sinon.stub(console, 'log'); sinon.stub(console, 'error'); sinon.stub(console, 'warn'); });
    afterEach(function(){ sinon.restore(); });
}

function markerWrites(writes){ return writes.filter(w => w[0] === KEY).map(w => w[1]); }

describe('ClientSync: verified-tip walk in windows @regression', function(){
    quiet();

    it('a 25-block range in windows of 10 writes the marker after each window', async function(){
        let { sync, state, writes } = buildClient({ tip: 125, config: { VERIFY_RANGE_BATCH: 10 } });
        state.set(KEY, '100');
        assert.strictEqual(await sync.verifyUnverifiedTail(), 'ok');
        assert.strictEqual(sync.blockHasher.computeBlockHashes.callCount, 25);
        assert.deepStrictEqual(markerWrites(writes), ['110', '120', '125']);
    });

    it('a mismatch in the second window halts there and leaves the marker on the block below', async function(){
        let { sync, state, writes, db } = buildClient({ tip: 125, bad: [115], config: { VERIFY_RANGE_BATCH: 10 } });
        state.set(KEY, '100');
        assert.strictEqual(await sync.verifyUnverifiedTail(), 'halted');
        assert.deepStrictEqual(markerWrites(writes), ['110', '114']);
        assert.strictEqual(sync.getHaltInfo().blockIndex, 115);
        assert.strictEqual(sync.getHaltInfo().reason, 'local-recompute-divergence');
        assert.ok(db.recordHalt.calledOnce);
        assert.strictEqual(sync.blockHasher.computeBlockHashes.callCount, 15, 'nothing above the failure recomputes');
    });

    it('a block whose committed hash is unresolvable holds the replica without halting', async function(){
        let { sync, state, db } = buildClient({ tip: 105 });
        state.set(KEY, '100');
        let real = db.getBlockHashRow;
        db.getBlockHashRow = sinon.stub().callsFake(async h => (h === 103 ? { block_index: 103, ledger_hash: null } : real(h)));
        assert.strictEqual(await sync.verifyUnverifiedTail(), 'held');
        assert.strictEqual(sync.isHalted(), false);
        assert.strictEqual(state.get(KEY), '102');
    });

    it('a stop request ends the walk between windows, after the window\'s marker write', async function(){
        let { sync, state, writes } = buildClient({ tip: 130, config: { VERIFY_RANGE_BATCH: 10 } });
        state.set(KEY, '100');
        let real = sync.blockHasher.computeBlockHashes;
        sync.blockHasher.computeBlockHashes = sinon.stub().callsFake(async h => { if(h === 105) sync._stopping = true; return real(h); });
        assert.strictEqual(await sync.verifyUnverifiedTail(), 'held');
        assert.deepStrictEqual(markerWrites(writes), ['110']);
    });

    it('concurrent callers share one walk', async function(){
        let { sync, state } = buildClient({ tip: 110 });
        state.set(KEY, '100');
        let [a, b] = await Promise.all([sync.verifyUnverifiedTail(), sync.verifyUnverifiedTail()]);
        assert.deepStrictEqual([a, b], ['ok', 'ok']);
        assert.strictEqual(sync.blockHasher.computeBlockHashes.callCount, 10);
    });

    it('is inert with VERIFY_RECOMPUTE off', async function(){
        let off = buildClient({ tip: 110, config: { VERIFY_RECOMPUTE: false } });
        assert.strictEqual(await off.sync.verifyUnverifiedTail(), 'ok');
        assert.strictEqual(off.db.getLastBlock.called, false);
    });
});
