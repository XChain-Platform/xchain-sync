// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// Three ways a block could be vouched for without passing every check: a state-hash halt
// whose sync_halt write was lost, an interior block of a from-height bootstrap window,
// and a reorg that deletes rows while a walk is reading them. Each asserts the safe outcome.
const assert = require('assert');
const sinon  = require('sinon');
const ClientSync = require('../../../src/client/sync');
const HashVerifier = require('../../../src/client/hash_verifier');
const Utility = require('../../../src/util');
const { buildClient, hashesAt, KEY } = require('../../helpers/verified_tip_client');
const H = require('../../helpers/offline_replica');

describe('ClientSync: a state-hash halt whose sync_halt write fails, then restart @regression', function(){
    beforeEach(function(){ sinon.stub(console, 'log'); sinon.stub(console, 'error'); sinon.stub(console, 'warn'); sinon.stub(console, 'info'); });
    afterEach(function(){ sinon.restore(); });

    it('a restart must not resume past a live state_hash divergence whose halt write was lost', async function(){
        let c = buildClient({ tip: 199, config: { VERIFY_STATE_HASH: true } });
        c.state.set(KEY, '199');
        c.sync.lastAppliedBlock = 199;
        c.applier.applyBlock = sinon.stub().callsFake(async () => { c.db.tip = 200; });   // block 200 commits
        c.sync.blockHasher.computeStateHash = sinon.stub().resolves('LOCAL_STATE');
        c.sync.blockHasher.computeStateHashPreimage = sinon.stub().resolves({});
        c.db.recordHalt = sinon.stub().rejects(new Error('db down'));                      // halt write lost
        await c.sync.applyBlockEvent(Object.assign({ block_index: 200, block_time: 1, state_hash: 'SOURCE_STATE' }, hashesAt(200)));
        assert.strictEqual(c.sync.isHalted(), true, 'precondition: halted in memory');
        assert.strictEqual(c.sync.getHaltInfo().reason, 'state-hash-divergence');
        c.sync.cancelHaltPersistRetry();                                                   // process dies before the retry lands

        // Restart over the same DB: no sync_halt row exists.
        let util = new Utility(); util.sleep = async () => {};
        let r2 = new ClientSync('bitcoin', 'mainnet', c.db, c.applier, c.rollback, new HashVerifier(),
            Object.assign({}, c.sync.config), util);
        r2.blockHasher.computeBlockHashes = sinon.stub().callsFake(async h => hashesAt(h));
        r2.blockHasher.computeStateHash = sinon.stub().resolves('LOCAL_STATE');
        r2.fetchAndApplySchema = sinon.stub().resolves();
        r2.incrementalCatchUp = sinon.stub().resolves();
        r2.running = false;
        assert.strictEqual(await r2.resolveStartupHalt(), 'continue');
        await r2.synchronizeStoredReplica();
        assert.ok(r2.isHalted() || !r2.incrementalCatchUp.called,
            'restart resumed past the state_hash divergence at 200: marker=' + c.state.get(KEY) +
            ' catch-up from ' + (r2.incrementalCatchUp.firstCall && r2.incrementalCatchUp.firstCall.args[0]));
    });
});

describe('ClientSync: bootstrapFromHeight walks every block of its window @regression', function(){
    beforeEach(function(){ H.captureLogs(); });
    afterEach(function(){ H.sinon.restore(); });

    it('a tampered interior block of the bootstrap window must halt or stay above the marker', async function(){
        let origin = new H.Origin();
        for(let h = 1; h <= 20; h++) await origin.mint(h, H.stdSpec(h));
        H.serveOrigin(origin);
        origin.fault = H.snapTamper('changed_row', 15);
        let r = H.makeReplica();
        // Paged lookups: copy the origin's append-only lookup rows (the harness does not route them).
        r.sync.syncLookupTablesPaged = async () => {
            for(const t of ['index_actions', 'index_addresses', 'index_tickers', 'index_transactions'])
                for(const row of origin.db.rows(t)) r.store.insertRow(t, H.clone(row), true, false);
        };
        assert.strictEqual(await r.sync.bootstrapFromHeight(10), true);
        assert.strictEqual(r.sync.lastAppliedBlock, 20);
        assert.strictEqual(r.store.rows('credits').filter(c => c.amount === '999.5').length, 1,
            'precondition: the tampered block 15 credit is in the replica');
        let walk = await r.sync.verifyUnverifiedTail();
        let marker = r.store.state.get(KEY);
        assert.ok(r.sync.isHalted() || Number(marker) < 15,
            'tampered block 15 is vouched for: halted=' + r.sync.isHalted() + ' marker=' + marker + ' walk=' + walk);
    });
});

describe('ClientSync: a reorg racing the verified-tip walk @regression', function(){
    beforeEach(function(){ sinon.stub(console, 'log'); sinon.stub(console, 'error'); sinon.stub(console, 'warn'); sinon.stub(console, 'info'); });
    afterEach(function(){ sinon.restore(); });

    it('the durable marker must never sit above rows a reorg deleted', async function(){
        let { sync, state, db } = buildClient({ tip: 125, config: { VERIFY_RANGE_BATCH: 10 } });
        state.set(KEY, '100');
        sync.lastAppliedBlock = 125;
        let real = sync.blockHasher.computeBlockHashes;
        sync.blockHasher.computeBlockHashes = sinon.stub().callsFake(async h => {
            if(h === 105){ db.tip = 102; await sync.applyReorgRollback({ block_index: 103 }); }
            return real(h);
        });
        let walk = await sync.verifyUnverifiedTail();
        assert.ok(Number(state.get(KEY)) <= db.tip,
            'marker ' + state.get(KEY) + ' above tip ' + db.tip + ' after the reorg (walk=' + walk + ')');
    });
});
