// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

'use strict';

const assert = require('assert');
const sinon = require('sinon');
const ClientSync = require('../../../../src/client/sync');
const Utility = require('../../../../src/util');
const HashVerifier = require('../../../../src/client/hash_verifier');

const BASE_KEY = 'bootstrap_base:indexer';
const BLOCK_KEY = 'verified_checkpoint_block:indexer';
const SEQ_KEY = 'verified_checkpoint_seq:indexer';
const GUARD_KEY = 'rollback_guard:indexer';
const HOLD = 'startup-guard-state-unreadable';

// The real Database.getSyncState resolves null on a fault unless opts.rethrow is set.
function productionShapedRead(state, faulted){
    return sinon.stub().callsFake(async (key, opts) => {
        if(faulted(key)){
            if(opts && opts.rethrow) throw new Error('lost query');
            return null;
        }
        return state.has(key) ? state.get(key) : null;
    });
}

function makeSync(getSyncState){
    const db = {
        dbName: 'test_db', dbType: 'indexer',
        getLastBlock: sinon.stub().resolves(null),
        getBlockHashRow: sinon.stub().resolves(null),
        getActiveHalt: sinon.stub().resolves(null),
        clearHalt: sinon.stub().resolves(1),
        getSyncState,
        setSyncState: sinon.stub().resolves(true),
        deleteSyncState: sinon.stub().resolves(true),
        doQuery: sinon.stub().resolves([])
    };
    const config = {
        SYNC_SOURCES: 'http://source:3006', MAX_ROLLBACK_DEPTH: 5,
        MAX_ROLLBACK_DEPTH_EXPLICIT: true, VERIFY_RECOMPUTE: false
    };
    const sync = new ClientSync('bitcoin', 'mainnet', db, {}, {}, new HashVerifier(), config, new Utility());
    sync.running = true;
    return { sync, db };
}

function registerHoldRestoreTests(){
    for(const [name, key, value] of [
        ['truncation floor', BASE_KEY, '900'],
        ['verified checkpoint block', BLOCK_KEY, '450'],
        ['verified checkpoint seq', SEQ_KEY, '7']
    ]){
        it('holds startup on a ' + name + ' read fault and restores it before the replica syncs', async function(){
            let faults = 1;
            const state = new Map([[key, value]]);
            const read = productionShapedRead(state, k => k === key && faults > 0);
            const { sync } = makeSync(read);
            const seen = [];
            sinon.stub(sync, 'synchronizeStoredReplica').callsFake(async () => {
                seen.push({ truncated: sync.isTruncated(), halted: sync._halted });
            });
            sinon.stub(sync, 'ensureLiveFollowTip');
            sinon.stub(sync, 'prepareLiveFollow').resolves();
            sinon.stub(sync, 'beginLiveFollow').callsFake(() => { sync.running = false; });
            const reasons = [];
            sync.util.sleep = async () => { reasons.push(sync._halted && sync._halted.reason); faults--; };

            await sync.start();

            assert.deepStrictEqual(reasons, [HOLD]);
            assert.strictEqual(seen.length, 1);
            assert.strictEqual(seen[0].halted, null);
            assert.strictEqual(seen[0].truncated, key === BASE_KEY);
            if(key === BASE_KEY) assert.strictEqual(sync.getBootstrapBase(), 900);
            if(key === BLOCK_KEY) assert.strictEqual(sync._lastVerifiedCheckpointBlock, 450);
            if(key === SEQ_KEY) assert.strictEqual(sync._lastVerifiedCheckpointSeq, 7);
        });
    }
}

function registerHoldFaultTests(){
    it('retries a persistent fault with doubling delays capped at a minute and never syncs', async function(){
        const read = productionShapedRead(new Map([[BASE_KEY, '900']]), k => k === BASE_KEY);
        const { sync } = makeSync(read);
        sinon.stub(sync, 'synchronizeStoredReplica').resolves();
        const delays = [];
        sync.util.sleep = async ms => { delays.push(ms); if(delays.length === 6) sync.running = false; };

        await sync.start();

        assert.deepStrictEqual(delays, [5000, 10000, 20000, 40000, 60000, 60000]);
        assert.strictEqual(sync.synchronizeStoredReplica.called, false);
        assert.strictEqual(sync.isTruncated(), false);
        assert.strictEqual(sync.getHaltInfo().reason, HOLD);
    });

    it('does not hold when the guard rows are absent on a full-history replica', async function(){
        const { sync } = makeSync(productionShapedRead(new Map(), () => false));
        const slept = sinon.stub().resolves();
        sync.util.sleep = slept;

        assert.strictEqual(await sync.holdForDurableStartupState(), 'continue');

        assert.strictEqual(slept.called, false);
        assert.strictEqual(sync.isTruncated(), false);
        assert.strictEqual(sync._halted, null);
    });

    it('holds on a rollback guard read fault through the production-shaped read', async function(){
        const state = new Map([[GUARD_KEY, JSON.stringify({ peak: 100, low: 98 })]]);
        let faults = 1;
        const { sync } = makeSync(productionShapedRead(state, k => k === GUARD_KEY && faults > 0));
        const reasons = [];
        sync.util.sleep = async () => { reasons.push(sync._halted && sync._halted.reason); faults--; };

        assert.strictEqual(await sync.holdForRollbackGuardState(), 'continue');

        assert.deepStrictEqual(reasons, ['rollback-guard-state-unreadable']);
        assert.deepStrictEqual(sync.rollbackGuardState(), { peak: 100, low: 98 });
    });
}

function registerOperatorClearTests(){
    for(const [hold, hang] of [[HOLD, 'startup'], ['rollback-guard-state-unreadable', 'guard']]){
        it('an operator clear during the ' + hold + ' hold leaves the database alone and the read still gates startup', async function(){
            const state = new Map([[BASE_KEY, '900'], [GUARD_KEY, JSON.stringify({ peak: 9, low: 8 })]]);
            let faults = 2;
            const read = productionShapedRead(state, k => (hang === 'startup' ? k === BASE_KEY : k === GUARD_KEY) && faults > 0);
            const { sync, db } = makeSync(read);
            const reasons = [];
            sync.util.sleep = async () => {
                reasons.push(sync._halted && sync._halted.reason);
                if(reasons.length === 1) await sync.clearHalt();
                faults--;
            };
            const runHold = hang === 'startup' ? () => sync.holdForDurableStartupState() : () => sync.holdForRollbackGuardState();

            assert.strictEqual(await runHold(), 'continue');

            assert.deepStrictEqual(reasons, [hold, hold]);
            assert.strictEqual(db.clearHalt.called, false);
            assert.strictEqual(db.deleteSyncState.called, false);
            assert.strictEqual(sync._halted, null);
        });
    }
}

describe('ClientSync durable startup guards', function(){
    beforeEach(function(){
        sinon.stub(console, 'log'); sinon.stub(console, 'error'); sinon.stub(console, 'warn');
    });
    afterEach(function(){ sinon.restore(); });

    registerHoldRestoreTests();
    registerHoldFaultTests();
    registerOperatorClearTests();
});
