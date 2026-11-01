// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

const assert     = require('assert');
const sinon      = require('sinon');
const ClientSync = require('../../../src/client/sync');
const Utility    = require('../../../src/util');
const HashVerifier = require('../../../src/client/hash_verifier');
const { withDbMixins } = require('../../helpers/db_mixins.js');

function createMockDb(overrides){
    return Object.assign({
        dbName:           'test_db',
        dbType:           'indexer',
        getLastBlock:     sinon.stub().resolves(null),
        getBlockHashRow:  sinon.stub().resolves(null),
        doQuery:          sinon.stub().resolves([]),
        getActiveHalt:    sinon.stub().resolves(null),
        getTableCount:    sinon.stub().resolves(0),
        addMissingColumns: sinon.stub().resolves(),
        recordHalt:       sinon.stub().resolves({ block_index: 0 }),
        clearHalt:        sinon.stub().resolves(1)
    }, overrides || {});
}

function createMockApplier(){
    return {
        applyBlock:               sinon.stub().resolves(),
        applyFullSnapshot:        sinon.stub().resolves(),
        applyIncrementalSnapshot: sinon.stub().resolves()
    };
}

function createMockRollback(){
    return { rollback: sinon.stub().resolves() };
}

function makeSync(configOverrides, dbOverrides){
    let db      = createMockDb(dbOverrides);
    let applier = createMockApplier();
    let rb      = createMockRollback();
    let hv      = new HashVerifier();
    let util    = new Utility();
    let config  = Object.assign({
        SYNC_SOURCES:          'http://src1:3006',
        VERIFY_HASHES:         false,
        CLIENT_RECONNECT_DELAY: 5000,
        HASH_CONFIRM_TIMEOUT:  5000,
        SNAPSHOT_MAX_CONTENT:  200 * 1024 * 1024,
        WS_MAX_PAYLOAD:        50 * 1024 * 1024,
        MAX_ROLLBACK_DEPTH:    10,
        GAP_LOG_INTERVAL_MS:   30000
    }, configOverrides || {});
    let sync = new ClientSync('bitcoin', 'mainnet', withDbMixins(db), applier, rb, hv, config, util);
    return { sync, db, applier, rb, hv, util, config };
}
describe('ClientSync: stop()', function(){
    let sync;

    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });
    afterEach(function(){ sinon.restore(); });

    it('clears _hbTimer, closes all ws connections, empties wsConns', function(){
        ({ sync } = makeSync());
        sync.running = true;

        let closed = [];
        let ws1 = { close: sinon.stub().callsFake(() => closed.push(1)) };
        let ws2 = { close: () => { closed.push(2); throw new Error('close error'); } };
        sync._hbTimer = setTimeout(() => {}, 9999);
        sync.wsConns  = [ws1, ws2];

        sync.stop();

        assert.strictEqual(sync.running, false);
        assert.strictEqual(sync._hbTimer, null);
        assert.deepStrictEqual(closed, [1, 2], 'both close() calls attempted');
        assert.strictEqual(sync.wsConns.length, 0);
    });

    it('tolerates missing _hbTimer', function(){
        ({ sync } = makeSync());
        sync._hbTimer = null;
        sync.wsConns  = [];

        // Should not throw
        sync.stop();
        assert.strictEqual(sync.running, false);
    });
});

describe('ClientSync: stop()', function(){
    let sync;

    beforeEach(function(){
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });
    afterEach(function(){ sinon.restore(); });

    it('cancels block and reorg quorum timers before draining', async function(){
        let clock = sinon.useFakeTimers();
        let built = makeSync({ SYNC_SOURCES: 'http://a:3006,http://b:3006' });
        sync = built.sync;
        sync.lastAppliedBlock = 100;
        let blockEvent = { type: 'block', block_index: 101,
            ledger_hash: 'lh', actions_hash: 'ah', contract_hash: 'ch' };
        sync.pendingHashes.set(101, new Map([[0, blockEvent]]));
        let applyBlock = sinon.stub(sync, 'applyBlockEvent').resolves();
        sync.armBlockQuorumTimer(blockEvent);
        sync.recordReorgVote({ type: 'reorg', block_index: 95 }, 0, false);
        let applyReorg = sinon.stub(sync, 'applyAgreedReorg').resolves();
        sync.armReorgQuorumTimer(95);

        await sync.stop();
        await clock.tickAsync(sync.config.HASH_CONFIRM_TIMEOUT + 100);

        assert.strictEqual(applyBlock.called, false);
        assert.strictEqual(applyReorg.called, false);
        assert.strictEqual(sync._applyTimers.size, 0);
        assert.strictEqual(sync._pendingReorgs.size, 0);
        clock.restore();
    });

    it('does not arm quorum timers after stop', async function(){
        let clock = sinon.useFakeTimers();
        ({ sync } = makeSync({ SYNC_SOURCES: 'http://a:3006,http://b:3006' }));
        await sync.stop();
        let event = { type: 'block', block_index: 101,
            ledger_hash: 'lh', actions_hash: 'ah', contract_hash: 'ch' };
        sync.pendingHashes.set(101, new Map([[0, event]]));
        sync.armBlockQuorumTimer(event);
        sync.recordReorgVote({ type: 'reorg', block_index: 95 }, 0, false);
        sync.armReorgQuorumTimer(95);
        await clock.tickAsync(sync.config.HASH_CONFIRM_TIMEOUT + 100);
        assert.strictEqual(sync._applyTimers.size, 0);
        assert.strictEqual(sync._pendingReorgs.get(95).timer, null);
        clock.restore();
    });
});
