// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

const assert = require('assert');
const sinon = require('sinon');
const ClientSync = require('../../../../src/client/sync');

function startupContext(depth, getLastBlock){
    const sync = Object.create(ClientSync.prototype);
    sync.db = { getLastBlock };
    sync['_truncatedDepth'] = depth;
    sync.restorePendingHalt = sinon.stub().resolves(false);
    sync.bootstrapFromSnapshot = sinon.stub().resolves();
    sync.bootstrapFromHeightRetry = sinon.stub().resolves();
    sync.fetchAndApplySchema = sinon.stub().resolves();
    sync.verifyUnverifiedTail = sinon.stub().resolves('ok');
    sync.incrementalCatchUp = sinon.stub().resolves();
    sync.sources = ['http://source'];
    sync.chain = 'bitcoin'; sync.network = 'mainnet'; sync.dbType = 'indexer';
    return sync;
}

function haltCopyContext(getSyncState, sleep){
    const sync = Object.create(ClientSync.prototype);
    sync.db = { getSyncState };
    sync.dbType = 'indexer'; sync.chain = 'bitcoin'; sync.network = 'mainnet';
    sync.replicaLabel = () => 'bitcoin/mainnet/indexer';
    sync.util = { sleep };
    sync.running = true;
    sync['_halted'] = null;
    return sync;
}

function deferred(){
    let resolve;
    const promise = new Promise(r => { resolve = r; });
    return { promise, resolve };
}

function stoppableStartupSync(){
    const sync = Object.create(ClientSync.prototype);
    Object.assign(sync, { chain: 'bitcoin', network: 'mainnet', dbType: 'indexer', running: false });
    Object.assign(sync, {
        pendingHashes: new Map(), _strictConfirmPending: new Set(), _applyTimers: new Map(),
        _pendingReorgs: new Map(), _strictReorgPending: new Set(), _reconnectTimers: new Map(),
        _streamWatchdogs: new Map(), _sourceLastWsEventAt: new Map(), _sourceLastReportedTip: new Map(),
        _sourcesNeedingRecovery: new Set(), _inFlightWork: new Set(), wsConns: []
    });
    sync['_stopping'] = false;
    sync.warnTrustPosture = sinon.stub();
    sync.resolveStartupHalt = sinon.stub().resolves('continue');
    sync.holdForDurableStartupState = sinon.stub().resolves('continue');
    sync.holdForRollbackGuardState = sinon.stub().resolves('continue');
    sync.ensureLiveFollowTip = sinon.stub();
    sync.prepareLiveFollow = sinon.stub().resolves();
    sync.beginLiveFollow = sinon.stub().callsFake(() => { sync.running = false; });
    sync.cancelHaltPersistRetry = sinon.stub();
    sync.clearAllPendingReorgs = sinon.stub();
    sync.forgetSourceRetry = sinon.stub();
    sync.util = { sleep: sinon.stub().resolves() };
    return sync;
}

// Startup tip reads: a read fault propagates, only a missing blocks table means an empty replica.
function startupTipReadCases(){
    it('propagates startup tip read faults without entering either bootstrap path', async function(){
        for(const depth of [0, 500]){
            const read = sinon.stub().rejects(Object.assign(new Error('lost query'), { errno: 2013 }));
            const sync = startupContext(depth, read);
            await assert.rejects(() => sync.synchronizeStoredReplica(), /lost query/);
            assert.strictEqual(sync.bootstrapFromSnapshot.called, false);
            assert.strictEqual(sync.bootstrapFromHeightRetry.called, false);
            assert.ok(read.calledOnceWithExactly(null, { rethrow: true }));
        }
    });

    it('treats only a missing blocks table as an empty replica', async function(){
        for(const depth of [0, 500]){
            const missingTable = sinon.stub().rejects(Object.assign(new Error('missing'), { errno: 1146 }));
            const empty = startupContext(depth, missingTable);
            await empty.synchronizeStoredReplica();
            assert.strictEqual(empty.bootstrapFromSnapshot.calledOnce, depth === 0);
            assert.strictEqual(empty.bootstrapFromHeightRetry.calledOnce, depth !== 0);
        }

        const missingColumn = sinon.stub().rejects(Object.assign(new Error('bad schema'), { errno: 1054 }));
        const truncated = startupContext(500, missingColumn);
        await assert.rejects(() => truncated.synchronizeStoredReplica(), /bad schema/);
        assert.strictEqual(truncated.bootstrapFromHeightRetry.called, false);
    });
}

// Halt-copy reads: a transient failure is retried and the recovered copy is installed or held.
function haltCopyRetryCases(){
    it('retries a transient halt-copy read and resumes when the copy is absent', async function(){
        const read = sinon.stub();
        read.onFirstCall().rejects(new Error('temporary'));
        read.onSecondCall().resolves(null);
        const sleep = sinon.stub().resolves();
        const sync = haltCopyContext(read, sleep);

        assert.strictEqual(await sync.restorePendingHalt(), false);
        assert.strictEqual(sync['_halted'], null);
        assert.deepStrictEqual(sleep.args.map(a => a[0]), [5000]);
        assert.strictEqual(read.callCount, 2);
    });

    it('installs a valid halt copy when the retry read recovers', async function(){
        const read = sinon.stub();
        read.onFirstCall().rejects(new Error('temporary'));
        read.onSecondCall().resolves(JSON.stringify({
            blockIndex: 44, reason: 'no-source-quorum', mismatches: [], sources: ['source']
        }));
        const sync = haltCopyContext(read, sinon.stub().resolves());
        sync.persistHalt = sinon.stub().resolves(true);

        assert.strictEqual(await sync.restorePendingHalt(), true);
        assert.strictEqual(sync['_halted'].reason, 'no-source-quorum');
        assert.strictEqual(sync['_halted'].blockIndex, 44);
        assert.strictEqual(sync.persistHalt.calledOnceWithExactly(sync['_halted']), true);
    });

    it('turns a malformed retry result into a clearable unreadable-copy hold', async function(){
        const read = sinon.stub();
        read.onFirstCall().rejects(new Error('temporary'));
        read.onSecondCall().resolves('{not json');
        const sync = haltCopyContext(read, sinon.stub().resolves());

        assert.strictEqual(await sync.restorePendingHalt(), true);
        assert.strictEqual(sync['_halted'].reason, 'halt-copy-unreadable');
        assert.strictEqual(sync['_pendingHaltCopy'], true);
    });
}

// Halt-copy retries that keep failing hold the client, with the delay doubling to a one minute cap.
function haltCopyHoldCases(){
    it('keeps holding when retries fail and stops retrying with the client', async function(){
        const read = sinon.stub().rejects(new Error('still down'));
        const state = {};
        const delays = [];
        const sleep = sinon.stub().callsFake(async delay => {
            delays.push(delay);
            if(delays.length === 4) state.sync.running = false;
        });
        const sync = haltCopyContext(read, sleep);
        state.sync = sync;

        assert.strictEqual(await sync.restorePendingHalt(), true);
        assert.strictEqual(sync['_halted'].reason, 'halt-state-check-failed');
        assert.deepStrictEqual(delays, [5000, 10000, 20000, 40000]);
        assert.strictEqual(read.callCount, 4);
    });

    it('caps repeated halt-copy retry delays at one minute', async function(){
        const read = sinon.stub().rejects(new Error('still down'));
        const delays = [];
        const sync = haltCopyContext(read, sinon.stub().callsFake(async delay => {
            delays.push(delay);
            if(delays.length === 6) sync.running = false;
        }));

        assert.strictEqual(await sync.restorePendingHalt(), true);
        assert.deepStrictEqual(delays, [5000, 10000, 20000, 40000, 60000, 60000]);
        assert.strictEqual(read.callCount, 6);
    });
}

function lifecycleTerminalCases(){
    it('returns stop when shutdown lands while a cleared startup halt is sleeping', async function(){
        const sync = Object.create(ClientSync.prototype);
        sync.running = true;
        sync['_stopping'] = false;
        sync['_halted'] = { reason: 'test-halt' };
        sync.util = { sleep: sinon.stub().callsFake(async () => {
            sync['_halted'] = null;
            sync.running = false;
            sync['_stopping'] = true;
        }) };

        assert.strictEqual(await sync.waitForStartupHalt(), 'stop');
        assert.strictEqual(sync.util.sleep.calledOnce, true);
    });

    it('does not enter startup after stop has made the client terminal', async function(){
        const sync = Object.create(ClientSync.prototype);
        sync.running = false;
        sync['_stopping'] = true;
        sync.warnTrustPosture = sinon.stub();
        sync.resolveStartupHalt = sinon.stub().resolves('stop');

        await sync.start();

        assert.strictEqual(sync.resolveStartupHalt.called, false);
        assert.strictEqual(sync.running, false);
    });

    it('restarts startup guards after an operator clears the copy-read hold', async function(){
        const sync = Object.create(ClientSync.prototype);
        Object.assign(sync, { chain: 'bitcoin', network: 'mainnet', dbType: 'indexer' });
        sync['_halted'] = null;
        sync.warnTrustPosture = sinon.stub();
        sync.resolveStartupHalt = sinon.stub().resolves('continue');
        sync.loadBootstrapBase = sinon.stub().resolves();
        sync.loadVerifiedCheckpointBlock = sinon.stub().resolves();
        sync.loadVerifiedCheckpointSeq = sinon.stub().resolves();
        sync.holdForRollbackGuardState = sinon.stub().resolves('continue');
        sync.synchronizeStoredReplica = sinon.stub();
        sync.synchronizeStoredReplica.onFirstCall().resolves('halted');
        sync.synchronizeStoredReplica.onSecondCall().callsFake(async () => { sync.lastAppliedBlock = 10; });
        sync.ensureLiveFollowTip = sinon.stub();
        sync.prepareLiveFollow = sinon.stub().resolves();
        sync.beginLiveFollow = sinon.stub().callsFake(() => { sync.running = false; });

        await sync.start();

        assert.strictEqual(sync.synchronizeStoredReplica.callCount, 2);
        assert.strictEqual(sync.beginLiveFollow.calledOnce, true);
    });
}

function shutdownDuringStartupCases(){
    it('drains paused startup and does not enter live follow after stop @regression', async function(){
        const sync = stoppableStartupSync();
        const gate = deferred();
        sync.synchronizeStoredReplica = sinon.stub().returns(gate.promise);
        const started = sync.start();
        await new Promise(resolve => setImmediate(resolve));

        let drained = false;
        const stopping = sync.stop().then(() => { drained = true; });
        await new Promise(resolve => setImmediate(resolve));
        assert.strictEqual(drained, false);

        gate.resolve();
        await Promise.all([started, stopping]);
        assert.strictEqual(sync.ensureLiveFollowTip.called, false);
        assert.strictEqual(sync.prepareLiveFollow.called, false);
        assert.strictEqual(sync.beginLiveFollow.called, false);
    });

    it('does not begin live follow when stop lands during preparation @regression', async function(){
        const sync = stoppableStartupSync();
        const gate = deferred();
        sync.synchronizeStoredReplica = sinon.stub().resolves();
        sync.prepareLiveFollow = sinon.stub().returns(gate.promise);
        const started = sync.start();
        await new Promise(resolve => setImmediate(resolve));

        const stopping = sync.stop();
        gate.resolve();
        await Promise.all([started, stopping]);

        assert.strictEqual(sync.beginLiveFollow.called, false);
    });

}

function shutdownBootstrapCases(){
    it('does not apply a snapshot downloaded after stop @regression', async function(){
        const sync = stoppableStartupSync();
        const gate = deferred();
        sync.sources = ['http://source'];
        sync.activeSourceIndices = sinon.stub().returns([0]);
        sync.primarySourceIndex = sinon.stub().returns(0);
        sync.fetchAndApplySchema = sinon.stub().resolves();
        sync.bootstrapDownloadSnapshot = sinon.stub().returns(gate.promise);
        sync.bootstrapApplySnapshot = sinon.stub().resolves(true);

        const bootstrapping = sync.bootstrapRotateSources();
        await new Promise(resolve => setImmediate(resolve));
        sync['_stopping'] = true;
        gate.resolve({ block_height: 10 });
        await bootstrapping;

        assert.strictEqual(sync.bootstrapApplySnapshot.called, false);
    });

    it('finalizes a snapshot marker when stop lands after its commit @regression', async function(){
        const sync = stoppableStartupSync();
        sync.primarySource = sinon.stub().returns('http://source');
        sync.config = {
            BOOTSTRAP_MAX_RETRIES: 1, BOOTSTRAP_RETRY_BASE_MS: 1,
            BOOTSTRAP_RETRY_MAX_MS: 1
        };
        sync.bootstrapRotateSources = sinon.stub().callsFake(async () => {
            sync.lastAppliedBlock = 10;
            sync['_stopping'] = true;
            return true;
        });
        sync.adoptVerifiedTip = sinon.stub().resolves();

        await sync.bootstrapFromSnapshot();

        assert.strictEqual(sync.adoptVerifiedTip.calledOnceWithExactly(10), true);
    });
}

// A retry sleep that never elapses: only the stop signal can end the hold.
async function assertStopSettlesHold(sync, run){
    sync.util = { sleep: sinon.stub().returns(new Promise(() => {})) };
    sync.running = true;
    sync.armStopSignal();
    sync.replicaLabel = () => 'bitcoin/mainnet/indexer';
    let result;
    const holding = run().then(value => { result = value; });
    await new Promise(resolve => setImmediate(resolve));
    assert.strictEqual(sync.util.sleep.called, true);
    await sync.stop();
    await holding;
    return result;
}

function shutdownHoldCases(){
    it('settles the durable guard retry hold when stop lands @regression', async function(){
        const sync = stoppableStartupSync();
        sync.loadBootstrapBase = sinon.stub().rejects(new Error('read failed'));
        const result = await assertStopSettlesHold(sync, () => ClientSync.prototype.holdForDurableStartupState.call(sync));
        assert.strictEqual(result, 'stop');
    });

    it('settles the rollback guard unreadable hold when stop lands @regression', async function(){
        const sync = stoppableStartupSync();
        sync.loadRollbackGuardState = sinon.stub().resolves('unreadable');
        const result = await assertStopSettlesHold(sync, () => ClientSync.prototype.holdForRollbackGuardState.call(sync));
        assert.strictEqual(result, 'stop');
    });

    it('settles the rollback guard invalid hold when stop lands @regression', async function(){
        const sync = stoppableStartupSync();
        sync.loadRollbackGuardState = sinon.stub().resolves('invalid');
        const result = await assertStopSettlesHold(sync, () => ClientSync.prototype.holdForRollbackGuardState.call(sync));
        assert.strictEqual(result, 'stop');
    });

    it('settles the halt copy read retry hold when stop lands @regression', async function(){
        const sync = stoppableStartupSync();
        sync.db = { getSyncState: sinon.stub().rejects(new Error('read failed')) };
        sync.pendingHaltKey = () => 'pending_halt';
        sync['_halted'] = null;
        const result = await assertStopSettlesHold(sync, () => ClientSync.prototype.restorePendingHalt.call(sync));
        assert.strictEqual(result, true);
    });
}

describe('ClientSync startup lifecycle integrity @regression', function(){
    beforeEach(function(){
        sinon.stub(console, 'log'); sinon.stub(console, 'error'); sinon.stub(console, 'warn');
    });
    afterEach(function(){ sinon.restore(); });

    startupTipReadCases();

    haltCopyRetryCases();

    haltCopyHoldCases();

    lifecycleTerminalCases();

    shutdownDuringStartupCases();

    shutdownBootstrapCases();

    shutdownHoldCases();
});
