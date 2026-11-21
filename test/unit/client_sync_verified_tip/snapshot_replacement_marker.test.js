// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// A full snapshot replaces every row, so the verified-tip marker that vouched for
// the old rows is lowered below the snapshot tip before the replacement, and a
// restart inside the window between the snapshot commit and the end of its
// bootstrap checks recomputes the tip and re-runs the source cross-check.

'use strict';

const assert = require('assert');
const sinon = require('sinon');
const axios = require('axios');
const ClientSync = require('../../../src/client/sync');
const Utility = require('../../../src/util');
const HashVerifier = require('../../../src/client/hash_verifier');
const { withDbMixins } = require('../../helpers/db_mixins.js');

const MARKER = 'verified_tip:indexer';
const PENDING = 'snapshot_pending:indexer';

function makeSync(configOverrides, state){
    state = state || new Map();
    const db = withDbMixins({
        dbName: 'test_db', dbType: 'indexer',
        getLastBlock: sinon.stub().resolves(null),
        getBlockHashRow: sinon.stub().resolves(null),
        getActiveHalt: sinon.stub().resolves(null),
        getTableCount: sinon.stub().resolves(0),
        doQuery: sinon.stub().resolves([]),
        recordHalt: sinon.stub().resolves({ block_index: 0 }),
        clearHalt: sinon.stub().resolves(1),
        getSyncState: sinon.stub().callsFake(async key => state.has(key) ? state.get(key) : null),
        setSyncState: sinon.stub().callsFake(async (key, value) => { state.set(key, value); return true; }),
        deleteSyncState: sinon.stub().callsFake(async key => { state.delete(key); return true; })
    });
    const applier = { applyBlock: sinon.stub().resolves(), applyFullSnapshot: sinon.stub().resolves(),
        applyIncrementalSnapshot: sinon.stub().resolves() };
    const config = Object.assign({
        SYNC_SOURCES: 'source', VERIFY_HASHES: false, VERIFY_RECOMPUTE: true, MAX_ROLLBACK_DEPTH: 10,
        SNAPSHOT_MAX_CONTENT: 1024, LOOKUP_PAGE_SIZE: 50, BOOTSTRAP_MAX_RETRIES: 0,
        BOOTSTRAP_RETRY_BASE_MS: 1, BOOTSTRAP_RETRY_MAX_MS: 1, HASH_CONFIRM_TIMEOUT: 5000
    }, configOverrides || {});
    const sync = new ClientSync('bitcoin', 'mainnet', db, applier,
        { rollback: sinon.stub().resolves() }, new HashVerifier(), config, new Utility());
    sinon.stub(sync, 'checkTrainActivation').resolves(false);
    sinon.stub(sync, 'refreshTipHashes').resolves();
    return { sync, db, applier, state };
}

const MULTI = { SYNC_SOURCES: 'a,b', VERIFY_HASHES: true };

// Apply a snapshot at `height` over a stored marker, recording what the replacement saw.
async function applyOver(marker, height, configOverrides){
    const ctx = makeSync(configOverrides);
    if(marker !== null) ctx.state.set(MARKER, marker);
    let seen = null;
    ctx.applier.applyFullSnapshot.callsFake(async () => {
        seen = { marker: ctx.state.get(MARKER), pending: ctx.state.get(PENDING) };
    });
    assert.strictEqual(await ctx.sync.bootstrapApplySnapshot({ block_height: height, tables: {} }, 0), true);
    return Object.assign(ctx, { seen });
}

describe('ClientSync full snapshot lowers the replaced marker @regression', function(){
    beforeEach(function(){ for(const m of ['log', 'warn', 'error']) sinon.stub(console, m); });
    afterEach(function(){ sinon.restore(); });

    it('lowers a same-height marker below the snapshot tip before the rows are replaced', async function(){
        const { seen } = await applyOver('200', 200);
        assert.strictEqual(seen.marker, '199');
    });

    it('lowers a marker above the snapshot tip to just below it', async function(){
        const { seen } = await applyOver('300', 200);
        assert.strictEqual(seen.marker, '199');
    });

    it('never raises a marker that is already below the snapshot tip', async function(){
        const { seen } = await applyOver('100', 200);
        assert.strictEqual(seen.marker, '100');
    });

    it('refuses the replacement when the marker lowering cannot be written', async function(){
        const ctx = makeSync();
        ctx.state.set(MARKER, '200');
        ctx.db.setSyncState.rejects(new Error('write failed'));
        await assert.rejects(() => ctx.sync.bootstrapApplySnapshot({ block_height: 200, tables: {} }, 0),
            /write failed/);
        assert.strictEqual(ctx.applier.applyFullSnapshot.called, false);
        assert.strictEqual(ctx.sync._rowRemovalsInFlight, 0);
    });

    it('records the pending cross-check before the replacement when peers exist', async function(){
        const { seen } = await applyOver('200', 200, MULTI);
        assert.deepStrictEqual(JSON.parse(seen.pending), { height: 200, source: 0 });
    });

    it('writes no pending cross-check on a single-source replica', async function(){
        const { seen } = await applyOver('200', 200);
        assert.strictEqual(seen.pending, undefined);
    });
});

// A restart at stored tip `tip` with the marker just below it and an optional pending record.
function restartAt(tip, pending){
    const state = new Map([[MARKER, String(tip - 1)]]);
    if(pending) state.set(PENDING, JSON.stringify(pending));
    const ctx = makeSync(MULTI, state);
    ctx.db.getLastBlock.resolves(tip);
    sinon.stub(ctx.sync, 'fetchAndApplySchema').resolves();
    sinon.stub(ctx.sync, 'healTailLookupsBeforeWalk').resolves('ok');
    sinon.stub(ctx.sync, 'incrementalCatchUp').resolves();
    sinon.stub(ctx.sync, 'bootstrapVerifySnapshot').resolves(true);
    sinon.stub(ctx.sync, 'evaluateCommittedBlock').resolves({ verdict: 'ok' });
    return ctx;
}

describe('ClientSync restart inside a snapshot bootstrap window @regression', function(){
    beforeEach(function(){ for(const m of ['log', 'warn', 'error', 'info']) sinon.stub(console, m); });
    afterEach(function(){ sinon.restore(); });

    it('re-runs the cross-check, then recomputes the snapshot tip, after a stop past the commit', async function(){
        const { sync, state } = restartAt(200, { height: 200, source: 1 });
        await sync.synchronizeStoredReplica();
        assert.deepStrictEqual(sync.bootstrapVerifySnapshot.firstCall.args, [1]);
        assert.ok(sync.bootstrapVerifySnapshot.calledBefore(sync.evaluateCommittedBlock));
        assert.deepStrictEqual(sync.evaluateCommittedBlock.args.map(a => a[0]), [200]);
        assert.strictEqual(state.has(PENDING), false);
        assert.strictEqual(sync.incrementalCatchUp.calledOnce, true);
    });

    it('stays halted with the record kept when the resumed cross-check halts', async function(){
        const { sync, state } = restartAt(200, { height: 200, source: 0 });
        sync.bootstrapVerifySnapshot.callsFake(async () => { sync._halted = { blockIndex: 200 }; return false; });
        assert.strictEqual(await sync.synchronizeStoredReplica(), 'halted');
        assert.strictEqual(sync.evaluateCommittedBlock.called, false);
        assert.strictEqual(sync.incrementalCatchUp.called, false);
        assert.strictEqual(state.has(PENDING), true);
    });

    it('drops a record for a snapshot that never committed without a cross-check', async function(){
        const { sync, state } = restartAt(150, { height: 200, source: 0 });
        await sync.synchronizeStoredReplica();
        assert.strictEqual(sync.bootstrapVerifySnapshot.called, false);
        assert.strictEqual(state.has(PENDING), false);
    });

    it('falls back to the primary source when the recorded source is gone', async function(){
        const { sync } = restartAt(200, { height: 200, source: 7 });
        await sync.synchronizeStoredReplica();
        assert.deepStrictEqual(sync.bootstrapVerifySnapshot.firstCall.args, [undefined]);
    });

});

describe('ClientSync resumed snapshot cross-check end to end @regression', function(){
    beforeEach(function(){ for(const m of ['log', 'warn', 'error', 'info']) sinon.stub(console, m); });
    afterEach(function(){ sinon.restore(); });

    it('halts on a disagreeing peer when the real cross-check resumes, keeping the record', async function(){
        const ctx = restartAt(200, { height: 200, source: 0 });
        ctx.sync.bootstrapVerifySnapshot.restore();
        ctx.sync.config.HALT_ON_DIVERGENCE = true;
        ctx.db.getBlockHashRow.resolves({ block_index: 200, ledger_hash: 'L200', actions_hash: 'A200', contract_hash: 'C200' });
        const peer = sinon.stub(axios, 'get').resolves({ data: {
            block_height: 200, ledger_hash: 'OTHER', actions_hash: 'A200', contract_hash: 'C200' } });
        sinon.stub(ctx.sync, 'haltOnDivergence').callsFake(async (h, m, s, reason) => {
            ctx.sync._halted = { blockIndex: h, reason };
        });
        assert.strictEqual(await ctx.sync.synchronizeStoredReplica(), 'halted');
        assert.ok(peer.firstCall.args[0].startsWith('b/status/indexer/'), peer.firstCall.args[0]);
        assert.strictEqual(ctx.sync.haltOnDivergence.firstCall.args[3], 'cross-source-divergence');
        assert.strictEqual(ctx.sync.evaluateCommittedBlock.called, false);
        assert.strictEqual(ctx.state.has(PENDING), true);
    });

    it('an uninterrupted bootstrap ends with the marker at the tip and no pending record', async function(){
        const ctx = makeSync(MULTI);
        ctx.state.set(MARKER, '50');
        sinon.stub(ctx.sync, 'fetchAndApplySchema').resolves();
        sinon.stub(ctx.sync, 'bootstrapDownloadSnapshot').resolves({ block_height: 200, tables: {} });
        sinon.stub(ctx.sync, 'bootstrapVerifySnapshot').resolves(true);
        await ctx.sync.bootstrapFromSnapshot();
        assert.strictEqual(ctx.state.get(MARKER), '200');
        assert.strictEqual(ctx.state.has(PENDING), false);
    });
});

describe('ClientSync operator clear of a bootstrap cross-check halt @regression', function(){
    beforeEach(function(){ for(const m of ['log', 'warn', 'error', 'info']) sinon.stub(console, m); });
    afterEach(function(){ sinon.restore(); });

    it('an operator clear of a bootstrap cross-check halt lets the restart resume and recompute the tip', async function(){
        sinon.stub(axios, 'get').resolves({ data: {
            block_height: 200, ledger_hash: 'OTHER', actions_hash: 'A200', contract_hash: 'C200' } });
        const hashRow = { block_index: 200, ledger_hash: 'L200', actions_hash: 'A200', contract_hash: 'C200' };
        const haltAs = sync => sinon.stub(sync, 'haltOnDivergence').callsFake(async (h, m, s, reason) => {
            sync._halted = { blockIndex: h, reason };
        });
        const boot = makeSync(Object.assign({ HALT_ON_DIVERGENCE: true }, MULTI));
        boot.state.set(MARKER, '200');
        boot.db.getBlockHashRow.resolves(hashRow);
        sinon.stub(boot.sync, 'fetchAndApplySchema').resolves();
        sinon.stub(boot.sync, 'bootstrapDownloadSnapshot').resolves({ block_height: 200, tables: {} });
        haltAs(boot.sync);
        await assert.rejects(() => boot.sync.bootstrapFromSnapshot(), /cross-source-divergence/);
        assert.strictEqual(boot.state.has(PENDING), true);
        await boot.sync.clearHalt();
        assert.strictEqual(boot.state.has(PENDING), false);
        assert.strictEqual(boot.state.get(MARKER), '199');

        const ctx = restartAt(200);
        for(const [k, v] of boot.state) ctx.state.set(k, v);
        ctx.sync.bootstrapVerifySnapshot.restore();
        ctx.sync.config.HALT_ON_DIVERGENCE = true;
        ctx.db.getBlockHashRow.resolves(hashRow);
        haltAs(ctx.sync);
        assert.notStrictEqual(await ctx.sync.synchronizeStoredReplica(), 'halted');
        assert.strictEqual(ctx.sync._halted, null);
        assert.deepStrictEqual(ctx.sync.evaluateCommittedBlock.args.map(a => a[0]), [200]);
        assert.strictEqual(ctx.sync.incrementalCatchUp.calledOnce, true);
    });

    it('an operator clear still lands when the pending record cannot be deleted', async function(){
        const ctx = makeSync(MULTI);
        ctx.state.set(PENDING, JSON.stringify({ height: 200, source: 0 }));
        ctx.sync._halted = { blockIndex: 200, reason: 'cross-source-divergence' };
        ctx.db.deleteSyncState.callsFake(async key => {
            if(key === PENDING) return false;
            ctx.state.delete(key);
            return true;
        });
        await ctx.sync.clearHalt();
        assert.strictEqual(ctx.sync._halted, null);
        assert.strictEqual(ctx.db.clearHalt.calledOnce, true);
        assert.strictEqual(ctx.state.has(PENDING), true);
        assert.ok(console.error.args.some(a => String(a[0]).includes('Could not remove the ' + PENDING)));
    });

    it('an operator clear of a halt at another height keeps the pending cross-check', async function(){
        const ctx = makeSync(MULTI);
        ctx.state.set(PENDING, JSON.stringify({ height: 200, source: 0 }));
        ctx.sync._halted = { blockIndex: 150, reason: 'train-activation' };
        await ctx.sync.clearHalt();
        assert.strictEqual(ctx.sync._halted, null);
        assert.strictEqual(ctx.state.has(PENDING), true);
    });
});
