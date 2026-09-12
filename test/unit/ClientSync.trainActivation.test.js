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
 * ClientSync: the platform-train activation gate on the follower.
 *
 * The property under test: a follower whose build carries no entry for the rule
 * set its signed release manifest requires STOPS at the activation height. It
 * does not apply the boundary block (or a snapshot range that reaches it) under
 * the old rules, it records the halt through the same durable sync_halt path as
 * a divergence so a restart cannot forget it, and the halt names the missing
 * rule set and the height. Continue-old is the failure this exists to prevent:
 * the recompute nets would halt one block later, after forked state was written
 * and served.
 *
 * The other half matters as much: a follower that DOES implement the required
 * rule set is clear at every height with no new log noise, and below the
 * boundary the lagging follower keeps applying while the verdict it publishes
 * already says where it will stop.
 ********************************************************************/

'use strict';

const assert = require('assert');
const sinon  = require('sinon');
const fs     = require('fs');
const os     = require('os');
const path   = require('path');
const axios  = require('axios');

const ClientSync   = require('../../src/ClientSync');
const Utility      = require('../../src/utility');
const HashVerifier = require('../../src/HashVerifier');

// An in-memory sync_halt: recordHalt stores the row getActiveHalt hands back, so a
// second ClientSync over the same db sees exactly what a restarted process would.
function createMockDb(){
    let db = {
        dbName: 'test_db',
        dbType: 'indexer',
        halt: null,
        getLastBlock: sinon.stub().resolves(null),
        getBlockHashRow: sinon.stub().resolves(null),
        doQuery: sinon.stub().resolves([]),
        getActiveHalt: sinon.stub().callsFake(async () => db.halt),
        clearHalt: sinon.stub().callsFake(async () => { db.halt = null; return 1; })
    };
    db.recordHalt = sinon.stub().callsFake(async (dbType, blockIndex, reason, mismatches, sources) => {
        if(db.halt && Number(db.halt.block_index) === Number(blockIndex)) return db.halt;
        db.halt = {
            db_type: dbType, block_index: blockIndex, reason: String(reason).slice(0, 64),
            mismatches: JSON.stringify(mismatches || []), sources: JSON.stringify(sources || []),
            detected_at: '2026-09-12 00:00:00', cleared_at: null
        };
        return db.halt;
    });
    return db;
}

function armed(version, heights){
    return { ruleSetVersion: version, classification: 'major', heights: heights,
             computedFromBtcTip: null };
}

// Every apply-time verification net off, so the only thing that can halt here is
// the train gate; the nets have their own suites.
const QUIET_CONFIG = {
    SYNC_SOURCES: 'http://a:3006', VERIFY_HASHES: true, HASH_CONFIRM_TIMEOUT: 5000,
    HALT_ON_DIVERGENCE: true, VERIFY_RECOMPUTE: false, VERIFY_STATE_HASH: false,
    VERIFY_STATE_COMMITMENT: false, SNAPSHOT_MAX_CONTENT: 536870912
};

function build(db, chain, required, config){
    let applier = {
        applyBlock: sinon.stub().resolves(),
        applyFullSnapshot: sinon.stub().resolves(),
        applyIncrementalSnapshot: sinon.stub().resolves()
    };
    let util = new Utility();
    let sync = new ClientSync(chain || 'bitcoin', 'mainnet', db, applier,
        { rollback: sinon.stub().resolves() }, new HashVerifier(), Object.assign({}, QUIET_CONFIG, config || {}), util);
    // The manifest requirement, injected where _resolveTrainActivationRequirement
    // caches it (undefined means "resolve from disk", which the file-path cases use).
    if(required !== undefined) sync._trainActivationRequired = required;
    return { sync, applier, util };
}

const block = (i) => ({ block_index: i, block_time: 1, ledger_hash: 'L', actions_hash: 'A', contract_hash: 'C' });

describe('ClientSync: platform-train activation halt @regression', function(){
    let db, logStub, errStub;

    beforeEach(function(){
        db = createMockDb();
        // The constructor's VERIFY_RECOMPUTE=false warning fires before the stubs
        // exist on purpose; only the apply path is measured for noise below.
        logStub = sinon.stub(console, 'log');
        errStub = sinon.stub(console, 'error');
        sinon.stub(console, 'warn');
    });
    afterEach(function(){ sinon.restore(); });

    it('HALTS at the activation height when the build lacks the required rule set, naming the set and the height', async function(){
        const { sync, applier } = build(db, 'bitcoin', armed('9.0.0', { mainnet: 970000 }));

        await sync._applyBlockEvent(block(970000));

        assert.strictEqual(applier.applyBlock.called, false, 'the boundary block must NOT be applied');
        assert.strictEqual(sync.isHalted(), true, 'a follower without the rule set must halt');
        assert.strictEqual(sync.lastAppliedBlock, null, 'the tip must not advance');

        const info = sync.getHaltInfo();
        assert.strictEqual(info.reason, 'train-activation');
        assert.strictEqual(info.blockIndex, 970000);
        assert.strictEqual(info.mismatches[0].required, '9.0.0', 'the halt names the missing rule set');
        assert.strictEqual(info.mismatches[0].at_height, 970000, 'the halt names the activation height');
        assert.match(info.mismatches[0].reason, /rule set 9\.0\.0/);
        assert.match(info.mismatches[0].reason, /activation height 970000/);

        assert.strictEqual(sync.trainActivation.status, 'halt');
        assert.strictEqual(sync.trainActivation.requiredRuleSet, '9.0.0');

        // Durable, through the same sync_halt path as a divergence.
        assert.ok(db.recordHalt.calledOnce, 'the halt must be persisted');
        assert.strictEqual(db.recordHalt.firstCall.args[2], 'train-activation');
        assert.strictEqual(db.halt.reason, 'train-activation');

        // The log names both as well, for an operator reading only the console.
        const said = errStub.getCalls().map(c => c.args.join(' ')).join('\n');
        assert.match(said, /TRAIN ACTIVATION HALT/);
        assert.match(said, /9\.0\.0/);
        assert.match(said, /970000/);
    });

    it('does NOT halt, and adds no log noise, when the build implements the required rule set', async function(){
        const { sync, applier } = build(db, 'bitcoin', armed('1.0.0', { mainnet: 0 }));

        await sync._applyBlockEvent(block(970000));

        assert.ok(applier.applyBlock.calledOnce, 'the block is applied');
        assert.strictEqual(sync.isHalted(), false);
        assert.strictEqual(sync.lastAppliedBlock, 970000);
        assert.strictEqual(sync.trainActivation.status, 'clear');
        assert.strictEqual(db.recordHalt.called, false);
        const chatter = [...logStub.getCalls(), ...errStub.getCalls()].map(c => c.args.join(' ')).join('\n');
        assert.ok(!/train/i.test(chatter), 'a clear verdict must say nothing: ' + chatter);
    });

    it('is inert before the activation height: the block applies and the verdict is pending', async function(){
        const { sync, applier } = build(db, 'bitcoin', armed('9.0.0', { mainnet: 970000 }));

        await sync._applyBlockEvent(block(969999));

        assert.ok(applier.applyBlock.calledOnce, 'the rolling-upgrade window keeps the follower advancing');
        assert.strictEqual(sync.isHalted(), false);
        assert.strictEqual(sync.lastAppliedBlock, 969999);
        assert.strictEqual(sync.trainActivation.status, 'pending');
        assert.strictEqual(sync.trainActivation.requiredAtHeight, 970000);
        assert.strictEqual(db.recordHalt.called, false, 'no durable marker before the boundary');
        // The announcement rides the error log so a monitor tailing it sees the height.
        const said = errStub.getCalls().map(c => c.args.join(' ')).join('\n');
        assert.match(said, /TRAIN ACTIVATION PENDING/);
        assert.match(said, /970000/);
    });

    it('is quiet on every apply except the periodic reminder while pending', async function(){
        const { sync } = build(db, 'bitcoin', armed('9.0.0', { mainnet: 970000 }));
        for(let i = 0; i < 59; i++) await sync._applyBlockEvent(block(900000 + i));
        const pendingLines = errStub.getCalls().filter(c => /TRAIN ACTIVATION PENDING/.test(c.args.join(' ')));
        assert.strictEqual(pendingLines.length, 1, 'one announcement per 60 applies, not one per block');
    });

    it('stays halted across a restart, the same way a divergence halt does', async function(){
        const first = build(db, 'bitcoin', armed('9.0.0', { mainnet: 970000 }));
        await first.sync._applyBlockEvent(block(970000));
        assert.strictEqual(first.sync.isHalted(), true);

        // A new process over the same replica: nothing in memory survives, only the
        // sync_halt row. start() must land in the idle-halted state without catch-up.
        const second = build(db, 'bitcoin', armed('9.0.0', { mainnet: 970000 }));
        sinon.stub(second.util, 'sleep').callsFake(() => { second.sync.running = false; return Promise.resolve(); });
        second.sync.running = true;
        await second.sync.start();

        assert.strictEqual(second.sync.isHalted(), true, 'the durable halt must hold after a restart');
        assert.strictEqual(second.sync.getHaltInfo().reason, 'train-activation');
        assert.strictEqual(second.sync.getHaltInfo().blockIndex, 970000);
        assert.strictEqual(second.sync.getHaltInfo().mismatches[0].required, '9.0.0');
        assert.strictEqual(db.getLastBlock.called, false, 'a halted follower must not begin catch-up');
        assert.strictEqual(second.applier.applyBlock.called, false);
    });

    it('refuses a catch-up window whose tip reaches the boundary, before any row lands', async function(){
        const { sync, applier } = build(db, 'bitcoin', armed('9.0.0', { mainnet: 970000 }));
        db.getLastBlock.resolves(969989);
        sinon.stub(axios, 'get').resolves({
            data: Buffer.from(JSON.stringify({ schema_version: 1, block_height: 970003, since_block: 969990, tables: {} }))
        });

        await sync._runIncrementalCatchUp();

        assert.strictEqual(applier.applyIncrementalSnapshot.called, false, 'the window must not be applied');
        assert.strictEqual(sync.isHalted(), true);
        assert.strictEqual(sync.getHaltInfo().reason, 'train-activation');
        assert.strictEqual(sync.getHaltInfo().blockIndex, 970003, 'halted at the window tip that crossed');
        assert.strictEqual(sync.lastAppliedBlock, null);
    });

    it('applies a catch-up window that stops short of the boundary', async function(){
        const { sync, applier } = build(db, 'bitcoin', armed('9.0.0', { mainnet: 970000 }));
        db.getLastBlock.resolves(969989);
        sinon.stub(axios, 'get').resolves({
            data: Buffer.from(JSON.stringify({ schema_version: 1, block_height: 969999, since_block: 969990, tables: {} }))
        });

        await sync._runIncrementalCatchUp();

        assert.ok(applier.applyIncrementalSnapshot.calledOnce);
        assert.strictEqual(sync.isHalted(), false);
        assert.strictEqual(sync.lastAppliedBlock, 969999);
    });

    it('refuses a full bootstrap snapshot whose tip reaches the boundary', async function(){
        const { sync, applier } = build(db, 'bitcoin', armed('9.0.0', { mainnet: 970000 }));
        sync._fetchAndApplySchema = sinon.stub().resolves();
        sinon.stub(axios, 'get').resolves({
            data: Buffer.from(JSON.stringify({ schema_version: 1, block_height: 970000, tables: {} }))
        });

        const ok = await sync._bootstrapRotateSources();

        assert.strictEqual(ok, false, 'the round reports failure so the retry ladder stops on the halt');
        assert.strictEqual(applier.applyFullSnapshot.called, false, 'nothing may be seeded');
        assert.strictEqual(sync.isHalted(), true);
        assert.strictEqual(sync.getHaltInfo().reason, 'train-activation');
        assert.strictEqual(sync.lastAppliedBlock, null);
    });

    it('halts an off-BTC follower at any height: it has no BTC clock to prove the boundary is ahead', async function(){
        const { sync, applier } = build(db, 'dogecoin', armed('9.0.0', { mainnet: 970000 }));

        await sync._applyBlockEvent(block(5000000));

        assert.strictEqual(applier.applyBlock.called, false);
        assert.strictEqual(sync.isHalted(), true);
        assert.match(sync.getHaltInfo().mismatches[0].reason, /no BTC height to compare against/);
    });

    it('does not halt an off-BTC follower whose build implements the required rule set', async function(){
        const { sync, applier } = build(db, 'litecoin', armed('1.0.0', { mainnet: 0 }));

        await sync._applyBlockEvent(block(4800000));

        assert.ok(applier.applyBlock.calledOnce);
        assert.strictEqual(sync.isHalted(), false);
        assert.strictEqual(sync.trainActivation.status, 'clear');
    });

    it('halts when the gate itself throws, rather than waving the block through', async function(){
        const { sync, applier } = build(db, 'bitcoin', null);
        sync._resolveTrainActivationRequirement = () => { throw new Error('boom'); };

        await sync._applyBlockEvent(block(1));

        assert.strictEqual(applier.applyBlock.called, false);
        assert.strictEqual(sync.isHalted(), true);
        assert.match(sync.getHaltInfo().mismatches[0].reason, /failed to evaluate \(boom\)/);
    });

    it('a divergence halt already in force is not overwritten by the gate', async function(){
        const { sync } = build(db, 'bitcoin', armed('9.0.0', { mainnet: 970000 }));
        await sync._haltOnDivergence(100, [{ field: 'contract_hash', a: 'x', b: 'y' }], [], 'cross-source-divergence');

        await sync._applyBlockEvent(block(970000));

        assert.strictEqual(sync.getHaltInfo().reason, 'cross-source-divergence');
        assert.strictEqual(sync.getHaltInfo().blockIndex, 100);
        assert.strictEqual(db.recordHalt.callCount, 1);
    });
});

describe('ClientSync: the release manifest is the source of the train requirement @regression', function(){
    let db, dir;

    beforeEach(function(){
        db  = createMockDb();
        dir = fs.mkdtempSync(path.join(os.tmpdir(), 'xchain-sync-train-'));
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });
    afterEach(function(){
        sinon.restore();
        fs.rmSync(dir, { recursive: true, force: true });
    });

    it('reads the requirement from RELEASE_MANIFEST_PATH and halts at its height', async function(){
        const file = path.join(dir, 'release-manifest.json');
        fs.writeFileSync(file, JSON.stringify({
            platform_version: '9.0.0',
            trainActivation: { ruleSetVersion: '9.0.0', classification: 'major',
                               heights: { mainnet: 970000, testnet: 150000, regtest: 0 } }
        }));
        const { sync, applier } = build(db, 'bitcoin', undefined, { RELEASE_MANIFEST_PATH: file });

        await sync._applyBlockEvent(block(969999));
        assert.ok(applier.applyBlock.calledOnce, 'below the boundary the block applies');
        assert.strictEqual(sync.trainActivation.status, 'pending');

        await sync._applyBlockEvent(block(970000));
        assert.strictEqual(applier.applyBlock.callCount, 1, 'the boundary block must not be applied');
        assert.strictEqual(sync.isHalted(), true);
        assert.strictEqual(sync.getHaltInfo().mismatches[0].required, '9.0.0');
    });

    it('a manifest that carries no trainActivation block requires nothing', async function(){
        const file = path.join(dir, 'release-manifest.json');
        fs.writeFileSync(file, JSON.stringify({ platform_version: '0.18.0', components: {} }));
        const { sync, applier } = build(db, 'bitcoin', undefined, { RELEASE_MANIFEST_PATH: file });

        await sync._applyBlockEvent(block(970000));

        assert.ok(applier.applyBlock.calledOnce);
        assert.strictEqual(sync.isHalted(), false);
        assert.strictEqual(sync.trainActivation.status, 'clear');
    });

    it('a manifest that exists but cannot be parsed halts fail-closed', async function(){
        const file = path.join(dir, 'release-manifest.json');
        fs.writeFileSync(file, '{ not json');
        const { sync, applier } = build(db, 'bitcoin', undefined, { RELEASE_MANIFEST_PATH: file });

        await sync._applyBlockEvent(block(1));

        assert.strictEqual(applier.applyBlock.called, false);
        assert.strictEqual(sync.isHalted(), true);
        assert.match(sync.getHaltInfo().mismatches[0].reason, /cannot read/);
    });

    it('no manifest at all is not a halt', async function(){
        const { sync, applier } = build(db, 'bitcoin', undefined, { RELEASE_MANIFEST_PATH: path.join(dir, 'absent.json') });
        // The default sibling-checkout location is resolved relative to src/; when it
        // is absent too the follower requires nothing, which is the honest reading of
        // an install with no manifest to require one.
        sinon.stub(fs, 'existsSync').returns(false);

        await sync._applyBlockEvent(block(970000));

        assert.ok(applier.applyBlock.calledOnce);
        assert.strictEqual(sync.isHalted(), false);
        assert.strictEqual(sync._trainActivationRequired, null, 'the absence is cached as "no requirement"');
    });
});
