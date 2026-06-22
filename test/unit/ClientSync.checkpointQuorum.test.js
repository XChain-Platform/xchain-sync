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
 * ClientSync: SPV checkpoint-quorum anchor (VERIFY_CHECKPOINT_QUORUM).
 *
 * The replica fetches the source's signed checkpoint, verifies its quorum
 * against an OUT-OF-BAND pinned validator set, and asserts the checkpoint's
 * committed state_root equals the replica's OWN recomputed state_tree_roots row.
 * A single lying source cannot forge a quorum, so it cannot pass a fake state.
 * Closes the gap the recompute checks (which compare only against the same
 * source) cannot. INERT without a pinned set; never halts on transport errors.
 ********************************************************************/

const assert = require('assert');
const sinon  = require('sinon');
const crypto = require('crypto');
const axios  = require('axios');
const ClientSync   = require('../../src/ClientSync');
const Utility      = require('../../src/utility');
const HashVerifier = require('../../src/HashVerifier');
const checkpoint   = require('../../src/checkpoint');

const ENVKEY = 'CHECKPOINT_VALIDATORS_BTC_REGTEST';

function makeSigner(){
    const { publicKey, privateKey } = crypto.generateKeyPairSync('ed25519');
    const spki = publicKey.export({ format: 'der', type: 'spki' });
    return { privateKey, pubkeyHex: spki.subarray(spki.length - 32).toString('hex') };
}
function signedCheckpoint(signer){
    const cp = {
        chain: 'BTC', network: 'regtest', block_index: 100, block_hash: 'c0'.repeat(32),
        ledger_hash: 'a1'.repeat(32), actions_hash: 'b2'.repeat(32), contract_hash: 'c3'.repeat(32),
        checkpoint_seq: 4, snapshot_block: 100, state_root: 'd4'.repeat(32), state_root_version: 1,
        block_merkle_root: 'e5'.repeat(32), block_merkle_version: 1, validator_signatures: []
    };
    cp.validator_signatures = [{ pubkey: signer.pubkeyHex,
        sig: crypto.sign(null, Buffer.from(checkpoint.canonicalCheckpoint(cp), 'utf8'), signer.privateKey).toString('hex') }];
    return cp;
}
function pin(signer){
    process.env[ENVKEY] = JSON.stringify([{ pubkey: signer.pubkeyHex, weight: '100', source: signer.pubkeyHex }]);
}
function createMockDb(){
    return {
        dbName: 'test_db', dbType: 'indexer',
        getLastBlock: sinon.stub().resolves(null),
        doQuery: sinon.stub().resolves([]),
        recordHalt: sinon.stub().resolves({ block_index: 0 }),
        getActiveHalt: sinon.stub().resolves(null),
        clearHalt: sinon.stub().resolves(1)
    };
}

describe('ClientSync: checkpoint-quorum anchor @regression', function(){
    let sync, db, getStub;

    beforeEach(function(){
        db = createMockDb();
        const applier = { applyBlock: sinon.stub().resolves() };
        const config = { SYNC_SOURCES: 'http://a:3006', VERIFY_RECOMPUTE: true,
            VERIFY_CHECKPOINT_QUORUM: true, CHECKPOINT_VERIFY_INTERVAL: 1 };
        sync = new ClientSync('BTC', 'regtest', db, applier, { rollback: sinon.stub().resolves() },
            new HashVerifier(), config, new Utility());
        sync.lastAppliedBlock = 100;
        getStub = sinon.stub(axios, 'get');
        sinon.stub(console, 'log');
        sinon.stub(console, 'error');
    });
    afterEach(function(){ sinon.restore(); delete process.env[ENVKEY]; });

    it('does NOT halt when the quorum verifies and the state_root matches the replica', async function(){
        const s = makeSigner(); pin(s);
        const cp = signedCheckpoint(s);
        getStub.resolves({ data: cp });
        db.doQuery.resolves([{ state_root: cp.state_root, block_merkle_root: cp.block_merkle_root }]);

        await sync._verifyCheckpointQuorum();

        assert.strictEqual(sync.isHalted(), false, 'a quorum-signed checkpoint matching local state must not halt');
        assert.ok(getStub.calledOnce, 'fetched the signed checkpoint');
        assert.ok(db.doQuery.calledOnce, 'read the local state_tree_roots row');
    });

    it('HALTS when the checkpoint quorum is INVALID under the pinned set (rogue signer)', async function(){
        const real = makeSigner(), rogue = makeSigner(); pin(real);
        getStub.resolves({ data: signedCheckpoint(rogue) });   // signed by a key not in the pinned set

        await sync._verifyCheckpointQuorum();

        assert.strictEqual(sync.isHalted(), true);
        assert.strictEqual(sync.getHaltInfo().reason, 'checkpoint-quorum-divergence');
        assert.strictEqual(db.doQuery.called, false, 'must not even read local roots when the quorum fails');
        assert.ok(db.recordHalt.calledOnce && db.recordHalt.firstCall.args[2] === 'checkpoint-quorum-divergence');
    });

    it('HALTS when the quorum-signed state_root disagrees with the replica\'s own recompute', async function(){
        const s = makeSigner(); pin(s);
        const cp = signedCheckpoint(s);
        getStub.resolves({ data: cp });
        db.doQuery.resolves([{ state_root: 'ab'.repeat(32), block_merkle_root: cp.block_merkle_root }]);  // local differs

        await sync._verifyCheckpointQuorum();

        assert.strictEqual(sync.isHalted(), true);
        assert.strictEqual(sync.getHaltInfo().reason, 'checkpoint-quorum-divergence');
        assert.strictEqual(sync.getHaltInfo().mismatches[0].field, 'state_root');
    });

    it('is INERT with no pinned set: no fetch, no halt', async function(){
        delete process.env[ENVKEY];
        await sync._verifyCheckpointQuorum();
        assert.strictEqual(getStub.called, false, 'no out-of-band trust root => skip entirely (never fetch)');
        assert.strictEqual(sync.isHalted(), false);
    });

    it('never halts on a transport error (404 / network)', async function(){
        const s = makeSigner(); pin(s);
        getStub.rejects(new Error('connect ECONNREFUSED'));
        await sync._verifyCheckpointQuorum();
        assert.strictEqual(sync.isHalted(), false, 'a transport fault is not a divergence');
    });

    it('skips (no halt, no local read) when the replica has not reached the checkpoint height', async function(){
        const s = makeSigner(); pin(s);
        const cp = signedCheckpoint(s);
        sync.lastAppliedBlock = 50;                            // checkpoint is at 100, replica behind
        getStub.resolves({ data: cp });
        await sync._verifyCheckpointQuorum();
        assert.strictEqual(sync.isHalted(), false);
        assert.strictEqual(db.doQuery.called, false, 'cannot compare a height not yet recomputed');
    });

    it('skips a pre-commitment checkpoint (null state_root)', async function(){
        const s = makeSigner(); pin(s);
        const cp = signedCheckpoint(s); cp.state_root = null;
        getStub.resolves({ data: cp });
        await sync._verifyCheckpointQuorum();
        assert.strictEqual(sync.isHalted(), false);
        assert.strictEqual(db.doQuery.called, false);
    });

    it('records the verified checkpoint_seq high-water mark on success', async function(){
        const s = makeSigner(); pin(s);
        const cp = signedCheckpoint(s);                       // checkpoint_seq 4
        getStub.resolves({ data: cp });
        db.doQuery.resolves([{ state_root: cp.state_root, block_merkle_root: cp.block_merkle_root }]);
        await sync._verifyCheckpointQuorum();
        assert.strictEqual(sync._lastVerifiedCheckpointSeq, 4);
    });

    it('REJECTS a checkpoint_seq regression (source rewound / withholding) without anchoring', async function(){
        const s = makeSigner(); pin(s);
        const warn = sinon.stub(console, 'warn');
        sync._lastVerifiedCheckpointSeq = 9;                  // already anchored a newer seq
        const cp = signedCheckpoint(s);                       // older: checkpoint_seq 4
        getStub.resolves({ data: cp });
        await sync._verifyCheckpointQuorum();
        assert.strictEqual(sync.isHalted(), false, 'a regression is suspicious but not proof of forgery');
        assert.strictEqual(db.doQuery.called, false, 'must not anchor (no local root compare) on a regressed seq');
        assert.ok(warn.getCalls().some(c => /seq regression/.test(c.args[0])), 'surfaces the rewind');
        assert.strictEqual(sync._lastVerifiedCheckpointSeq, 9, 'high-water mark is not lowered');
    });

    it('fetches the anchor from CHECKPOINT_ANCHOR_URL out-of-band when configured', async function(){
        const s = makeSigner(); pin(s);
        sync.config['CHECKPOINT_ANCHOR_URL'] = 'http://hub-anchor:9000';
        const cp = signedCheckpoint(s);
        getStub.resolves({ data: cp });
        db.doQuery.resolves([{ state_root: cp.state_root, block_merkle_root: cp.block_merkle_root }]);
        await sync._verifyCheckpointQuorum();
        assert.ok(getStub.firstCall.args[0].startsWith('http://hub-anchor:9000/'),
            'fetched from the out-of-band anchor, not the audited source');
    });

    it('warns (no halt) when the anchor is staler than CHECKPOINT_FRESHNESS_BLOCKS behind the tip', async function(){
        const s = makeSigner(); pin(s);
        const warn = sinon.stub(console, 'warn');
        sync.config['CHECKPOINT_FRESHNESS_BLOCKS'] = 500;
        sync.lastAppliedBlock = 1000;                         // checkpoint is at 100 => 900 behind
        const cp = signedCheckpoint(s);
        getStub.resolves({ data: cp });
        db.doQuery.resolves([{ state_root: cp.state_root, block_merkle_root: cp.block_merkle_root }]);
        await sync._verifyCheckpointQuorum();
        assert.strictEqual(sync.isHalted(), false, 'staleness is advisory, never a halt');
        assert.ok(warn.getCalls().some(c => /stale anchor/.test(c.args[0])), 'alarms the freshness gap');
    });

    it('does not warn freshness when the anchor is within CHECKPOINT_FRESHNESS_BLOCKS of the tip', async function(){
        const s = makeSigner(); pin(s);
        const warn = sinon.stub(console, 'warn');
        sync.config['CHECKPOINT_FRESHNESS_BLOCKS'] = 500;
        sync.lastAppliedBlock = 100;                          // checkpoint at 100 => 0 behind
        const cp = signedCheckpoint(s);
        getStub.resolves({ data: cp });
        db.doQuery.resolves([{ state_root: cp.state_root, block_merkle_root: cp.block_merkle_root }]);
        await sync._verifyCheckpointQuorum();
        assert.ok(!warn.getCalls().some(c => /stale anchor/.test(c.args[0])));
    });
});

// Sign a checkpoint at a given height/snapshot/state_root (the rotation walk needs
// configurable heights, unlike the fixed-100 signedCheckpoint above).
function signedCpAt(signer, o){
    const cp = {
        chain: 'BTC', network: 'regtest', block_index: o.block_index, block_hash: 'c0'.repeat(32),
        ledger_hash: 'a1'.repeat(32), actions_hash: 'b2'.repeat(32), contract_hash: 'c3'.repeat(32),
        checkpoint_seq: o.checkpoint_seq == null ? 0 : o.checkpoint_seq, snapshot_block: o.snapshot_block,
        state_root: o.state_root, state_root_version: 1,
        block_merkle_root: o.block_merkle_root || 'e5'.repeat(32), block_merkle_version: 1,
        validator_signatures: []
    };
    cp.validator_signatures = [{ pubkey: signer.pubkeyHex,
        sig: crypto.sign(null, Buffer.from(checkpoint.canonicalCheckpoint(cp), 'utf8'), signer.privateKey).toString('hex') }];
    return cp;
}
const SEEDKEY = 'CHECKPOINT_SEED_BTC_REGTEST';
function setSeed(o){
    process.env[SEEDKEY] = JSON.stringify({ block_index: o.block_index, snapshot_block: o.snapshot_block,
        checkpoint_seq: 0, state_root: o.state_root, state_root_version: 1,
        block_merkle_root: o.block_merkle_root || 'aa'.repeat(32), block_merkle_version: 1 });
}

describe('ClientSync: checkpoint-quorum rotation following @regression', function(){
    let sync, db, getStub, rootsByHeight, stakeByHeight;

    beforeEach(function(){
        rootsByHeight = {}; stakeByHeight = {};
        db = {
            dbName: 'test_db', dbType: 'indexer',
            getLastBlock: sinon.stub().resolves(null),
            recordHalt: sinon.stub().resolves({ block_index: 0 }),
            getActiveHalt: sinon.stub().resolves(null),
            clearHalt: sinon.stub().resolves(1),
            doQuery: sinon.stub().callsFake(async (sql, params) => {
                if(sql.includes('state_tree_roots')){ const h = params[0]; return rootsByHeight[h] ? [rootsByHeight[h]] : []; }
                return [];
            }),
            getStakeWeightsByCapability: sinon.stub().callsFake(async (cap, height) => stakeByHeight[height] || [])
        };
        const config = { SYNC_SOURCES: 'http://a:3006', VERIFY_RECOMPUTE: true,
            VERIFY_CHECKPOINT_QUORUM: true, CHECKPOINT_VERIFY_INTERVAL: 1 };
        sync = new ClientSync('BTC', 'regtest', db, { applyBlock: sinon.stub().resolves() },
            { rollback: sinon.stub().resolves() }, new HashVerifier(), config, new Utility());
        sync.lastAppliedBlock = 1000;
        getStub = sinon.stub(axios, 'get');
        sinon.stub(console, 'log'); sinon.stub(console, 'error');
    });
    afterEach(function(){ sinon.restore(); delete process.env[ENVKEY]; delete process.env[SEEDKEY]; });

    // latest -> the served (rotated) checkpoint; range -> the chain to walk.
    function route(cp, rangeCps){
        getStub.callsFake(async (url) => {
            if(url.includes('/latest')) return { data: cp };
            if(url.includes('/range'))  return { data: { checkpoints: rangeCps } };
            throw new Error('unexpected url ' + url);
        });
    }

    it('follows the pinned seed forward to a rotated checkpoint and does NOT halt', async function(){
        const launch = makeSigner(), s1 = makeSigner(); pin(launch);   // launch set rotated OUT
        const SR0 = 'b0'.repeat(32), SR1 = 'b1'.repeat(32);
        setSeed({ block_index: 90, snapshot_block: 84, state_root: SR0 });
        const cp = signedCpAt(s1, { block_index: 100, snapshot_block: 90, state_root: SR1, checkpoint_seq: 1 });
        rootsByHeight[90]  = { state_root: SR0, block_merkle_root: 'aa'.repeat(32) };
        rootsByHeight[100] = { state_root: SR1, block_merkle_root: cp.block_merkle_root };
        stakeByHeight[90]  = [{ pubkey: s1.pubkeyHex, source: 'R1', weight: '100' }];
        route(cp, [cp]);

        await sync._verifyCheckpointQuorum();

        assert.strictEqual(sync.isHalted(), false, 'a rotated checkpoint provable from the seed must not halt');
        assert.ok(getStub.getCalls().some(c => c.args[0].includes('/range')), 'walked the checkpoint range');
    });

    it('HALTS when a rotated checkpoint is not signed by the authoritative set (forged quorum)', async function(){
        const launch = makeSigner(), s1 = makeSigner(), rogue = makeSigner(); pin(launch);
        const SR0 = 'b0'.repeat(32), SR1 = 'b1'.repeat(32);
        setSeed({ block_index: 90, snapshot_block: 84, state_root: SR0 });
        const cp = signedCpAt(rogue, { block_index: 100, snapshot_block: 90, state_root: SR1 });   // signed by rogue
        rootsByHeight[90]  = { state_root: SR0, block_merkle_root: 'aa'.repeat(32) };
        rootsByHeight[100] = { state_root: SR1, block_merkle_root: cp.block_merkle_root };
        stakeByHeight[90]  = [{ pubkey: s1.pubkeyHex, source: 'R1', weight: '100' }];   // authoritative = s1, not rogue
        route(cp, [cp]);

        await sync._verifyCheckpointQuorum();

        assert.strictEqual(sync.isHalted(), true);
        assert.strictEqual(sync.getHaltInfo().reason, 'checkpoint-quorum-divergence');
        assert.strictEqual(sync.getHaltInfo().mismatches[0].field, 'checkpoint_quorum');
    });

    it('HALTS when a rotated checkpoint\'s committed state_root disagrees with the recompute', async function(){
        const launch = makeSigner(), s1 = makeSigner(); pin(launch);
        const SR0 = 'b0'.repeat(32), SR1 = 'b1'.repeat(32);
        setSeed({ block_index: 90, snapshot_block: 84, state_root: SR0 });
        const cp = signedCpAt(s1, { block_index: 100, snapshot_block: 90, state_root: SR1 });
        rootsByHeight[90]  = { state_root: SR0, block_merkle_root: 'aa'.repeat(32) };
        rootsByHeight[100] = { state_root: 'ff'.repeat(32), block_merkle_root: cp.block_merkle_root };  // differs
        stakeByHeight[90]  = [{ pubkey: s1.pubkeyHex, source: 'R1', weight: '100' }];
        route(cp, [cp]);

        await sync._verifyCheckpointQuorum();

        assert.strictEqual(sync.isHalted(), true);
        assert.strictEqual(sync.getHaltInfo().mismatches[0].field, 'state_root');
    });

    it('does NOT halt (waits) when the rotated set\'s snapshot is not yet attested', async function(){
        const launch = makeSigner(), s1 = makeSigner(); pin(launch);
        const SR0 = 'b0'.repeat(32), SR1 = 'b1'.repeat(32);
        setSeed({ block_index: 90, snapshot_block: 84, state_root: SR0 });
        const cp = signedCpAt(s1, { block_index: 100, snapshot_block: 95, state_root: SR1 });   // snapshot 95 > trusted 90
        rootsByHeight[90]  = { state_root: SR0, block_merkle_root: 'aa'.repeat(32) };
        rootsByHeight[100] = { state_root: SR1, block_merkle_root: cp.block_merkle_root };
        stakeByHeight[95]  = [{ pubkey: s1.pubkeyHex, source: 'R1', weight: '100' }];
        route(cp, [cp]);

        await sync._verifyCheckpointQuorum();

        assert.strictEqual(sync.isHalted(), false, 'cannot attest the signer set at snapshot 95 from a trust root at 90');
    });

    it('does NOT halt (waits) when the seed height is not yet recomputed locally', async function(){
        const launch = makeSigner(), s1 = makeSigner(); pin(launch);
        setSeed({ block_index: 90, snapshot_block: 84, state_root: 'b0'.repeat(32) });
        const cp = signedCpAt(s1, { block_index: 100, snapshot_block: 90, state_root: 'b1'.repeat(32) });
        // no rootsByHeight[90] -> seed roots 'missing'
        rootsByHeight[100] = { state_root: cp.state_root, block_merkle_root: cp.block_merkle_root };
        stakeByHeight[90]  = [{ pubkey: s1.pubkeyHex, source: 'R1', weight: '100' }];
        route(cp, [cp]);

        await sync._verifyCheckpointQuorum();

        assert.strictEqual(sync.isHalted(), false);
    });

    it('HALTS when the replica\'s recompute disagrees with the pinned seed (different chain)', async function(){
        const launch = makeSigner(), s1 = makeSigner(); pin(launch);
        setSeed({ block_index: 90, snapshot_block: 84, state_root: 'b0'.repeat(32) });
        const cp = signedCpAt(s1, { block_index: 100, snapshot_block: 90, state_root: 'b1'.repeat(32) });
        rootsByHeight[90]  = { state_root: 'cc'.repeat(32), block_merkle_root: 'aa'.repeat(32) };  // != seed
        rootsByHeight[100] = { state_root: cp.state_root, block_merkle_root: cp.block_merkle_root };
        stakeByHeight[90]  = [{ pubkey: s1.pubkeyHex, source: 'R1', weight: '100' }];
        route(cp, [cp]);

        await sync._verifyCheckpointQuorum();

        assert.strictEqual(sync.isHalted(), true);
        assert.strictEqual(sync.getHaltInfo().mismatches[0].field, 'state_root');
    });

    it('does NOT halt on a transport error while fetching the checkpoint range', async function(){
        const launch = makeSigner(), s1 = makeSigner(); pin(launch);
        setSeed({ block_index: 90, snapshot_block: 84, state_root: 'b0'.repeat(32) });
        const cp = signedCpAt(s1, { block_index: 100, snapshot_block: 90, state_root: 'b1'.repeat(32) });
        rootsByHeight[90] = { state_root: 'b0'.repeat(32), block_merkle_root: 'aa'.repeat(32) };
        getStub.callsFake(async (url) => {
            if(url.includes('/latest')) return { data: cp };
            if(url.includes('/range'))  throw new Error('connect ECONNREFUSED');
            throw new Error('unexpected url ' + url);
        });

        await sync._verifyCheckpointQuorum();

        assert.strictEqual(sync.isHalted(), false, 'a range transport fault is not a divergence');
    });
});
