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
 * ClientSync: checkpoint anchor binding and strict freshness.
 *
 * A served checkpoint must name this replica's chain and network before its roots
 * are compared by height, because one federation set signs every chain. Strict
 * freshness is measured from the newest checkpoint this replica VERIFIED, so a source
 * that only serves unanchorable checkpoints (or none) cannot keep strict mode quiet.
 ********************************************************************/

const assert = require('assert');
const sinon  = require('sinon');
const crypto = require('crypto');
const axios  = require('axios');
const ClientSync   = require('../../../src/client/sync');
const Utility      = require('../../../src/util');
const HashVerifier = require('../../../src/client/hash_verifier');
const checkpoint   = require('../../../src/checkpoint');
const { withDbMixins } = require('../../helpers/db_mixins.js');

const ENVKEY  = 'CHECKPOINT_VALIDATORS_BTC_REGTEST';
const SEEDKEY = 'CHECKPOINT_SEED_BTC_REGTEST';

function makeSigner(){
    const { publicKey, privateKey } = crypto.generateKeyPairSync('ed25519');
    const spki = publicKey.export({ format: 'der', type: 'spki' });
    return { privateKey, pubkeyHex: spki.subarray(spki.length - 32).toString('hex') };
}
function signedCp(signer, o){
    const cp = Object.assign({
        chain: 'BTC', network: 'regtest', block_index: 990, block_hash: 'c0'.repeat(32),
        ledger_hash: 'a1'.repeat(32), actions_hash: 'b2'.repeat(32), contract_hash: 'c3'.repeat(32),
        checkpoint_seq: 9, snapshot_block: 984, state_root: 'd4'.repeat(32), state_root_version: 1,
        block_merkle_root: 'e5'.repeat(32), block_merkle_version: 1
    }, o || {});
    cp.validator_signatures = [{ pubkey: signer.pubkeyHex,
        sig: crypto.sign(null, Buffer.from(checkpoint.canonicalCheckpoint(cp), 'utf8'), signer.privateKey).toString('hex') }];
    return cp;
}

let sync, rootsByHeight, stakeByHeight, syncState, getStub, signer;

function makeSync(chain, extra){
    const db = {
        dbName: 'test_db', dbType: 'indexer',
        getLastBlock: sinon.stub().resolves(null),
        recordHalt: sinon.stub().resolves({ block_index: 0 }),
        getActiveHalt: sinon.stub().resolves(null),
        clearHalt: sinon.stub().resolves(1),
        getSyncState: sinon.stub().callsFake(async key => syncState.has(key) ? syncState.get(key) : null),
        setSyncState: sinon.stub().callsFake(async (key, value) => { syncState.set(key, value); }),
        doQuery: sinon.stub().callsFake(async (sql, params) => {
            if(sql.includes('state_tree_roots')) return rootsByHeight[params[0]] ? [rootsByHeight[params[0]]] : [];
            return [];
        }),
        getStakeWeightsByCapability:     sinon.stub().callsFake(async (cap, h) => stakeByHeight[h] || []),
        getStakeWeightsByCapabilityAsOf: sinon.stub().callsFake(async (cap, h) => stakeByHeight[h] || [])
    };
    const config = Object.assign({ SYNC_SOURCES: 'http://a:3006', VERIFY_RECOMPUTE: true,
        VERIFY_CHECKPOINT_QUORUM: true, CHECKPOINT_VERIFY_INTERVAL: 1,
        CHECKPOINT_FRESHNESS_BLOCKS: 500, CHECKPOINT_FRESHNESS_STRICT: true }, extra || {});
    sync = new ClientSync(chain, 'regtest', withDbMixins(db), { applyBlock: sinon.stub().resolves() },
        { rollback: sinon.stub().resolves() }, new HashVerifier(), config, new Utility());
    sync.lastAppliedBlock = 1000;
}

// latest -> the served checkpoint (or a rejection); range -> the chain to walk.
function serve(latest, range){
    getStub.callsFake(async (url) => {
        if(url.includes('/latest')){
            if(latest instanceof Error) throw latest;
            return { data: latest };
        }
        if(url.includes('/range')) return { data: { checkpoints: range || [] } };
        throw new Error('unexpected url ' + url);
    });
}

function registerHooks(chain, extra){
    beforeEach(function(){
        rootsByHeight = {}; stakeByHeight = {}; syncState = new Map();
        signer = makeSigner();
        process.env[ENVKEY] = JSON.stringify([{ pubkey: signer.pubkeyHex, weight: '100', source: signer.pubkeyHex }]);
        getStub = sinon.stub(axios, 'get');
        sinon.stub(console, 'log'); sinon.stub(console, 'warn'); sinon.stub(console, 'error');
        makeSync(chain || 'BTC', extra);
    });
    afterEach(function(){ sinon.restore(); delete process.env[ENVKEY]; delete process.env[SEEDKEY]; });
}

// Armed: one anchor verified at block 100, and the tip is 900 blocks past it.
function armStale(){ sync._lastVerifiedCheckpointSeq = 4; sync._lastVerifiedCheckpointBlock = 100; }
function haltReason(){ return (sync.getHaltInfo() || {}).reason; }

describe('ClientSync: strict freshness counts unanchored cycles @regression', function(){
    registerHooks();

    it('HALTS when a fresh-height checkpoint carries no state_root', async function(){
        armStale();
        serve(Object.assign(signedCp(signer), { state_root: null }));
        await sync.verifyCheckpointQuorum();
        assert.strictEqual(haltReason(), 'checkpoint-freshness-stale');
    });

    it('HALTS when a fresh-height checkpoint is rootless past the commitment flag day', async function(){
        armStale();
        serve(Object.assign(signedCp(signer), { block_merkle_root: null }));
        await sync.verifyCheckpointQuorum();
        assert.strictEqual(haltReason(), 'checkpoint-freshness-stale');
    });

    it('HALTS when the source serves a regressed seq', async function(){
        armStale();
        serve(signedCp(signer, { checkpoint_seq: 3 }));
        await sync.verifyCheckpointQuorum();
        assert.strictEqual(haltReason(), 'checkpoint-freshness-stale');
    });

    it('HALTS when the checkpoint fetch fails', async function(){
        armStale();
        serve(new Error('Request failed with status code 404'));
        await sync.verifyCheckpointQuorum();
        assert.strictEqual(haltReason(), 'checkpoint-freshness-stale');
    });

    it('HALTS when the checkpoint past the tip fails the pinned quorum', async function(){
        armStale();
        serve(signedCp(makeSigner(), { block_index: 2000, snapshot_block: 1994 }));
        await sync.verifyCheckpointQuorum();
        assert.strictEqual(haltReason(), 'checkpoint-freshness-stale');
    });
});

describe('ClientSync: strict freshness counts unanchored cycles @regression', function(){
    registerHooks();

    it('does not halt while catching up to a quorum-valid checkpoint past the tip', async function(){
        armStale();
        serve(signedCp(signer, { block_index: 2000, snapshot_block: 1994 }));
        await sync.verifyCheckpointQuorum();
        assert.strictEqual(sync.isHalted(), false);
    });

    it('does not halt on an unanchorable checkpoint while the last anchor is within the bound', async function(){
        sync._lastVerifiedCheckpointSeq = 4; sync._lastVerifiedCheckpointBlock = 900;
        serve(Object.assign(signedCp(signer), { state_root: null }));
        await sync.verifyCheckpointQuorum();
        assert.strictEqual(sync.isHalted(), false);
    });

    it('does not halt before any checkpoint has been verified', async function(){
        serve(Object.assign(signedCp(signer), { state_root: null }));
        await sync.verifyCheckpointQuorum();
        assert.strictEqual(sync.isHalted(), false);
    });

    it('records the verified height and never lowers it', async function(){
        const cp = signedCp(signer);
        rootsByHeight[990] = { state_root: cp.state_root, block_merkle_root: cp.block_merkle_root };
        serve(cp);
        await sync.verifyCheckpointQuorum();
        assert.strictEqual(sync.isHalted(), false);
        assert.strictEqual(sync._lastVerifiedCheckpointBlock, 990);
        await sync.recordVerifiedCheckpointBlock(500);
        assert.strictEqual(sync._lastVerifiedCheckpointBlock, 990);
        assert.strictEqual(syncState.get('verified_checkpoint_block:indexer'), '990');
    });

    it('retries persistence after a sync-state write failure', async function(){
        const cp = signedCp(signer);
        rootsByHeight[990] = { state_root: cp.state_root, block_merkle_root: cp.block_merkle_root };
        serve(cp);
        sync.db.setSyncState.onFirstCall().rejects(new Error('sync-state write failed'));

        await assert.rejects(sync.verifyCheckpointQuorum(), /sync-state write failed/);
        assert.strictEqual(sync._lastVerifiedCheckpointBlock, null);
        assert.strictEqual(syncState.has('verified_checkpoint_block:indexer'), false);

        await sync.verifyCheckpointQuorum();
        assert.strictEqual(sync.db.setSyncState.callCount, 2);
        assert.strictEqual(sync._lastVerifiedCheckpointBlock, 990);
        assert.strictEqual(syncState.get('verified_checkpoint_block:indexer'), '990');
    });
});

describe('ClientSync: strict freshness counts unanchored cycles @regression', function(){
    registerHooks();

    it('restores the verified height during start and enforces strict freshness', async function(){
        syncState.set('verified_checkpoint_block:indexer', '990');
        makeSync('BTC');
        serve(new Error('anchor unavailable after restart'));
        sinon.stub(sync, 'loadBootstrapBase').resolves();
        sinon.stub(sync, 'loadRollbackGuardState').resolves();
        sinon.stub(sync, 'synchronizeStoredReplica').callsFake(async function(){
            assert.strictEqual(this._lastVerifiedCheckpointBlock, 990);
            this.lastAppliedBlock = 1600;
            await this.verifyCheckpointQuorum();
        });
        sinon.stub(sync, 'prepareLiveFollow').resolves();
        sinon.stub(sync, 'beginLiveFollow');
        sinon.stub(sync.util, 'sleep').callsFake(async function(){ sync.running = false; });

        await sync.start();
        assert.strictEqual(haltReason(), 'checkpoint-freshness-stale');
    });
});

describe('ClientSync: strict freshness counts unanchored cycles @regression', function(){
    registerHooks('BTC', { CHECKPOINT_FRESHNESS_STRICT: false });

    it('stays advisory with strict off', async function(){
        armStale();
        serve(Object.assign(signedCp(signer), { state_root: null }));
        await sync.verifyCheckpointQuorum();
        assert.strictEqual(sync.isHalted(), false);
    });
});

describe('ClientSync: served checkpoint binding to this replica @regression', function(){
    registerHooks();

    it('HALTS as checkpoint-binding-mismatch on a genuine checkpoint for another chain', async function(){
        const cp = signedCp(signer, { chain: 'LTC' });
        rootsByHeight[990] = { state_root: 'ff'.repeat(32), block_merkle_root: cp.block_merkle_root };
        serve(cp);
        await sync.verifyCheckpointQuorum();
        assert.strictEqual(haltReason(), 'checkpoint-binding-mismatch');
        assert.deepStrictEqual(sync.getHaltInfo().mismatches.map(m => m.field), ['checkpoint_chain']);
    });

    it('HALTS as checkpoint-binding-mismatch on a checkpoint for another network', async function(){
        serve(signedCp(signer, { network: 'mainnet' }));
        await sync.verifyCheckpointQuorum();
        assert.strictEqual(haltReason(), 'checkpoint-binding-mismatch');
    });

    it('HALTS as checkpoint-binding-mismatch on a range step for another chain', async function(){
        const s1 = makeSigner();
        const SR0 = 'b0'.repeat(32), SR1 = 'b1'.repeat(32);
        process.env[SEEDKEY] = JSON.stringify({ block_index: 90, snapshot_block: 84, checkpoint_seq: 0,
            state_root: SR0, state_root_version: 1, block_merkle_root: 'aa'.repeat(32), block_merkle_version: 1 });
        const cp = signedCp(s1, { block_index: 100, snapshot_block: 90, state_root: SR1, checkpoint_seq: 1 });
        const foreign = signedCp(s1, { chain: 'DOGE', block_index: 95, snapshot_block: 90, state_root: SR1 });
        rootsByHeight[90] = { state_root: SR0, block_merkle_root: 'aa'.repeat(32) };
        stakeByHeight[90] = [{ pubkey: s1.pubkeyHex, source: 'R1', weight: '100' }];
        serve(cp, [foreign, cp]);
        await sync.verifyCheckpointQuorum();
        assert.strictEqual(haltReason(), 'checkpoint-binding-mismatch');
    });
});

describe('ClientSync: served checkpoint binding to this replica @regression', function(){
    registerHooks('bitcoin');

    it('anchors a ticker-form checkpoint on a replica named by the full coin name', async function(){
        const cp = signedCp(signer);
        rootsByHeight[990] = { state_root: cp.state_root, block_merkle_root: cp.block_merkle_root };
        serve(cp);
        await sync.verifyCheckpointQuorum();
        assert.strictEqual(sync.isHalted(), false);
        assert.strictEqual(sync._lastVerifiedCheckpointBlock, 990);
    });
});
