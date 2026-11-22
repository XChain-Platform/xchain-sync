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
 * ClientSync: the pinned checkpoint anchor after an incremental catch-up pass.
 *
 * A source that sends only status heartbeats moves the replica's tip entirely through
 * catch-up, so the pinned quorum anchor and strict freshness must run from that path
 * too, on the same interval cadence the live block path uses.
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

const ENVKEY = 'CHECKPOINT_VALIDATORS_BTC_REGTEST';

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

let sync, rootsByHeight, syncState, getStub, signer;

function makeSync(extra, dbType){
    const db = {
        dbName: 'test_db', dbType: dbType || 'indexer',
        getLastBlock: sinon.stub().resolves(10),
        recordHalt: sinon.stub().resolves({ block_index: 0 }),
        getActiveHalt: sinon.stub().resolves(null),
        clearHalt: sinon.stub().resolves(1),
        getSyncState: sinon.stub().callsFake(async key => syncState.has(key) ? syncState.get(key) : null),
        setSyncState: sinon.stub().callsFake(async (key, value) => { syncState.set(key, value); }),
        doQuery: sinon.stub().callsFake(async (sql, params) => {
            if(sql.includes('state_tree_roots')) return rootsByHeight[params[0]] ? [rootsByHeight[params[0]]] : [];
            return [];
        })
    };
    const config = Object.assign({ SYNC_SOURCES: 'http://a:3006', VERIFY_RECOMPUTE: true,
        VERIFY_CHECKPOINT_QUORUM: true, CHECKPOINT_VERIFY_INTERVAL: 50,
        CHECKPOINT_FRESHNESS_BLOCKS: 500, CHECKPOINT_FRESHNESS_STRICT: false }, extra || {});
    sync = new ClientSync('bitcoin', 'regtest', withDbMixins(db), { applyIncrementalSnapshot: sinon.stub().resolves() },
        { rollback: sinon.stub().resolves() }, new HashVerifier(), config, new Utility());
    // A catch-up pass that applies cleanly and lands the tip at `tip`.
    sinon.stub(sync, 'downloadIncrementalCatchUp').resolves({ snapshotData: { block_height: 1000 }, skipLookups: false });
    sinon.stub(sync, 'applyIncrementalCatchUpSnapshot').callsFake(async function(){ this.lastAppliedBlock = 1000; return false; });
    sinon.stub(sync, 'verifyUnverifiedTail').resolves('ok');
    sync.lastAppliedBlock = 10;
}

function serve(latest){
    getStub.callsFake(async (url) => {
        if(url.includes('/latest')){
            if(latest instanceof Error) throw latest;
            return { data: latest };
        }
        throw new Error('unexpected url ' + url);
    });
}

function haltReason(){ return (sync.getHaltInfo() || {}).reason; }
function latestFetches(){ return getStub.getCalls().filter(c => c.args[0].includes('/latest')).length; }

function registerHooks(){
    beforeEach(function(){
        rootsByHeight = {}; syncState = new Map();
        signer = makeSigner();
        process.env[ENVKEY] = JSON.stringify([{ pubkey: signer.pubkeyHex, weight: '100', source: signer.pubkeyHex }]);
        getStub = sinon.stub(axios, 'get');
        sinon.stub(console, 'log'); sinon.stub(console, 'warn'); sinon.stub(console, 'error');
    });
    afterEach(function(){ sinon.restore(); delete process.env[ENVKEY]; });
}

describe('ClientSync: pinned anchor after an incremental catch-up pass @regression', function(){
    registerHooks();

    it('anchors a catch-up pass that advanced the tip a full interval, with no block event', async function(){
        makeSync();
        const cp = signedCp(signer);
        rootsByHeight[990] = { state_root: cp.state_root, block_merkle_root: cp.block_merkle_root };
        serve(cp);
        await sync.runIncrementalCatchUp();
        assert.strictEqual(latestFetches(), 1);
        assert.strictEqual(sync._lastCheckpointVerifyBlock, 1000);
        assert.strictEqual(sync._lastVerifiedCheckpointBlock, 990);
        assert.strictEqual(haltReason(), undefined);
    });

    it('halts on a quorum-signed root that differs from the replica own recomputed root', async function(){
        makeSync();
        const cp = signedCp(signer);
        rootsByHeight[990] = { state_root: 'ff'.repeat(32), block_merkle_root: cp.block_merkle_root };
        serve(cp);
        await sync.runIncrementalCatchUp();
        assert.strictEqual(haltReason(), 'checkpoint-quorum-divergence');
    });

    it('enforces strict freshness from a catch-up pass when no checkpoint is anchorable', async function(){
        makeSync({ CHECKPOINT_FRESHNESS_STRICT: true });
        sync._lastVerifiedCheckpointSeq = 4; sync._lastVerifiedCheckpointBlock = 100;
        serve(new Error('anchor unavailable'));
        await sync.runIncrementalCatchUp();
        assert.strictEqual(haltReason(), 'checkpoint-freshness-stale');
    });

});

describe('ClientSync: no anchor after a catch-up pass that did not apply cleanly @regression', function(){
    registerHooks();

    it('does not anchor when the pass advanced the tip less than one interval', async function(){
        makeSync({ CHECKPOINT_VERIFY_INTERVAL: 5000 });
        serve(signedCp(signer));
        await sync.runIncrementalCatchUp();
        assert.strictEqual(latestFetches(), 0);
    });

    it('does not anchor when the apply step consumed the pass', async function(){
        makeSync();
        sync.applyIncrementalCatchUpSnapshot.resolves(true);
        serve(signedCp(signer));
        await sync.runIncrementalCatchUp();
        assert.strictEqual(latestFetches(), 0);
    });

    it('does not anchor when range verification held or halted the pass', async function(){
        makeSync();
        sync.verifyUnverifiedTail.onCall(0).resolves('ok');
        sync.verifyUnverifiedTail.onCall(1).resolves('held');
        serve(signedCp(signer));
        await sync.runIncrementalCatchUp();
        assert.strictEqual(latestFetches(), 0);
    });

    it('does not anchor a decoder replica', async function(){
        makeSync({}, 'decoder');
        serve(signedCp(signer));
        await sync.runIncrementalCatchUp();
        assert.strictEqual(latestFetches(), 0);
    });

    it('keeps an anchor fault out of the catch-up failure path', async function(){
        makeSync();
        sinon.stub(sync, 'verifyCheckpointQuorum').rejects(new Error('anchor store fault'));
        const failure = sinon.stub(sync, 'handleIncrementalCatchUpFailure').resolves();
        await sync.runIncrementalCatchUp();
        assert.strictEqual(failure.called, false);
        assert.ok(console.error.getCalls().some(c => String(c.args[0]).includes('anchor after catch-up failed')));
    });
});
