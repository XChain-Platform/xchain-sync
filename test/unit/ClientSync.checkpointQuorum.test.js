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
});
