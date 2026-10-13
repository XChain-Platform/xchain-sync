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
 * ClientSync: the checkpoint-quorum anchor only trusts what a quorum signed.
 *
 * Covers the gates that keep an unsigned or unverifiable value from reading as
 * anchored: a root served below the commitment flag day, a signed
 * block_merkle_root with no local value to check, a signer set cut short by the
 * stake-weight cap, and a pinned seed on a chain that cannot follow rotation.
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
const { makeTestDatabase } = require('../support/fake_db');

function makeSigner(){
    const { publicKey, privateKey } = crypto.generateKeyPairSync('ed25519');
    const spki = publicKey.export({ format: 'der', type: 'spki' });
    return { privateKey, pubkeyHex: spki.subarray(spki.length - 32).toString('hex') };
}
function signedCpAt(signer, o){
    const cp = {
        chain: o.chain || 'BTC', network: o.network || 'regtest', block_index: o.block_index,
        block_hash: 'c0'.repeat(32), ledger_hash: 'a1'.repeat(32), actions_hash: 'b2'.repeat(32),
        contract_hash: 'c3'.repeat(32), checkpoint_seq: o.checkpoint_seq == null ? 0 : o.checkpoint_seq,
        snapshot_block: o.snapshot_block, state_root: o.state_root, state_root_version: 1,
        block_merkle_root: o.block_merkle_root || 'e5'.repeat(32), block_merkle_version: 1,
        validator_signatures: []
    };
    cp.validator_signatures = [{ pubkey: signer.pubkeyHex,
        sig: crypto.sign(null, Buffer.from(checkpoint.canonicalCheckpoint(cp), 'utf8'), signer.privateKey).toString('hex') }];
    return cp;
}
const envKeys = [];
function setEnv(name, value){ envKeys.push(name); process.env[name] = JSON.stringify(value); }
function pin(chain, network, signer){
    setEnv('CHECKPOINT_VALIDATORS_' + chain + '_' + network.toUpperCase(),
        [{ pubkey: signer.pubkeyHex, weight: '100', source: signer.pubkeyHex }]);
}
function setSeed(chain, network, o){
    setEnv('CHECKPOINT_SEED_' + chain + '_' + network.toUpperCase(), { block_index: o.block_index,
        snapshot_block: o.snapshot_block, checkpoint_seq: 0, state_root: o.state_root, state_root_version: 1,
        block_merkle_root: 'aa'.repeat(32), block_merkle_version: 1 });
}

let db, getStub, rootsByHeight, stakeByHeight, rootReads;
function makeSync(chain, network){
    rootsByHeight = {}; stakeByHeight = {}; rootReads = [];
    db = {
        dbName: 'test_db', dbType: 'indexer',
        getLastBlock: sinon.stub().resolves(null),
        recordHalt: sinon.stub().resolves({ block_index: 0 }),
        getActiveHalt: sinon.stub().resolves(null),
        clearHalt: sinon.stub().resolves(1),
        doQuery: sinon.stub().callsFake(async (sql, params) => {
            if(!sql.includes('state_tree_roots')) return [];
            rootReads.push(params[0]);
            return rootsByHeight[params[0]] ? [rootsByHeight[params[0]]] : [];
        }),
        getStakeWeightsByCapability:     sinon.stub().callsFake(async (cap, h) => stakeByHeight[h] || []),
        getStakeWeightsByCapabilityAsOf: sinon.stub().callsFake(async (cap, h) => stakeByHeight[h] || [])
    };
    const config = { SYNC_SOURCES: 'http://a:3006', VERIFY_RECOMPUTE: true,
        VERIFY_CHECKPOINT_QUORUM: true, CHECKPOINT_VERIFY_INTERVAL: 1 };
    const s = new ClientSync(chain, network, withDbMixins(db), { applyBlock: sinon.stub().resolves() },
        { rollback: sinon.stub().resolves() }, new HashVerifier(), config, new Utility());
    s.lastAppliedBlock = 1000;
    return s;
}
function route(cp, rangeCps){
    getStub.callsFake(async (url) => {
        if(url.includes('/latest')) return { data: cp };
        if(url.includes('/range'))  return { data: { checkpoints: rangeCps || [] } };
        throw new Error('unexpected url ' + url);
    });
}
function sawRange(){ return getStub.getCalls().some(c => c.args[0].includes('/range')); }

function registerHooks(){
    beforeEach(function(){
        getStub = sinon.stub(axios, 'get');
        sinon.stub(console, 'log'); sinon.stub(console, 'error');
    });
    afterEach(function(){ sinon.restore(); while(envKeys.length) delete process.env[envKeys.pop()]; });
}

describe('ClientSync: checkpoint anchor ignores roots below the commitment flag day @regression', function(){
    registerHooks();

    it('does not anchor a genuine pre-flag-day checkpoint that carries attached roots', async function(){
        const sync = makeSync('BTC', 'mainnet'), launch = makeSigner(); pin('BTC', 'mainnet', launch);
        const cp = signedCpAt(launch, { network: 'mainnet', block_index: 900010, snapshot_block: 900000,
            state_root: 'd4'.repeat(32), checkpoint_seq: 7 });
        assert.strictEqual(checkpoint.commitmentActive(cp), false, 'fixture must sit below the flag day');
        rootsByHeight[900010] = { state_root: cp.state_root, block_merkle_root: cp.block_merkle_root };
        sync.lastAppliedBlock = 900020;
        route(cp);

        await sync.verifyCheckpointQuorum();

        assert.strictEqual(sync.isHalted(), false);
        assert.deepStrictEqual(rootReads, [], 'unsigned roots must never be compared as if signed');
        assert.strictEqual(sync._lastVerifiedCheckpointBlock, null, 'no anchor may be recorded on unsigned roots');
    });

    it('a rotation step below the flag day waits before its signer set is computed', async function(){
        const sync = makeSync('BTC', 'mainnet'), s1 = makeSigner();
        const next = signedCpAt(s1, { network: 'mainnet', block_index: 900010, snapshot_block: 900000,
            state_root: 'd4'.repeat(32) });
        sync.lastAppliedBlock = 900020;

        const stop = await sync.attestRotationStep(next, { block_index: 900005 });

        assert.strictEqual(stop.verdict, 'wait');
        assert.match(stop.reason, /roots not signed/);
        assert.strictEqual(db.getStakeWeightsByCapabilityAsOf.called, false);
    });
});

describe('ClientSync: a signed block_merkle_root needs a local value to match @regression', function(){
    registerHooks();

    it('reads a null local block_merkle_root as missing, never as a match', async function(){
        const sync = makeSync('BTC', 'regtest'), launch = makeSigner(); pin('BTC', 'regtest', launch);
        const cp = signedCpAt(launch, { block_index: 100, snapshot_block: 100, state_root: 'd4'.repeat(32), checkpoint_seq: 4 });
        rootsByHeight[100] = { state_root: cp.state_root, block_merkle_root: null };
        route(cp);

        assert.strictEqual((await sync.checkpointRootsMatchLocal(cp)).status, 'missing');
        await sync.verifyCheckpointQuorum();

        assert.strictEqual(sync.isHalted(), false);
        assert.strictEqual(sync._lastVerifiedCheckpointBlock, null, 'an unchecked signed root is not an anchor');
    });

    it('still halts on a state_root divergence when the local block_merkle_root is null', async function(){
        const sync = makeSync('BTC', 'regtest'), launch = makeSigner(); pin('BTC', 'regtest', launch);
        const cp = signedCpAt(launch, { block_index: 100, snapshot_block: 100, state_root: 'd4'.repeat(32) });
        rootsByHeight[100] = { state_root: 'ff'.repeat(32), block_merkle_root: null };
        route(cp);

        await sync.verifyCheckpointQuorum();

        assert.strictEqual(sync.isHalted(), true);
        assert.strictEqual(sync.getHaltInfo().reason, 'checkpoint-quorum-divergence');
        assert.strictEqual(sync.getHaltInfo().mismatches[0].field, 'state_root');
    });
});

describe('ClientSync: a truncated signer set never attests a rotation step @regression', function(){
    registerHooks();

    it('waits, without verifying or halting, when the as-of oracle_publish set is truncated', async function(){
        const sync = makeSync('BTC', 'regtest'), launch = makeSigner(), s1 = makeSigner(); pin('BTC', 'regtest', launch);
        const SR0 = 'b0'.repeat(32), SR1 = 'b1'.repeat(32);
        setSeed('BTC', 'regtest', { block_index: 90, snapshot_block: 84, state_root: SR0 });
        const cp = signedCpAt(s1, { block_index: 100, snapshot_block: 90, state_root: SR1, checkpoint_seq: 1 });
        rootsByHeight[90]  = { state_root: SR0, block_merkle_root: 'aa'.repeat(32) };
        rootsByHeight[100] = { state_root: SR1, block_merkle_root: cp.block_merkle_root };
        const set = [{ pubkey: s1.pubkeyHex, source: 'R1', weight: '100' }];
        set.truncated = true;
        stakeByHeight[84] = set;
        route(cp, [cp]);

        await sync.verifyCheckpointQuorum();

        assert.strictEqual(sync.isHalted(), false, 'a cap overflow is not proof of divergence');
        assert.strictEqual(sync._lastVerifiedCheckpointBlock, null, 'the step must not be attested');
        assert.ok(sawRange(), 'the walk ran and stopped at the truncated set');
    });

    it('oraclePublishSetAt carries the truncated flag only when the database set is flagged', async function(){
        const sync = makeSync('BTC', 'regtest');
        const flagged = [{ pubkey: 'aa', source: 'R1', weight: '100' }];
        flagged.truncated = true;
        stakeByHeight[90] = flagged;
        stakeByHeight[91] = [{ pubkey: 'aa', source: 'R1', weight: '100' }];

        assert.strictEqual((await sync.oraclePublishSetAt(90)).truncated, true);
        assert.strictEqual((await sync.oraclePublishSetAt(91)).truncated, undefined);
    });

    it('the stake-weight getters flag a set that hit the legacy row cap', async function(){
        const real = makeTestDatabase('replica_db', 'u', 'p', { isNull: (v) => v == null,
            throwError: (m) => { throw new Error(m); }, sleep: sinon.stub().resolves(), logError: sinon.stub() }, 'indexer');
        sinon.stub(console, 'warn');
        sinon.stub(real, 'getStatusId').resolves(1);
        const rows = [{ pubkey: 'a', source: 's1', weight: '5' }, { pubkey: 'b', source: 's2', weight: '5' }];
        sinon.stub(real, 'doQuery').callsFake(async () => rows);

        assert.strictEqual((await real.getStakeWeightsByCapabilityAsOf('oracle_publish', 106, '1', 2)).truncated, true);
        assert.strictEqual((await real.getStakeWeightsByCapability('oracle_publish', 106, '1', 2)).truncated, true);
        assert.strictEqual((await real.getStakeWeightsByCapabilityAsOf('oracle_publish', 106, '1', 3)).truncated, undefined);
    });
});

describe('ClientSync: a pinned seed on a chain that cannot follow rotation halts @regression', function(){
    registerHooks();

    for(const chain of ['LTC', 'DOGE']){
        it(chain + ' with a pinned seed halts on a checkpoint the pinned set does not sign, as with no seed', async function(){
            const sync = makeSync(chain, 'regtest'), launch = makeSigner(), other = makeSigner();
            pin(chain, 'regtest', launch);
            setSeed(chain, 'regtest', { block_index: 90, snapshot_block: 84, state_root: 'b0'.repeat(32) });
            const cp = signedCpAt(other, { chain, block_index: 100, snapshot_block: 100, state_root: 'd4'.repeat(32) });
            route(cp, [cp]);

            await sync.verifyCheckpointQuorum();

            assert.strictEqual(sync.isHalted(), true, 'a seed this chain cannot follow must not turn a halt into a wait');
            assert.strictEqual(sync.getHaltInfo().reason, 'checkpoint-quorum-divergence');
            assert.strictEqual(sync.getHaltInfo().mismatches[0].field, 'checkpoint_quorum');
            assert.strictEqual(sawRange(), false);
        });
    }
});
