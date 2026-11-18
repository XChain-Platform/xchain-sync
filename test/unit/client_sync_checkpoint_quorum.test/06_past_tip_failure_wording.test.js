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
 ********************************************************************/

'use strict';

const assert = require('assert');
const crypto = require('crypto');
const sinon = require('sinon');
const ClientSync = require('../../../src/client/sync');
const Utility = require('../../../src/util');
const HashVerifier = require('../../../src/client/hash_verifier');
const checkpoint = require('../../../src/checkpoint');
const { withDbMixins } = require('../../helpers/db_mixins.js');

function makeSigner(){
    const { publicKey, privateKey } = crypto.generateKeyPairSync('ed25519');
    const spki = publicKey.export({ format: 'der', type: 'spki' });
    return { privateKey, pubkeyHex: spki.subarray(spki.length - 32).toString('hex') };
}

function makeSync(network){
    const state = new Map();
    const db = withDbMixins({ dbName: 'test_db', dbType: 'indexer',
        recordHalt: sinon.stub().resolves({ block_index: 0 }),
        getActiveHalt: sinon.stub().resolves(null), clearHalt: sinon.stub().resolves(1),
        getSyncState: sinon.stub().callsFake(async key => state.get(key) || null),
        setSyncState: sinon.stub().callsFake(async (key, value) => { state.set(key, value); }) });
    const config = { SYNC_SOURCES: 'http://a:3006', VERIFY_CHECKPOINT_QUORUM: true, CHECKPOINT_FRESHNESS_STRICT: false };
    const sync = new ClientSync('BTC', network, db, {}, {}, new HashVerifier(), config, new Utility());
    sync.lastAppliedBlock = 100;
    return sync;
}

// A checkpoint past the tip signed by `signer` only.
function signedCp(signer, network, snapshotBlock){
    const cp = { chain: 'BTC', network: network, block_index: 2000, block_hash: 'c0'.repeat(32),
        ledger_hash: 'a1'.repeat(32), actions_hash: 'b2'.repeat(32), contract_hash: 'c3'.repeat(32),
        checkpoint_seq: 9, snapshot_block: snapshotBlock, state_root: 'd4'.repeat(32), state_root_version: 1,
        block_merkle_root: 'e5'.repeat(32), block_merkle_version: 1 };
    cp.validator_signatures = [{ pubkey: signer.pubkeyHex,
        sig: crypto.sign(null, Buffer.from(checkpoint.canonicalCheckpoint(cp), 'utf8'), signer.privateKey).toString('hex') }];
    return cp;
}

function pastTipWarning(network, snapshotBlock, validatorsFor){
    const signer = makeSigner();
    const others = [makeSigner(), makeSigner()];
    const sync = makeSync(network);
    const verdict = sync.verifyServedCheckpoint(signedCp(signer, network, snapshotBlock), validatorsFor(signer, others));
    assert.strictEqual(verdict.valid, false, 'fixture must fail the quorum');
    assert.strictEqual(sync.judgeCheckpointPastTip(signedCp(signer, network, snapshotBlock),
        validatorsFor(signer, others)), 'unanchored');
    return { line: console.warn.getCalls().map(c => String(c.args[0] || '')).find(l => /FAILED the pinned quorum/.test(l)), verdict };
}

describe('ClientSync: the past-tip quorum warning names the regime that decided it @regression', function(){
    beforeEach(function(){ sinon.stub(console, 'log'); sinon.stub(console, 'warn'); sinon.stub(console, 'error'); });
    afterEach(function(){ sinon.restore(); });

    it('reports a stake shortfall without the count-regime quorum figure', function(){
        const { line, verdict } = pastTipWarning('regtest', 984, (s, o) =>
            [s, o[0], o[1]].map(v => ({ pubkey: v.pubkeyHex, weight: '100', source: v.pubkeyHex })));
        assert.strictEqual(verdict.weighted, true);
        assert.ok(line, 'no past-tip quorum warning was logged');
        assert.ok(/stake-weighted quorum not met/.test(line), line);
        assert.ok(!new RegExp('\\b1/' + verdict.quorum + ' valid sigs').test(line), line);
    });

    it('keeps the signature count against the count quorum below the stake-weighted flag day', function(){
        const { line, verdict } = pastTipWarning('mainnet', 900005, (s, o) => [s, o[0], o[1]].map(v => v.pubkeyHex));
        assert.strictEqual(verdict.weighted, false);
        assert.ok(line.includes('(1/' + verdict.quorum + ' valid sigs, weighted=false)'), line);
    });
});
