/*********************************************************************
 *
 * Copyright © 2025-2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC - https://dankest.llc
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This file is part of XChain Platform. Licensed under the GNU Affero
 * General Public License v3.0 or later; see LICENSE.md. A commercial
 * license (without AGPL source-disclosure terms) is available -
 * contact legal@dankest.llc.
 *
 **********************************************************************
 *
 * XChain Sync - superseding checkpoint verification and following
 *
 ********************************************************************/

'use strict';

const assert = require('assert');
const crypto = require('crypto');
const checkpoint = require('../../../src/checkpoint');
const ClientSync = require('../../../src/client/sync');

const RULES_DIGEST = 'ab'.repeat(32);

function makeSigner(){
    const { publicKey, privateKey } = crypto.generateKeyPairSync('ed25519');
    const spki = publicKey.export({ format: 'der', type: 'spki' });
    return { privateKey, pubkey: spki.subarray(spki.length - 32).toString('hex') };
}

function checkpointAt(overrides){
    return Object.assign({
        chain: 'BTC', network: 'regtest', block_index: 120,
        block_hash: 'c0'.repeat(32), ledger_hash: 'a1'.repeat(32),
        actions_hash: 'b2'.repeat(32), contract_hash: 'c3'.repeat(32),
        checkpoint_seq: 12, snapshot_block: 120,
        state_root: 'd4'.repeat(32), state_root_version: 1,
        block_merkle_root: 'e5'.repeat(32), block_merkle_version: 1,
        validator_signatures: [],
    }, overrides || {});
}

function sign(cp, signer){
    cp.validator_signatures = [{
        pubkey: signer.pubkey,
        sig: crypto.sign(null, Buffer.from(checkpoint.canonicalCheckpoint(cp), 'utf8'),
            signer.privateKey).toString('hex'),
    }];
    return cp;
}

describe('Checkpoint supersession @regression', function(){
    it('verifies a superseding checkpoint with the gated suffix inside the EQUIV wrapper', function(){
        const signer = makeSigner();
        const cp = checkpointAt({ supersedes_from: 100, rules_digest: RULES_DIGEST });
        const rootSuffix = '|' + cp.state_root + '|1|' + cp.block_merkle_root + '|1';
        const raw = 'XCHECKPOINT|BTC|regtest|120|' + cp.block_hash + '|' + cp.ledger_hash +
            '|' + cp.actions_hash + '|' + cp.contract_hash + '|12|120' + rootSuffix +
            '|SUPERSEDE|100|' + RULES_DIGEST;
        const expected = 'EQUIV|XCHECKPOINT|BTC|regtest|120|12|0||' + raw;

        assert.strictEqual(checkpoint.canonicalCheckpoint(cp), expected);
        sign(cp, signer);
        const result = checkpoint.verifyCheckpoint(cp,
            [{ pubkey: signer.pubkey, weight: '100', source: 'validator-1' }]);
        assert.strictEqual(result.valid, true);
        assert.strictEqual(result.canonical, expected);
    });

    it('keeps a plain checkpoint byte-identical to the pre-supersession canonical', function(){
        const cp = checkpointAt();
        const raw = 'XCHECKPOINT|BTC|regtest|120|' + cp.block_hash + '|' + cp.ledger_hash +
            '|' + cp.actions_hash + '|' + cp.contract_hash + '|12|120|' +
            cp.state_root + '|1|' + cp.block_merkle_root + '|1';

        assert.strictEqual(checkpoint.canonicalCheckpoint(cp),
            'EQUIV|XCHECKPOINT|BTC|regtest|120|12|0||' + raw);
    });

    it('refuses supersession fields below the activation gate', function(){
        const signer = makeSigner();
        const cp = checkpointAt({
            network: 'mainnet', block_index: 900123, checkpoint_seq: 417,
            snapshot_block: 900120, supersedes_from: 900000, rules_digest: RULES_DIGEST,
        });
        sign(cp, signer);

        const result = checkpoint.verifyCheckpoint(cp, [signer.pubkey]);
        assert.strictEqual(result.valid, false);
        assert.strictEqual(result.validSigs, 0);
    });

    it('follow-forward advances past a superseded step without a divergence halt', async function(){
        const seed = checkpointAt({ block_index: 90, checkpoint_seq: 9, snapshot_block: 84 });
        const superseded = checkpointAt({ block_index: 110, checkpoint_seq: 11, snapshot_block: 90 });
        const superseder = checkpointAt({
            block_index: 120, checkpoint_seq: 12, snapshot_block: 110,
            supersedes_from: 100, rules_digest: RULES_DIGEST,
        });
        const visited = [];
        const sync = Object.assign(Object.create(ClientSync.prototype), {
            chain: 'BTC', coinTicker: 'BTC', network: 'regtest', lastAppliedBlock: 120,
            config: {}, sources: ['source'],
            checkpointRootsMatchLocal: async () => ({ status: 'match', mismatches: [] }),
            fetchCheckpointRange: async () => ({ chain: [superseded, superseder] }),
            attestRotationStep: async (next) => {
                visited.push(next.checkpoint_seq);
                if(next === superseded)
                    return { verdict: 'divergence', mismatches: [{ field: 'checkpoint_quorum' }] };
                return null;
            },
        });

        const result = await sync.followCheckpointForward(superseder, seed, 'source');

        assert.strictEqual(result.verdict, 'ok');
        assert.strictEqual(result.attested, superseder);
        assert.deepStrictEqual(visited, [12], 'the superseded step must be non-binding');
    });
});
