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
 * Vendored checkpoint verifier conformance. src/checkpoint.js (+ its
 * stake_weighted_quorum / equivocation_header / checkpoint_commitment_activation
 * siblings) is a byte-identical TWIN of the xchain-sdk copies. This guards that
 * the vendored copy actually verifies a real federation-signed checkpoint and
 * rejects a tampered one, so drift from the SDK is caught here rather than in
 * production (mirrors how merkle.js is golden-vector guarded).
 ********************************************************************/

const assert = require('assert');
const crypto = require('crypto');
const fs     = require('fs');
const path   = require('path');
const checkpoint = require('../../src/checkpoint');

// Sibling resolution + hard-fail policy: same conventions as
// blockhash-conformance-twin.test.js. Skip when the SDK checkout is absent,
// throw where XCHAIN_REQUIRE_SIBLINGS=1 makes green-by-skip impossible.
const SYNC_SRC = path.resolve(__dirname, '..', '..', 'src');
const SDK_SRC  = path.resolve(__dirname, '..', '..', '..', 'xchain-sdk', 'src');
const SIBLING_REQUIRED = process.env.XCHAIN_REQUIRE_SIBLINGS === '1';
// The four files this suite's header declares twins of the SDK copies.
const TWINS = ['checkpoint.js', 'stake_weighted_quorum.js', 'equivocation_header.js',
    'checkpoint_commitment_activation.js'];

// Cut unquoted // comments (tracking ' " ` quote state per line) so the two
// sides compare on CODE. The prose is independently worded in both copies and
// always has been; it is the code that has to stay lockstep.
function stripComments(src){
    return src.split('\n').map(line => {
        let q = null;
        for(let i = 0; i < line.length; i++){
            const ch = line[i];
            if(q){ if(ch === q && line[i-1] !== '\\') q = null; continue; }
            if(ch === "'" || ch === '"' || ch === '`'){ q = ch; continue; }
            if(ch === '/' && line[i+1] === '/') return line.slice(0, i);
        }
        return line;
    }).join('\n');
}

// Code-only form: block comments out, blank lines out, indentation collapsed.
function codeOnly(src){
    return stripComments(src.replace(/\/\*[\s\S]*?\*\//g, ''))
        .split('\n').map(l => l.trim()).filter(Boolean).join('\n');
}

// Raw 32-byte Ed25519 pubkey + a signer, matching the federation's key encoding.
function makeSigner(){
    const { publicKey, privateKey } = crypto.generateKeyPairSync('ed25519');
    const spki = publicKey.export({ format: 'der', type: 'spki' });
    return { privateKey, pubkeyHex: spki.subarray(spki.length - 32).toString('hex') };
}
function sign(privateKey, canonical){
    return crypto.sign(null, Buffer.from(canonical, 'utf8'), privateKey).toString('hex');
}
// A checkpoint signed by `signer`. regtest => CHECKPOINT_COMMITMENT + stake-weighted
// quorum are active (activation height 0), so the canonical commits the roots and
// the quorum is weighted, the realistic launch-epoch shape.
function signedCheckpoint(signer, overrides){
    const cp = Object.assign({
        chain: 'BTC', network: 'regtest', block_index: 100, block_hash: 'c0'.repeat(32),
        ledger_hash: 'a1'.repeat(32), actions_hash: 'b2'.repeat(32), contract_hash: 'c3'.repeat(32),
        checkpoint_seq: 0, snapshot_block: 100, state_root: 'd4'.repeat(32), state_root_version: 1,
        block_merkle_root: 'e5'.repeat(32), block_merkle_version: 1, validator_signatures: []
    }, overrides || {});
    cp.validator_signatures = [{ pubkey: signer.pubkeyHex, sig: sign(signer.privateKey, checkpoint.canonicalCheckpoint(cp)) }];
    return cp;
}

describe('vendored checkpoint verifier (twin conformance) @regression', function(){
    // Byte-parity guard for the canonical signing string: every other case in this suite signs and verifies
    // through sync's own builder, so a symmetric one-sided edit (drop/reorder a field, change a delimiter)
    // would keep sign==verify green while real hub-signed federation checkpoints silently fail to verify.
    // Pins the exact output to the same golden literal xchain-sdk/test/unit/checkpoint.test.js uses, so drift
    // of sync's canonicalCheckpoint from the SDK/hub/indexer/explorer copies fails here, not in production.
    // (mainnet CHECKPOINT_COMMITMENT is inert, so the canonical carries no SPV root suffix, matching the SDK's golden vector.)
    it('canonicalCheckpoint matches the ANCHOR spec byte-for-byte (SDK-pinned golden vector)', function(){
        const cp = { chain: 'BTC', network: 'mainnet', block_index: 900123,
            block_hash: 'ab'.repeat(32), ledger_hash: 'cd'.repeat(32),
            actions_hash: 'ef'.repeat(32), contract_hash: '01'.repeat(32),
            checkpoint_seq: 417, snapshot_block: 900120 };
        assert.strictEqual(
            checkpoint.canonicalCheckpoint(cp),
            'XCHECKPOINT|BTC|mainnet|900123|' + 'ab'.repeat(32) + '|' + 'cd'.repeat(32) +
            '|' + 'ef'.repeat(32) + '|' + '01'.repeat(32) + '|417|900120',
            'sync canonicalCheckpoint drifted from the canonical XCHECKPOINT signing string; re-align the vendored twin to the SDK copy');
    });

    // Byte-parity guard for the ACTIVE (post-CHECKPOINT_COMMITMENT) canonical, the launch-epoch shape real
    // hub-signed federation checkpoints use. Every quorum case below signs and verifies through sync's own
    // builder, so a one-sided drift of the appended state_root|state_root_version|block_merkle_root|block_merkle_version
    // suffix (or the EQUIV header wrap) would keep sign==verify green and keep the rootless mainnet golden
    // green, while sync silently fails to verify real hub checkpoints. Regtest activates both CHECKPOINT_COMMITMENT
    // and the EQUIV header at height 0, so the canonical commits the four SPV-root fields and is EQUIV-wrapped;
    // the expected string below is reconstructed from the documented spec parts (not from the builder) so
    // drift fails here rather than in production. Mirrors the explorer's 'regtest row with SPV roots' cross-check.
    it('canonicalCheckpoint matches the ACTIVE SPV-root spec byte-for-byte (EQUIV-wrapped, roots committed)', function(){
        const cp = { chain: 'BTC', network: 'regtest', block_index: 100,
            block_hash: 'c0'.repeat(32), ledger_hash: 'a1'.repeat(32),
            actions_hash: 'b2'.repeat(32), contract_hash: 'c3'.repeat(32),
            checkpoint_seq: 0, snapshot_block: 100,
            state_root: 'd4'.repeat(32), state_root_version: 1,
            block_merkle_root: 'e5'.repeat(32), block_merkle_version: 1 };
        // RAW base + additive SPV-root suffix (spec §6.1):
        const raw = 'XCHECKPOINT|BTC|regtest|100|' + 'c0'.repeat(32) + '|' + 'a1'.repeat(32) +
            '|' + 'b2'.repeat(32) + '|' + 'c3'.repeat(32) + '|0|100' +
            '|' + 'd4'.repeat(32) + '|1|' + 'e5'.repeat(32) + '|1';
        // EQUIV wrap: 'EQUIV|' + engineTag|roundId|view + '||' + raw, where
        // roundId = chain|network|block_index|checkpoint_seq and view = 0.
        const expected = 'EQUIV|XCHECKPOINT|BTC|regtest|100|0|0||' + raw;
        assert.strictEqual(
            checkpoint.canonicalCheckpoint(cp),
            expected,
            'sync canonicalCheckpoint drifted from the ACTIVE SPV-root XCHECKPOINT signing string; re-align the vendored twin to the SDK/hub/indexer/explorer copies');
    });

    it('verifies a real Ed25519 quorum-signed checkpoint against its pinned set', function(){
        const s = makeSigner();
        const cp = signedCheckpoint(s);
        const validators = [{ pubkey: s.pubkeyHex, source: s.pubkeyHex, weight: '100' }];
        const r = checkpoint.verifyCheckpoint(cp, validators);
        assert.strictEqual(r.valid, true);
        assert.strictEqual(r.weighted, true, 'regtest activates the stake-weighted regime');
    });

    it('rejects a checkpoint with a tampered field (signature no longer matches the canonical)', function(){
        const s = makeSigner();
        const cp = signedCheckpoint(s);
        cp.state_root = 'ff'.repeat(32);                         // mutate after signing
        const validators = [{ pubkey: s.pubkeyHex, source: s.pubkeyHex, weight: '100' }];
        assert.strictEqual(checkpoint.verifyCheckpoint(cp, validators).valid, false);
    });

    it('rejects a checkpoint whose signer is not in the pinned set', function(){
        const real = makeSigner(), rogue = makeSigner();
        const cp = signedCheckpoint(rogue);                      // signed by a non-pinned key
        const validators = [{ pubkey: real.pubkeyHex, source: real.pubkeyHex, weight: '100' }];
        assert.strictEqual(checkpoint.verifyCheckpoint(cp, validators).valid, false);
    });

    it('an empty validator set can never verify', function(){
        const s = makeSigner();
        assert.strictEqual(checkpoint.verifyCheckpoint(signedCheckpoint(s), []).valid, false);
    });

    it('a garbage-then-valid duplicate for one signer still PASSES (seen marked after verify)', function(){
        const s1 = makeSigner(), s2 = makeSigner();
        const validators = [{ pubkey: s1.pubkeyHex, source: s1.pubkeyHex, weight: '10' },
                            { pubkey: s2.pubkeyHex, source: s2.pubkeyHex, weight: '30' }];
        const cp = signedCheckpoint(s1);                         // 10 of 40 alone: 3·10 < 2·40
        cp.validator_signatures.push({ pubkey: s2.pubkeyHex, sig: sign(s2.privateKey, checkpoint.canonicalCheckpoint(cp)) });
        // The signature list is server-supplied (attacker-influenceable): prepend an
        // INVALID entry for s2 ordered before its genuine one. Marking "seen" on first
        // encounter would suppress the real signature and false-reject a quorate
        // checkpoint (order-dependent quorum under-count); the hardened order
        // (matching the SDK/explorer copies) must still count it.
        cp.validator_signatures = [{ pubkey: s2.pubkeyHex, sig: '00'.repeat(64) }].concat(cp.validator_signatures);
        assert.strictEqual(checkpoint.verifyCheckpoint(cp, validators).valid, true);
    });

    // The cases above sign and verify through sync's OWN copy, so they catch a
    // behaviour divergence only where a golden vector happens to cover it. Two
    // one-sided SDK edits still rode that channel unnoticed (AML #5402): a
    // backtracking URL-trim replaced with a loop, and commitmentMissing added to
    // module.exports. Neither touched canonicalCheckpoint, so nothing here went
    // red. This compares the whole file, code only, against the sibling.
    for(const file of TWINS) it('src/' + file + ' is code-identical to the xchain-sdk copy (comments excepted)', function(){
        const theirs = path.join(SDK_SRC, file);
        if(!fs.existsSync(theirs)){
            if(SIBLING_REQUIRED)
                throw new Error('checkpoint twin guard cannot run: sibling missing at ' + theirs
                    + ' (check out xchain-sdk, or unset XCHAIN_REQUIRE_SIBLINGS to accept the gap)');
            this.skip();
            return;
        }
        const mine = codeOnly(fs.readFileSync(path.join(SYNC_SRC, file), 'utf8'));
        const sdk  = codeOnly(fs.readFileSync(theirs, 'utf8'));
        if(mine === sdk) return;
        const a = mine.split('\n'), b = sdk.split('\n');
        let at = 0;
        while(at < a.length && at < b.length && a[at] === b[at]) at++;
        assert.fail('src/' + file + ' has drifted from the xchain-sdk twin at code line ' + (at + 1)
            + '\n  sync: ' + JSON.stringify(a[at])
            + '\n  sdk : ' + JSON.stringify(b[at])
            + '\nPort the edit to BOTH copies; this set carries one of the six XCHECKPOINT canonical builders.');
    });
});
