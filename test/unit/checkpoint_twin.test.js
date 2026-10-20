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
 * consensus/stake_weighted_quorum and consensus/equivocation_header siblings; the
 * CHECKPOINT_COMMITMENT flag day is a registry row) is a code-identical TWIN of the
 * xchain-sdk copies. This guards that
 * the vendored copy actually verifies a real federation-signed checkpoint and
 * rejects a tampered one, so drift from the SDK is caught here rather than in
 * production (mirrors how merkle.js is golden-vector guarded).
 ********************************************************************/

const assert = require('assert');
const crypto = require('crypto');
const fs     = require('fs');
const path   = require('path');
const checkpoint = require('../../src/checkpoint');
const swq    = require('../../src/consensus/stake_weighted_quorum');
const { siblingCheckout, skipOrFail } = require('../helpers/sibling_checkout.js');

// Sibling resolution goes through siblingCheckout: skip when the SDK checkout is
// absent or links into a live main checkout, and throw where
// XCHAIN_REQUIRE_SIBLINGS=1 makes green-by-skip impossible.
const SYNC_SRC = path.join(__dirname, '../../src');
const SDK_SRC  = path.join(__dirname, '../../../xchain-sdk/src');
// The three code files kept code-identical to the SDK copies. The CHECKPOINT_COMMITMENT
// gate is a row in consensus/gate_registry/, whose part files are byte-checked against the
// reference-impl in consensus_primitive_conformance.test.js, so they are not listed here.
const TWINS =['checkpoint.js', 'consensus/stake_weighted_quorum.js', 'consensus/equivocation_header.js'];

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
    // Pins the exact output to the same golden literal xchain-sdk/test/unit/consensus/checkpoint.test.js uses,
    // so drift of sync's canonicalCheckpoint from the SDK/hub/indexer/explorer copies fails here, not in production.
    // (snapshot_block 900120 sits below the mainnet 961000 flag day, so this pins the legacy rootless,
    // unwrapped shape the SDK's golden vector uses; the boundary case below pins the armed shape.)
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

    // Byte-parity guard for the ACTIVE (post-CHECKPOINT_COMMITMENT) canonical: the
    // launch-epoch shape real hub-signed federation checkpoints use. Every quorum
    // case below signs and verifies through sync's OWN builder, so a one-sided drift
    // of the appended state_root|state_root_version|block_merkle_root|block_merkle_version
    // suffix (or the EQUIV header wrap) keeps sign==verify green AND keeps the rootless
    // mainnet golden green, while sync silently fails to verify real hub checkpoints.
    // regtest activates both CHECKPOINT_COMMITMENT and the EQUIV header at height 0, so
    // the canonical commits the four SPV-root fields and is EQUIV-wrapped. The expected
    // string is reconstructed here from the documented spec parts (NOT from the builder),
    // so a drift of canonicalCheckpoint fails here rather than in production. Mirrors the
    // explorer's 'regtest row with SPV roots' cross-check, but for the sync verifier.
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
});

describe('vendored checkpoint verifier (twin conformance) @regression', function(){

    // Mainnet arms CHECKPOINT_COMMITMENT, the EQUIV header and the stake-weighted quorum
    // together at snapshot_block 961000. Expected strings are written from the spec parts,
    // not the builder, and the height is a literal, so moving the gate must touch this test.
    it('canonicalCheckpoint pins the mainnet flag day (960999 legacy, 961000 roots and EQUIV wrap)', function(){
        const base = { chain: 'BTC', network: 'mainnet', block_index: 961003,
            block_hash: 'ab'.repeat(32), ledger_hash: 'cd'.repeat(32),
            actions_hash: 'ef'.repeat(32), contract_hash: '01'.repeat(32), checkpoint_seq: 418,
            state_root: 'd4'.repeat(32), state_root_version: 1,
            block_merkle_root: 'e5'.repeat(32), block_merkle_version: 1 };
        const head = 'XCHECKPOINT|BTC|mainnet|961003|' + 'ab'.repeat(32) + '|' + 'cd'.repeat(32) +
            '|' + 'ef'.repeat(32) + '|' + '01'.repeat(32) + '|418|';
        assert.strictEqual(
            checkpoint.canonicalCheckpoint(Object.assign({}, base, { snapshot_block: 960999 })),
            head + '960999',
            'one block below the mainnet flag day the canonical must stay rootless and unwrapped; the vendored builder has drifted');
        const raw = head + '961000' + '|' + 'd4'.repeat(32) + '|1|' + 'e5'.repeat(32) + '|1';
        assert.strictEqual(
            checkpoint.canonicalCheckpoint(Object.assign({}, base, { snapshot_block: 961000 })),
            'EQUIV|XCHECKPOINT|BTC|mainnet|961003|418|0||' + raw,
            'at the mainnet flag day the canonical must commit the roots and carry the EQUIV wrap; the vendored builder has drifted');
    });

    it('the mainnet checkpoint gates flip exactly at snapshot_block 961000', function(){
        const eq  = require('../../src/consensus/equivocation_header');
        const swq = require('../../src/consensus/stake_weighted_quorum');
        assert.strictEqual(eq.isEquivHeaderActive(960999, 'mainnet'), false);
        assert.strictEqual(eq.isEquivHeaderActive(961000, 'mainnet'), true);
        assert.strictEqual(swq.isStakeWeightedQuorumActive(960999, 'mainnet'), false);
        assert.strictEqual(swq.isStakeWeightedQuorumActive(961000, 'mainnet'), true);
        const rootless = { chain: 'BTC', network: 'mainnet', state_root: null, block_merkle_root: null };
        assert.strictEqual(checkpoint.commitmentMissing(Object.assign({}, rootless, { snapshot_block: 960999 })), false,
            'a rootless row below the flag day is legacy, not missing its commitment');
        assert.strictEqual(checkpoint.commitmentMissing(Object.assign({}, rootless, { snapshot_block: 961000 })), true,
            'a rootless row at the flag day cannot be verified');
    });
});

describe('vendored checkpoint verifier (twin conformance) @regression', function(){

    // Testnet is the one network whose checkpoint gates differ: the EQUIV wrap and the
    // stake-weighted quorum are active from genesis while the roots join the signed bytes
    // only at snapshot_block 146000, so below it the canonical is wrapped but rootless.
    it('canonicalCheckpoint pins the testnet flag day (145999 EQUIV-wrapped rootless, 146000 roots committed)', function(){
        const base = { chain: 'BTC', network: 'testnet', block_index: 146003,
            block_hash: 'ab'.repeat(32), ledger_hash: 'cd'.repeat(32),
            actions_hash: 'ef'.repeat(32), contract_hash: '01'.repeat(32), checkpoint_seq: 418,
            state_root: 'd4'.repeat(32), state_root_version: 1,
            block_merkle_root: 'e5'.repeat(32), block_merkle_version: 1 };
        const head = 'XCHECKPOINT|BTC|testnet|146003|' + 'ab'.repeat(32) + '|' + 'cd'.repeat(32) +
            '|' + 'ef'.repeat(32) + '|' + '01'.repeat(32) + '|418|';
        const wrap = 'EQUIV|XCHECKPOINT|BTC|testnet|146003|418|0||';
        assert.strictEqual(
            checkpoint.canonicalCheckpoint(Object.assign({}, base, { snapshot_block: 145999 })),
            wrap + head + '145999',
            'one block below the testnet flag day the canonical must be EQUIV-wrapped but rootless; the vendored builder has drifted');
        assert.strictEqual(
            checkpoint.canonicalCheckpoint(Object.assign({}, base, { snapshot_block: 146000 })),
            wrap + head + '146000' + '|' + 'd4'.repeat(32) + '|1|' + 'e5'.repeat(32) + '|1',
            'at the testnet flag day the canonical must commit the roots inside the EQUIV wrap; the vendored builder has drifted');
    });

    it('the testnet checkpoint gates: EQUIV and stake-weighted from genesis, commitment at snapshot_block 146000', function(){
        const eq  = require('../../src/consensus/equivocation_header');
        assert.strictEqual(eq.isEquivHeaderActive(0, 'testnet'), true);
        assert.strictEqual(swq.isStakeWeightedQuorumActive(0, 'testnet'), true);
        const rootless = { chain: 'BTC', network: 'testnet', state_root: null, block_merkle_root: null };
        assert.strictEqual(checkpoint.commitmentActive(Object.assign({}, rootless, { snapshot_block: 145999 })), false);
        assert.strictEqual(checkpoint.commitmentActive(Object.assign({}, rootless, { snapshot_block: 146000 })), true);
        assert.strictEqual(checkpoint.commitmentMissing(Object.assign({}, rootless, { snapshot_block: 145999 })), false,
            'a rootless testnet row below the flag day is legacy, not missing its commitment');
        assert.strictEqual(checkpoint.commitmentMissing(Object.assign({}, rootless, { snapshot_block: 146000 })), true,
            'a rootless testnet row at the flag day cannot be verified');
    });
});

describe('vendored checkpoint verifier (twin conformance) @regression', function(){

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

    it('rejects a post-flag-day empty-string root even when the signature covers it', function(){
        const s = makeSigner();
        const validators = [{ pubkey: s.pubkeyHex, source: s.pubkeyHex, weight: '100' }];
        for(const field of ['state_root', 'block_merkle_root']){
            const cp = signedCheckpoint(s, { [field]: '' });
            assert.strictEqual(checkpoint.commitmentMissing(cp), true, 'empty ' + field + ' is missing');
            assert.strictEqual(checkpoint.verifyCheckpoint(cp, validators).valid, false);
        }
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
});

// Add each signer's signature over cp's canonical to its signature list.
function addSignatures(cp, signers){
    const canonical = checkpoint.canonicalCheckpoint(cp);
    for(const s of signers) cp.validator_signatures.push({ pubkey: s.pubkeyHex, sig: sign(s.privateKey, canonical) });
    return cp;
}
const weighted = (s, source, weight) => ({ pubkey: s.pubkeyHex, source: source || s.pubkeyHex, weight: weight || '100' });

// Behavioural vectors for the verifier's fail-closed branches. The twin loop below skips
// without the SDK checkout and passes on an edit made to both copies, so these must hold alone.
describe('vendored checkpoint verifier (twin conformance) @regression', function(){

    it('a malformed validator_signatures value counts as zero signatures', function(){
        const s = makeSigner();
        const good = signedCheckpoint(s);
        const list = good.validator_signatures;
        const verify = (value) => checkpoint.verifyCheckpoint(Object.assign({}, good, { validator_signatures: value }), [weighted(s)]);
        assert.strictEqual(verify(JSON.stringify(list)).valid, true, 'a JSON-string list parses and verifies');
        const warn = console.warn;
        console.warn = () => {};
        try {
            for(const bad of [JSON.stringify(list) + 'x', 'not json', list[0], JSON.stringify(list[0]), 42, null]){
                const r = verify(bad);
                assert.strictEqual(r.valid, false, 'malformed ' + JSON.stringify(bad) + ' must not verify');
                assert.strictEqual(r.validSigs, 0, 'malformed ' + JSON.stringify(bad) + ' counts zero signatures');
            }
        } finally { console.warn = warn; }
    });

    it('a weighted-regime set that mixes weighted and unweighted entries never verifies', function(){
        const s1 = makeSigner(), s2 = makeSigner();
        const cp = addSignatures(signedCheckpoint(s1), [s2]);
        const mixes = [[weighted(s1), s2.pubkeyHex], [weighted(s1), { pubkey: s2.pubkeyHex, source: s2.pubkeyHex }]];
        for(const set of mixes) assert.strictEqual(checkpoint.verifyCheckpoint(cp, set).valid, false);
        // meetsStakeThreshold fails closed on these sets too, so stub it open to pin the
        // every-entry and non-empty gate on its own.
        const real = swq.meetsStakeThreshold;
        swq.meetsStakeThreshold = () => true;
        try {
            assert.strictEqual(checkpoint.verifyCheckpoint(cp, [weighted(s1), weighted(s2)]).valid, true,
                'the stub must be reached for a fully weighted set');
            for(const set of mixes) assert.strictEqual(checkpoint.verifyCheckpoint(cp, set).valid, false,
                'one unweighted entry must fail the set before the stake predicate runs');
            assert.strictEqual(checkpoint.verifyCheckpoint(cp, []).valid, false, 'an empty set must fail before the stake predicate runs');
        } finally { swq.meetsStakeThreshold = real; }
    });
});

describe('vendored checkpoint verifier (twin conformance) @regression', function(){

    it('two pubkeys that share a source count that source once', function(){
        const a = makeSigner(), b = makeSigner(), c = makeSigner();
        // S = 10 (source X, counted once) + 15 (source Y) = 25; the bar is 3 * tally > 50.
        const set = [weighted(a, 'X', '10'), weighted(b, 'X', '10'), weighted(c, 'Y', '15')];
        assert.strictEqual(checkpoint.verifyCheckpoint(addSignatures(signedCheckpoint(a), [b]), set).valid, false,
            'a and b share X, so the tally is 10, not 20');
        assert.strictEqual(checkpoint.verifyCheckpoint(addSignatures(signedCheckpoint(a), [c]), set).valid, true,
            'X plus Y is 25 of 25');
    });

    it('below the mainnet flag day the count regime needs 2f+1 valid signatures', function(){
        const signers = Array.from({ length: 7 }, makeSigner);
        const countCheckpoint = (k) => addSignatures({ chain: 'BTC', network: 'mainnet', block_index: 900123,
            block_hash: 'ab'.repeat(32), ledger_hash: 'cd'.repeat(32), actions_hash: 'ef'.repeat(32),
            contract_hash: '01'.repeat(32), checkpoint_seq: 417, snapshot_block: 900120,
            validator_signatures: [] }, signers.slice(0, k));
        const cases = [[4, 3, true], [4, 2, false], [7, 5, true], [7, 4, false]];
        for(const [n, k, valid] of cases){
            const r = checkpoint.verifyCheckpoint(countCheckpoint(k), signers.slice(0, n).map(s => s.pubkeyHex));
            assert.strictEqual(r.weighted, false, 'mainnet below 961000 is the count regime');
            assert.strictEqual(r.quorum, n === 4 ? 3 : 5, n + ' validators need 2f+1');
            assert.strictEqual(r.valid, valid, k + ' of ' + n + ' signatures');
        }
        const none = checkpoint.verifyCheckpoint(countCheckpoint(7), []);
        assert.strictEqual(none.quorum, 1);
        assert.strictEqual(none.valid, false, 'an empty count-regime set never verifies');
    });
});

describe('vendored checkpoint verifier (twin conformance) @regression', function(){

    // The cases above sign and verify through sync's OWN copy, so they catch a
    // behaviour divergence only where a golden vector happens to cover it. Two
    // one-sided SDK edits still rode that channel unnoticed (AML #5402): a
    // backtracking URL-trim replaced with a loop, and commitmentMissing added to
    // module.exports. Neither touched canonicalCheckpoint, so nothing here went
    // red. This compares the whole file, code only, against the sibling.
    for(const file of TWINS) it('src/' + file + ' is code-identical to the xchain-sdk copy (comments excepted)', function(){
        const theirs = path.join(SDK_SRC, file);
        if(!skipOrFail(this, siblingCheckout(__dirname, theirs), 'the checkpoint twin guard')) return;
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
