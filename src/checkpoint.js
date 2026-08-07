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
 *
 * XChain SDK - State Checkpoint Verifier
 *
 * Client-side verification of quorum-signed state checkpoints: the
 * light-client primitive that lets a wallet or application verify an
 * indexer/explorer's state against `2f+1` `oracle_publish` validator
 * signatures instead of trusting any single operator. Verification is
 * pure local crypto (Node built-in Ed25519): nothing here trusts the
 * server's own `verified` flag.
 *
 * Spec: xchain-documentation/protocol/actions/ANCHOR.md
 *
 ********************************************************************/

const crypto = require('crypto');
const eq     = require('./equivocation_header.js');
const swq    = require('./stake_weighted_quorum.js');
const ckpt   = require('./checkpoint_commitment_activation.js');

// ASN.1 DER prefix for Ed25519 SPKI. Mirrors the hub's ValidatorIdentity and
// the indexer's ed25519.js, so validator signatures verify identically here.
const SPKI_ED25519_PREFIX = Buffer.from('302a300506032b6570032100', 'hex');

// Build the canonical signing string for a checkpoint object. MUST stay
// byte-identical to the hub's StateCheckpointEngine.canonicalCheckpoint, the
// indexer's ANCHOR verifier, xchain-sdk/src/checkpoint.js canonicalCheckpoint,
// xchain-explorer/src/XChainExplorer.js canonicalCheckpointString,
// xchain-indexer/src/recovery.js's _wrapperCanonical, and
// xchain-hub/src/StateAnchorPublisher.js's _archiveCanonical:
//   XCHECKPOINT|CHAIN|NETWORK|BLOCK_INDEX|BLOCK_HASH|LEDGER_HASH|ACTIONS_HASH|CONTRACT_HASH|CHECKPOINT_SEQ|SNAPSHOT_BLOCK
function canonicalCheckpoint(cp){
    if (!cp) throw new Error('CheckpointVerifier: checkpoint object required');
    let raw = ['XCHECKPOINT', cp.chain, cp.network, String(cp.block_index), cp.block_hash,
            cp.ledger_hash, cp.actions_hash, cp.contract_hash,
            String(cp.checkpoint_seq), String(cp.snapshot_block)].join('|');
    // SPV Phase 2 (spec §6.1): at/above the CHECKPOINT_COMMITMENT flag-day the signed
    // string additively commits the light-client roots + version bytes (read from the
    // checkpoint row the explorer serves). Appended to the RAW string BEFORE the EQUIV
    // wrap. Append only when the roots are present (post-flag-day the hub never signs a
    // rootless checkpoint, so this is always true for real post-flag-day rows; the guard
    // keeps legacy/null-root rows on their original rootless canonical). MUST stay byte-
    // identical to the hub engine + indexer ANCHOR + explorer.
    if(ckpt.isCheckpointCommitmentActive(cp.snapshot_block, cp.network) &&
       cp.state_root != null && cp.block_merkle_root != null &&
       cp.state_root_version != null && cp.block_merkle_version != null)
        raw += '|' + [String(cp.state_root).toLowerCase(), String(cp.state_root_version),
                      String(cp.block_merkle_root).toLowerCase(), String(cp.block_merkle_version)].join('|');
    // At/above the EQUIV flag-day (gated on the BTC snapshot_block + network) the v0
    // canonical is wrapped in the uniform header (TAG=XCHECKPOINT, v0 ROUND_ID, VIEW=0);
    // below it the bare bytes (must byte-match the hub + indexer).
    if(eq.isEquivHeaderActive(cp.snapshot_block, cp.network))
        return eq.buildEquivCanonical(eq.ENGINE_TAGS.CHECKPOINT,
            cp.chain + '|' + cp.network + '|' + cp.block_index + '|' + cp.checkpoint_seq, 0, raw);
    return raw;
}

// Verify one Ed25519 signature over a UTF-8 payload (never throws).
function verifySignature(payload, sigHex, pubkeyHex){
    if (!payload || !sigHex || !pubkeyHex) return false;
    if (!/^[0-9a-fA-F]{64}$/.test(pubkeyHex)) return false;
    if (!/^[0-9a-fA-F]{128}$/.test(sigHex)) return false;
    try {
        let spkiDer = Buffer.concat([SPKI_ED25519_PREFIX, Buffer.from(pubkeyHex, 'hex')]);
        let pubkeyObj = crypto.createPublicKey({ key: spkiDer, format: 'der', type: 'spki' });
        return crypto.verify(null, Buffer.from(payload, 'utf8'), pubkeyObj, Buffer.from(sigHex, 'hex'));
    } catch (e) {
        return false;
    }
}

// Verify a checkpoint against a qualifying validator set.
//   checkpoint: { chain, network, block_index, block_hash, ledger_hash,
//                 actions_hash, contract_hash, checkpoint_seq, snapshot_block,
//                 validator_signatures } (signatures as a JSON string or array)
//   validators: the `oracle_publish` set qualified at the checkpoint's
//               snapshot_block. Each entry is either a bare 64-hex pubkey (legacy
//               count regime) or an object { pubkey, weight, source } (required at
//               or above the stake-weighted flag-day). Typically the explorer
//               verify endpoint's `validators`, or an independently fetched set.
// Returns { valid, validSigs, quorum, weighted, canonical }. Below the flag-day
// `valid` means validSigs reached the count `2f+1`; at/above it `valid` means the
// VALID signers' distinct sources clear the source-deduped 3·Σ > 2·S stake
// predicate. Pure local verification: the `weighted` decision is derived locally
// from the checkpoint's snapshot_block + network (the SDK trusts neither the
// server's `verified` flag nor any server-supplied `is_weighted`); only the stake
// WEIGHTS themselves come from the supplied set, exactly as the pubkeys always have.

// Does one validator entry carry the stake fields the weighted regime requires?
// Blank source and missing weight are BOTH disqualifying: meetsStakeThreshold
// fails closed on the former but silently reads the latter as '0', so the
// weight check has to live here (). A negative weight is rejected
// there and again here, since a caller reaching this gate should never see one.
// Is the commitment active for this checkpoint while a required field is absent?
// True means the row cannot be verified at all: the canonical it would be checked
// against is the legacy rootless one, which is not what a post-flag-day producer signs.
function commitmentMissing(cp){
    if(!cp || !ckpt.isCheckpointCommitmentActive(cp.snapshot_block, cp.network)) return false;
    return cp.state_root === null || cp.state_root === undefined
        || cp.block_merkle_root === null || cp.block_merkle_root === undefined
        || cp.state_root_version === null || cp.state_root_version === undefined
        || cp.block_merkle_version === null || cp.block_merkle_version === undefined;
}

function isWeightedEntry(v){
    if(!v || typeof v !== 'object') return false;
    if(v.source === null || v.source === undefined || String(v.source).trim() === '') return false;
    if(v.weight === null || v.weight === undefined || String(v.weight).trim() === '') return false;
    let w = Number(v.weight);
    return Number.isFinite(w) && w >= 0;
}

function verifyCheckpoint(checkpoint, validators){
    let canonical = canonicalCheckpoint(checkpoint);
    let vset      = validators || [];
    let qualified = new Set(vset.map(p => String(p && p.pubkey !== undefined ? p.pubkey : p).toLowerCase()));
    let quorum    = (qualified.size <= 1) ? 1 : Math.max(2 * Math.floor((qualified.size - 1) / 3) + 1, Math.ceil((qualified.size + 1) / 2));

    // Weighted-or-count is gated locally on the BTC-anchored snapshot_block +
    // network, byte-for-byte the same flag-day the hub/indexer flip on, and the
    // same gating the canonical signing string already applies for the equiv header.
    let weighted = swq.isStakeWeightedQuorumActive(checkpoint && checkpoint.snapshot_block, checkpoint && checkpoint.network);

    // Post-activation a checkpoint MUST carry the commitment fields. canonicalCheckpoint
    // appends the suffix only when all four are present, and that guard is correct there
    // (the canonical bytes must stay identical to the hub, indexer and explorer), but it
    // means a ROOTLESS post-flag-day row silently falls back to the legacy preimage and
    // its signatures then verify against it. Structural rejection therefore lives here,
    // in the verifier, where failing closed masks nothing ().
    if (commitmentMissing(checkpoint))
        return { valid: false, validSigs: 0, quorum: quorum, weighted: weighted, canonical: canonical };

    let sigs = checkpoint.validator_signatures;
    if (typeof sigs === 'string'){
        try { sigs = JSON.parse(sigs); } catch (e) {
            console.warn('[checkpoint] malformed validator_signatures at checkpoint_seq ' +
                          (checkpoint && checkpoint.checkpoint_seq) + '; treating as zero signatures (fail-closed):', e.message);
            sigs = [];
        }
    }
    if (!Array.isArray(sigs)) sigs = [];

    let validSigs = 0, seen = new Set(), validSigners = [];
    for (let s of sigs){
        let pk  = String(s && s.pubkey || '').toLowerCase();
        let sig = String(s && s.sig || '');
        if (!pk || seen.has(pk) || !qualified.has(pk)) continue;
        // Only mark a pubkey "seen" once its signature actually verifies. Marking on
        // first encounter would let a garbage-then-valid pair of entries for the same
        // qualified validator suppress the real signature (order-dependent quorum
        // under-count), which fails a legitimately-quorate checkpoint closed.
        if (verifySignature(canonical, sig, pk)){ seen.add(pk); validSigs++; validSigners.push(pk); }
    }

    let valid;
    if (weighted){
        // Stake-weighted regime: the supplied set MUST carry per-validator weight
        // + source. If it doesn't (e.g. a legacy bare-pubkey list, or an explorer
        // that hasn't been upgraded), we cannot confirm stake quorum; fail closed
        // (false-reject leaning, the fail-safe direction for a light client).
        //
        // EVERY entry, not some: `.some` let a set mix one weighted entry with
        // unweighted ones and still pass, and meetsStakeThreshold reads a missing
        // weight as '0', so the omitted stake left the denominator while the
        // weighted signer kept the numerator. One 100-weight signature then
        // cleared 3*100 > 2*100 against a set whose true stake was unknown.
        // Empty sets fail here too (`.every` is vacuously true on []).
        let hasWeights = vset.length > 0 && vset.every(v => isWeightedEntry(v));
        valid = hasWeights && swq.meetsStakeThreshold(vset, validSigners);
    } else {
        valid = qualified.size > 0 && validSigs >= quorum;
    }

    return {
        valid:     valid,
        validSigs: validSigs,
        quorum:    quorum,
        weighted:  weighted,
        canonical: canonical
    };
}

// Convenience: fetch a checkpoint (+ the qualifying validator set) from an
// explorer's verify endpoint and re-verify LOCALLY. The server's `verified`
// flag is ignored; only local crypto decides.
//   explorerUrl: e.g. 'https://explorer.xchain.io'
//   coin:        explorer coin code (e.g. 'BTC', 'TBTC', 'RDOGE')
//   blockIndex:  checkpointed height
async function fetchAndVerifyCheckpoint(explorerUrl, coin, blockIndex, fetchImpl){
    let f = fetchImpl || (typeof fetch === 'function' ? fetch : null);
    if (!f) throw new Error('CheckpointVerifier: no fetch implementation available');
    let base = String(explorerUrl || '').replace(/\/+$/, '');
    let url  = base + '/' + encodeURIComponent(String(coin)) + '/api/checkpoint/' +
               encodeURIComponent(String(blockIndex)) + '/verify';
    let res  = await f(url);
    if (!res.ok) throw new Error('CheckpointVerifier: explorer returned HTTP ' + res.status);
    let body = await res.json();
    if (!body || !body.checkpoint) throw new Error('CheckpointVerifier: no checkpoint in response');
    let result = verifyCheckpoint(body.checkpoint, body.validators || []);
    return Object.assign({ checkpoint: body.checkpoint, snapshotAvailable: !!body.snapshot_available }, result);
}

module.exports = {
    canonicalCheckpoint,
    verifySignature,
    verifyCheckpoint,
    fetchAndVerifyCheckpoint
};
