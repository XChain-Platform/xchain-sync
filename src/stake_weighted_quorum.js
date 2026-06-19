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
 *
 * XChain SDK: Stake-Weighted Quorum (STAKE_WEIGHTED_QUORUM / WI-1)
 *
 * The single, CONSENSUS-CRITICAL implementation of the stake-weighted quorum
 * predicate for the SDK. The SDK's checkpoint verifier (checkpoint.js) decides
 * client-side whether a quorum-signed checkpoint meets quorum. It MUST flip on
 * the same flag-day and apply the same predicate as the hub + indexer, or it
 * will report the wrong verdict (false-reject a stake-heavy minority the
 * federation finalized, or bless a key-count majority that lacks stake majority).
 *
 * The hub/indexer keep byte-equivalent copies (xchain-hub/src/stake_weighted_quorum.js,
 * xchain-indexer/src/stake_weighted_quorum.js); the cross-service regression suite
 * asserts the activation map + predicate agree (a divergence forks the chain).
 *
 * Self-contained on purpose: verifyCheckpoint is a pure function with no bound
 * utility instance, so the bignumber math lives here (mathjs bignumber, exact,
 * never a JS double) rather than being injected.
 *
 ********************************************************************/

const mathjs = require('mathjs');

// Per-network activation height (LOCAL COPY of the canonical map in
// xchain-documentation/protocol/constants.js (kept equal by the cross-service
// regression suite). Keyed on the BTC-anchored snapshot_block, NOT each chain's
// local height, so every chain + the hub flip on the same anchor.
const STAKE_WEIGHTED_QUORUM_ACTIVATION = {
    mainnet: 999999999,   // PLACEHOLDER: set the real BTC flag-day height before mainnet enable
    testnet: 0,
    regtest: 0,
};

// Whether stake-weighted quorum is in effect for a checkpoint whose BTC-anchored
// snapshot is at `snapshotBlock` on `network`. Below this → legacy count quorum.
function isStakeWeightedQuorumActive(snapshotBlock, network){
    let sb = parseInt(snapshotBlock);
    if(!Number.isFinite(sb)) return false;
    let threshold = STAKE_WEIGHTED_QUORUM_ACTIVATION[network];
    if(threshold === undefined) return false;   // unknown network → off (safe)
    return sb >= threshold;
}

// Coerce to a full-precision bignumber (never a JS double); non-numeric → 0.
function bcnum(v){
    let s = (v === null || v === undefined) ? '0' : String(v).trim();
    if(s === '' || !/^[+-]?(\d+\.?\d*|\.\d+)$/.test(s)) return mathjs.bignumber(0);
    return mathjs.bignumber(s);
}

// Source-deduped stake-weighted quorum test.
//   validators      full snapshot: [{ pubkey, source, weight }]  (every key of a
//                   source carries the SAME source + weight)
//   signerPubkeys   iterable of pubkeys that produced a VALID signature
// Returns true iff 3·Σ(distinct signing-source weight) > 2·S, where
// S = Σ(weight over DISTINCT sources). A source counts ONCE no matter how many of
// its keys signed (DELEGATE v0 is additive). Degenerate cases fall out with no
// special-casing: single source → 3S>2S true; empty/zero-stake set → 0>0 false.
function meetsStakeThreshold(validators, signerPubkeys){
    let weightBySource = new Map();   // source -> weight (first wins; all equal per source)
    let pubkeyToSource = new Map();   // pubkey(lower) -> source
    for(let v of (validators || [])){
        // Fail CLOSED on a blank/missing source: an empty-string (snapshot schema NOT NULL
        // DEFAULT '') or undefined source collapses every row into ONE dedupe bucket, dropping
        // the threshold to 1-of-N (a single signature would finalize). A malformed snapshot
        // must never finalize. Mirrors xchain-hub / xchain-indexer (cross-service conformance).
        if(!v || v.source === null || v.source === undefined || String(v.source).trim() === '') return false;
        let src = String(v && v.source);
        let pk  = String(v && v.pubkey).toLowerCase();
        pubkeyToSource.set(pk, src);
        if(!weightBySource.has(src))
            weightBySource.set(src, (v && v.weight !== null && v.weight !== undefined) ? String(v.weight) : '0');
    }
    // S = Σ weight over distinct sources in the snapshot.
    let S = mathjs.bignumber(0);
    for(let w of weightBySource.values()) S = mathjs.add(S, bcnum(w));
    // Tally = Σ weight over the DISTINCT sources represented by valid signers.
    let countedSources = new Set();
    let seenPubkeys    = new Set();
    let tally          = mathjs.bignumber(0);
    for(let pk of (signerPubkeys || [])){
        let lpk = String(pk).toLowerCase();
        if(seenPubkeys.has(lpk)) continue;
        seenPubkeys.add(lpk);
        let src = pubkeyToSource.get(lpk);
        if(src === undefined) continue;          // signer not in snapshot
        if(countedSources.has(src)) continue;    // source already counted
        countedSources.add(src);
        tally = mathjs.add(tally, bcnum(weightBySource.get(src)));
    }
    // Strictly greater than two-thirds of stake, integer-free: 3·tally > 2·S.
    // Use the bignumber's exact `.gt` (decimal.js), NOT mathjs.larger, which
    // applies a ~1e-12 relative epsilon and would treat large near-equal stake
    // totals as equal, flipping the verdict at the boundary.
    return mathjs.multiply(tally, 3).gt(mathjs.multiply(S, 2));
}

module.exports = {
    STAKE_WEIGHTED_QUORUM_ACTIVATION,
    isStakeWeightedQuorumActive,
    meetsStakeThreshold,
};
