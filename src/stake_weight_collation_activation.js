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
 * Stake-weight snapshot ORDERING binary-collation flag-day.
 *
 * The source-keyed stake-weight snapshot (_cappedStakeWeightsSql, plus the
 * pre-source-cap `ORDER BY source, pubkey LIMIT ?` branch) ranks, partitions
 * and orders on `source`, which resolves through `index_addresses.address`
 * (_stakeWeightsSql: `sa.address AS source`). That column is declared
 * CHARSET=utf8 COLLATE=utf8_general_ci (case- AND accent-folding) in
 * src/sql/index_addresses.sql, so the comparison the ranking performs is a
 * FOLDING one, decided by the column's collation as the database happens to
 * carry it rather than by anything this tree pins.
 *
 * That matters because the ordering is a TRUNCATION boundary, not a display
 * order. Three consensus-deciding selections read it:
 *
 *   1. DENSE_RANK() OVER (ORDER BY source) -> _sr decides which distinct
 *      staking SOURCES survive STAKE_WEIGHT_MAX_SOURCES, and therefore whether
 *      the snapshot is flagged `truncated` (on which stake-weighted quorum
 *      fails closed).
 *   2. ROW_NUMBER() OVER (PARTITION BY source ORDER BY pubkey) -> _kr decides
 *      which keys of a source survive STAKE_WEIGHT_MAX_KEYS_PER_SOURCE.
 *   3. The legacy `ORDER BY source, pubkey LIMIT VALIDATOR_QUERY_LIMIT` branch
 *      decides which key rows survive the raw row cap.
 *
 * The surviving row SET is what stateCommitment.gatherStakeEntries hashes into
 * the committed `stakes_root`. Row order within that set is irrelevant (the SMT
 * keys on pubkey+capability); WHICH rows survive is not.
 *
 * Every other consensus-facing read of index_addresses.address in this tree
 * already pins a binary collation for exactly this reason - getBlockHashes
 * (`a1.address COLLATE utf8_bin ASC`), getList, stateHash.js - so that the
 * result does not depend on each node's schema collation. The stake-weight
 * ranking was the one such read left folding. Two folding-equal sources cannot
 * coexist today (index_addresses carries a full-column UNIQUE INDEX on
 * `address`), so the residual exposure is COLLATION DRIFT: a node whose column
 * collation deviates from src/sql sorts these values differently and can
 * therefore truncate to a different set - a silent stakes_root fork.
 *
 * Pinning `COLLATE utf8_bin` closes that, but it CHANGES the sort order used at
 * the truncation boundary, so an ungated flip would re-evaluate already-valid
 * blocks differently and fork against deployed nodes wherever a cap ever bit.
 * It is therefore height-gated per chain, exactly like
 * state_key_collation_activation.js / swq_source_cap_activation.js: below the
 * chain's activation height the legacy unpinned ordering runs (historical
 * replay stays byte-identical); at/after it the binary-collation ordering runs.
 *
 * Gate semantics MIRROR state_key_collation_activation.js: keyed on the
 * processing chain's OWN local `block_index`, '<COIN>:<network>' lookup first,
 * then the bare network key; unknown -> inert/off (legacy unpinned path, which
 * preserves deployed behavior).
 *
 * The byte-identical twin lives in xchain-sync/src/ (the follower rebuilds the
 * same stakes_root and HALTs on divergence, so a one-sided COLLATE edit IS the
 * fork this file exists to prevent); the xchain-sync twin guard
 * (test/unit/rollback-coverage.test.js) locks the two files equal. BOTH repos
 * must deploy fleet-wide before any armed height is reached.
 *
 ********************************************************************/

// Per-chain activation height, interpreted as the processing chain's OWN
// block_index. At/after the height the binary-collation (utf8_bin) ordering
// runs; below it the legacy unpinned (utf8_general_ci) ordering runs.
//
// *** NOT ARMED ON MAINNET ***  Every mainnet chain is deliberately ABSENT from
// this map, which resolves to undefined -> inert/off, so this ships as a no-op
// on mainnet: the emitted SQL below an armed height is byte-for-byte what the
// fleet runs today. Arming a mainnet height is a separate, coordinated step,
// sequenced with the next flag-day cohort and only after BOTH xchain-indexer and
// xchain-sync carry this file fleet-wide (a straggler on either side truncates
// to a different set and forks the stakes_root at the boundary).
//
// Testnet and regtest are armed from genesis so fresh stacks and the e2e
// conformance scenario exercise the binary path end to end. Testnet restarted at
// the 2026-08-10 fresh genesis, so there is no pre-rule testnet history to
// re-evaluate; regtest stacks are always fresh.
const STAKE_WEIGHT_COLLATION_ACTIVATION = {
    // 'BTC:mainnet':  <unarmed>,   // pending a coordinated arming step
    // 'LTC:mainnet':  <unarmed>,   // (LTC/DOGE commit the EMPTY stakes_root;
    // 'DOGE:mainnet': <unarmed>,   //  pin them with BTC for pattern parity)
    'BTC:testnet':  0,
    'LTC:testnet':  0,
    'DOGE:testnet': 0,
    regtest: 0,                 // armed from genesis: fresh regtest stacks exercise the binary path end to end
};

// Resolve the per-chain threshold: '<COIN>:<network>' key first, then the bare
// network key (regtest keeps one key). Unknown -> undefined -> inert/off.
function _activationThreshold(network, coin){
    if(coin != null && STAKE_WEIGHT_COLLATION_ACTIVATION[coin + ':' + network] !== undefined)
        return STAKE_WEIGHT_COLLATION_ACTIVATION[coin + ':' + network];
    return STAKE_WEIGHT_COLLATION_ACTIVATION[network];
}

// Whether the binary stake-weight ordering collation is in effect at
// `blockIndex` on `network` for `coin`. Below the threshold / unknown chain ->
// off (legacy unpinned ordering, byte-identical historical replay).
function isStakeWeightBinCollationActive(blockIndex, network, coin){
    let b = parseInt(blockIndex);
    if(!Number.isFinite(b)) return false;
    let threshold = _activationThreshold(network, coin);
    if(threshold === undefined) return false;
    return b >= threshold;
}

module.exports = {
    STAKE_WEIGHT_COLLATION_ACTIVATION,
    isStakeWeightBinCollationActive
};
