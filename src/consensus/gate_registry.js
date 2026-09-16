/*********************************************************************
 *
 * Copyright © 2025-2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC - https://dankest.llc
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This file is part of XChain Platform. Licensed under the GNU Affero
 * General Public License v3.0 or later; see LICENSE.md.
 *
 ********************************************************************/

'use strict';

const UNARMED = 9999999999;
const UNPINNED = null;
const gates = new Map();

class RegistryMissError extends Error {
    constructor(key) {
        super('gate registry has no row for ' + key);
        this.name = 'RegistryMissError';
    }
}

function frozenCopy(value) {
    if (value instanceof RegExp) return Object.freeze(new RegExp(value.source, value.flags));
    if (Array.isArray(value)) return Object.freeze(value.map(frozenCopy));
    if (value && typeof value === 'object') {
        const copy = {};
        for (const key of Object.keys(value)) copy[key] = frozenCopy(value[key]);
        return Object.freeze(copy);
    }
    return value;
}

function mutableCopy(value) {
    if (value instanceof RegExp) return new RegExp(value.source, value.flags);
    if (Array.isArray(value)) return value.map(mutableCopy);
    if (value && typeof value === 'object') {
        const copy = {};
        for (const key of Object.keys(value)) copy[key] = mutableCopy(value[key]);
        return copy;
    }
    return value;
}

function addGate(key, unit, table) {
    if (gates.has(key)) throw new Error('duplicate gate registry key: ' + key);
    gates.set(key, Object.freeze({ unit, table: frozenCopy(table) }));
}

function entry(key) {
    if (!gates.has(key)) throw new RegistryMissError(key);
    return gates.get(key);
}

function get(key) { return mutableCopy(entry(key).table); }
function has(key) { return gates.has(key); }
function keys() { return Array.from(gates.keys()); }
function rows() { return Array.from(gates, ([key, value]) => [key, value.table]); }

function activeAt(key, network, coin, height, time) {
    const gate = entry(key);
    if (gate.unit === 'constant') return gate.table;
    const coinKey = coin && network ? coin + ':' + network : null;
    const threshold = coinKey && Object.prototype.hasOwnProperty.call(gate.table, coinKey)
        ? gate.table[coinKey] : gate.table[network];
    if (threshold === undefined || threshold === UNPINNED) return false;
    const position = gate.unit === 'time' ? time : height;
    return Number.isFinite(Number(position)) && Number(position) >= threshold;
}

// SHARED-GATES BEGIN
addGate('archive_rollback_author_scope_activation.ARCHIVE_AUTHOR_SCOPE_JOIN_SQL', 'constant', 'JOIN actions         pact ON pact.action_index = p.action_index ' +
    'JOIN index_addresses padr ON padr.id = pact.source_id ' +
    'JOIN actions         cact ON cact.action_index = c.action_index ' +
    'JOIN index_addresses cadr ON cadr.id = cact.source_id AND cadr.address = padr.address ');
addGate('archive_rollback_author_scope_activation.ARCHIVE_ROLLBACK_AUTHOR_SCOPE_ACTIVATION', 'height', {
    mainnet: 0,            // ARMED at genesis by the 2026-09-09 ruling: identity on the indexed mainnet history (0 archive chunks, measured 2026-09-09), and ARCHIVE_BATCH_AUTHOR is 0 there too, so the precondition holds
    testnet: 67915000,     // testnet runs a public chain with live history, so 0 would be retroactive rather than a flag day; TDOGE tip 67881714 on 2026-09-09 + 33286 blocks @1440/day = ~23 days, to ride the v0.17.0 train
    regtest: 9999999999,   // INERT sentinel: keeps the flag-day-off control path drivable on a throwaway stack
});
addGate('checkpoint_commitment_activation.CHECKPOINT_COMMITMENT_ACTIVATION', 'height', {
    mainnet: 961000,      // ARMED 2026-07-07: BTC anchor ~2026-08-04; deploy hub + ALL indexers (+ sdk/explorer/sync copies) before this height
    testnet: 146000,      // ARMED 2026-07-22: first BTC-testnet anchor past all three STATE_COMMITMENT testnet thresholds; was 0, which forced the SPV root suffix from testnet genesis before the indexer computes roots, so the hub refused to sign every testnet checkpoint
    regtest: 0,
});
addGate('equivocation_header.ENGINE_TAGS', 'constant', {
    DEX:        'XDEX',
    XCALL:      'XCALL',
    ATTEST:     'XATTEST',
    ORACLE:     'XORACLE',
    // PRICE batches. A DISTINCT tag from ORACLE, not a reuse: a batch canonical
    // carries first_round/last_round and no scalar `round`, and SLASH v0 reads
    // `round` out of an ORACLE-tagged content to judge equivocation, skipping its
    // distinct-rounds guard when either side lacks one. Under a shared tag an
    // honest validator that signed one per-round consensus canonical and one batch
    // at the same BTC anchor would be provably equivocating, for a full bond burn plus permanent
    // capability disqualification. The batch ROUND_ID is
    // `<anchor>|<first_round>|<last_round>` (pipes are safe here; equivKey treats
    // the round id as opaque), so two honest batches that split one window
    // differently do not collide on one key either.
    ORACLE_BATCH: 'XORACLEB',
    CHECKPOINT: 'XCHECKPOINT',
    CONFIG:     'XCONFIG',
    NODEPROOF:  'XNODEPROOF',
    // ROLLCALL presence proofs. Namespacing ONLY, exactly like XNODEPROOF: the
    // tag is deliberately absent from SLASH's ENGINE_CAPABILITY map, so no
    // ROLLCALL canonical is a slashable family. Several valid ROLLCALLs per
    // epoch are expected (a leader's, sweepers', self-publishes), every one
    // carrying signatures over the SAME canonical for that epoch, so two of
    // them are never conflicting content for one key. ROUND_ID is the BTC
    // EPOCH_HEIGHT in decimal, VIEW is 0.
    ROLLCALL:   'XROLLCALL',
    // Cross-chain bridge transfer records. ROUND_ID is the transfer_id, VIEW is the live
    // PBFT view on the hub and the row's finalizing_view on an indexer. Mapped to the
    // cross_chain capability in SLASH's ENGINE_CAPABILITY: a forged transfer record directs
    // value, so two conflicting canonicals for one transfer_id must be slashable.
    BRIDGE:     'XBRIDGE',
    // Per-token policy snapshots (allow list, block list, sleep) carried from an origin row
    // to every bridged copy. A DISTINCT tag from BRIDGE, not a reuse: the two canonicals
    // share no field layout, and SLASH judges equivocation within one tag family, so one tag
    // over both would make a validator that signed one transfer and one snapshot at the same
    // round id provably equivocating. ROUND_ID is the snapshot_id.
    POLICY:     'XPOLICY',
});
addGate('equivocation_header.EQUIV_HEADER_ACTIVATION', 'height', {
    mainnet: 961000,      // ARMED 2026-07-07: BTC anchor ~2026-08-04; deploy hub + ALL indexers (+ sdk/explorer/sync copies) before this height
    testnet: 0,
    regtest: 0,
});
addGate('stake_weight_collation_activation.STAKE_WEIGHT_COLLATION', 'constant', 'utf8_bin');
addGate('stake_weight_collation_activation.STAKE_WEIGHT_COLLATION_ACTIVATION', 'height', {
    'BTC:mainnet':  0,      // ARMED at genesis by the 2026-09-09 ruling: identity on the indexed mainnet history (0 stakes, measured 2026-09-09)
    'LTC:mainnet':  0,
    'DOGE:mainnet': 0,
    'BTC:testnet':  null,
    'LTC:testnet':  null,
    'DOGE:testnet': null,
    regtest: 0,
});
addGate('stake_weight_collation_activation.STAKE_WEIGHT_ORDERING_COLUMNS', 'constant', [
    { table: 'index_addresses', column: 'address', charset: 'utf8', collation: 'utf8_general_ci' },
    { table: 'index_pubkeys',   column: 'pubkey',  charset: 'utf8', collation: 'utf8_general_ci' },
]);
addGate('stake_weighted_quorum.STAKE_WEIGHTED_QUORUM_ACTIVATION', 'height', {
    mainnet: 961000,      // ARMED 2026-07-07: BTC anchor ~2026-08-04; deploy hub + ALL indexers (+ sdk/explorer/sync copies) before this height
    testnet: 0,
    regtest: 0,
});
addGate('stateHash.ARCHIVE_CHUNK_HEIGHT_COL', 'constant', 'c.block_index_doge');
addGate('stateHash.ARCHIVE_CHUNK_HEIGHT_COL_LEGACY', 'constant', 'c.block_index');
addGate('stateHash.ARCHIVE_HEAD_VERSIONS', 'constant', [1]);
addGate('stateHash.ARCHIVE_HEAD_VERSIONS_SQL', 'constant', "IN (1)");
addGate('stateHash.ARCHIVE_INVALID_HEIGHT_KEY_ACTIVATION', 'height', {
    'BTC:mainnet':  0,          // ARMED at genesis by the 2026-09-09 ruling: identity on the indexed mainnet history (0 archive chunks, measured 2026-09-09)
    'LTC:mainnet':  0,
    'DOGE:mainnet': 0,          // DOGE is the anchor chain, and the 56 mainnet ANCHORs there carry no archive chunk
    'BTC:testnet':  155000,     // tip 151701 (2026-09-09) + 3299 blocks @144/day = ~23 days
    'LTC:testnet':  4896000,    // tip 4883295 + 12705 blocks @576/day = ~22 days
    'DOGE:testnet': 67915000,   // tip 67881714 + 33286 blocks @1440/day = ~23 days
    regtest: 0,                 // armed from genesis: fresh regtest stacks exercise the repaired class end to end
});
addGate('stateHash.ARCHIVE_INVALID_STATE_HASH_ACTIVATION', 'height', {
    'BTC:mainnet':  0,          // ARMED at genesis by the 2026-09-09 ruling: identity on the indexed mainnet history (0 archive chunks, measured 2026-09-09)
    'LTC:mainnet':  0,
    'DOGE:mainnet': 0,
    'BTC:testnet':  0,          // armed from genesis 2026-08-11 ruling
    'LTC:testnet':  0,          // armed from genesis 2026-08-11 ruling
    'DOGE:testnet': 0,          // armed from genesis 2026-08-11 ruling
    regtest: 0,                 // armed from genesis: fresh regtest stacks exercise the widened class end to end
});
addGate('stateHash.BET_STATUS_STATE_HASH_ACTIVATION', 'height', {
    // Heights pinned via roundUp1000(tip + 21 days x nominal blocks/day), never
    // lowered once set: a height that falls in the past is not a flag day at all,
    // since a node replaying from genesis applies the rule from it while a
    // long-running node never did, and the two diverge at the first hash
    // comparison (caught once on LTC:testnet, whose first pinned height the chain
    // had already passed). Re-verify these against live tips before each deploy.
    'BTC:mainnet':  963000,   // tip 959,853 (2026-07-27) + 21d @144/day = 962,877
    'LTC:mainnet':  3162000,   // tip 3,149,481 + 21d @576/day = 3,161,577
    'DOGE:mainnet': 6338000,   // tip 6,307,307 + 21d @1440/day = 6,337,547
    // Testnet is genesis-active as of the 2026-08-10 fresh testnet genesis: the
    // chain restarts at firstBlock (BTC 147500 / LTC 4855000 / DOGE 67815000) with
    // no pre-rule history, so there is nothing for a mid-chain boundary to protect.
    // Kept value-equal to CARET_REF_STRICT_ACTIVATION and
    // LIST_EDIT_RESOLUTION_ACTIVATION, which CI asserts.
    'BTC:testnet':  0,
    'LTC:testnet':  0,
    'DOGE:testnet': 0,
    regtest: 0,                 // armed from genesis: fresh regtest stacks exercise the class end to end
});
addGate('stateHash.COOLDOWN_TABLES', 'constant', ['unstakes', 'contract_unstakes']);
addGate('stateHash.DEACTIVATION_TABLES', 'constant', ['stakes', 'delegations', 'contract_stakes', 'contract_delegations']);
addGate('stateHash.INDEX_MAP_STATE_HASH_ACTIVATION', 'height', {
    // Heights are the chain's OWN local block_index at/after which the index-map
    // folds into state_hash. One-way door: once a chain crosses its height, any
    // process still on a different height computes a divergent state_hash and a
    // follower HALTS, so every indexer + sync process must run this exact map
    // BEFORE the chain reaches the height. Keep this map byte-identical to the
    // xchain-{indexer,sync}/src/stateHash.js twin.
    mainnet: 0,           // ARMED at the genesis launch reindex (folds from genesis, no mid-chain flag-day)
    testnet: 0,           // ARMED at the genesis launch reindex (clean reseed accompanies it)
    regtest: 0,           // ARMED from genesis 2026-07-16: fresh stacks exercise the class end to end; pre-existing regtest venues need a clean reseed
});
addGate('stateHash.POLL_FINALIZE_STATE_HASH_ACTIVATION', 'height', {
    'BTC:mainnet':  958500,     // armed 2026-07-07 at tip 957062; ~10 days of margin
    'LTC:mainnet':  3143000,    // armed 2026-07-07 at tip 3138154; ~8 days
    'DOGE:mainnet': 6291000,    // armed 2026-07-07 at tip 6280094; ~7.5 days
    'BTC:testnet':  145000,     // armed 2026-07-07 at tip 143299
    'LTC:testnet':  4805000,    // armed 2026-07-07 at tip 4797675
    'DOGE:testnet': 67000000,   // armed 2026-07-07 at tip 66498605 (fast chain, wide margin)
    regtest: 0,                 // armed from genesis: fresh regtest stacks exercise the class end to end
});
addGate('stateHash.REQUEST_STATUS_TABLES', 'constant', ['attests', 'xcalls']);
addGate('stateHash.SLASH_SPECS', 'constant', [
    { table: 'stakes',            debits: 'capability_slash_debits', target: 'stakes'            },
    { table: 'unstakes',          debits: 'capability_slash_debits', target: 'unstakes'          },
    { table: 'contract_stakes',   debits: 'contract_slash_debits',   target: 'contract_stakes'   },
    { table: 'contract_unstakes', debits: 'contract_slash_debits',   target: 'contract_unstakes' }
]);
addGate('stateHash.STATE_HASH_VERSION', 'constant', 1);
addGate('stateHash.TOKEN_SUPPLY_STATE_HASH_ACTIVATION', 'height', {
    'BTC:mainnet':  958500,     // armed 2026-07-07, same heights as POLL_FINALIZE
    'LTC:mainnet':  3143000,
    'DOGE:mainnet': 6291000,
    'BTC:testnet':  145000,
    'LTC:testnet':  4805000,
    'DOGE:testnet': 67000000,
    regtest: 0,
});
addGate('state_commitment_activation.STATE_COMMITMENT_ACTIVATION', 'height', {
    'BTC:mainnet':  958500,     // ARMED 2026-07-07 at tip 957062; ~10 days of margin
    'LTC:mainnet':  3143000,    // ARMED 2026-07-07 at tip 3138154; ~8 days
    'DOGE:mainnet': 6291000,    // ARMED 2026-07-07 at tip 6280094; ~7.5 days
    'BTC:testnet':  145000,     // ARMED 2026-07-07 at tip 143299
    'LTC:testnet':  4805000,    // ARMED 2026-07-07 at tip 4797675
    'DOGE:testnet': 67000000,   // ARMED 2026-07-07 at tip 66498605 (fast chain, wide margin)
    regtest: 0,                 // armed from genesis: fresh regtest stacks exercise the roots end to end
});
addGate('state_key_collation_activation.STATE_KEY_COLLATION_ACTIVATION', 'height', {
    'BTC:mainnet':  962500,     // ARMED 2026-07-10 at tip 957491 (~Aug 13; ~10 days past Cohort-B 961000)
    'LTC:mainnet':  3160000,    // ARMED 2026-07-10 at tip 3140024 (~Aug 14)
    'DOGE:mainnet': 6335000,    // ARMED 2026-07-10 at tip 6284614 (~Aug 15)
    // Testnet is genesis-active as of the 2026-08-10 fresh testnet genesis. LTC was
    // the one entry still ahead of the new firstBlock (4890000 > 4855000); BTC and
    // DOGE were already behind it and are zeroed with it so the map reads as one
    // rule rather than three coincidences. The documented ordering still holds:
    // state_commitment is genesis-active too, so this never precedes it.
    'BTC:testnet':  0,
    'LTC:testnet':  0,
    'DOGE:testnet': 0,
    regtest: 0,                 // armed from genesis: fresh regtest stacks exercise the binary path end to end
});
addGate('state_subtree_activation.ESCROW_LOCKED_LEAF_ACTIVATION', 'height', {
    'BTC:regtest':  11200,
    'BTC:testnet':  0,
    'LTC:testnet':  0,
    'DOGE:testnet': 0,
});
addGate('state_subtree_activation.ESCROW_LOCKED_LEAF_SHADOW', 'height', {});
addGate('state_subtree_activation.RESERVED_SUBTREES', 'constant', ['ownership_root', 'tokens_root', 'contract_state_root']);
addGate('state_subtree_activation.STATE_SUBTREE_ACTIVATION', 'height', {
    ownership_root:      {},
    tokens_root:         {},
    contract_state_root: {
        'BTC:regtest':  10000,
        'BTC:testnet':  0,
        'LTC:testnet':  0,
        'DOGE:testnet': 0,
    },
});
addGate('state_subtree_activation.STATE_SUBTREE_SHADOW', 'height', {
    ownership_root:      {},
    tokens_root:         {},
    contract_state_root: {},
});
addGate('swq_source_cap_activation.STAKE_WEIGHT_MAX_KEYS_PER_SOURCE', 'constant', 64);
addGate('swq_source_cap_activation.STAKE_WEIGHT_MAX_SOURCES', 'constant', 1000);
addGate('swq_source_cap_activation.SWQ_SOURCE_CAP_ACTIVATION', 'height', {
    'BTC:mainnet':  960000,     // Option B: after STATE_COMMITMENT (958500), before STAKE_WEIGHTED_QUORUM (961000)
    'LTC:mainnet':  3143000,    // inert (LTC commits the EMPTY stakes_root; capability staking is BTC-only) - pinned == STATE_COMMITMENT for parity
    'DOGE:mainnet': 6291000,    // inert (DOGE stakes_root empty) - pinned == STATE_COMMITMENT for parity
    'BTC:testnet':  0,          // capped from genesis; STATE_COMMITMENT testnet (145000) > 0, so testnet only ever commits capped roots (no discontinuity)
    'LTC:testnet':  0,
    'DOGE:testnet': 0,
    regtest: 0,                 // armed from genesis: fresh regtest stacks exercise the capped path end to end
});
addGate('train_activation.TRAIN_ACTIVATION', 'ruleset', {
    // The launch rule set and the floor. Zero on every network because there is no
    // earlier rule set to migrate from: the launch binary IS the first rule set, and
    // a floor above genesis would leave the pre-floor range resolving to nothing.
    '1.0.0': { mainnet: 0, testnet: 0, regtest: 0 },
});
// SHARED-GATES END

// SYNC-ONLY GATES
const coins = require('../coins');

function activationDelays() {
    const table = {};
    for (const tick of coins.ALLOWED_COINS) {
        table[tick] = coins.getCoinConfig(tick, 'mainnet').STAKING.ACTIVATION_DELAY_BLOCKS;
    }
    return table;
}

function stakeCapabilities() {
    const table = {};
    const capabilities = coins.getCoinConfig('BTC', 'mainnet').STAKING.CAPABILITIES;
    for (const name of Object.keys(capabilities)) table[name] = capabilities[name].MIN_STAKE;
    return table;
}

addGate('consensus-constants.ACTIVATION_DELAY_BLOCKS_BY_COIN', 'constant', activationDelays());
addGate('consensus-constants.BTC_STAKE_CAPABILITIES', 'constant', stakeCapabilities());
addGate('consensus-constants.GAS_TICK', 'constant', 'XCHAIN');
addGate('consensus-constants.VALIDATOR_QUERY_LIMIT', 'constant', coins.getCoinConfig('BTC', 'mainnet').VALIDATOR_QUERY_LIMIT);

module.exports = {
    addGate: undefined,
    get,
    has,
    keys,
    rows,
    activeAt,
    UNARMED,
    UNPINNED,
    RegistryMissError,
};
