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
 * Armed-map fingerprint v2: the xchain-sync row source.
 *
 * v2 hashes the armed map by MEANING, so it needs the list of (key, value)
 * rows rather than a list of files. The registry owns that list from W3, so
 * this module wraps each registry value in the resolver shape collectRows()
 * has always consumed.
 *
 * Each key is registry data, shared with the indexer for the twin population.
 *
 ********************************************************************/

'use strict';

const { canonicalValue } = require('./canonical');
const registryRows = require('../gate_registry').rows;

// The W1 manifest's exact key census. Registry rows supply the values, while
// this independent list keeps an omitted addGate() call from becoming a
// plausible fingerprint over a smaller set.
const EXPECTED_KEYS = Object.freeze([
    'archive_rollback_author_scope_activation.ARCHIVE_AUTHOR_SCOPE_JOIN_SQL',
    'archive_rollback_author_scope_activation.ARCHIVE_ROLLBACK_AUTHOR_SCOPE_ACTIVATION',
    'checkpoint_commitment_activation.CHECKPOINT_COMMITMENT_ACTIVATION',
    'consensus-constants.ACTIVATION_DELAY_BLOCKS_BY_COIN',
    'consensus-constants.BTC_STAKE_CAPABILITIES',
    'consensus-constants.GAS_TICK',
    'consensus-constants.VALIDATOR_QUERY_LIMIT',
    'equivocation_header.ENGINE_TAGS',
    'equivocation_header.EQUIV_HEADER_ACTIVATION',
    'stake_weight_collation_activation.STAKE_WEIGHT_COLLATION',
    'stake_weight_collation_activation.STAKE_WEIGHT_COLLATION_ACTIVATION',
    'stake_weight_collation_activation.STAKE_WEIGHT_ORDERING_COLUMNS',
    'stake_weighted_quorum.STAKE_WEIGHTED_QUORUM_ACTIVATION',
    'stateHash.ARCHIVE_CHUNK_HEIGHT_COL',
    'stateHash.ARCHIVE_CHUNK_HEIGHT_COL_LEGACY',
    'stateHash.ARCHIVE_HEAD_VERSIONS',
    'stateHash.ARCHIVE_HEAD_VERSIONS_SQL',
    'stateHash.ARCHIVE_INVALID_HEIGHT_KEY_ACTIVATION',
    'stateHash.ARCHIVE_INVALID_STATE_HASH_ACTIVATION',
    'stateHash.BET_STATUS_STATE_HASH_ACTIVATION',
    'stateHash.COOLDOWN_TABLES',
    'stateHash.DEACTIVATION_TABLES',
    'stateHash.INDEX_MAP_STATE_HASH_ACTIVATION',
    'stateHash.POLL_FINALIZE_STATE_HASH_ACTIVATION',
    'stateHash.REQUEST_STATUS_TABLES',
    'stateHash.SLASH_SPECS',
    'stateHash.STATE_HASH_VERSION',
    'stateHash.TOKEN_SUPPLY_STATE_HASH_ACTIVATION',
    'state_commitment_activation.STATE_COMMITMENT_ACTIVATION',
    'state_key_collation_activation.STATE_KEY_COLLATION_ACTIVATION',
    'state_subtree_activation.ESCROW_LOCKED_LEAF_ACTIVATION',
    'state_subtree_activation.ESCROW_LOCKED_LEAF_SHADOW',
    'state_subtree_activation.RESERVED_SUBTREES',
    'state_subtree_activation.STATE_SUBTREE_ACTIVATION',
    'state_subtree_activation.STATE_SUBTREE_SHADOW',
    'swq_source_cap_activation.STAKE_WEIGHT_MAX_KEYS_PER_SOURCE',
    'swq_source_cap_activation.STAKE_WEIGHT_MAX_SOURCES',
    'swq_source_cap_activation.SWQ_SOURCE_CAP_ACTIVATION',
    'train_activation.TRAIN_ACTIVATION',
]);

const EXPECTED_KEY_SET = new Set(EXPECTED_KEYS);
const ENTRIES = Object.freeze(registryRows()
    .filter(([key]) => EXPECTED_KEY_SET.has(key))
    .map(([key, value]) => [key, () => value]));

/**
 * Resolve every row. Any failure, a carrier that will not load, a missing
 * export or a value the canonicaliser refuses, makes the whole set unusable,
 * because a fingerprint over the rows that happened to resolve would be a
 * plausible value for a build that is not this one.
 *
 * @returns {{ok: true, rows: Array<[string, *]>}|{ok: false, reason: string}}
 */
function collectRows() {
    const present = new Set(ENTRIES.map(([key]) => key));
    const missing = EXPECTED_KEYS.find((key) => !present.has(key));
    if (missing) return { ok: false, reason: missing + ': missing registry row' };
    const unexpected = ENTRIES.find(([key]) => !EXPECTED_KEYS.includes(key));
    if (unexpected) return { ok: false, reason: unexpected[0] + ': unexpected registry row' };
    const out = [];
    for (const [key, resolve] of ENTRIES) {
        let value;
        try {
            value = resolve();
            canonicalValue(value);
        } catch (e) {
            return { ok: false, reason: key + ': ' + e.message };
        }
        out.push([key, value]);
    }
    return { ok: true, rows: out };
}

module.exports = { ENTRIES, EXPECTED_KEYS, collectRows };
