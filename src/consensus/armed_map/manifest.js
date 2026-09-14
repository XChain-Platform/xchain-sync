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
 * rows rather than a list of files. This is that list for sync, written out
 * by hand on purpose: a directory listing would let a file that appeared by
 * accident join the fingerprint, and would tie the value back to the layout
 * v2 exists to be free of. Membership is enforced by a test instead
 * (test/unit/consensus/armed_map/completeness.test.js), which scans src/
 * for activation-map declarations and fails on any data export of a carrier
 * that is not a row here.
 *
 * Each key is today's `<module stem>.<EXPORT>` spelling, the same one the
 * indexer manifest and knownGateKeys() use for shared keys, so indexer and
 * sync rows compare one for one. A key is data: when a carrier moves, only
 * its loader line below changes, never a key.
 *
 * This file is per repo, not a twin. It is transitional: at the registry
 * window the loaders give way to the registry's own row list.
 *
 ********************************************************************/

'use strict';

const { canonicalValue } = require('./canonical');

// One resolver per export. hasOwnProperty rather than a plain read, because a
// deleted export reads as undefined, and a row that silently resolves to
// nothing is exactly the omission the fingerprint exists to expose.
function rows(stem, load, exportNames) {
    return exportNames.map((name) => [stem + '.' + name, () => {
        const mod = load();
        if (!Object.prototype.hasOwnProperty.call(mod, name)) {
            throw new Error('carrier ' + stem + ' no longer exports ' + name);
        }
        return mod[name];
    }]);
}

// Every non-function export of every sync carrier present today: the eight
// *_activation.js files, the three twin carriers that hold armed maps outside
// that naming convention, and the four consensus-constants.js values (design
// 2.1 item 3). Loaders keep a literal require path so a census that greps for
// a carrier's require still finds this file.
const ENTRIES = Object.freeze([
    ...rows('archive_rollback_author_scope_activation', () => require('../../archive_rollback_author_scope_activation'), [
        'ARCHIVE_ROLLBACK_AUTHOR_SCOPE_ACTIVATION',
        'ARCHIVE_AUTHOR_SCOPE_JOIN_SQL',
    ]),
    ...rows('checkpoint_commitment_activation', () => require('../../checkpoint_commitment_activation'), [
        'CHECKPOINT_COMMITMENT_ACTIVATION',
    ]),
    // VALIDATOR_QUERY_LIMIT and BTC_STAKE_CAPABILITIES are derived from the
    // vendored coin registry at load, so they are hashed as resolved, which is
    // what a replica actually uses to build stakes_root.
    ...rows('consensus-constants', () => require('../../consensus-constants'), [
        'ACTIVATION_DELAY_BLOCKS_BY_COIN',
        'BTC_STAKE_CAPABILITIES',
        'GAS_TICK',
        'VALIDATOR_QUERY_LIMIT',
    ]),
    ...rows('equivocation_header', () => require('../../equivocation_header'), [
        'EQUIV_HEADER_ACTIVATION',
        'ENGINE_TAGS',
    ]),
    ...rows('stake_weight_collation_activation', () => require('../../stake_weight_collation_activation'), [
        'STAKE_WEIGHT_COLLATION',
        'STAKE_WEIGHT_COLLATION_ACTIVATION',
        'STAKE_WEIGHT_ORDERING_COLUMNS',
    ]),
    ...rows('stake_weighted_quorum', () => require('../../stake_weighted_quorum'), [
        'STAKE_WEIGHTED_QUORUM_ACTIVATION',
    ]),
    // The state-hash constants ride with its activation maps: they are
    // consensus inputs that v1 covered only through the file's bytes.
    ...rows('stateHash', () => require('../../stateHash'), [
        'STATE_HASH_VERSION',
        'DEACTIVATION_TABLES',
        'SLASH_SPECS',
        'REQUEST_STATUS_TABLES',
        'COOLDOWN_TABLES',
        'INDEX_MAP_STATE_HASH_ACTIVATION',
        'POLL_FINALIZE_STATE_HASH_ACTIVATION',
        'TOKEN_SUPPLY_STATE_HASH_ACTIVATION',
        'BET_STATUS_STATE_HASH_ACTIVATION',
        'ARCHIVE_HEAD_VERSIONS',
        'ARCHIVE_HEAD_VERSIONS_SQL',
        'ARCHIVE_INVALID_STATE_HASH_ACTIVATION',
        'ARCHIVE_INVALID_HEIGHT_KEY_ACTIVATION',
        'ARCHIVE_CHUNK_HEIGHT_COL',
        'ARCHIVE_CHUNK_HEIGHT_COL_LEGACY',
    ]),
    ...rows('state_commitment_activation', () => require('../../state_commitment_activation'), [
        'STATE_COMMITMENT_ACTIVATION',
    ]),
    ...rows('state_key_collation_activation', () => require('../../state_key_collation_activation'), [
        'STATE_KEY_COLLATION_ACTIVATION',
    ]),
    ...rows('state_subtree_activation', () => require('../../state_subtree_activation'), [
        'RESERVED_SUBTREES',
        'STATE_SUBTREE_ACTIVATION',
        'STATE_SUBTREE_SHADOW',
        'ESCROW_LOCKED_LEAF_ACTIVATION',
        'ESCROW_LOCKED_LEAF_SHADOW',
    ]),
    ...rows('swq_source_cap_activation', () => require('../../swq_source_cap_activation'), [
        'STAKE_WEIGHT_MAX_SOURCES',
        'STAKE_WEIGHT_MAX_KEYS_PER_SOURCE',
        'SWQ_SOURCE_CAP_ACTIVATION',
    ]),
    ...rows('train_activation', () => require('../../train_activation'), [
        'TRAIN_ACTIVATION',
    ]),
]);

/**
 * Resolve every row. Any failure, a carrier that will not load, a missing
 * export or a value the canonicaliser refuses, makes the whole set unusable,
 * because a fingerprint over the rows that happened to resolve would be a
 * plausible value for a build that is not this one.
 *
 * @returns {{ok: true, rows: Array<[string, *]>}|{ok: false, reason: string}}
 */
function collectRows() {
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

module.exports = { ENTRIES, collectRows };
