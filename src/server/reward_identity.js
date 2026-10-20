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
 * XChain Sync - Validator-reward identity
 *
 * The one definition of which columns make two validator_rewards rows the same
 * reward. Every payload merge that dedupes reward rows keys on it, so it must
 * match the replica's reward_unique key column for column: a narrower key
 * treats two distinct rewards as one and drops the second before it reaches a
 * replica, which no block hash would show.
 *
 ********************************************************************/

// The reward_unique columns, in key order (src/sql/migrations/2026-10-19-replica-keys.sql).
const REWARD_IDENTITY_COLUMNS = ['source_id', 'signing_pubkey_id', 'reward_type', 'round_reference', 'round_qualifier'];

// Build the dedup key for one reward row (string concatenation, so null reads as 'null').
function validatorRewardKey(r){
    let key = '' + r[REWARD_IDENTITY_COLUMNS[0]];
    for(let i = 1; i < REWARD_IDENTITY_COLUMNS.length; i++)
        key += ':' + r[REWARD_IDENTITY_COLUMNS[i]];
    return key;
}

module.exports = { REWARD_IDENTITY_COLUMNS, validatorRewardKey };
