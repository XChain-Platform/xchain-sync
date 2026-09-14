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
 * The replicated validator_rewards table, read by the source-side forward
 * channels: the rewards a block's payload must carry even though the row was
 * not written in that block.
 *
 * A Database mixin: every method below is installed on Database.prototype by
 * db/index.js, so `this` is the Database instance and call sites are unchanged.
 *
 * Both queries project SELECT vr.* on purpose. Their rows are merged into the
 * same payload array as the block-scoped rows, and the replica derives its
 * INSERT column list from the first row alone, so a narrower projection would
 * drop the auto-increment id and let the replica mint its own diverging one.
 *
 ********************************************************************/

module.exports = {

    /**
     * Rewards a derivation backdated into this window: minted by a later block
     * than the one they are earned in. A row whose earn-block equals its
     * materialization block already streams with its own block and is excluded.
     *
     * @param {number} from first block of the window, inclusive
     * @param {number} to   last block of the window, inclusive
     * @param {object} [conn] a connection to read on, when the caller holds one
     * @returns {Promise<object[]>} the driver's row array
     */
    async findDerivedValidatorRewards(from, to, conn){
        return await this.doQuery(
            "SELECT vr.* " +
            "FROM validator_rewards vr " +
            "WHERE vr.derive_block_index BETWEEN ? AND ? " +
            "  AND vr.block_index < vr.derive_block_index",
            [from, to], conn);
    },

    /**
     * Rewards a recovery redrive re-applied inside this window, matched back to
     * their pending-reward record on every column that identifies one. The
     * round reference compares with <=>, the NULL-safe equality, so two NULL
     * references still match where a plain = would treat them as unequal.
     *
     * @param {number} from first applied block of the window, inclusive
     * @param {number} to   last applied block of the window, inclusive
     * @param {object} [conn] a connection to read on, when the caller holds one
     * @returns {Promise<object[]>} the driver's row array
     */
    async findRedrivenValidatorRewards(from, to, conn){
        return await this.doQuery(
            "SELECT vr.* " +
            "FROM validator_rewards vr " +
            "JOIN recovery_pending_rewards rpr " +
            "     ON rpr.source_id = vr.source_id AND rpr.reward_type = vr.reward_type " +
            "    AND rpr.round_reference <=> vr.round_reference " +
            "    AND rpr.amount = vr.amount AND rpr.block_index = vr.block_index " +
            "JOIN index_pubkeys ip ON ip.id = vr.signing_pubkey_id AND ip.pubkey = rpr.validator_pubkey " +
            "WHERE rpr.applied = 1 AND rpr.applied_block IS NOT NULL " +
            "  AND rpr.applied_block BETWEEN ? AND ? " +
            "  AND vr.block_index < rpr.applied_block",
            [from, to], conn);
    },

    /**
     * Delete every validator_rewards row a replicated reconcile-log pre-image
     * names, keyed on the full five-column reward identity. round_qualifier is part
     * of the key because two distinct archive rewards can share the other four.
     *
     * @param {string} scopeSql  predicate over the log alias `d` bounding its rows to the applied window
     * @param {Array} scopeArgs  the predicate's bind values
     * @returns {Promise<object>} the driver's result
     */
    async deleteReconciledValidatorRewards(scopeSql, scopeArgs){
        return await this.doQuery(
            "DELETE vr FROM validator_rewards vr " +
            "JOIN anchor_reward_reconcile_log d " +
            "  ON d.source_id = vr.source_id AND d.signing_pubkey_id = vr.signing_pubkey_id " +
            " AND d.reward_type = vr.reward_type AND d.round_reference <=> vr.round_reference " +
            " AND d.round_qualifier = vr.round_qualifier " +
            "WHERE " + scopeSql,
            scopeArgs);
    },

};
