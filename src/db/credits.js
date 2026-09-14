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
 * The replicated credits table, read by the source-side forward channel that
 * carries unstake cooldown refunds: a refund is credited in the block where the
 * cooldown MATURES, which is not the block that created the unstake, so a
 * block-scoped read of the unstake would never reach it.
 *
 * A Database mixin: every method below is installed on Database.prototype by
 * db/index.js, so `this` is the Database instance and call sites are unchanged.
 *
 ********************************************************************/

module.exports = {

    /**
     * Refund credits for capability unstakes whose cooldown matured in this
     * window. A capability refund is paid in GAS, so the tick is matched by
     * symbol, and the credit is tied back to its unstake by the unstake's own
     * action index and source address.
     *
     * @param {string} gasTick           the GAS tick symbol
     * @param {number} completedStatusId the status id an unstake carries once complete
     * @param {number} from              first block of the window, inclusive
     * @param {number} to                last block of the window, inclusive
     * @param {object} [conn]            a connection to read on, when the caller holds one
     * @returns {Promise<object[]>} the driver's row array
     */
    async findMaturedCapabilityCooldownCredits(gasTick, completedStatusId, from, to, conn){
        return await this.doQuery(
            "SELECT c.* FROM credits c " +
            "JOIN unstakes u ON u.action_index = c.action_index AND u.source_id = c.address_id " +
            "JOIN index_tickers g ON g.id = c.tick_id AND g.tick = ? " +
            "WHERE u.status_id = ? AND u.cooldown_end_block BETWEEN ? AND ?",
            [gasTick, completedStatusId, from, to], conn);
    },

    /**
     * Refund credits for contract unstakes whose cooldown matured in this
     * window. A contract refund is paid in the unstake's OWN tick, so the tick
     * is part of the join rather than looked up by symbol.
     *
     * @param {number} completedStatusId the status id an unstake carries once complete
     * @param {number} from              first block of the window, inclusive
     * @param {number} to                last block of the window, inclusive
     * @param {object} [conn]            a connection to read on, when the caller holds one
     * @returns {Promise<object[]>} the driver's row array
     */
    async findMaturedContractCooldownCredits(completedStatusId, from, to, conn){
        return await this.doQuery(
            "SELECT c.* FROM credits c " +
            "JOIN contract_unstakes cu ON cu.action_index = c.action_index AND cu.source_id = c.address_id AND cu.tick_id = c.tick_id " +
            "WHERE cu.status_id = ? AND cu.cooldown_end_block BETWEEN ? AND ?",
            [completedStatusId, from, to], conn);
    },

    /**
     * The current tokens row of every tick a credit, debit or escrow touched in
     * this window, which is exactly the set whose supply may have moved. Each
     * ledger table joins actions on its own and the tick ids are UNIONed, so the
     * block-range predicate drives from actions into each table's index instead of
     * materializing all three ledgers first. UNION, not UNION ALL, keeps the tick
     * set distinct.
     *
     * @param {number} from   first block of the window, inclusive
     * @param {number} to     last block of the window, inclusive
     * @param {object} [conn] a connection to read on, when the caller holds one
     * @returns {Promise<object[]>} the driver's row array
     */
    async findLedgerTouchedTokens(from, to, conn){
        return await this.doQuery(
            "SELECT t.* FROM `tokens` t WHERE t.tick_id IN (" +
                "SELECT c.tick_id FROM credits c JOIN actions a ON a.action_index = c.action_index " +
                    "WHERE a.block_index BETWEEN ? AND ? AND c.tick_id IS NOT NULL " +
                "UNION " +
                "SELECT d.tick_id FROM debits d JOIN actions a ON a.action_index = d.action_index " +
                    "WHERE a.block_index BETWEEN ? AND ? AND d.tick_id IS NOT NULL " +
                "UNION " +
                "SELECT e.tick_id FROM escrows e JOIN actions a ON a.action_index = e.action_index " +
                    "WHERE a.block_index BETWEEN ? AND ? AND e.tick_id IS NOT NULL)",
            [from, to, from, to, from, to], conn);
    },

};
