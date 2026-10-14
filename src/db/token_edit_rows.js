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
 * The tokens rows an ISSUE edit rewrote, read by the updated_rows tokens-EDIT
 * class (src/server/updated_rows/token_rows.js).
 *
 * A Database mixin: every method below is installed on Database.prototype by
 * db/index.js, so `this` is the Database instance.
 *
 ********************************************************************/

module.exports = {

    /**
     * The current tokens row of every tick with a valid `issues` row in this
     * window. `issues` stores every ISSUE, valid or not, and only a valid one
     * reaches createToken, so the status join keeps an invalid edit from
     * re-emitting a row that never changed.
     *
     * @param {number} from   first block of the window, inclusive
     * @param {number} to     last block of the window, inclusive
     * @param {object} [conn] a connection to read on, when the caller holds one
     * @returns {Promise<object[]>} the driver's row array
     */
    async findEditedTokens(from, to, conn){
        return await this.doQuery(
            "SELECT t.* FROM `tokens` t WHERE t.tick_id IN (" +
                "SELECT i.tick_id FROM issues i " +
                    "JOIN actions a ON a.action_index = i.action_index " +
                    "JOIN index_statuses s ON s.id = i.status_id AND s.status = 'valid' " +
                    "WHERE a.block_index BETWEEN ? AND ? AND i.tick_id IS NOT NULL)",
            [from, to], conn);
    },

};
