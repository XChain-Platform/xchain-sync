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
 * The replicated transactions table, scoped by block.
 *
 * A Database mixin: every method below is installed on Database.prototype by
 * db/index.js, so `this` is the Database instance and call sites are unchanged.
 *
 ********************************************************************/



module.exports = {

    // Get all rows from a table for transactions in a given block (tx_index-scoped tables).
    // Used for decoder DB tables like transaction_outputs, which key off tx_index and
    // join to the transactions table to recover the block scope.
    async getTxScopedRows(table, block_index, conn){
        let query = `SELECT t.* FROM \`${table}\` t
            INNER JOIN transactions tx ON (tx.tx_index = t.tx_index)
            WHERE tx.block_index = ?
            ORDER BY t.tx_index ASC, 1 ASC`;
        return await this.doQuery(query, [block_index], conn);
    },

    async getTransactions(block_index, conn){
        let query = "SELECT * FROM transactions WHERE block_index = ? ORDER BY tx_index ASC";
        return await this.doQuery(query, [block_index], conn);
    },

    // Rows of a tx_index-scoped table for every transaction from a block onward,
    // the catch-up window form of getTxScopedRows.
    async findTxScopedRowsFromBlock(table, sinceBlock, conn){
        return await this.doQuery(
            "SELECT t.* FROM `" + table + "` t " +
            "INNER JOIN transactions tx ON (tx.tx_index = t.tx_index) " +
            "WHERE tx.block_index >= ? ORDER BY t.tx_index",
            [sinceBlock],
            conn
        );
    },

};
