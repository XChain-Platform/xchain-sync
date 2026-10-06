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
 * Snapshot and catch-up reads: row streams, paged and windowed scans, table listings.
 *
 * A Database mixin composed by db/tables.js and installed on Database.prototype
 * by db/index.js, so `this` is the Database instance and call sites are unchanged.
 *
 ********************************************************************/

module.exports = {

    // The set of BASE TABLEs that actually exist in this database.
    //
    // Exists so a caller can skip a table instead of discovering its absence by
    // failing a query against it. That distinction is not cosmetic: the /status
    // handlers enumerate the STATIC replicated-table list, which grows whenever a
    // new family lands in this repo, so on any replica whose source predates that
    // family a poll that queries such a table raises ER_NO_SUCH_TABLE, logs a
    // multi-line SqlError and swallows it, every time. That error is expected and
    // tolerated, yet indistinguishable in the journal from a real fault, and it is
    // raised for tables the SOURCE does not have either.
    //
    // doQueryStrict, so a failure to LIST is never silently read as "nothing
    // exists": that would empty table_counts and make an incomplete replica look
    // complete to verifyTableCounts. Callers fall back to probing instead.
    async listExistingTables(conn){
        let rows = await this.doQueryStrict(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_type = 'BASE TABLE'",
            [this.dbName], conn);
        return new Set(rows.map(r => r.table_name || r.TABLE_NAME));
    },

    /**
     * The name of every base table in this database, unordered: the snapshot
     * builder imposes its own dependency order.
     *
     * @param {object} [conn] a connection to read on, when the caller holds one
     * @returns {Promise<object[]>} the driver's row array, one row per table
     */
    async findStreamableTableNames(conn){
        return await this.doQuery(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_type = 'BASE TABLE'",
            [this.dbName],
            conn
        );
    },

    /**
     * Every row of a table from a block onward, keyed by a block_index column.
     * The table is interpolated because an identifier cannot be a bind parameter;
     * the caller passes a name from the replicated-table topology.
     *
     * @param {string} table
     * @param {number} sinceBlock first block, inclusive
     * @param {object} [conn] a connection to read on, when the caller holds one
     * @returns {Promise<object[]>} the driver's row array
     */
    async findRowsFromBlockIndex(table, sinceBlock, conn){
        return await this.doQuery("SELECT * FROM `" + table + "` WHERE block_index >= ? ORDER BY block_index", [sinceBlock], conn);
    },

    /**
     * Every row of a table from a block onward, keyed by the column the table
     * lifecycle registry names for it. Both names are interpolated from the
     * registry, never user input.
     *
     * @param {string} table
     * @param {string} key the table's block scope column
     * @param {number} sinceBlock first block, inclusive
     * @param {object} [conn] a connection to read on, when the caller holds one
     * @returns {Promise<object[]>} the driver's row array
     */
    async findRowsFromBlockKey(table, key, sinceBlock, conn){
        return await this.doQuery("SELECT * FROM `" + table + "` WHERE " + key + " >= ? ORDER BY " + key, [sinceBlock], conn);
    },

    /**
     * Every row of a table from an action index onward. The table is interpolated
     * from the replicated-table topology, never user input.
     *
     * @param {string} table
     * @param {number} firstActionIndex first action, inclusive
     * @param {object} [conn] a connection to read on, when the caller holds one
     * @returns {Promise<object[]>} the driver's row array
     */
    async findRowsFromActionIndex(table, firstActionIndex, conn){
        return await this.doQuery("SELECT * FROM `" + table + "` WHERE action_index >= ? ORDER BY action_index", [firstActionIndex], conn);
    },

    /**
     * One page of an append-only lookup table after an id cursor. The table and
     * cursor column come from the replicated-table allowlist, never user input.
     *
     * @param {string} table
     * @param {string} col the cursor column
     * @param {number} after the last cursor value already read
     * @param {number} limit the page size
     * @param {object} [conn] a connection to read on, when the caller holds one
     * @returns {Promise<object[]>} the driver's row array
     */
    async findLookupPageAfter(table, col, after, limit, conn){
        return await this.doQuery(
            "SELECT * FROM `" + table + "` WHERE `" + col + "` > ? ORDER BY `" + col + "` ASC LIMIT ?",
            [after, limit],
            conn
        );
    },

};
