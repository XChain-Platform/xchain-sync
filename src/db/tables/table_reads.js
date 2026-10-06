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
 * Single-table reads: counts, stats, id and key lookups, schema probes.
 *
 * A Database mixin composed by db/tables.js and installed on Database.prototype
 * by db/index.js, so `this` is the Database instance and call sites are unchanged.
 *
 ********************************************************************/

const { assertValidIdentifier } = require('../shared.js');

module.exports = {

    // Get total row count for a table.
    //
    // doQueryStrict, NOT doQuery, and the reason is a live defect rather than
    // tidiness. Outside a transaction doQuery is fail-soft: it logs the
    // SqlError and returns [], so `rows[0].cnt` then threw a TypeError with NO
    // errno. Every caller that classifies the failure by errno was therefore
    // reading a different error than the one the database raised, and the one
    // that matters is ClientSync.verifyTableCounts: its catch routes errno 1146
    // ("the source's schema moved ahead of this replica") into the debounced
    // schema heal that CREATEs the missing table. A TypeError carries no errno,
    // so that heal could never fire from here, and the replica stayed missing the
    // table forever while logging the same SqlError on every status poll.
    // Observed on a production BTC regtest replica: `bet_resolves` absent and
    // erroring 11,542 times over two days with the heal wired and unreachable.
    //
    // Strict here is safe for the tolerant callers too: /status wraps this in a
    // try/catch and omits the table, which is unchanged. What changes is that the
    // error they swallow, and the one ClientSync inspects, is now the database's
    // own errno-bearing SqlError.
    async getTableCount(table, conn){
        assertValidIdentifier(table);
        let query = "SELECT COUNT(*) as cnt FROM `" + table + "`";
        let rows = await this.doQueryStrict(query, null, conn);
        return Number(rows[0].cnt);
    },

    // Per-database size + table-count stats for every replicated XChain_* DB on this
    // server. information_schema is server-wide, so one connection reports them all.
    // Used by the /catalog endpoint. Rows: {db_name, tables, data_bytes, index_bytes}.
    async getDatabaseStats(){
        let query = "SELECT table_schema AS db_name, COUNT(*) AS tables, " +
                    "COALESCE(SUM(data_length),0) AS data_bytes, " +
                    "COALESCE(SUM(index_length),0) AS index_bytes " +
                    "FROM information_schema.tables " +
                    "WHERE table_type='BASE TABLE' AND table_schema LIKE 'XChain\\_%' " +
                    "GROUP BY table_schema";
        return await this.doQuery(query);
    },

    /**
     * The information_schema row for one table in this database, or none. Used to
     * decide whether a table must be created before rows are written into it.
     *
     * @param {string} tableName
     * @returns {Promise<object[]>} the driver's row array, empty when the table is absent
     */
    async findTableInSchema(tableName){
        return await this.doQuery(
            "SELECT * FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
            [this.dbName, tableName]
        );
    },

    /**
     * The highest value in one column of one table. Both names are interpolated,
     * because an identifier cannot be a bind parameter; the caller passes names it
     * read from the replicated-table registry, never user input.
     *
     * @param {string} table
     * @param {string} col
     * @returns {Promise<object[]>} the driver's row array, one row carrying `m`
     */
    async getMaxColumnValue(table, col){
        return await this.doQuery('SELECT MAX(`' + col + '`) AS m FROM `' + table + '`');
    },

    /**
     * Rows of any replicated table for a set of ids. The table is interpolated,
     * because an identifier cannot be a bind parameter; the caller iterates the
     * replicated-table registry, never user input.
     *
     * @param {string} table
     * @param {Array<number>} ids at least one
     * @param {object} [conn] a connection to read on, when the caller holds one
     * @returns {Promise<object[]>} the driver's row array
     */
    async findRowsByIds(table, ids, conn){
        return await this.doQuery("SELECT * FROM `" + table + "` WHERE id IN (" + ids.map(() => '?').join(',') + ")", ids, conn);
    },

    /**
     * The name of every base table in this database, in name order. Views are
     * excluded, because only a base table has DDL a client can recreate.
     *
     * @returns {Promise<object[]>} the driver's row array, one row per table
     */
    async findBaseTableNames(){
        return await this.doQuery(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_type = 'BASE TABLE' ORDER BY table_name",
            [this.dbName]
        );
    },

    /**
     * Every row of a table. The table is interpolated from the replicated-table
     * topology, never user input.
     *
     * @param {string} table
     * @param {object} [conn] a connection to read on, when the caller holds one
     * @returns {Promise<object[]>} the driver's row array
     */
    async findAllRows(table, conn){
        return await this.doQuery("SELECT * FROM `" + table + "`", null, conn);
    },

    /**
     * The decoder's dispensers rows after a keyset cursor, read strictly so a
     * query error throws rather than reading as an empty table.
     *
     * @param {number} afterTx   the cursor's tx_index
     * @param {number} afterAddr the cursor's address_id
     * @returns {Promise<object[]>} the driver's row array
     */
    async findDispensersAfter(afterTx, afterAddr){
        return await this.doQueryStrict(
            "SELECT * FROM `dispensers` WHERE (tx_index > ? OR (tx_index = ? AND address_id > ?)) " +
            "ORDER BY tx_index ASC, address_id ASC",
            [afterTx, afterTx, afterAddr]
        );
    },

    /**
     * Every row of the decoder's dispensers table, read strictly.
     *
     * @returns {Promise<object[]>} the driver's row array
     */
    async findAllDispensers(){
        return await this.doQueryStrict(
            "SELECT * FROM `dispensers` ORDER BY tx_index ASC, address_id ASC"
        );
    },

    /**
     * The row holding one surrogate id, if any.
     *
     * @param {string} table
     * @param {number} id
     * @returns {Promise<object[]>} the driver's row array, at most one row
     */
    async findRowIdById(table, id){
        return await this.doQuery('SELECT id FROM `' + table + '` WHERE id = ? LIMIT 1', [id]);
    },

    /**
     * The ids of rows matching one natural key. LIMIT 2, because the caller only
     * needs to tell "exactly one holder" from "none" or "ambiguous".
     *
     * @param {string} table
     * @param {Array<string>} keyColumns validated column names of the key
     * @param {Array} values             one value per key column
     * @returns {Promise<object[]>} the driver's row array, at most two rows
     */
    async findRowIdsByKeyColumns(table, keyColumns, values){
        return await this.doQuery(
            'SELECT id FROM `' + table + '` WHERE ' +
                keyColumns.map(c => '`' + c + '` = ?').join(' AND ') + ' LIMIT 2',
            values);
    },

    /**
     * The ordered column names of one index of one table in this database.
     *
     * @param {string} table
     * @param {string} indexName
     * @returns {Promise<object[]>} the driver's row array, one row per column in index order
     */
    async findIndexColumnNames(table, indexName){
        return await this.doQuery(
            "SELECT column_name FROM information_schema.statistics " +
            "WHERE table_schema = ? AND table_name = ? AND index_name = ? ORDER BY seq_in_index ASC",
            [this.dbName, table, indexName]);
    },

};
