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
 * Row-level writes: keyed deletes and multi-row inserts.
 *
 * A Database mixin composed by db/tables.js and installed on Database.prototype
 * by db/index.js, so `this` is the Database instance and call sites are unchanged.
 *
 ********************************************************************/

module.exports = {

    /**
     * Clear every row already holding one of these natural-key values, so a
     * re-sent row replaces rather than collides. Both names are interpolated
     * because an identifier cannot be a bind parameter; the caller validates them.
     *
     * @param {string} table
     * @param {string} naturalKey the column the values belong to
     * @param {Array} slice       the values, a bounded chunk of them
     * @returns {Promise<object>} the driver's result
     */
    async deleteRowsByKeyValues(table, naturalKey, slice){
        return await this.doQuery(
            'DELETE FROM `' + table + '` WHERE `' + naturalKey + '` IN (' +
                slice.map(() => '?').join(', ') + ')',
            slice);
    },

    /**
     * One multi-row INSERT of `rowCount` rows over `columns`, with `args` holding
     * every row's values in column order. `useIgnore` skips a row whose key already
     * exists; `useUpsert` overwrites the existing row with the carried values. The
     * caller validates every identifier and decodes the values.
     *
     * @param {string} table
     * @param {Array<string>} columns
     * @param {number} rowCount
     * @param {Array} args
     * @param {boolean} useIgnore
     * @param {boolean} useUpsert
     * @returns {Promise<object>} the driver's result
     */
    async insertRowValues(table, columns, rowCount, args, useIgnore, useUpsert){
        let colList      = columns.map(c => '`' + c + '`').join(', ');
        let placeholders = columns.map(() => '?').join(', ');

        let insertPrefix = useIgnore
            ? 'INSERT IGNORE INTO `' + table + '` (' + colList + ') VALUES '
            : 'INSERT INTO `' + table + '` (' + colList + ') VALUES ';
        // VALUES(col) back-reference is the MariaDB idiom for "the value this row
        // would have inserted"; updating the key column to itself is a harmless no-op.
        let updateSuffix = useUpsert
            ? ' ON DUPLICATE KEY UPDATE ' + columns.map(c => '`' + c + '` = VALUES(`' + c + '`)').join(', ')
            : '';

        let valueClauses = [];
        for(let i = 0; i < rowCount; i++) valueClauses.push('(' + placeholders + ')');

        let query = insertPrefix + valueClauses.join(', ') + updateSuffix;
        return await this.doQuery(query, args);
    },

    /**
     * Delete one row by its surrogate id.
     *
     * @param {string} table
     * @param {number} holderId
     * @returns {Promise<object>} the driver's result
     */
    async deleteRowById(table, holderId){
        return await this.doQuery('DELETE FROM `' + table + '` WHERE id = ?', [holderId]);
    },

};
