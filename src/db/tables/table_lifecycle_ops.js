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
 * Emptying a replicated table ahead of a re-import.
 *
 * A Database mixin composed by db/tables.js and installed on Database.prototype
 * by db/index.js, so `this` is the Database instance and call sites are unchanged.
 *
 ********************************************************************/

const { assertValidIdentifier } = require('../shared.js');

module.exports = {

    async truncateTable(table){
        assertValidIdentifier(table);
        await this.doQuery("TRUNCATE TABLE `" + table + "`");
    },

    /**
     * Empty one table on the replica before a full snapshot re-imports it. DELETE
     * rather than TRUNCATE, because MariaDB refuses TRUNCATE on a table a foreign
     * key references. The caller validates the name first.
     *
     * @param {string} table
     * @returns {Promise<object>} the driver's result
     */
    async deleteAllRows(table){
        return await this.doQuery('DELETE FROM `' + table + '`');
    },

    // Re-establish an AUTO_INCREMENT allocator from the rows a snapshot kept.
    // Treat missing legacy tables or id columns as benign schema gaps.
    async resetAutoIncrementToMax(table){
        assertValidIdentifier(table);
        try {
            const rows = await this.doQuery(
                'SELECT COALESCE(MAX(id), 0) + 1 AS next_id FROM `' + table + '`',
                [], null, { rethrow: true });
            if(!rows || rows.length === 0) return false;
            const raw = rows[0].next_id === undefined ? rows[0].NEXT_ID : rows[0].next_id;
            const text = String(raw);
            if(!/^[0-9]+$/.test(text) || BigInt(text) < 1n)
                throw new Error('Unsafe AUTO_INCREMENT value for ' + table + ': ' + text);
            await this.runSqlMigrationStatement(
                '2026-10-19-replica-columns.sql', 'reset-auto-increment',
                { table, next_id: BigInt(text).toString() }, { rethrow: true });
            return true;
        } catch(e){
            if(e && (e.errno === 1146 || e.errno === 1054)) return false;
            throw e;
        }
    },

    /**
     * Empty the decoder's dispensers table ahead of a reconcile re-insert, inside
     * the caller's transaction.
     *
     * @returns {Promise<object>} the driver's result
     */
    async deleteAllDispensers(){
        return await this.doQuery('DELETE FROM `dispensers`');
    },

};
