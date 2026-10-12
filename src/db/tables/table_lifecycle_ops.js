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
