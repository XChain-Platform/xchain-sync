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
 * The index lookup tables a forward payload is hydrated from: the transaction
 * and address index rows a block references by id, and the public keys held by
 * the addresses it references. Every query takes an id list whose length is
 * only known at run time, so its placeholders are built per call.
 *
 * A Database mixin: every method below is installed on Database.prototype by
 * db/index.js, so `this` is the Database instance and call sites are unchanged.
 *
 ********************************************************************/

module.exports = {

    /**
     * Index transaction rows for a set of ids.
     *
     * @param {Array<number>} ids distinct index_transactions ids, at least one
     * @param {object} [conn] a connection to read on, when the caller holds one
     * @returns {Promise<object[]>} the driver's row array
     */
    async findIndexTransactionsByIds(ids, conn){
        return await this.doQuery("SELECT * FROM index_transactions WHERE id IN (" + ids.map(() => '?').join(',') + ")", ids, conn);
    },

    /**
     * Index address rows for a set of ids.
     *
     * @param {Array<number>} ids distinct index_addresses ids, at least one
     * @param {object} [conn] a connection to read on, when the caller holds one
     * @returns {Promise<object[]>} the driver's row array
     */
    async findIndexAddressesByIds(ids, conn){
        return await this.doQuery("SELECT * FROM index_addresses WHERE id IN (" + ids.map(() => '?').join(',') + ")", ids, conn);
    },

    /**
     * Every public key held by any of a set of addresses.
     *
     * @param {Array<number>} ids address ids, at least one
     * @param {object} [conn] a connection to read on, when the caller holds one
     * @returns {Promise<object[]>} the driver's row array
     */
    async findPubkeysByAddressIds(ids, conn){
        return await this.doQuery("SELECT * FROM pubkeys WHERE address_id IN (" + ids.map(() => '?').join(',') + ")", ids, conn);
    },

    /**
     * The address string for each of a set of index_addresses ids.
     *
     * @param {Array<number>} aIn distinct index_addresses ids, at least one
     * @returns {Promise<object[]>} the driver's row array, rows of { id, address }
     */
    async findIndexAddressTextByIds(aIn){
        return await this.doQuery(
            'SELECT id, address FROM index_addresses WHERE id IN (' + aIn.map(() => '?').join(',') + ')', aIn);
    },

    /**
     * The tick string for each of a set of index_tickers ids.
     *
     * @param {Array<number>} tIn distinct index_tickers ids, at least one
     * @returns {Promise<object[]>} the driver's row array, rows of { id, tick }
     */
    async findIndexTickTextByIds(tIn){
        return await this.doQuery(
            'SELECT id, tick FROM index_tickers WHERE id IN (' + tIn.map(() => '?').join(',') + ')', tIn);
    },

};
