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
 * The merkle_epochs and merkle_reorgs tables, written and read by the source
 * transparency log: one committed Merkle root per epoch of sync_meta leaves, and
 * an append-only audit marker for every committed epoch a reorg invalidates.
 *
 * Every method is a plain doQuery, never a transaction, because the log runs
 * from the poll loop and orders its steps so each one is idempotent on retry.
 *
 * A Database mixin: every method below is installed on Database.prototype by
 * db/index.js, so `this` is the Database instance and call sites are unchanged.
 *
 ********************************************************************/

module.exports = {

    /**
     * The end block of the highest committed epoch that ends at or below a height.
     *
     * @param {number} cutoff the highest block the epoch may end on
     * @returns {Promise<object[]>} the driver's row array, one row carrying `eb`
     */
    async getHighestEpochEndAtOrBelow(cutoff){
        return await this.doQuery(
            "SELECT MAX(end_block) AS eb FROM merkle_epochs WHERE end_block <= ?", [cutoff]
        );
    },

    /**
     * How many committed epochs straddle a block, starting at or below it and
     * ending above it.
     *
     * @param {number} boundary the block to test
     * @returns {Promise<object[]>} the driver's row array, one row carrying `c`
     */
    async countEpochsStraddling(boundary){
        return await this.doQuery(
            "SELECT COUNT(*) AS c FROM merkle_epochs WHERE start_block <= ? AND end_block > ?",
            [boundary, boundary]
        );
    },

    /**
     * The id of a committed epoch, if it is committed.
     *
     * @param {number} epoch
     * @returns {Promise<object[]>} the driver's row array, empty when not committed
     */
    async findMerkleEpochId(epoch){
        return await this.doQuery(
            "SELECT id FROM merkle_epochs WHERE epoch = ?", [epoch]
        );
    },

    /**
     * Commit one epoch's Merkle root.
     *
     * @param {number} epoch
     * @param {number} startBlock first leaf's block
     * @param {number} endBlock   last leaf's block
     * @param {string} root       the Merkle root over the leaves
     * @param {number} leafCount  how many leaves the root was built from
     * @returns {Promise<object>} the driver's write result
     */
    async insertMerkleEpoch(epoch, startBlock, endBlock, root, leafCount){
        return await this.doQuery(
            `INSERT INTO merkle_epochs (epoch, start_block, end_block, merkle_root, leaf_count)
             VALUES (?, ?, ?, ?, ?)`,
            [epoch, startBlock, endBlock, root, leafCount]
        );
    },

    /**
     * Fill the new root into an epoch's pending reorg marker. A no-op when the
     * epoch was never reorged.
     *
     * @param {string} root  the freshly recomputed root
     * @param {number} epoch
     * @returns {Promise<object>} the driver's write result
     */
    async backfillReorgMarkerRoot(root, epoch){
        return await this.doQuery(
            "UPDATE merkle_reorgs SET new_root = ? WHERE epoch = ? AND new_root IS NULL",
            [root, epoch]
        );
    },

    /**
     * Committed epochs whose block range reaches a height or above it.
     *
     * @param {number} block_index the first orphaned block
     * @returns {Promise<object[]>} the driver's row array
     */
    async findEpochsEndingAtOrAfter(block_index){
        return await this.doQuery(
            `SELECT epoch, start_block, end_block, merkle_root
             FROM merkle_epochs WHERE end_block >= ?`,
            [block_index]
        );
    },

    /**
     * The id of an epoch's reorg marker that has not been re-committed yet.
     *
     * @param {number} epoch
     * @returns {Promise<object[]>} the driver's row array, at most one row
     */
    async findPendingReorgMarker(epoch){
        return await this.doQuery(
            "SELECT id FROM merkle_reorgs WHERE epoch = ? AND new_root IS NULL LIMIT 1", [epoch]
        );
    },

    /**
     * Record the audit marker for a committed epoch a reorg invalidates.
     *
     * @param {number} block_index the reorg height
     * @param {number} epoch
     * @param {number} startBlock  the epoch's first block
     * @param {number} endBlock    the epoch's last block
     * @param {string} oldRoot     the root being invalidated
     * @returns {Promise<object>} the driver's write result
     */
    async insertReorgMarker(block_index, epoch, startBlock, endBlock, oldRoot){
        return await this.doQuery(
            `INSERT INTO merkle_reorgs (reorg_block, epoch, start_block, end_block, old_root)
                     VALUES (?, ?, ?, ?, ?)`,
            [block_index, epoch, startBlock, endBlock, oldRoot]
        );
    },

    /**
     * Delete every committed epoch that reaches a height or above it.
     *
     * @param {number} block_index the first orphaned block
     * @returns {Promise<object>} the driver's write result
     */
    async deleteEpochsEndingAtOrAfter(block_index){
        return await this.doQuery("DELETE FROM merkle_epochs WHERE end_block >= ?", [block_index]);
    },

    /**
     * One committed epoch row.
     *
     * @param {number} epoch
     * @returns {Promise<object[]>} the driver's row array, empty when not committed
     */
    async getMerkleEpoch(epoch){
        return await this.doQuery(
            "SELECT * FROM merkle_epochs WHERE epoch = ?", [epoch]
        );
    },

    /**
     * Delete one committed epoch so it can be re-committed.
     *
     * @param {number} epoch
     * @returns {Promise<object>} the driver's write result
     */
    async deleteMerkleEpoch(epoch){
        return await this.doQuery("DELETE FROM merkle_epochs WHERE epoch = ?", [epoch]);
    },

    /**
     * The most recently committed epoch row.
     *
     * @returns {Promise<object[]>} the driver's row array, at most one row
     */
    async getLatestMerkleEpoch(){
        return await this.doQuery(
            "SELECT * FROM merkle_epochs ORDER BY epoch DESC LIMIT 1"
        );
    },

};
