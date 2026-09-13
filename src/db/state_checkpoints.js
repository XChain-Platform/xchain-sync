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
 * The replicated state_checkpoints table: the signed checkpoints a client pulls
 * to anchor what it replicates. A block can carry more than one checkpoint, so
 * checkpoint_seq breaks the tie and the newest one at a height is the one served.
 *
 * A Database mixin: every method below is installed on Database.prototype by
 * db/index.js, so `this` is the Database instance and call sites are unchanged.
 *
 ********************************************************************/

// The columns a checkpoint is served with. One list for every read below, so the
// latest, the range and the single-height views can never disagree on shape.
const CHECKPOINT_COLS = 'chain, network, block_index, block_hash, ledger_hash, actions_hash, ' +
    'contract_hash, checkpoint_seq, snapshot_block, state_root, state_root_version, ' +
    'block_merkle_root, block_merkle_version, validator_signatures';

module.exports = {

    /**
     * The newest checkpoint of all.
     *
     * @returns {Promise<object[]>} the driver's row array, at most one row
     */
    async getLatestCheckpoint(){
        return await this.doQuery(
            'SELECT ' + CHECKPOINT_COLS + ' FROM state_checkpoints ORDER BY block_index DESC, checkpoint_seq DESC LIMIT 1');
    },

    /**
     * One checkpoint per block across a range, oldest first, keeping only the
     * newest checkpoint at each height.
     *
     * @param {number} from  first block, inclusive
     * @param {number} to    last block, inclusive
     * @param {number} limit the most rows to return, so a client pages by advancing from
     * @returns {Promise<object[]>} the driver's row array
     */
    async findCheckpointsInRange(from, to, limit){
        return await this.doQuery(
            'SELECT ' + CHECKPOINT_COLS + ' FROM state_checkpoints sc ' +
            'WHERE block_index >= ? AND block_index <= ? ' +
            'AND checkpoint_seq = (SELECT MAX(s2.checkpoint_seq) FROM state_checkpoints s2 WHERE s2.block_index = sc.block_index) ' +
            'ORDER BY block_index ASC LIMIT ?',
            [from, to, limit]);
    },

    /**
     * The newest checkpoint at one height.
     *
     * @param {number} height
     * @returns {Promise<object[]>} the driver's row array, at most one row
     */
    async getCheckpointAtHeight(height){
        return await this.doQuery(
            'SELECT ' + CHECKPOINT_COLS + ' FROM state_checkpoints WHERE block_index=? ORDER BY checkpoint_seq DESC LIMIT 1',
            [height]);
    },

};
