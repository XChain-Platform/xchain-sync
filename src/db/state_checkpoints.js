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
 * The hub mirrors every chain's checkpoints into every indexer DB, so one table
 * holds BTC, LTC and DOGE rows side by side. Every read is scoped to one chain
 * and network: an unscoped "newest" picks whichever chain sits highest (DOGE's
 * heights dwarf the others), and the replica rejects it as a binding mismatch.
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
     * The newest checkpoint of one chain and network.
     *
     * @param {string} chain   the checkpointed chain's ticker (BTC, LTC, DOGE), as the column holds it
     * @param {string} network mainnet, testnet or regtest
     * @returns {Promise<object[]>} the driver's row array, at most one row
     */
    async getLatestCheckpoint(chain, network){
        return await this.doQuery(
            'SELECT ' + CHECKPOINT_COLS + ' FROM state_checkpoints WHERE chain=? AND network=? ' +
            'ORDER BY block_index DESC, checkpoint_seq DESC LIMIT 1',
            [chain, network]);
    },

    /**
     * One checkpoint per block of one chain and network across a range, oldest
     * first, keeping only the newest checkpoint at each height.
     *
     * The MAX(checkpoint_seq) subquery is correlated on chain and network as well
     * as height: another chain's checkpoint at the same block_index carries its own
     * seq series, and an uncorrelated MAX would drop this chain's row there.
     *
     * @param {string} chain   the checkpointed chain's ticker (BTC, LTC, DOGE)
     * @param {string} network mainnet, testnet or regtest
     * @param {number} from    first block, inclusive
     * @param {number} to      last block, inclusive
     * @param {number} limit   the most rows to return, so a client pages by advancing from
     * @returns {Promise<object[]>} the driver's row array
     */
    async findCheckpointsInRange(chain, network, from, to, limit){
        return await this.doQuery(
            'SELECT ' + CHECKPOINT_COLS + ' FROM state_checkpoints sc ' +
            'WHERE sc.chain=? AND sc.network=? AND sc.block_index >= ? AND sc.block_index <= ? ' +
            'AND sc.checkpoint_seq = (SELECT MAX(s2.checkpoint_seq) FROM state_checkpoints s2 ' +
            'WHERE s2.chain = sc.chain AND s2.network = sc.network AND s2.block_index = sc.block_index) ' +
            'ORDER BY sc.block_index ASC LIMIT ?',
            [chain, network, from, to, limit]);
    },

    /**
     * The newest checkpoint of one chain and network at one height.
     *
     * @param {string} chain   the checkpointed chain's ticker (BTC, LTC, DOGE)
     * @param {string} network mainnet, testnet or regtest
     * @param {number} height
     * @returns {Promise<object[]>} the driver's row array, at most one row
     */
    async getCheckpointAtHeight(chain, network, height){
        return await this.doQuery(
            'SELECT ' + CHECKPOINT_COLS + ' FROM state_checkpoints WHERE chain=? AND network=? AND block_index=? ' +
            'ORDER BY checkpoint_seq DESC LIMIT 1',
            [chain, network, height]);
    },

};
