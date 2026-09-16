'use strict';

// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

const fixtures = require('../../e2e/helpers/fixtures');

class DataGenerator {

    constructor(db) {
        this.db = db;
    }

    /**
     * Seed a contiguous range of blocks (1 action each).
     * Wraps fixtures.seedBlocks().
     */
    async seedBlockRange(start, end, opts) {
        return await fixtures.seedBlocks(this.db, start, end, opts);
    }

    /**
     * Seed a single block with many actions.
     * Wraps fixtures.seedLargeBlock().
     */
    async seedLargeBlock(blockIndex, actionCount) {
        return await fixtures.seedLargeBlock(this.db, blockIndex, actionCount);
    }

    /**
     * Delete blocks from a given index onward.
     * Wraps fixtures.deleteBlocksFrom().
     */
    async deleteBlocksFrom(blockIndex) {
        return await fixtures.deleteBlocksFrom(this.db, blockIndex);
    }

    /**
     * Seed a large number of blocks in batches to avoid overwhelming the DB.
     *
     * @param {number} blockCount      - Total blocks to seed (starting from 1)
     * @param {number} actionsPerBlock  - Actions per block (1 = normal, >1 = large blocks)
     * @param {object} opts             - Options passed to seedBlocks (sourceAddr, tickName, etc.)
     */
    async seedBulk(blockCount, actionsPerBlock, opts) {
        const batchSize = 50;
        for (let start = 1; start <= blockCount; start += batchSize) {
            let end = Math.min(start + batchSize - 1, blockCount);
            if (actionsPerBlock > 1) {
                for (let i = start; i <= end; i++) {
                    await fixtures.seedLargeBlock(this.db, i, actionsPerBlock);
                }
            } else {
                await fixtures.seedBlocks(this.db, start, end, opts);
            }
        }
    }

    /**
     * Seed blocks with varying action counts for mixed-load testing.
     * Alternates between small (1 action) and large blocks.
     *
     * @param {number} blockCount       - Total blocks to seed
     * @param {number} largeEveryN      - Every Nth block is large
     * @param {number} largeActionCount - Actions in large blocks
     */
    async seedMixed(blockCount, largeEveryN, largeActionCount) {
        for (let i = 1; i <= blockCount; i++) {
            if (i % largeEveryN === 0) {
                await fixtures.seedLargeBlock(this.db, i, largeActionCount);
            } else {
                await fixtures.seedBlocks(this.db, i, i);
            }
        }
    }
}

module.exports = DataGenerator;
