/*********************************************************************
 *
 * Copyright © 2025 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC – https://dankest.llc
 *
 * Licensed under the Dankest Community License (Apache License 2.0 + Additional Terms).
 * You may not use this file except in compliance with that License.
 *
 * A copy of the License is available at:
 *     https://dankest.llc/license
 *
 * This software is provided "AS IS", without warranties or conditions of any kind.
 *
 **********************************************************************
 *
 * XChain Indexer Sync - Transparency Log
 *
 * Append-only per-block hash log with Merkle tree epochs.
 * Records three block hashes (ledger, actions, contracts) per block
 * and periodically commits Merkle roots over epochs for
 * cryptographic inclusion proofs.
 *
 ********************************************************************/

const MerkleTree = require('./MerkleTree.js');

class TransparencyLog {

    constructor(db, epochSize) {
        this.db        = db;
        this.epochSize = epochSize || 100;
    }

    // Record a block's hashes in the transparency log
    // Automatically commits a Merkle epoch when an epoch boundary is crossed
    async recordBlock(block_index, block_time, ledger_hash, actions_hash, contract_hash){
        let query = `INSERT IGNORE INTO sync_meta
            (block_index, block_time, ledger_hash, actions_hash, contract_hash)
            VALUES (?, ?, ?, ?, ?)`;
        await this.db.doQuery(query, [block_index, block_time, ledger_hash, actions_hash, contract_hash]);

        // Check if we crossed an epoch boundary
        if (block_index > 0 && block_index % this.epochSize === 0) {
            let epoch = Math.floor(block_index / this.epochSize);
            await this.commitEpoch(epoch).catch(e =>
                console.error('Error committing Merkle epoch ' + epoch + ':', e.message)
            );
        }
    }

    // Commit a Merkle epoch — build tree from epoch blocks and store root
    async commitEpoch(epoch) {
        let startBlock = (epoch - 1) * this.epochSize + 1;
        let endBlock   = epoch * this.epochSize;

        // Check if already committed
        let existing = await this.db.doQuery(
            "SELECT id FROM merkle_epochs WHERE epoch = ?", [epoch]
        );
        if (existing.length > 0) return;

        // Read blocks for this epoch
        let rows = await this.db.doQuery(
            `SELECT block_index, ledger_hash, actions_hash, contract_hash
             FROM sync_meta
             WHERE block_index >= ? AND block_index <= ?
             ORDER BY block_index ASC`,
            [startBlock, endBlock]
        );

        if (rows.length === 0) return;

        // Compute leaves
        let leaves = rows.map(r =>
            MerkleTree.computeLeaf(r.ledger_hash, r.actions_hash, r.contract_hash)
        );

        // Build tree
        let tree = MerkleTree.buildTree(leaves);
        if (!tree.root) return;

        // Store the epoch root
        await this.db.doQuery(
            `INSERT INTO merkle_epochs (epoch, start_block, end_block, merkle_root, leaf_count)
             VALUES (?, ?, ?, ?, ?)`,
            [epoch, rows[0].block_index, rows[rows.length - 1].block_index, tree.root, leaves.length]
        );

        console.log('Merkle: Epoch ' + epoch + ' committed (blocks ' + startBlock + '-' + endBlock +
            ', root: ' + tree.root.substring(0, 16) + '...)');
    }

    // Generate an inclusion proof for a specific block
    async getProof(blockIndex) {
        blockIndex = parseInt(blockIndex);
        if (isNaN(blockIndex) || blockIndex < 1) return null;

        // Determine which epoch this block belongs to
        let epoch = Math.ceil(blockIndex / this.epochSize);

        // Get the committed epoch
        let epochRows = await this.db.doQuery(
            "SELECT * FROM merkle_epochs WHERE epoch = ?", [epoch]
        );
        if (epochRows.length === 0) return { error: 'epoch not yet committed' };

        let epochData = epochRows[0];

        // Rebuild the tree for this epoch
        let rows = await this.db.doQuery(
            `SELECT block_index, ledger_hash, actions_hash, contract_hash
             FROM sync_meta
             WHERE block_index >= ? AND block_index <= ?
             ORDER BY block_index ASC`,
            [epochData.start_block, epochData.end_block]
        );

        if (rows.length === 0) return null;

        // Find the leaf index for this block
        let leafIndex = -1;
        let leaves = [];
        for (let i = 0; i < rows.length; i++) {
            leaves.push(MerkleTree.computeLeaf(rows[i].ledger_hash, rows[i].actions_hash, rows[i].contract_hash));
            if (Number(rows[i].block_index) === blockIndex) leafIndex = i;
        }

        if (leafIndex === -1) return { error: 'block not found in epoch' };

        let tree = MerkleTree.buildTree(leaves);
        let proof = MerkleTree.generateProof(tree.layers, leafIndex);

        return {
            blockIndex:  blockIndex,
            epoch:       epoch,
            leaf:        leaves[leafIndex],
            merkleRoot:  epochData.merkle_root,
            proof:       proof,
            verified:    MerkleTree.verifyProof(leaves[leafIndex], proof, epochData.merkle_root)
        };
    }

    // Get the latest committed Merkle root
    async getLatestRoot() {
        let rows = await this.db.doQuery(
            "SELECT * FROM merkle_epochs ORDER BY epoch DESC LIMIT 1"
        );
        return rows.length > 0 ? rows[0] : null;
    }

    // Get a paginated page of transparency log entries
    async getPage(page, limit){
        page  = Math.max(0, parseInt(page) || 0);
        limit = Math.min(1000, Math.max(1, parseInt(limit) || 100));
        let offset = page * limit;

        let countQuery = "SELECT COUNT(*) as total FROM sync_meta";
        let countRows  = await this.db.doQuery(countQuery);
        let total      = Number(countRows[0].total);

        let query = `SELECT block_index, block_time, ledger_hash, actions_hash, contract_hash, logged_at
            FROM sync_meta
            ORDER BY block_index DESC
            LIMIT ? OFFSET ?`;
        let rows = await this.db.doQuery(query, [limit, offset]);

        return { page, limit, total, results: rows };
    }
}

module.exports = TransparencyLog;
