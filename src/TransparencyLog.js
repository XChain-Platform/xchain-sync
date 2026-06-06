/*********************************************************************
 *
 * Copyright © 2025–2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC – https://dankest.llc
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This file is part of XChain Platform. Licensed under the GNU Affero
 * General Public License v3.0 or later; see LICENSE.md. A commercial
 * license (without AGPL source-disclosure terms) is available —
 * contact legal@dankest.llc.
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
                console.error('Error committing Merkle epoch ' + epoch + ':', e)
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

        // If this epoch was previously invalidated by a reorg, backfill its audit
        // marker with the freshly recomputed root to complete the old->new trail.
        // No-op when the epoch was never reorged; tolerant of an older schema with
        // no merkle_reorgs table.
        await this.db.doQuery(
            "UPDATE merkle_reorgs SET new_root = ? WHERE epoch = ? AND new_root IS NULL",
            [tree.root, epoch]
        ).catch(e => console.error('Error backfilling reorg marker for epoch ' + epoch + ':', e));

        console.log('Merkle: Epoch ' + epoch + ' committed (blocks ' + startBlock + '-' + endBlock +
            ', root: ' + tree.root.substring(0, 16) + '...)');
    }

    // Prune the transparency log on a server-side reorg to `block_index` — the new
    // canonical tip + 1, so every block >= block_index was orphaned. The source's
    // own sync_meta / merkle_epochs are sync-service-owned tables that nothing else
    // rolls back (the indexer rolls back its data tables, not these), and
    // recordBlock writes sync_meta with INSERT IGNORE on a UNIQUE block_index — so
    // without this, re-added blocks keep their pre-reorg hashes (the new hashes
    // silently dropped) and the node serves wrong Merkle proofs. Mirrors and
    // completes the client's sync_meta prune in ClientRollback, on the source side.
    //
    // Policy (operator decision): track the canonical chain. Capture every
    // committed merkle_epoch the reorg invalidates as an append-only audit marker
    // (old_root), delete those epochs so they re-commit from the canonical chain
    // when the boundary is re-crossed (commitEpoch backfills new_root), and delete
    // the orphaned sync_meta rows so recordBlock re-inserts fresh hashes.
    //
    // Deliberately uses plain doQuery (no beginTransaction) — like recordBlock and
    // the rest of the poll loop. A transaction here drives db.transactionConnection,
    // the single shared field the snapshot-read path (beginReadSnapshot) also uses;
    // adding the poll loop's first transaction races that field under fault. Instead
    // the steps are ordered so a partial failure self-heals on the next poll's retry
    // (the reorg branch leaves lastPolledBlock un-rewound until this completes), and
    // every step is idempotent: the marker insert is guarded against duplicates and
    // both DELETEs are range-deletes that no-op once the rows are gone. If any step
    // throws (DB fault), it propagates so the poll loop retries the whole prune.
    async pruneFrom(block_index){
        // Committed epochs whose block range overlaps the orphaned suffix.
        let invalidated = await this.db.doQuery(
            `SELECT epoch, start_block, end_block, merkle_root
             FROM merkle_epochs WHERE end_block >= ?`,
            [block_index]
        );
        for(let e of invalidated){
            // Idempotent: skip if a pending (not-yet-re-committed) marker for this
            // epoch already exists, so a retry after a mid-prune fault can't pile up
            // duplicate audit rows during a sustained outage.
            let pending = await this.db.doQuery(
                "SELECT id FROM merkle_reorgs WHERE epoch = ? AND new_root IS NULL LIMIT 1", [e.epoch]
            );
            if(pending.length === 0){
                await this.db.doQuery(
                    `INSERT INTO merkle_reorgs (reorg_block, epoch, start_block, end_block, old_root)
                     VALUES (?, ?, ?, ?, ?)`,
                    [block_index, e.epoch, e.start_block, e.end_block, e.merkle_root]
                );
            }
        }
        // Epochs before sync_meta: if we fault between, the retry re-reads
        // merkle_epochs (now empty for this range, so no duplicate marker) and still
        // prunes sync_meta. Both are idempotent range-deletes.
        await this.db.doQuery("DELETE FROM merkle_epochs WHERE end_block >= ?", [block_index]);
        await this.db.doQuery("DELETE FROM sync_meta WHERE block_index >= ?", [block_index]);
        if(invalidated.length > 0)
            console.log('Transparency log pruned at reorg to block ' + block_index + ': ' +
                invalidated.length + ' committed epoch(s) invalidated (will re-commit from canonical chain)');
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
