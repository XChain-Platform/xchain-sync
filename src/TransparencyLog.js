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

    // Commit a Merkle epoch: build tree from epoch blocks and store root
    async commitEpoch(epoch) {
        let startBlock = (epoch - 1) * this.epochSize + 1;
        let endBlock   = epoch * this.epochSize;

        let existing = await this.db.doQuery(
            "SELECT id FROM merkle_epochs WHERE epoch = ?", [epoch]
        );
        if (existing.length > 0) return;

        let rows = await this.db.doQuery(
            `SELECT block_index, ledger_hash, actions_hash, contract_hash
             FROM sync_meta
             WHERE block_index >= ? AND block_index <= ?
             ORDER BY block_index ASC`,
            [startBlock, endBlock]
        );

        if (rows.length === 0) return;

        let leaves = rows.map(r =>
            MerkleTree.computeLeaf(r.ledger_hash, r.actions_hash, r.contract_hash)
        );

        let tree = MerkleTree.buildTree(leaves);
        if (!tree.root) return;

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

    // Prune the transparency log on a server-side reorg to `block_index` (the new
    // canonical tip + 1, so every block >= block_index was orphaned). The source's
    // own sync_meta / merkle_epochs are sync-service-owned tables that nothing else
    // rolls back (the indexer rolls back its data tables, not these), and
    // recordBlock writes sync_meta with INSERT IGNORE on a UNIQUE block_index, so
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
    // Deliberately uses plain doQuery (no beginTransaction), like recordBlock and
    // the rest of the poll loop. A transaction here would drive the shared
    // db.transactionConnection; keeping the poll loop transactionless avoids any
    // contention on it between concurrent writers (the snapshot-read path now uses
    // its own dedicated connection via beginReadSnapshot, so it is no longer a
    // party to that field). Instead the steps are ordered so a partial failure
    // self-heals on the next poll's retry
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

    async getProof(blockIndex) {
        blockIndex = parseInt(blockIndex);
        if (isNaN(blockIndex) || blockIndex < 1) return null;

        let epoch = Math.ceil(blockIndex / this.epochSize);

        let epochRows = await this.db.doQuery(
            "SELECT * FROM merkle_epochs WHERE epoch = ?", [epoch]
        );
        if (epochRows.length === 0) return { error: 'epoch not yet committed' };

        let epochData = epochRows[0];

        let rows = await this.db.doQuery(
            `SELECT block_index, ledger_hash, actions_hash, contract_hash
             FROM sync_meta
             WHERE block_index >= ? AND block_index <= ?
             ORDER BY block_index ASC`,
            [epochData.start_block, epochData.end_block]
        );

        if (rows.length === 0) return null;

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

    // Highest block recorded in the transparency log. This is the durable high-water
    // mark for how far this node has actually recorded and broadcast, used to resume
    // the poll cursor across restarts. Distinct from the source DB tip (db.getLastBlock):
    // resuming from the source tip instead would skip every block the co-located
    // indexer advanced while the sync server was down, leaving a permanent hole in
    // sync_meta. Returns null when sync_meta is empty (fresh node, nothing recorded).
    async getHighWaterMark() {
        let rows = await this.db.doQuery("SELECT MAX(block_index) AS tip FROM sync_meta");
        if (rows.length > 0 && rows[0].tip !== null)
            return Number(rows[0].tip);
        return null;
    }

    // Find interior gaps in the transparency log: source blocks that fall strictly
    // between the log's own lowest and highest recorded block but were never
    // recorded. These are the permanent holes left by the pre-fix restart behaviour
    // (resuming the poll cursor from the live source tip skipped every block
    // advanced during downtime). Pre-history below the first recorded block is
    // intentionally excluded; the transparency log only ever covers blocks witnessed
    // since the sync server first ran, so those are not gaps. Returns an ascending
    // block_index list; empty on a healthy log (the common case, so the bounded
    // anti-join is cheap on the indexed block_index column).
    async findGaps() {
        let bounds = await this.db.doQuery(
            "SELECT MIN(block_index) AS lo, MAX(block_index) AS hi FROM sync_meta"
        );
        if (bounds.length === 0 || bounds[0].lo === null) return [];
        let lo = Number(bounds[0].lo);
        let hi = Number(bounds[0].hi);
        if (hi - lo < 2) return [];  // no room for an interior hole

        let rows = await this.db.doQuery(
            `SELECT b.block_index AS block_index
             FROM blocks b
             LEFT JOIN sync_meta s ON s.block_index = b.block_index
             WHERE b.block_index > ? AND b.block_index < ? AND s.block_index IS NULL
             ORDER BY b.block_index ASC`,
            [lo, hi]
        );
        return rows.map(r => Number(r.block_index));
    }

    // Re-commit an epoch from scratch: drop any existing committed root and rebuild
    // it from the (now gap-filled) sync_meta rows. Used by transparency backfill to
    // repair an epoch that was committed with a partial Merkle tree while a downtime
    // hole was open in its range. commitEpoch returns early when the epoch range has
    // no rows, so this is safe to call for any epoch.
    //
    // highWaterMark guards against committing a partial root for an epoch that is
    // still in progress: if the epoch's endBlock has not yet been reached by the
    // log, the full block set is not available and the resulting root would be
    // incomplete. The real boundary commit (via recordBlock) will run when the
    // boundary block arrives; re-committing now would lock in a partial root that
    // the existing-row guard in commitEpoch would then prevent from being corrected.
    async recommitEpoch(epoch, highWaterMark) {
        let endBlock = epoch * this.epochSize;
        if(highWaterMark !== undefined && highWaterMark !== null && endBlock > highWaterMark)
            return;  // epoch not yet complete; let recordBlock commit it at the boundary
        await this.db.doQuery("DELETE FROM merkle_epochs WHERE epoch = ?", [epoch]);
        await this.commitEpoch(epoch);
    }

    async getLatestRoot() {
        let rows = await this.db.doQuery(
            "SELECT * FROM merkle_epochs ORDER BY epoch DESC LIMIT 1"
        );
        return rows.length > 0 ? rows[0] : null;
    }

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
