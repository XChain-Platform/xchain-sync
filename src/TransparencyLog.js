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

    // readOnly: this log is a replica of an upstream server's log, maintained by
    // something outside this process (MariaDB binlog replication on the web tier).
    // Every write entry point below becomes a no-op; the rows arrive already
    // recorded, the box is read_only=1, and re-writing them here would either fail
    // outright or fork the log from its source. Read paths (proofs, pages, roots,
    // high-water mark, gap scan) are untouched, so the tier still serves
    // /transparency exactly like the origin does.
    // retentionBlocks: OPT-IN sync_meta retention window, default OFF (0). See
    // pruneSyncMeta for what a positive value gives up. A non-numeric or negative
    // value is treated as off, never as "prune everything".
    constructor(db, epochSize, readOnly, retentionBlocks) {
        this.db        = db;
        this.epochSize = epochSize || 100;
        this.readOnly  = readOnly === true;
        let keep = parseInt(retentionBlocks, 10);
        this.retentionBlocks = (Number.isFinite(keep) && keep > 0) ? keep : 0;
    }

    // Crossing an epoch boundary commits that epoch's Merkle root as a side effect.
    async recordBlock(block_index, block_time, ledger_hash, actions_hash, contract_hash){
        if(this.readOnly) return;
        let query = `INSERT IGNORE INTO sync_meta
            (block_index, block_time, ledger_hash, actions_hash, contract_hash)
            VALUES (?, ?, ?, ?, ?)`;
        await this.db.doQuery(query, [block_index, block_time, ledger_hash, actions_hash, contract_hash]);

        if (block_index > 0 && block_index % this.epochSize === 0) {
            let epoch = Math.floor(block_index / this.epochSize);
            await this.commitEpoch(epoch).catch(e =>
                console.error('Error committing Merkle epoch ' + epoch + ':', e)
            );
            // Retention runs at epoch boundaries only (once per epochSize blocks) and
            // AFTER the boundary commit, so the epoch that just closed is committed
            // before anything is considered for pruning. Inert unless the operator set
            // a window. A retention failure must never stall the poll loop: the sweep
            // is idempotent and the next boundary retries it.
            if (this.retentionBlocks > 0) {
                await this.pruneSyncMeta().catch(e =>
                    console.error('Error pruning sync_meta at epoch ' + epoch + ':', e)
                );
            }
        }
    }

    // OPT-IN retention for the per-block sync_meta rows. DEFAULT OFF: with
    // SYNC_META_RETENTION_BLOCKS unset (or 0) this is a no-op and the log keeps full
    // history, which stays the shipped behaviour. Mirrors the indexer's
    // retention.pruneStateRoots (xchain-indexer/src/retention.js): a positive window
    // means "keep the last N blocks", a zero/absent window means "keep everything".
    //
    // WHAT IS GIVEN UP: sync_meta holds the Merkle LEAVES, so a pruned block can no
    // longer be served as an inclusion proof (getProof needs every leaf of the epoch
    // to rebuild the tree). merkle_epochs is deliberately NOT pruned: the committed
    // roots are tiny, one row per epochSize blocks, and keeping them preserves the
    // published root chain (and the merkle_reorgs audit trail that references it)
    // even for ranges whose leaves are gone.
    //
    // EPOCH-ALIGNED, COMMITTED-ONLY: the delete boundary is the end_block of a
    // COMMITTED epoch, never an arbitrary height. Deleting part of an epoch would be
    // worse than useless: getProof would rebuild that epoch's tree from the surviving
    // subset and hand out a proof against a root that no longer matches the committed
    // one (verified:false at best, a silently wrong path at worst). Aligning to a
    // committed epoch boundary also guarantees no epoch is ever left half-pruned,
    // because epoch ranges never straddle a boundary. The straddle guard below is the
    // belt-and-braces check for a log whose MERKLE_EPOCH_SIZE was changed mid-life.
    //
    // Plain doQuery, no transaction, like pruneFrom: this runs from the poll loop and
    // a transaction here would contend with other writers. The delete is an idempotent
    // range-delete, so a fault just retries at the next epoch boundary.
    async pruneSyncMeta(retentionBlocks) {
        let keep = (retentionBlocks === undefined || retentionBlocks === null)
            ? this.retentionBlocks
            : parseInt(retentionBlocks, 10);
        // No-op at zero (and at anything unparseable or negative): retention is off.
        if (!Number.isFinite(keep) || keep <= 0) return { enabled: false, deleted: 0 };
        // A replicated log prunes at the source; the DELETEs arrive over replication.
        if (this.readOnly) return { enabled: true, skipped: true, reason: 'read_only', deleted: 0 };

        let tip = await this.getHighWaterMark();
        if (tip === null) return { enabled: true, skipped: false, deleted: 0, tip: null, cutoff: null };

        // Blocks at or below cutoff are outside the retention window.
        let cutoff = tip - keep;
        if (cutoff < 1) return { enabled: true, skipped: false, deleted: 0, tip, cutoff: null };

        // Highest committed epoch that lies wholly outside the window. Its end_block is
        // the only safe delete boundary; if no epoch qualifies there is nothing to do.
        let boundRows = await this.db.doQuery(
            "SELECT MAX(end_block) AS eb FROM merkle_epochs WHERE end_block <= ?", [cutoff]
        );
        let boundary = (boundRows.length > 0 && boundRows[0].eb !== null && boundRows[0].eb !== undefined)
            ? Number(boundRows[0].eb) : null;
        if (boundary === null || boundary < 1)
            return { enabled: true, skipped: false, deleted: 0, tip, cutoff, prunedThrough: null };

        // Refuse to cut through a committed epoch. Impossible with a stable
        // MERKLE_EPOCH_SIZE (ranges are disjoint and aligned), reachable only if the
        // epoch size changed after epochs were already committed; pruning then would
        // leave a committed epoch with a partial leaf set and unprovable proofs.
        let straddling = await this.db.doQuery(
            "SELECT COUNT(*) AS c FROM merkle_epochs WHERE start_block <= ? AND end_block > ?",
            [boundary, boundary]
        );
        if (straddling.length > 0 && Number(straddling[0].c) > 0) {
            console.error('sync_meta retention skipped: a committed epoch straddles block ' + boundary +
                ' (MERKLE_EPOCH_SIZE likely changed after epochs were committed); refusing a partial prune');
            return { enabled: true, skipped: true, reason: 'straddling_epoch', deleted: 0, tip, cutoff };
        }

        let result = await this.db.doQuery(
            "DELETE FROM sync_meta WHERE block_index <= ?", [boundary]
        );
        let deleted = (result && result.affectedRows) ? Number(result.affectedRows) : 0;
        if (deleted > 0)
            console.log('sync_meta retention: pruned ' + deleted + ' row(s) at or below block ' + boundary +
                ' (tip ' + tip + ', window ' + keep + ' blocks); inclusion proofs below that block are no longer serveable');
        return { enabled: true, skipped: false, deleted, tip, cutoff, prunedThrough: boundary };
    }

    async commitEpoch(epoch) {
        if(this.readOnly) return;
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

    // Prunes the log on a server-side reorg to `block_index` (the new canonical tip + 1,
    // so every block at or above it was orphaned). sync_meta and merkle_epochs are
    // sync-service-owned, and nothing else rolls them back: the indexer rolls back its
    // data tables, not these. Since recordBlock writes sync_meta with INSERT IGNORE on a
    // UNIQUE block_index, skipping this prune would leave re-added blocks carrying their
    // pre-reorg hashes and the node serving wrong Merkle proofs. This is the source-side
    // mirror of the client's sync_meta prune in ClientRollback.
    //
    // The policy is to track the canonical chain: capture every invalidated committed
    // epoch as an append-only audit marker (old_root), delete those epochs so they
    // re-commit from the canonical chain when the boundary is re-crossed (commitEpoch
    // backfills new_root), and delete the orphaned sync_meta rows so recordBlock
    // re-inserts fresh hashes.
    //
    // Deliberately plain doQuery, no transaction, like the rest of the poll loop: a
    // transaction here would drive the shared db.transactionConnection and contend with
    // other writers. Instead the steps are ordered and individually idempotent (the
    // marker insert is duplicate-guarded, both DELETEs are range-deletes that no-op once
    // the rows are gone), so a partial failure self-heals on the next poll, which the
    // reorg branch guarantees by leaving lastPolledBlock un-rewound until this completes.
    // A DB fault propagates so the poll loop retries the whole prune.
    async pruneFrom(block_index){
        // A replicated log prunes at the source; the DELETEs arrive over replication.
        if(this.readOnly) return;
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

        // No leaves on hand for a committed epoch. Normal for an epoch whose rows were
        // dropped by the retention window (pruneSyncMeta), where the honest answer is
        // "not available here"; the API turns this into a 404.
        if (rows.length === 0) return null;

        // A committed epoch whose surviving leaf count disagrees with the count the root
        // was built from cannot produce a valid proof: the rebuilt tree hashes to a
        // different root. Retention only ever deletes whole epochs, so this fires on a
        // hand-run DELETE or a mid-life MERKLE_EPOCH_SIZE change, and refusing beats
        // handing out a proof that does not verify. leaf_count is absent on very old
        // rows, in which case the check is skipped.
        if (epochData.leaf_count !== undefined && epochData.leaf_count !== null &&
            rows.length !== Number(epochData.leaf_count))
            return { error: 'epoch leaves incomplete' };

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

    // Return the DURABLE recorded ledger_hash for a height (the value this node
    // broadcast when it recorded the block), or null if the block was never
    // recorded. Used by ServerPoller to seed its net-forward reorg guard from the
    // pre-reorg recorded hash rather than a fresh (post-reorg) source read, so a
    // reorg that completed entirely during downtime is still detected on the first
    // poll after restart. Indexer-only (the decoder has no transparency log).
    async getRecordedHash(height) {
        let rows = await this.db.doQuery(
            "SELECT ledger_hash FROM sync_meta WHERE block_index=? LIMIT 1", [height]
        );
        if (rows.length > 0 && rows[0].ledger_hash !== null && rows[0].ledger_hash !== undefined)
            return rows[0].ledger_hash;
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
        if(this.readOnly) return;
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
