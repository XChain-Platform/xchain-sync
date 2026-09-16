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
 * The sync_meta table, where the source transparency log records the three
 * block hashes of every block it broadcasts, and the replica's durable
 * sync_state key/value markers kept beside it.
 *
 * A Database mixin: every method below is installed on Database.prototype by
 * db/index.js, so `this` is the Database instance and call sites are unchanged.
 *
 ********************************************************************/



const util = require('node:util');
const { getLogger } = require('../observability');
const logger = getLogger();
module.exports = {

    // --- Durable client key/value markers (sync_state) ------------------------
    // Small durable store for per-client state that must survive a restart but is
    // not block data (mirrors the sync_halt durable-state pattern). Created on
    // demand so no schema file / migration is needed on existing replicas. Keys are
    // namespaced by the caller (e.g. 'bootstrap_base:indexer'). Used by ClientSync
    // to persist the truncated-replica join floor (_bootstrapBase): an in-memory-only
    // field is lost on restart, dropping the join-block recompute skip and the
    // truncation floor that protects against an in-window reorg below `base`.
    async ensureSyncStateTable(){
        if(this._syncStateReady) return;
        await this.doQuery(
            "CREATE TABLE IF NOT EXISTS sync_state (" +
            "  state_key   VARCHAR(128) NOT NULL PRIMARY KEY," +
            "  state_value TEXT," +
            "  updated_at  DATETIME DEFAULT CURRENT_TIMESTAMP" +
            ") ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci"
        );
        this._syncStateReady = true;
    },

    // Read a durable client marker. Returns the stored string, or null if absent
    // (or if the table cannot be reached: fail-soft, the caller treats null as
    // "no persisted value" and falls back to its in-memory default).
    async getSyncState(key){
        try {
            await this.ensureSyncStateTable();
            let rows = await this.doQuery("SELECT state_value FROM sync_state WHERE state_key=? LIMIT 1", [key]);
            return (rows && rows.length) ? rows[0].state_value : null;
        } catch(e){
            logger.error(util.format('getSyncState(' + key + ') failed (treating as unset):', e));
            return null;
        }
    },

    // Write a durable client marker (upsert). Fail-soft: a persistence failure is
    // logged, never thrown, so it cannot abort a bootstrap/catch-up.
    async setSyncState(key, value){
        try {
            await this.ensureSyncStateTable();
            await this.doQuery(
                "INSERT INTO sync_state (state_key, state_value) VALUES (?, ?) " +
                "ON DUPLICATE KEY UPDATE state_value=VALUES(state_value), updated_at=CURRENT_TIMESTAMP",
                [key, value == null ? null : String(value)]
            );
            return true;
        } catch(e){
            logger.error(util.format('setSyncState(' + key + ') failed (continuing):', e));
            return false;
        }
    },

    // Delete a durable client marker. Fail-soft: a persistence failure is logged,
    // never thrown, so it cannot abort a bootstrap/catch-up.
    async deleteSyncState(key){
        try {
            await this.ensureSyncStateTable();
            await this.doQuery("DELETE FROM sync_state WHERE state_key=?", [key]);
            return true;
        } catch(e){
            logger.error(util.format('deleteSyncState(' + key + ') failed (continuing):', e));
            return false;
        }
    },

    // --- Transparency log leaves (sync_meta) ---------------------------------
    // The source transparency log's per-block rows. Plain doQuery throughout: the
    // log runs from the poll loop, where a transaction would contend with other
    // writers, and each step is idempotent on retry.

    // Record one block's three hashes. INSERT IGNORE on the UNIQUE block_index,
    // so a block already recorded keeps the hashes it was broadcast with.
    async recordSyncMetaHashes(block_index, block_time, ledger_hash, actions_hash, contract_hash){
        let query = `INSERT IGNORE INTO sync_meta
            (block_index, block_time, ledger_hash, actions_hash, contract_hash)
            VALUES (?, ?, ?, ?, ?)`;
        return await this.doQuery(query, [block_index, block_time, ledger_hash, actions_hash, contract_hash]);
    },

    // Delete every recorded block at or below a committed epoch boundary.
    async deleteSyncMetaThrough(boundary){
        return await this.doQuery(
            "DELETE FROM sync_meta WHERE block_index <= ?", [boundary]
        );
    },

    // The Merkle leaves of one block range, in block order.
    async findSyncMetaLeaves(startBlock, endBlock){
        return await this.doQuery(
            `SELECT block_index, ledger_hash, actions_hash, contract_hash
             FROM sync_meta
             WHERE block_index >= ? AND block_index <= ?
             ORDER BY block_index ASC`,
            [startBlock, endBlock]
        );
    },

    // Delete every recorded block at or above an orphaned height.
    async deleteSyncMetaFrom(block_index){
        return await this.doQuery("DELETE FROM sync_meta WHERE block_index >= ?", [block_index]);
    },

    // The highest recorded block, one row carrying `tip`.
    async getSyncMetaTip(){
        return await this.doQuery("SELECT MAX(block_index) AS tip FROM sync_meta");
    },

    // The ledger hash recorded for one height, at most one row.
    async getRecordedLedgerHash(height){
        return await this.doQuery(
            "SELECT ledger_hash FROM sync_meta WHERE block_index=? LIMIT 1", [height]
        );
    },

    // The lowest and highest recorded block, one row carrying `lo` and `hi`.
    async getSyncMetaBounds(){
        return await this.doQuery(
            "SELECT MIN(block_index) AS lo, MAX(block_index) AS hi FROM sync_meta"
        );
    },

    // Source blocks strictly between two heights that have no recorded row.
    async findUnrecordedBlocksBetween(lo, hi){
        return await this.doQuery(
            `SELECT b.block_index AS block_index
             FROM blocks b
             LEFT JOIN sync_meta s ON s.block_index = b.block_index
             WHERE b.block_index > ? AND b.block_index < ? AND s.block_index IS NULL
             ORDER BY b.block_index ASC`,
            [lo, hi]
        );
    },

    // How many blocks are recorded, one row carrying `total`.
    async countSyncMetaRows(){
        return await this.doQuery("SELECT COUNT(*) as total FROM sync_meta");
    },

    // One page of recorded blocks, newest first.
    async findSyncMetaPage(limit, offset){
        let query = `SELECT block_index, block_time, ledger_hash, actions_hash, contract_hash, logged_at
            FROM sync_meta
            ORDER BY block_index DESC
            LIMIT ? OFFSET ?`;
        return await this.doQuery(query, [limit, offset]);
    },

};
