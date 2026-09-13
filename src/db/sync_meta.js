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
 * The sync_meta table: the replica's own key/value cursor store, read and
 * written on every applied block.
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
    async _ensureSyncStateTable(){
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
            await this._ensureSyncStateTable();
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
            await this._ensureSyncStateTable();
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
            await this._ensureSyncStateTable();
            await this.doQuery("DELETE FROM sync_state WHERE state_key=?", [key]);
            return true;
        } catch(e){
            logger.error(util.format('deleteSyncState(' + key + ') failed (continuing):', e));
            return false;
        }
    },

};
