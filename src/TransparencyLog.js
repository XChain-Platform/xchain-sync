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
 * Append-only per-block hash log stored in the sync_meta table.
 * Records the three block hashes (ledger, actions, contracts) for
 * each block, providing a publicly auditable integrity trail.
 *
 ********************************************************************/

class TransparencyLog {

    constructor(db) {
        this.db = db;
    }

    // Record a block's hashes in the transparency log
    async recordBlock(block_index, block_time, ledger_hash, actions_hash, contract_hash){
        let query = `INSERT IGNORE INTO sync_meta
            (block_index, block_time, ledger_hash, actions_hash, contract_hash)
            VALUES (?, ?, ?, ?, ?)`;
        await this.db.doQuery(query, [block_index, block_time, ledger_hash, actions_hash, contract_hash]);
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
