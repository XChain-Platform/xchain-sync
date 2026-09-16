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
 * The sync_halt table: the durable record of a replica that stopped because a
 * hash it recomputed did not match the one it was served.
 *
 * A Database mixin: every method below is installed on Database.prototype by
 * db/index.js, so `this` is the Database instance and call sites are unchanged.
 *
 ********************************************************************/



module.exports = {

    async recordHalt(dbType, blockIndex, reason, mismatches, sources){
        // Idempotent: don't stack duplicate active halts for the same block. Tolerate a
        // transient read failure in this pre-check by falling through to the INSERT -
        // recording the halt IS the durability guarantee, so it must never be skipped
        // on a read blip (getActiveHalt now fails closed / throws).
        let existing = null;
        try { existing = await this.getActiveHalt(dbType); } catch(e){ existing = null; }
        if(existing && Number(existing.block_index) === Number(blockIndex)) return existing;
        await this.doQuery(
            "INSERT INTO sync_halt (db_type, block_index, reason, mismatches, sources) VALUES (?, ?, ?, ?, ?)",
            [dbType, blockIndex, String(reason || 'divergence').slice(0, 64),
             JSON.stringify(mismatches || []), JSON.stringify(sources || [])]
        );
        try { return await this.getActiveHalt(dbType); } catch(e){ return null; }
    },

    // Fail CLOSED: pass rethrow so a transient query error PROPAGATES instead of
    // returning [] (which the caller would read as "no active halt" and silently
    // resume a possibly-halted replica onto a contested chain). Callers that must
    // tolerate a read failure (recordHalt's pre-check) catch it explicitly.
    async getActiveHalt(dbType){
        let rows = await this.doQuery(
            "SELECT * FROM sync_halt WHERE db_type=? AND cleared_at IS NULL ORDER BY id DESC LIMIT 1",
            [dbType], null, { rethrow: true }
        );
        return (rows && rows.length) ? rows[0] : null;
    },

    async clearHalt(dbType){
        let res = await this.doQuery(
            "UPDATE sync_halt SET cleared_at=NOW() WHERE db_type=? AND cleared_at IS NULL",
            [dbType]
        );
        return res ? res.affectedRows : 0;
    },

};
