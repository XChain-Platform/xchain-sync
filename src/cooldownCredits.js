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
 * XChain Sync - Cooldown-maturity refund credits (source side, forward)
 *
 * When a capability/contract UNSTAKE cooldown matures, the indexer's
 * processCooldownCompletions writes a refund credit that REUSES the unstake's
 * own (earlier-block) action_index and carries NO block_index, and flips the
 * surviving unstake row's status_id to 'completed' in place at the maturity
 * block (= cooldown_end_block under sequential processing).
 *
 * Because the credit's only block placement is the unstake's original (much
 * earlier) block, none of the three forward channels reach it on their own:
 *   - the per-block stream's action-scoped join selects credits by the action's
 *     block, so it misses the credit at the maturity block (wrong block) and at
 *     the original block (the credit did not exist yet);
 *   - the updated_rows channel is keyed by a UNIQUE action_index and applied
 *     with ON DUPLICATE KEY UPDATE — credits has no unique key, so it cannot
 *     ride that channel;
 *   - the incremental snapshot scopes credits by action_index >= cursor, and
 *     the backdated credit sits below the cursor.
 * A follower (which does not run processCooldownCompletions) therefore stays
 * permanently short by every matured refund — steady-state, hash-blind balance
 * divergence (the credit is in no per-block ledger hash at the maturity height).
 *
 * This selects those credits by MATURITY block (cooldown_end_block) so they can
 * be merged into the normal `credits` table payload — the exact FORWARD mirror
 * of ClientRollback's reverse delete (same join keys, same cooldown_end_block /
 * status='completed' predicate, GAS tick for the capability refund). They then
 * flow through the existing credits apply path (_insertRows + balance rebuild),
 * which is idempotent at the block/window boundary exactly as the normal
 * action-scoped credits are. Indexer dbType only.
 *
 ********************************************************************/

const { gasTickSymbol } = require('./consensus-constants');

// Select the matured cooldown-refund credit rows whose maturity block
// (cooldown_end_block) falls in the inclusive window [fromBlock, toBlock].
// Returns raw `credits` rows (action_index, address_id, tick_id, amount),
// deduped by their logical identity (action_index, address_id, tick_id) — the
// credits table has no unique key, so the dedup is explicit. The caller merges
// these into the block / snapshot `credits` array (and dedups the union).
//
//   db        the source Database (indexer dbType only — callers must gate)
//   fromBlock inclusive lower maturity-block bound
//   toBlock   inclusive upper maturity-block bound (for a single live block,
//             pass fromBlock === toBlock)
//   conn      optional connection (so a snapshot's REPEATABLE READ view reads
//             these at the same height as the rest of the payload)
async function collectMaturedCooldownCredits(db, fromBlock, toBlock, conn){
    let from = Number(fromBlock);
    let to   = Number(toBlock);

    let completedStatusId = await db.getStatusId('completed');
    if(completedStatusId === null || completedStatusId === undefined) return [];

    // Dedup by the credit's logical identity (credits has no unique key). An
    // unstake created AND matured inside the same incremental window can be
    // reached both here and by the caller's action-scoped selection; the caller
    // dedups the union on the same triple.
    let acc = new Map();
    function add(rows){
        for(let r of (rows || [])){
            if(r && r.action_index != null && r.address_id != null && r.tick_id != null)
                acc.set(r.action_index + ':' + r.address_id + ':' + r.tick_id, r);
        }
    }

    // Capability maturity refund — paid in GAS, keyed by the unstake's
    // action_index. Forward mirror of ClientRollback's capability reverse delete
    // (same join keys + cooldown_end_block/status predicate); the GAS tick is the
    // frozen consensus constant, never a hub poll.
    let gasTick = gasTickSymbol();
    if(gasTick){
        try {
            let rows = await db.doQuery(
                "SELECT c.* FROM credits c " +
                "JOIN unstakes u ON u.action_index = c.action_index AND u.source_id = c.address_id " +
                "JOIN index_tickers g ON g.id = c.tick_id AND g.tick = ? " +
                "WHERE u.status_id = ? AND u.cooldown_end_block BETWEEN ? AND ?",
                [gasTick, completedStatusId, from, to], conn);
            add(rows);
        } catch(e){
            // Table/column may not exist on older source schemas — skip.
        }
    }

    // Contract maturity refund — paid in the unstake's own tick. Forward mirror
    // of ClientRollback's contract reverse delete.
    try {
        let rows = await db.doQuery(
            "SELECT c.* FROM credits c " +
            "JOIN contract_unstakes cu ON cu.action_index = c.action_index AND cu.source_id = c.address_id AND cu.tick_id = c.tick_id " +
            "WHERE cu.status_id = ? AND cu.cooldown_end_block BETWEEN ? AND ?",
            [completedStatusId, from, to], conn);
        add(rows);
    } catch(e){
        // Table may not exist on older source schemas — skip.
    }

    return Array.from(acc.values());
}

module.exports = { collectMaturedCooldownCredits };
