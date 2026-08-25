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
 * XChain Sync - Recovery-redriven validator-reward delivery (source side, forward)
 *
 * On a reorg to block B, the indexer's rollback re-arms recovery_pending_rewards and
 * re-drains each surviving staged reward into validator_rewards stamping block_index =
 * the archived EARN block E (verbatim, so COLLECT stays consensus-correct). A survivor
 * with E < B yields a validator_rewards row whose block_index sits BELOW the post-reorg
 * replication window, so none of the forward channels reach it on their own:
 *   - the per-block stream selects validator_rewards by block_index = the streamed block
 *     (forward from B), and E < B is never streamed;
 *   - the incremental snapshot scopes validator_rewards by block_index >= sinceBlock, and
 *     the backdated row sits below the cursor.
 * A follower therefore stays permanently short by every reorg-redriven survivor (a silent
 * record-table row-count divergence; surfaced only by /status table_counts).
 *
 * This selects those rows by their materialization block (recovery_pending_rewards.
 * applied_block, set to B by the re-drain) so they can be merged into the normal
 * validator_rewards payload, keeping their block_index = E. It is the forward analogue of
 * the cooldown-maturity collector (cooldownCredits.js). No reverse-delete twin is needed:
 * validator_rewards rows carry block_index = E, so ClientRollback's existing block_index
 * >= B delete already reverses an injected row correctly (deleted iff E is orphaned).
 * The follower applies via the normal validator_rewards path (INSERT IGNORE on the UNIQUE
 * key, so re-injection across the live + snapshot channels is idempotent). Indexer dbType only.
 *
 ********************************************************************/

// Selects validator_rewards rows re-materialized (recovery_pending_rewards.applied_block)
// inside the inclusive window [fromBlock, toBlock] whose own block_index (earn-block E)
// is BELOW that window, so the normal block-scoped channels missed them. Returns raw
// rows for the caller to merge into the `validator_rewards` array, deduped on the UNIQUE
// identity (source_id, signing_pubkey_id, reward_type, round_reference). `db` must be an
// indexer-dbType Database (callers gate that), and passing `conn` lets a snapshot's
// REPEATABLE READ view read these at the same height as the rest of its payload.
async function collectRedrivenValidatorRewards(db, fromBlock, toBlock, conn){
    let from = Number(fromBlock);
    let to   = Number(toBlock);

    // The same survivor can be reached by both the live per-block channel and the
    // incremental snapshot when their windows overlap. The follower's INSERT IGNORE
    // already makes a duplicate a no-op; dedup here anyway to keep the payload minimal.
    let acc = new Map();
    try {
        // applied_block (= the reorg point B) is the forward-window key, mirroring
        // ClientRollback's block_index >= B reverse delete, and the vr.block_index <
        // applied_block guard restricts this to genuine survivors (E < B): a row whose
        // earn-block is at or above the window already streams via getBlockScopedRows and
        // must NOT ride this channel too.
        //
        // SELECT vr.* rather than a column subset is load-bearing. These rows are merged
        // into the SAME payload array as the block-scoped rows (which use SELECT *), and
        // ClientApplier._insertRows derives its INSERT column list from rows[0] only. A
        // narrower projection drops `id`, the AUTO_INCREMENT PK, so a redriven-only or
        // mixed batch would let the replica mint its own diverging id, invisible to the
        // count-only parity check and later swallowed by INSERT IGNORE on PK collision.
        let rows = await db.doQuery(
            "SELECT vr.* " +
            "FROM validator_rewards vr " +
            "JOIN recovery_pending_rewards rpr " +
            "     ON rpr.source_id = vr.source_id AND rpr.reward_type = vr.reward_type " +
            "    AND rpr.round_reference <=> vr.round_reference " +
            "    AND rpr.amount = vr.amount AND rpr.block_index = vr.block_index " +
            "JOIN index_pubkeys ip ON ip.id = vr.signing_pubkey_id AND ip.pubkey = rpr.validator_pubkey " +
            "WHERE rpr.applied = 1 AND rpr.applied_block IS NOT NULL " +
            "  AND rpr.applied_block BETWEEN ? AND ? " +
            "  AND vr.block_index < rpr.applied_block",
            [from, to], conn);
        for(let r of (rows || [])){
            if(r && r.source_id != null && r.signing_pubkey_id != null)
                acc.set(r.source_id + ':' + r.signing_pubkey_id + ':' + r.reward_type + ':' + r.round_reference, r);
        }
    } catch(e){
        // recovery_pending_rewards / applied_block may not exist on a non-recovery stack
        // or an older source schema; skip silently ONLY on a genuine schema gap (1146
        // missing table / 1054 unknown column). Anything else is transient/operational
        // and must surface: both callers wrap this in the isSchemaGapError gate that
        // freezes the block cursor / aborts the snapshot stream, and swallowing here
        // makes those gates dead code and ships the block short its backdated rows.
        // Mirrors derivedRewards.js on the same rail.
        if(!(e && typeof e.errno === 'number' && (e.errno === 1146 || e.errno === 1054))) throw e;
    }

    return Array.from(acc.values());
}

module.exports = { collectRedrivenValidatorRewards };
