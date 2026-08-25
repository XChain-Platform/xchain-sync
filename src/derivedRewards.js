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
 * XChain Sync - Derived anchor/archive validator-reward delivery (source side, forward)
 *
 * The BTC-side anchor/archive derivation (xchain-indexer/src/anchor_reward_derive.js)
 * writes a validator_rewards row while processing BTC block B but stamps block_index =
 * the checkpoint's SNAPSHOT_BLOCK E (the earn-block COLLECT reads), with E <= B minus
 * the mirror-maturity watermark, and derive_block_index = B (the MATERIALIZATION block).
 * None of the block-keyed forward channels reach that row on their own:
 *   - the per-block stream selects validator_rewards by block_index = the streamed block
 *     (forward from B), and E < B is never streamed again;
 *   - the incremental snapshot scopes validator_rewards by block_index >= sinceBlock, so
 *     the row rides it only when a follower's gap already spans back to E.
 * A continuously-live follower therefore never received a derived anchor/archive reward
 * (a silent record-table divergence: validator_rewards is in no consensus hash).
 *
 * This selects those rows by their materialization block (derive_block_index) so they can
 * be merged into the normal validator_rewards payload, keeping their block_index = E. It is
 * the forward twin of ClientRollback's `DELETE FROM validator_rewards WHERE
 * derive_block_index >= B` reverse delete (the reverse path already knew this key; the
 * forward path did not), and the exact analogue of the recovery-redriven collector
 * (recoveryRewards.js, keyed on recovery_pending_rewards.applied_block). The follower
 * applies via the normal validator_rewards path (INSERT IGNORE on the UNIQUE key, so
 * re-injection across the live + snapshot channels is idempotent). Indexer dbType only.
 *
 * Coverage boundary: only writers that stamp derive_block_index ride this channel. The
 * pre-flag-day writers (actions/anchor.js at SNAPSHOT_BLOCK, the pushvalidatorrewards hub
 * push) backdate block_index and leave derive_block_index NULL; their rows remain
 * reachable by a snapshot whose window spans the earn-block only, and their shortfall
 * surfaces through ClientSync._verifyTableCounts.
 *
 ********************************************************************/

// Selects validator_rewards rows MATERIALIZED (derive_block_index) inside the inclusive
// window [fromBlock, toBlock] whose own block_index (earn-block E) is BELOW their
// materialization block, so the block-keyed channels missed them. Returns raw rows for
// the caller to merge into the `validator_rewards` array, deduped on the UNIQUE identity
// (source_id, signing_pubkey_id, reward_type, round_reference, round_qualifier). `db` must be an
// indexer-dbType Database (callers gate that), and passing `conn` lets a snapshot's
// REPEATABLE READ view read these at the same height as the rest of its payload.
async function collectDerivedAnchorRewards(db, fromBlock, toBlock, conn){
    let from = Number(fromBlock);
    let to   = Number(toBlock);

    // The same row can be reached by both the live per-block channel and the incremental
    // snapshot when their windows overlap. The follower's INSERT IGNORE already makes a
    // duplicate a no-op; dedup here anyway to keep the payload minimal.
    let acc = new Map();
    try {
        // derive_block_index (= the BTC block that minted the row) is the forward-window
        // key, mirroring ClientRollback's derive_block_index >= B reverse delete, and the
        // vr.block_index < vr.derive_block_index guard restricts this to genuinely
        // backdated rows: a row whose earn-block is its own materialization block already
        // streams via getBlockScopedRows and must NOT ride this channel too.
        //
        // SELECT vr.* rather than a column subset is load-bearing. These rows are merged
        // into the SAME payload array as the block-scoped rows (which use SELECT *), and
        // ClientApplier._insertRows derives its INSERT column list from rows[0] only. A
        // narrower projection drops `id`, the AUTO_INCREMENT PK, so a derived-only or
        // mixed batch would let the replica mint its own diverging id, invisible to the
        // count-only parity check and later swallowed by INSERT IGNORE on PK collision.
        let rows = await db.doQuery(
            "SELECT vr.* " +
            "FROM validator_rewards vr " +
            "WHERE vr.derive_block_index BETWEEN ? AND ? " +
            "  AND vr.block_index < vr.derive_block_index",
            [from, to], conn);
        for(let r of (rows || [])){
            if(r && r.source_id != null && r.signing_pubkey_id != null)
                // Keyed on the reward's FULL UNIQUE identity, round_qualifier included. This is
                // the derived-anchor channel, so it is exactly where two archive rewards that
                // share a reissued MATCH_BATCH_SEQ and differ only in snapshot_block arrive;
                // without the qualifier this dedup would drop one of them from the payload and
                // the follower would never receive it.
                acc.set(r.source_id + ':' + r.signing_pubkey_id + ':' + r.reward_type + ':' +
                        r.round_reference + ':' + r.round_qualifier, r);
        }
    } catch(e){
        // derive_block_index may not exist on an older source schema (pre-RB-ANCHOR);
        // such a source has derived nothing either, so skip silently ONLY on a genuine
        // schema gap. A transient fault must surface so the caller retries the block
        // rather than broadcasting it short (mirrors ServerPoller's isSchemaGapError gate).
        if(!(e && typeof e.errno === 'number' && (e.errno === 1146 || e.errno === 1054))) throw e;
    }

    return Array.from(acc.values());
}

module.exports = { collectDerivedAnchorRewards };
