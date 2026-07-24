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
 * XChain Sync - Replicated Table Topology
 *
 * Single source of truth for the set of tables that replicate per block from a
 * source node to its followers, keyed by dbType. Two consumers read this:
 *
 *   - ServerPoller reads rows from these tables (scoped by block/tx/action
 *     index) to build the live block payloads it broadcasts.
 *   - The status/verification path counts rows in the same set to detect
 *     replica incompleteness (see api.buildStatusRow and
 *     ClientSync._verifyAgainstSource).
 *
 * Keeping both consumers on one definition means the row-count completeness
 * check can never silently drift from the set of tables that are actually
 * streamed: a table added to live sync automatically joins the check, and a
 * table removed from it automatically leaves.
 *
 * Scope note: this is deliberately the *per-block streamed* set. Tables that
 * converge through other channels (a full/incremental snapshot ride-along, or
 * no xchain-sync channel at all) are intentionally excluded, because their
 * counts legitimately diverge between nodes and comparing them would raise
 * false incompleteness alarms instead of catching real ones:
 *   - attest_validator_stats          running aggregate, full-snapshot only
 *   - markets                         derived OHLCV, full-snapshot only
 *   - mempool_transactions            non-deterministic across nodes
 *   - dispensers (decoder)            soft-expired (UPDATE expired_block_index) with
 *                                     hard-purge deferred to purgeExpiredDispensers;
 *                                     neither mutation rides the per-block stream, so it
 *                                     is NOT per-block-streamed. It IS listed in the
 *                                     decoder `special` bucket so it joins the /status
 *                                     completeness count: the source's live count drift
 *                                     (soft-expire UPDATEs + hard-purge DELETEs the
 *                                     bootstrap full dump cannot replay) now surfaces as
 *                                     a TABLE_COUNT_MISMATCH a complete replica can act
 *                                     on, instead of drifting silently. The apply-side
 *                                     reconcile has landed: ClientApplier.applyDispensersReplace
 *                                     via ClientSync._reconcileDispensers, gated by
 *                                     DISPENSERS_RECONCILE_EVERY / DISPENSERS_RECONCILE_MAX_INTERVAL_MS.
 *                                     The count signal is now a backstop for detecting
 *                                     residual drift between reconcile runs.
 *   - cross_chain_calls,              hub-mirrored (hub_db_sync), NOT produced by
 *     cross_chain_matches,            block processing. Pushed/retracted by the hub
 *     oracle_prices,                  (pushpricereorg / pushdexreorg) out-of-band with
 *     capability_snapshots,           block apply, so they cannot ride the per-block
 *     state_checkpoints               stream. They are NEVER replicated by xchain-sync:
 *                                     excluded from the per-block stream, the incremental
 *                                     catch-up, AND the full/incremental snapshots
 *                                     (SnapshotBuilder.OPERATOR_LOCAL_TABLES). On a source
 *                                     node they converge via hub_db_sync. The serving node
 *                                     does NOT serve these from any local replica copy:
 *                                     the explorer reads the consensus-relevant ones
 *                                     (state_checkpoints, capability_snapshots,
 *                                     cross_chain_matches) authoritatively from the
 *                                     MANDATORY co-located hub DB on the same server. There
 *                                     is no local-mirror fallback: a serving node without a
 *                                     co-located hub DB fails loud (explorer db.js
 *                                     _checkpointSource / _matchSource throw, and the
 *                                     explorer asserts the hub DB at startup) rather than
 *                                     silently serving stale/empty local rows (#4138).
 *   - icons                           replication: 'local' (OPERATOR_LOCAL): never
 *                                     leaves the node. NEVER replicated by xchain-sync:
 *                                     excluded from the per-block stream, the incremental
 *                                     catch-up, AND the full/incremental snapshots
 *                                     (SnapshotBuilder.OPERATOR_LOCAL_TABLES), not a
 *                                     snapshot ride-along.
 *   - price_snapshots                 replication: 'hub-mirror': carried by the hub
 *                                     (hub_db_sync), never by xchain-sync in any channel
 *                                     (stream, incremental catch-up, or snapshots), same
 *                                     posture as the cross_chain_* / oracle_prices group
 *                                     above.
 *
 ********************************************************************/

const lifecycle = require('./tableLifecycle');

// Per-block replicated table topology by dbType. ServerPoller consumes the
// structured form (it needs the per-scope split for its index joins); the
// verification path consumes the flattened union via getReplicatedTables().
//
// The INDEXER topology is generated from the table-lifecycle registry
// (src/tableLifecycle.js, byte-identical twin of the xchain-indexer copy):
// each indexer table's registry entry declares its stream scope, so adding a
// table there simultaneously adds it to the per-block stream, the /status
// completeness count, and both rollback sets. The DECODER topology stays
// declared literally below: that schema is owned by xchain-decoder and has
// its own, much smaller lifecycle (see the scope notes above).
const TOPOLOGY = {

    // Decoder schema: 9 tables, much smaller surface area than indexer.
    // mempool_transactions is intentionally excluded (non-deterministic across
    // nodes; see xchain-sync-decoder-db-decisions).
    decoder: {
        // Block-scoped tables (key off block_index directly)
        blockScoped:  ['blocks', 'transactions'],
        // Tx-scoped tables (key off tx_index -> transactions.block_index).
        // dispensers is deliberately NOT per-block-streamed: per-block replication
        // captures only rows *inserted* in a block (via the tx_index->block_index
        // join), but the decoder also soft-expires dispensers (UPDATE
        // expired_block_index) and defers the hard-purge to purgeExpiredDispensers.
        // Neither mutation rides the block stream, so streaming inserts alone would
        // let a follower's dispensers count drift away from the source. dispensers
        // is instead listed in `special` below so it joins the /status completeness
        // count, turning that previously-silent drift into a detectable mismatch.
        txScoped:     ['transaction_outputs'],
        // Decoder doesn't have action-scoped tables
        actionScoped: [],
        // Append-only lookup tables that may grow as new blocks are processed.
        // events is operational/logging; included so consumers see decoder activity.
        index:        ['index_addresses', 'index_transactions', 'pubkeys', 'events'],
        // Replicated-for-completeness-counting but NOT extracted by ServerPoller's
        // per-scope loops (ServerPoller reads only blockScoped/txScoped/actionScoped/
        // index). dispensers lives here so it enters the /status row-count
        // completeness check (getReplicatedTables) without being streamed per block:
        // it converges via full snapshot + per-catch-up re-dump, and any residual
        // drift (the hard-purge DELETE gap) surfaces as a TABLE_COUNT_MISMATCH a
        // complete replica can act on, instead of drifting silently.
        special:      ['dispensers']
    },

    // Indexer schema: generated from the table-lifecycle registry. Notable
    // structural facts that used to live in comments here now live with the
    // registry entries; the two that trip people up: balances/events have no
    // action_index column (the per-block action join would throw), and
    // sync_meta rides the `special` bucket because TransparencyLog.recordBlock
    // writes the source row AFTER _buildBlockPayload runs, so a blockScoped
    // read would always see the current block's row missing.
    indexer: lifecycle.streamTopology()
};

// Return the structured topology (per-scope table lists) for a dbType.
// Unknown / undefined dbTypes fall back to 'indexer' (the default db.dbType).
function getTopology(dbType){
    return TOPOLOGY[dbType === 'decoder' ? 'decoder' : 'indexer'];
}

// Flattened, de-duplicated union of every per-block replicated table for a
// dbType. This is the set whose row counts must agree between a source and a
// complete follower, published by the /status endpoint and compared by the
// client verifier.
function getReplicatedTables(dbType){
    let t = getTopology(dbType);
    // `special` carries replicated-but-not-per-scope-extracted tables (sync_meta) so
    // they join the completeness count check without being read by ServerPoller's
    // per-scope loops. Guarded with `|| []` for forward-compat with older topologies.
    let all = [].concat(t.blockScoped, t.txScoped, t.actionScoped, t.index, t.special || []);
    return [...new Set(all)];
}

// The cursor column for id-ordered paging of an append-only lookup table
// (SnapshotBuilder.streamTableRowsById / ClientSync._syncLookupTablesPaged). Almost
// every lookup table has an AUTO_INCREMENT `id` PK and pages by it. The decoder
// `pubkeys` table is the special case: its PRIMARY KEY is `address_id` (an FK into
// index_addresses.id), which is NOT monotonic with respect to INSERT order. A
// pubkeys row is inserted at first-SPEND, but its address_id was assigned earlier
// at first-SEEN, so a freshly inserted row can carry an address_id BELOW the
// replica's current high-water and would be permanently skipped by an address_id
// cursor (an indexer getDecoderBlockData() LEFT JOIN then resolves source_pubkey to
// NULL -> consensus divergence; #4413). pubkeys therefore pages by the surrogate
// AUTO_INCREMENT `id` (added in src/sql/pubkeys.sql + the
// 2026-06-17-pubkeys-add-monotonic-id migration), which increases with insert
// (first-spend) order. That `id` is a replication cursor only and is never hashed.
// All cursor columns are now monotonic and the tables are INSERT-only, so
// `<col> > cursor ORDER BY <col>` is a stable cursor.
function lookupCursorColumn(table){
    // eslint-disable-next-line no-unused-vars
    return 'id';
}

module.exports = { getTopology, getReplicatedTables, lookupCursorColumn };
