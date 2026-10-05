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
 * XChain Sync - Snapshot Table Sets
 * Classification and ordering of the tables a snapshot streams.
 *
 ********************************************************************/

const replicatedTables = require('../../schema/replicated_tables');
const tableLifecycle = require('../../table_lifecycle');

// Tables holding operator-local bookkeeping state (fetch timestamps, retry counters) that
// legitimately diverges between nodes and must not appear in consensus snapshots.
//
// DERIVED from the tableLifecycle registry: every entry whose `replication`
// mode is 'local', 'hub-mirror', or 'follower-derived' is snapshot-excluded,
// the same registry binding the per-block stream (streamTopology) and the
// rollback lists (replicaRollbackTables) already have. A new registry entry
// with one of those modes is excluded automatically; before this derivation a
// new local table passed every guard and silently rode consensus snapshots
// unless someone also hand-edited this Set. The three names appended
// after the spread have no registry entry (mempool_transactions is a
// decoder-DB table; sync_halt / sync_state are replica-created control
// tables) and are the ONLY permitted non-registry members; F-5 in
// test/unit/rollback_coverage.test.js pins both directions against the registry.
//
// Why the registry-derived members are excluded:
//  - icons, pending_hub_pushes, cross_chain_call_rejections ('local'): operator-local
//    fetch/push bookkeeping that legitimately diverges between nodes.
//  - recovery_pending_rewards ('local'): restore-time scratch staging for archived
//    validator rewards (xchain-indexer F1a id-determinism fix). Recovery-local, drained
//    into validator_rewards by the reindex apply hook, and never consensus-hashed. It
//    must not ride snapshots (a follower has no recovery in progress, so its count
//    legitimately differs).
//  - Hub-mirrored tables: every registry entry with replication 'hub-mirror', which
//    table_lifecycle.js defines and this comment only reads (the rows live in
//    table_lifecycle/block_and_special_tables.js). They are pushed/retracted by
//    hub_db_sync out-of-band with block apply, so they vary by WS arrival timing and
//    must not appear in consensus snapshots. price_snapshots is the one to notice: it
//    is quorum-class hashed (its registry entry's hashed.classes) and feeds
//    getOracleDataForVM (see the reference_block reorg delete in ClientRollback), so a
//    node without it is short consensus-relevant state, not merely bookkeeping.
//    These are NEVER replicated by xchain-sync
//    (excluded from the per-block stream, the incremental catch-up, AND these
//    snapshots). A serving node does not converge them via sync: the explorer serves
//    the consensus-relevant ones (state_checkpoints, capability_snapshots,
//    cross_chain_matches) from the MANDATORY co-located hub DB on the same server,
//    with no local-mirror fallback (it fails loud if the hub DB is absent).
//    IMPORTANT: a follower node that lacks a co-located hub DB (hub-less validator)
//    will have these hub-mirrored tables empty. This is by design and not a replication gap:
//    these rows are hub-driven state that the node must receive from its OWN hub
//    subscription (hub_db_sync), not from the sync snapshot stream. A hub-less
//    validator cannot serve hub-dependent API surfaces correctly; provision a hub
//    connection before running in validator mode.
//  - state_tree_roots ('follower-derived'): the follower recomputes its OWN row at
//    every snapshot apply (ClientApplier.seedSnapshotRoots, rebuilt from local
//    balances/stakes; never compared to the source). The table also carries
//    computed_at (CURRENT_TIMESTAMP), a per-node wall-clock value, so shipping the
//    source's rows adds pure nondeterminism to the snapshot and is never consumed by
//    the follower.
const OPERATOR_LOCAL_TABLES = new Set([
    ...tableLifecycle.tablesWhere(t =>
        ['local', 'hub-mirror', 'follower-derived'].includes(t.replication)),
    // Keep mempool_transactions out of the FULL snapshot too: it is node-local decoder
    // state with no registry entry, and every other channel (block stream, incremental
    // snapshot, /status count) already skips it, so shipping it would freeze the
    // source's bootstrap-instant mempool on full-bootstrap replicas.
    'mempool_transactions',
    // sync_halt, sync_state: replica-local durable CONTROL tables the source never
    // ships (created by db.verifySyncTables for both dbTypes; no registry entry).
    // sync_halt holds the durable divergence-halt audit record; sync_state holds the
    // bootstrap_base:<dbType> truncation-floor marker and the
    // index_map_mismatch_count counters. They are not in any snapshot payload, so
    // without listing them here the full-snapshot clear loop (ClientApplier:
    // enumerate all BASE tables, DELETE every one not in this set) would wipe both
    // on every full-snapshot apply, including the runtime oversized-incremental
    // recovery fallback on a live replica: erasing halt/forensic history and the
    // persisted bootstrap/verification posture persistBootstrapBase wrote. They are
    // exactly the node-local control state this exclusion set exists to protect.
    'sync_halt', 'sync_state',
]);

// Tables the SOURCE never streams (full or incremental) but that are deliberately
// NOT in OPERATOR_LOCAL_TABLES: the applier's full-snapshot clear loop must keep
// clearing them so a replica that imported a foreign copy before the exclusion
// existed is healed on its next full-snapshot apply (OPERATOR_LOCAL_TABLES is
// clear-protected, which would freeze those foreign rows forever).
//
// merkle_reorgs: node-local transparency-log reorg audit (server-side/indexer-only
// per its schema header; written by TransparencyLog.pruneFrom, new_root backfilled
// by commitEpoch). No cursor column, no replica reader, not in getReplicatedTables.
// Before this exclusion it rode full snapshots (shipping another node's audit
// trail, wall-clock detected_at included) and hit the incremental path's
// action_index fall-through, where its errno 1054 was silently swallowed; now the
// omission is a classification on record. The registry-exhaustiveness test
// (test/unit/sync_table_classification.test.js) keeps the next sync-owned table from
// falling through the same way.
const SOURCE_UNSTREAMED_TABLES = new Set([
    'merkle_reorgs',
]);

// Priority ordering for tables that must come first (index/dedup, then core).
// Any tables not in this list are included alphabetically after these.
const PRIORITY_TABLES = [
    'index_actions', 'index_addresses', 'index_coins', 'index_fiats',
    'index_memos', 'index_mime_types', 'index_pubkeys', 'index_statuses',
    'index_tickers', 'index_transactions',
    'blocks', 'transactions', 'actions'
];

// Tables to put last (derived/computed; they depend on everything else).
// pubkeys trails index_addresses: pubkeys.sql declares a FK on index_addresses(id),
// so the reverse-delete path in applyFullSnapshot must drop pubkeys rows before
// index_addresses rows.  Appended after sync_meta because neither balances nor
// sync_meta carries a FK on pubkeys.
const TRAILING_TABLES = ['balances', 'sync_meta', 'pubkeys'];

// Indexer full-dump tables outside the lookup topology that are still append-only,
// mapped to the column that pages them in-band. Neither is synced out of band, so
// both stream under skipLookups too. pubkeys has no surrogate id and pages by its
// address_id PRIMARY KEY: sound only inside one read view, since across requests
// that cursor skips late rows (see replicatedTables.lookupCursorColumn), so it
// must never join the out-of-band rows route.
const INDEXER_INBAND_PAGED = Object.freeze({ events: 'id', pubkeys: 'address_id' });

// Order a set of snapshot table names into the builder's dependency order:
// priority tables first (in declared order), everything else alphabetically,
// trailing tables last. Exported (alongside OPERATOR_LOCAL_TABLES) so
// ClientApplier.applyFullSnapshot can clear the union of payload tables and
// the local snapshot-eligible set in the same FK-safe order the builder
// streams them (reverse-delete: children before parents).
function orderSnapshotTables(allTables){
    let prioritySet  = new Set(PRIORITY_TABLES);
    let trailingSet  = new Set(TRAILING_TABLES);

    let ordered = [];
    for(let t of PRIORITY_TABLES){
        if(allTables.includes(t)) ordered.push(t);
    }
    let middle = allTables.filter(t => !prioritySet.has(t) && !trailingSet.has(t)).sort();
    ordered.push(...middle);
    for(let t of TRAILING_TABLES){
        if(allTables.includes(t)) ordered.push(t);
    }

    return ordered;
}

// Classify the decoder tables for an incremental snapshot. The three streamed
// buckets derive from TOPOLOGY.decoder so they cannot drift from the per-block
// stream; only the skip set is declared, and a unit test proves the four exhaustive.
//
// Decoder full-dump tables: index_* and pubkeys are small + append-only;
//   the client uses INSERT IGNORE so re-sending existing rows is a no-op.
//   `events` is full-dumped too: it carries no block_index/tx_index cursor
//   to scope incrementally, so the only way an incrementally-caught-up
//   follower converges its events table is a complete re-dump. It is safe
//   to re-send because events has an AUTO_INCREMENT `id` PK and the client
//   applies all incremental rows with INSERT IGNORE (existing ids are no-ops).
//
// dispensers is skipped here for the same reason it is not per-block
// streamed: the decoder mutates and deletes dispensers rows off the stream
// (the five writes are listed in src/schema/replicated_tables.js). An insert-only
// incremental delta (the tx_index->block_index join) would re-introduce the
// count divergence on any follower that catches up incrementally, and a plain
// re-dump would collide on the (tx_index, address_id) PK (dispensers is not in
// ClientApplier.ignoreTables, so it is not INSERT IGNORE). dispensers seeds
// from the full snapshot and is then held in parity SOLELY by the apply-side
// reconcile: ClientApplier.applyDispensersReplace via
// ClientSync.reconcileDispensers, gated by DISPENSERS_RECONCILE_EVERY /
// DISPENSERS_RECONCILE_MAX_INTERVAL_MS. Its decoder /status completeness count
// (replicatedTables `special`) is a post-replace equality sanity check, not a
// backstop: a soft-expire UPDATE leaves counts equal and a hard-purge DELETE
// leaves the replica ahead, which verifyTableCounts does not report.
function decoderIncrementalSets(){
    let topology = replicatedTables.getTopology('decoder');
    return {
        blockScoped: new Set(topology.blockScoped),
        txScoped:    new Set(topology.txScoped),
        fullDump:    new Set(topology.index),
        skip:        new Set(['mempool_transactions', 'dispensers'])
    };
}

module.exports = {
    OPERATOR_LOCAL_TABLES,
    SOURCE_UNSTREAMED_TABLES,
    PRIORITY_TABLES,
    TRAILING_TABLES,
    INDEXER_INBAND_PAGED,
    orderSnapshotTables,
    decoderIncrementalSets,
};
