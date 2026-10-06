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

// Indexer block-scoped set. These tables carry a block_index but no
// action_index, so the action_index branch below cannot reach them.
// They must be filtered by block_index here to appear in incremental
// snapshots. slash_events is block-scoped for the same reason
// (see ServerPoller.blockScopedTables).
//
// Tables with neither a block_index nor an action_index cursor, such as
// icons (token-icon processing state, keyed by token_id) and
// price_snapshots (mirrored from the cross-chain hub's price channel,
// keyed by round_number/coin_pair), cannot be scoped incrementally and
// are intentionally omitted. They ride along in the full snapshot only;
// for price_snapshots, live convergence is handled by the hub DB sync
// mirror, not this block stream.
// attest_validator_stats, markets, and merkle_epochs are also unscoped
// but are included in indexerFullDump below so a follower does not
// freeze those tables at bootstrap height (see comment there).
//
// Every block_index-scoped streamed table must be filtered by block_index here:
// these tables carry NO action_index column (e.g. the slash debit logs key off
// execution_index / slash_action_index, not action_index), so the action_index
// branch below cannot reach them. A follower catching up incrementally over their
// range would hit SELECT ... WHERE action_index >= ? -> ER_BAD_FIELD_ERROR ->
// caught -> continue, silently dropping every row (short /status count; the reorg
// restore, which JOINs the debit logs, then finds nothing to restore).
//
// Derive the set from the streaming topology (the single source of truth) rather
// than a hand-maintained literal, so the next block-scoped table added to
// replicatedTables can never re-open this gap. sync_meta is appended because it is
// streamed inline by ServerPoller (not via the blockScoped topology) but is still
// block_index-scoped for incremental catch-up.
function indexerBlockScopedSet(){
    return new Set([...replicatedTables.getTopology('indexer').blockScoped, 'sync_meta']);
}

// Append-only lookup/dedup tables (index_actions, index_addresses,
// index_transactions, ...). They carry neither a block_index nor an
// action_index cursor, so they can't be range-scoped. A follower that
// heals a gap via incremental still needs the index_* rows those blocks
// reference, or it is left short on them (row-count + ledger-hash mismatch
// after the heal; blocks/transactions carry *_hash_id FKs into
// index_transactions, so even action-less blocks need it). They are
// therefore re-dumped in full; the client applies index_* with INSERT
// IGNORE (ClientApplier.ignoreTables), so re-sending existing rows is a
// no-op. Mirrors the decoder full-dump path. Sourced from the replicated
// topology so it can't drift from the per-block streamed set.
//
// Also included:
//   - merkle_epochs: transparency epoch records with no block_index or
//     action_index cursor. Absent from every topology bucket, so it never
//     enters the action_index or block_index scoping branches. Without an
//     explicit full-dump here, a relay follower's merkle_epochs table
//     freezes at bootstrap height and getProof returns "epoch not yet
//     committed" for every post-bootstrap epoch.
//   - markets: derived OHLCV aggregate keyed by tick pair with no
//     action_index. Without a full-dump here, a follower's markets table
//     freezes at bootstrap height. VALUE changes converge post-reorg via
//     this full-dump UPSERT (ON DUPLICATE KEY UPDATE); ROW REMOVAL cannot,
//     which is why ClientRollback mirrors both of the source's markets
//     deletes (orphaned-tick sweep and the pair-scoped IDX-2 delete).
//   - attest_validator_stats: running per-validator aggregate counters
//     with no action_index. Without a full-dump here, these counters
//     freeze at bootstrap height. ClientRollback drops affected rows on
//     reorg; the next incremental catch-up restores current values.
//   - events: append-only operational audit log (records REORG events)
//     with no block_index or action_index cursor, so it never enters the
//     scoping branches and, without a full-dump here, freezes at bootstrap
//     height on an incrementally-caught-up follower (source count grows,
//     replica frozen, and it is replication:'snapshot' so it never shows in
//     the /status TABLE_COUNT_MISMATCH signal). Mirrors the decoder events
//     full-dump. It is in ClientApplier.ignoreTables, so the re-dump is
//     idempotent (INSERT IGNORE on the AUTO_INCREMENT id PK). rollback:
//     'exempt', so nothing rolls it back; the full re-dump is its only
//     convergence path.
//   - pubkeys: INSERT IGNORE cache keyed by address_id, replication:
//     'snapshot', so streamTopology() puts it in no per-block bucket and
//     it reaches neither the spread above nor indexerBlockScoped. Without
//     a full-dump here it fell to the action_index branch, where the
//     missing column raised errno 1054 and was swallowed as a schema gap,
//     so it rode NO incremental snapshot and froze at bootstrap height on
//     every incrementally-caught-up follower (silent: replication:
//     'snapshot' keeps it out of the /status count check, and it is not
//     consensus-hashed). Mirrors the decoder pubkeys full-dump. It is in
//     ClientApplier.ignoreTables, so the re-dump is idempotent.
function indexerFullDumpSet(){
    return new Set([
        ...replicatedTables.getTopology('indexer').index,
        'merkle_epochs',
        'markets',
        'attest_validator_stats',
        'events',
        'pubkeys',
    ]);
}

module.exports = {
    OPERATOR_LOCAL_TABLES,
    SOURCE_UNSTREAMED_TABLES,
    PRIORITY_TABLES,
    TRAILING_TABLES,
    INDEXER_INBAND_PAGED,
    orderSnapshotTables,
    decoderIncrementalSets,
    indexerBlockScopedSet,
    indexerFullDumpSet,
};
