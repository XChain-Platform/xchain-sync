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
 *   - dispensers (decoder)            soft-expired by UPDATE and hard-purged later by
 *                                     purgeExpiredDispensers; neither mutation rides the
 *                                     per-block stream. It IS in the decoder `special`
 *                                     bucket so it joins the /status completeness count,
 *                                     but that count is a post-replace equality sanity
 *                                     check, never a drift detector: a soft-expire leaves
 *                                     the counts equal and a hard-purge leaves the replica
 *                                     AHEAD, which _verifyTableCounts does not report.
 *                                     Parity rests entirely on the apply-side reconcile,
 *                                     ClientApplier.applyDispensersReplace via
 *                                     ClientSync._reconcileDispensers.
 *   - cross_chain_calls,              hub-mirrored via hub_db_sync, not produced by block
 *     cross_chain_matches,            processing, and pushed or retracted by the hub out
 *     oracle_prices,                  of band with block apply, so they cannot ride the
 *     capability_snapshots,           per-block stream. xchain-sync NEVER replicates them
 *     state_checkpoints,              on any channel, snapshots included. A serving node
 *     price_snapshots                 does not fall back to a local mirror either: the
 *                                     explorer reads the consensus-relevant ones from the
 *                                     MANDATORY co-located hub DB and fails loud without
 *                                     it, rather than serving stale local rows.
 *   - icons                           replication 'local': never leaves the node, on any
 *                                     channel, and is not a snapshot ride-along.
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
    // mempool_transactions is intentionally excluded, being non-deterministic
    // across nodes.
    decoder: {
        blockScoped:  ['blocks', 'transactions'],
        // Keyed off tx_index -> transactions.block_index. dispensers is deliberately
        // absent: that join captures only rows INSERTED in a block, while the decoder
        // also soft-expires and later hard-purges dispensers off-stream, so streaming
        // inserts alone would let a follower's count drift. It rides `special` instead.
        txScoped:     ['transaction_outputs'],
        actionScoped: [],
        // Append-only lookups that grow as blocks are processed. events is
        // operational/logging, included so consumers can see decoder activity.
        index:        ['index_addresses', 'index_transactions', 'pubkeys', 'events'],
        // Counted for completeness but NOT read by ServerPoller's per-scope loops.
        // dispensers converges only through the full snapshot plus the periodic
        // re-dump/replace reconcile, and _incrementalCatchUp excludes it on every
        // non-reconcile cycle; see the header note on why its count detects nothing.
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

// The advisory content-parity plan for a dbType: the replicated tables whose
// CONTENT (not merely their row count) a follower can prove against the source,
// each paired with the bound its checksum window uses.
//
// Coverage is the per-block replicated set minus the two exclusion classes the
// registry declares (src/tableLifecycle.js CONTENT_PARITY_*): the operator
// carve-outs (markets, decoder dispensers) and the in-place mutated tables,
// which the enforced state_hash already commits and which have no stable window
// content. Derived from the same topology the stream and the row counts use, so
// a table added to replication joins this check with no second list to update.
//
// bound values, consumed by BlockHasher.computeTableContentChecksums:
//   'block'    the table carries block_index; window it directly
//   'action'   action-scoped; window through the actions join, exactly as
//              ServerPoller streams it (a.block_index, never a tx join)
//   'tx'       decoder tx-scoped; window through the transactions join
//   'emission' contract_emissions only: its action_index is NULL for internal
//              emissions, so it windows through the execution_index chain,
//              matching db.getEmissionRowsForBlock rather than the generic join
//   'id'      inert append-only lookup with no block column; windowed by a
//              source-published id ceiling instead of a block range
function contentParityPlan(dbType){
    let type = (dbType === 'decoder') ? 'decoder' : 'indexer';
    let t = getTopology(type);
    let mutable = new Set(lifecycle.contentParityMutableTables());
    let plan = [];
    let add = (table, bound) => {
        if(mutable.has(table)) return;                                   // committed by state_hash instead
        if(lifecycle.contentParityCarveOut(table, type) !== null) return; // operator ruling
        if(plan.some(p => p.table === table)) return;                    // topology buckets can overlap
        plan.push({ table: table, bound: bound });
    };
    for(let table of (t.blockScoped || [])) add(table, 'block');
    for(let table of (t.txScoped    || [])) add(table, 'tx');
    for(let table of (t.actionScoped|| [])) add(table, table === 'contract_emissions' ? 'emission' : 'action');
    for(let table of (t.index       || [])) add(table, lifecycle.contentParityLookupBound(table, type));
    // `special` carries replicated-but-not-per-scope-extracted tables. sync_meta is
    // block-keyed (one row per block); the decoder's dispensers is carved out above.
    for(let table of (t.special     || [])) add(table, 'block');
    return plan;
}

// Every replicated table that is NOT in the content-parity plan, mapped to the
// reason it is out. Exists so the coverage guard can assert the complement is
// exactly the two declared exclusion classes: a replicated table that is neither
// checked nor knowingly excluded is the defect this whole check exists to catch.
function contentParityExclusions(dbType){
    let type = (dbType === 'decoder') ? 'decoder' : 'indexer';
    let mutable = new Set(lifecycle.contentParityMutableTables());
    let out = {};
    for(let table of getReplicatedTables(type)){
        let carve = lifecycle.contentParityCarveOut(table, type);
        if(carve !== null) out[table] = 'operator-carve-out: ' + carve;
        else if(mutable.has(table)) out[table] = 'in-place mutated; committed by the enforced state_hash class instead';
    }
    return out;
}

// Per-block replicated tables that do NOT exist in a schema.
//
// `present` is the Set of local base-table names from db.listExistingTables().
// Every apply path tolerates MariaDB errno 1146 so an older replica schema
// cannot wedge on a table the source has gained; the cost of that tolerance is
// that a schema gap degrades to SILENT partial replication (halted:false,
// lag_blocks:0, whole tables never arriving). This is the one function that
// names the gap: any table in the per-block replicated set that this schema
// lacks is a table replication will skip without ever failing.
//
// Deliberately a superset signal. A table absent on BOTH the source and this
// replica (source older than this build) is reported too, because from here the
// two cases are indistinguishable and reporting the harmless one costs an
// operator one migration check, while missing the real one costs silent data
// loss. Returns null when the table listing itself is unavailable: "unknown"
// must not read as "nothing missing".
function missingReplicatedTables(present, dbType){
    if(!present || typeof present.has !== 'function') return null;
    return getReplicatedTables(dbType).filter(t => !present.has(t)).sort();
}

// The cursor column for id-ordered paging of an append-only lookup table
// (SnapshotBuilder.streamTableRowsById / ClientSync._syncLookupTablesPaged).
//
// It is always the AUTO_INCREMENT `id`, and the decoder `pubkeys` table is why that
// is stated rather than assumed: its PRIMARY KEY is `address_id`, which is NOT
// monotonic with INSERT order, because a pubkeys row is inserted at first-SPEND while
// its address_id was assigned earlier at first-SEEN. An address_id cursor therefore
// skips a fresh row that lands below the replica's high-water mark permanently, and
// the indexer's LEFT JOIN then resolves source_pubkey to NULL, which is a consensus
// divergence. pubkeys carries a surrogate AUTO_INCREMENT `id` for exactly this, used
// as a replication cursor only and never hashed. Every cursor column is monotonic and
// every such table is INSERT-only, so `<col> > cursor ORDER BY <col>` is stable.
function lookupCursorColumn(table){
    // eslint-disable-next-line no-unused-vars
    return 'id';
}

module.exports = {
    getTopology, getReplicatedTables, missingReplicatedTables, lookupCursorColumn,
    contentParityPlan, contentParityExclusions
};
