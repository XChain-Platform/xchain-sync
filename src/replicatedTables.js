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
 * Scope note — this is deliberately the *per-block streamed* set. Tables that
 * only ride along in full/incremental snapshots and converge through other
 * channels are intentionally excluded, because their counts legitimately
 * diverge between nodes and comparing them would raise false incompleteness
 * alarms instead of catching real ones:
 *   - icons, price_snapshots          operator-local / hub-mirrored
 *   - attestation_validator_stats     running aggregate, full-snapshot only
 *   - markets                         derived OHLCV, full-snapshot only
 *   - mempool_transactions            non-deterministic across nodes
 *   - dispensers (decoder)            live-pruned each block; the block stream
 *                                     carries only inserts, so its count would
 *                                     diverge upward forever — full-snapshot only
 *
 ********************************************************************/

// Per-block replicated table topology by dbType. ServerPoller consumes the
// structured form (it needs the per-scope split for its index joins); the
// verification path consumes the flattened union via getReplicatedTables().
const TOPOLOGY = {

    // Decoder schema: 9 tables, much smaller surface area than indexer.
    // mempool_transactions is intentionally excluded (non-deterministic across
    // nodes — see xchain-sync-decoder-db-decisions).
    decoder: {
        // Block-scoped tables (key off block_index directly)
        blockScoped:  ['blocks', 'transactions'],
        // Tx-scoped tables (key off tx_index → transactions.block_index).
        // dispensers is deliberately excluded: per-block replication captures only
        // rows *inserted* in a block (via the tx_index→block_index join), but the
        // decoder prunes expired dispensers every block with a count-reducing
        // DELETE (deleteOpenDispensers) that the block stream cannot represent.
        // Streaming inserts alone would let a follower's dispensers count grow
        // monotonically above the source forever and publish a /status count no
        // complete replica could ever match. It converges via the full snapshot
        // instead (SnapshotBuilder.streamFullSnapshot dumps current rows in full).
        txScoped:     ['transaction_outputs'],
        // Decoder doesn't have action-scoped tables
        actionScoped: [],
        // Append-only lookup tables that may grow as new blocks are processed.
        // events is operational/logging — included so consumers see decoder activity.
        index:        ['index_addresses', 'index_transactions', 'pubkeys', 'events']
    },

    // Indexer schema: full set of 60+ tables
    indexer: {
        // Tables scoped by block_index (not action_index).
        // slash_events keys off block_index (it has execution_index +
        // block_index but no action_index), so it is block-scoped.
        blockScoped:  ['blocks', 'transactions', 'validator_rewards', 'contract_state', 'slash_events'],

        txScoped:     [],  // indexer joins via actions, not directly via tx_index

        // Action-scoped tables to include in block payloads
        actionScoped: [
            'actions', 'addresses', 'airdrops',
            // attests is the consolidated ATTEST table: one row per ATTEST
            // action_index (v0 request, v1 response, v2 expire), so it rides the
            // per-block action_index join like any other action-scoped table.
            'attests',
            'batches', 'broadcasts', 'callbacks',
            'coinpays', 'coinpay_obligations', 'coinpay_expires', 'coinpay_statuses',
            'contracts', 'contract_executions', 'contract_emissions', 'contract_balances',
            'contract_stakes', 'contract_unstakes', 'contract_delegations',
            'credits', 'debits', 'escrows',
            'delegations',
            'deposits', 'destroys',
            'dispensers', 'dispenser_cancels', 'dispenser_closes', 'dispenser_edits',
            'dispenser_expires', 'dispenser_statuses', 'dispenses',
            'dividends', 'events',
            'fees', 'files', 'gated_files',
            'issues',
            'links', 'lists', 'list_edits', 'list_items', 'list_items_invalid',
            'mappings_actions', 'mappings_files',
            // markets is intentionally NOT action-scoped: it has no action_index
            // column (it's a derived OHLCV aggregate keyed by tick pair), so it can't
            // ride the per-block action_index join. It is refreshed via full/incremental
            // snapshots only; live per-block accuracy would require rebuilding it on the
            // follower from order_matches/dispenses.
            'messages', 'mints',
            'orders', 'order_cancels', 'order_edits', 'order_expires', 'order_matches', 'order_statuses',
            'prices',
            'reward_claims',
            'sends', 'sleeps',
            'stakes',
            'swaps', 'swap_cancels', 'swap_edits', 'swap_expires', 'swap_matches', 'swap_statuses',
            'sweeps', 'tokens', 'unstakes',
            'withdrawals',
            'balances'
        ],

        // Index tables that may have new entries per block
        index:        [
            'index_actions', 'index_addresses', 'index_coins', 'index_fiats',
            'index_memos', 'index_mime_types', 'index_pubkeys', 'index_statuses',
            'index_tickers', 'index_transactions'
        ]
    }
};

// Return the structured topology (per-scope table lists) for a dbType.
// Unknown / undefined dbTypes fall back to 'indexer' (the default db.dbType).
function getTopology(dbType){
    return TOPOLOGY[dbType === 'decoder' ? 'decoder' : 'indexer'];
}

// Flattened, de-duplicated union of every per-block replicated table for a
// dbType. This is the set whose row counts must agree between a source and a
// complete follower — published by the /status endpoint and compared by the
// client verifier.
function getReplicatedTables(dbType){
    let t = getTopology(dbType);
    let all = [].concat(t.blockScoped, t.txScoped, t.actionScoped, t.index);
    return [...new Set(all)];
}

module.exports = { getTopology, getReplicatedTables };
