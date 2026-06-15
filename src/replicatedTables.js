/*********************************************************************
 *
 * Copyright © 2025–2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC – https://dankest.llc
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This file is part of XChain Platform. Licensed under the GNU Affero
 * General Public License v3.0 or later; see LICENSE.md. A commercial
 * license (without AGPL source-disclosure terms) is available —
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
 * Scope note — this is deliberately the *per-block streamed* set. Tables that
 * only ride along in full/incremental snapshots and converge through other
 * channels are intentionally excluded, because their counts legitimately
 * diverge between nodes and comparing them would raise false incompleteness
 * alarms instead of catching real ones:
 *   - icons, price_snapshots          operator-local / hub-mirrored
 *   - attest_validator_stats          running aggregate, full-snapshot only
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
        index:        ['index_addresses', 'index_transactions', 'pubkeys', 'events'],
        // Decoder has no transparency log (sync_meta), so nothing here.
        special:      []
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
            // NOTE: balances, contract_balances, and events are intentionally NOT
            // listed here. None has an action_index column, so the per-block
            // `INNER JOIN actions a ON (a.action_index = t.action_index)` in
            // db.getActionScopedRows() throws ER_BAD_FIELD_ERROR on every poll.
            // balances and contract_balances are derived aggregates the follower
            // recomputes from credits/debits and deposits/withdrawals
            // (ClientApplier._rebuildBalances / _rebuildContractBalances), so they
            // must not ride the per-block payload. events is an unscoped operational
            // log carried by full snapshots only — same rationale as markets below.
            // state_checkpoints is also intentionally NOT here: it is hub-mirrored
            // (hub_db_sync, like price_snapshots), not produced by block processing.
            'actions', 'addresses', 'airdrops',
            // anchor_actions holds the parsed DOGE-only ANCHOR rows (one per
            // action_index), so it rides the per-block action_index join.
            'anchor_actions',
            // attests is the consolidated ATTEST table: one row per ATTEST
            // action_index (v0 request, v1 response, v2 expire), so it rides the
            // per-block action_index join like any other action-scoped table.
            'attests',
            'batches', 'broadcasts', 'callbacks',
            'coinpays', 'coinpay_obligations', 'coinpay_expires', 'coinpay_statuses',
            'contracts', 'contract_executions', 'contract_emissions',
            // deploy_chunks: one row per DEPLOY v4 carrier action_index (rollback-able, never
            // mutated in-place), so it rides the per-block action_index join. Streaming it
            // per block keeps a follower's chunk store current between full snapshots and
            // pairs with ClientRollback dropping orphaned-range chunk rows on reorg.
            'deploy_chunks',
            'contract_stakes', 'contract_unstakes', 'contract_delegations',
            'credits', 'debits', 'escrows',
            'delegations',
            'deposits', 'destroys',
            'dispensers', 'dispenser_cancels', 'dispenser_closes', 'dispenser_edits',
            'dispenser_expires', 'dispenser_statuses', 'dispenses',
            'dividends',
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
            // Cross-chain action tables. Each carries internally-minted action rows
            // (one row per rollback-able action_index: settlement legs, XCALL
            // requests/expiries, injected XEXEC executions, processed callbacks), so
            // they ride the per-block action_index join like any other action-scoped
            // table. Streaming them per block keeps a follower's cross-chain settled
            // state correct between full snapshots, and pairs with ClientRollback
            // dropping their orphaned-range rows on reorg.
            'cross_chain_settlements', 'cross_chain_call_executions',
            'cross_chain_call_callbacks', 'xcalls',
            // stake_key_revocations: one row per revocation action_index (rollback-able),
            // so it rides the per-block action_index join. A follower needs it to know
            // which stake signing keys are still valid signers.
            'stake_key_revocations',
            'sweeps', 'tokens', 'unstakes',
            'withdrawals',
            // Programmable-policy controller bind/unbind event logs. One row per
            // action_index (rollback-able, never mutated in-place), so they ride the
            // per-block action_index join. Streaming them per block keeps a follower's
            // access-policy state (which action-classes are guard-gated) correct
            // between full snapshots, and pairs with ClientRollback dropping their
            // orphaned-range rows on reorg.
            'token_controllers', 'address_controllers', 'contract_permissions'
        ],

        // Index tables that may have new entries per block
        index:        [
            'index_actions', 'index_addresses', 'index_coins', 'index_fiats',
            'index_memos', 'index_mime_types', 'index_pubkeys', 'index_statuses',
            'index_tickers', 'index_transactions'
        ],

        // Replicated for completeness-counting but NOT extracted by ServerPoller's
        // per-scope loops. sync_meta (the transparency log) is streamed inline by
        // ServerPoller — _buildBlockPayload builds the row from the block hashes —
        // and carried by snapshots (SnapshotBuilder discovers tables via
        // information_schema), so it never rides the blockScoped getBlockScopedRows
        // path. Listing it HERE rather than in blockScoped puts it in the /status
        // row-count completeness check (getReplicatedTables) without tripping the
        // record-after-build ordering trap: TransparencyLog.recordBlock writes the
        // source's sync_meta row AFTER _buildBlockPayload runs (see ServerPoller._poll),
        // so a blockScoped read would always see the current block's row missing.
        // The replica replicates the source's actual sync_meta rows (not derived from
        // blocks), so source and replica counts track each other and the check is
        // meaningful — a persistent shortfall means the replica genuinely dropped
        // transparency rows the source streamed.
        special:      ['sync_meta']
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
    // `special` carries replicated-but-not-per-scope-extracted tables (sync_meta) so
    // they join the completeness count check without being read by ServerPoller's
    // per-scope loops. Guarded with `|| []` for forward-compat with older topologies.
    let all = [].concat(t.blockScoped, t.txScoped, t.actionScoped, t.index, t.special || []);
    return [...new Set(all)];
}

module.exports = { getTopology, getReplicatedTables };
