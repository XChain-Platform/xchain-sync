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
 * XChain Indexer Sync - Client Rollback
 *
 * Handles rolling back the local replica database to a given block.
 * Table lists are copied from xchain-indexer/src/Rollback.js and
 * MUST be kept in sync when new tables are added to the indexer.
 *
 ********************************************************************/

const balanceHelpers = require('./balance-helpers');

class ClientRollback {

    constructor(db, util) {
        this.db   = db;
        this.util = util;

        // IMPORTANT: These lists mirror xchain-indexer/src/rollback.js. They MUST be kept
        // in sync — any table the indexer rolls back AND xchain-sync replicates must also
        // be rolled back here, or the replica keeps orphaned rows after a reorg and silently
        // diverges from the source. test/unit/rollback-coverage.test.js guards this against
        // drift by checking every table ServerPoller replicates is handled below.

        // Tables that store data using block_index
        this.blockTables = [
            'blocks',
            'transactions',
            'validator_rewards',
            'contract_state',
            'slash_events'
        ];

        // Tables that store data using action_index
        this.dataTables = [
            'actions',
            'addresses',
            'airdrops',
            'batches',
            'broadcasts',
            'callbacks',
            'credits',
            'debits',
            'coinpay_expires',
            'coinpay_obligations',
            'coinpay_statuses',
            'coinpays',
            'destroys',
            'dispensers',
            'dispenser_cancels',
            'dispenser_closes',
            'dispenser_edits',
            'dispenser_expires',
            'dispenser_statuses',
            'dispenses',
            'dividends',
            'escrows',
            'fees',
            'files',
            'gated_files',
            'issues',
            'links',
            'lists',
            'list_edits',
            'list_items',
            'list_items_invalid',
            'mappings_actions',
            'mappings_files',
            'messages',
            'mints',
            'orders',
            'order_cancels',
            'order_edits',
            'order_expires',
            'order_matches',
            'order_statuses',
            'sends',
            'sleeps',
            'swaps',
            'swap_cancels',
            'swap_edits',
            'swap_expires',
            'swap_matches',
            'swap_statuses',
            // Cross-chain action tables. Each holds internally-minted action rows
            // (settlement legs, XCALL requests/expiries, injected XEXEC executions,
            // and processed callbacks) keyed by a rollback-able action_index. The
            // source rolls them back by action_index, so a reorg here must drop the
            // orphaned-range rows or the replica keeps serving cross-chain swaps as
            // settled / calls as executed that the source chain never finalized.
            'cross_chain_settlements',
            'cross_chain_call_executions',
            'cross_chain_call_callbacks',
            'xcalls',
            'sweeps',
            'tokens',
            'stakes',
            'unstakes',
            'delegations',
            // Revoked stake signing keys (rollback-able action_index). Consensus
            // state a replica needs to know which keys are valid signers; the source
            // rolls it back by action_index, so it must be dropped here on reorg too.
            'stake_key_revocations',
            'reward_claims',
            'contracts',
            // deploy_chunks: one row per DEPLOY v4 carrier action (rollback-able action_index),
            // delivered to followers via snapshots. The source rolls it back by
            // action_index (xchain-indexer/src/rollback.js dataTables); mirror it here or
            // orphaned chunk rows for a DEPLOY the source chain never finalized linger on
            // the replica until the next full snapshot (the explorer's per-chunk status
            // view then serves them). Unhashed, so no checkpoint fork — but it is genuine
            // rollback-able per-action state, not an indexer-local artifact.
            'deploy_chunks',
            'contract_stakes',
            'contract_unstakes',
            'contract_delegations',
            'contract_executions',
            'deposits',
            'withdrawals',
            'anchor_actions',
            'attests',
            'prices',
            // Programmable-policy controller bind/unbind event logs. Append-only,
            // keyed by action_index, never mutated in-place (cooldown expiry is
            // computed at read time), so the generic action_index delete reverts
            // orphaned binds/unbinds exactly. The source rolls these back, and they
            // are snapshot-replicated to followers, so a reorg here must drop the
            // orphaned-range rows or the replica keeps serving stale access policy
            // (which action-classes are guard-gated) that the source never finalized.
            'token_controllers',
            'address_controllers',
            // contract_permissions: the DEPLOY permissions manifest (which action-classes
            // are guard-gated for a contract), keyed by the DEPLOY action_index and rolled
            // back as a dataTable by the source — mirror it so a reorg drops orphaned
            // manifests too, else the replica keeps enforcing policy the source never finalized.
            'contract_permissions'
        ];

        // ── Decoder-DB rollback (used by _rollbackDecoder) ──
        // Decoder schema has no actions / balances / sync_meta. Tx-scoped tables
        // are deleted before the block-scoped transactions row that gave them their
        // tx_index scope. index_*/pubkeys/events are append-only and left untouched
        // (the sync stream re-introduces them with INSERT IGNORE).
        //
        // Leaving orphan index_* rows is safe ONLY because their AUTO_INCREMENT ids are
        // purely local artifacts that no longer feed any consensus value: as of
        // BLOCK_HASH_VERSION 2 the block hashes are computed from the RESOLVED strings
        // (address/tick/action/status), not from address_id/tick_id/etc. (see
        // xchain-indexer/src/db.js getBlockHashes + xchain-sync/src/BlockHasher.js). If a
        // lookup id is ever reintroduced into a consensus-visible projection, these orphan
        // rows would silently fork hashes after a reorg and this skip would become a bug.

        // Block-scoped tables, deleted by block_index. Order matters: transactions
        // is listed before blocks (tx rows scope the tx-scoped tables above them).
        this.decoderBlockTables = ['transactions', 'blocks'];

        // Tx-scoped tables, deleted by tx_index for the rolled-back blocks' transactions.
        // dispensers is intentionally absent: it is no longer per-block replicated
        // (the decoder live-prunes it, which the block stream can't model — see
        // replicatedTables.js), so it converges via the full snapshot only. Deleting
        // its rows on a reorg would corrupt that full-snapshot state with no live
        // stream to restore them, so a reorg leaves dispensers untouched.
        this.decoderTxScopedTables = ['transaction_outputs'];
    }

    // Roll back all data at or after the given block_index.
    // Branches on db.dbType — decoder has a different table layout (no actions,
    // no balances, tx-scoped tables instead of action-scoped) and no
    // contract_emissions / sync_meta.
    async rollback(block_index){
        let dbType = (this.db && this.db.dbType) || 'indexer';
        if(dbType === 'decoder'){
            return this._rollbackDecoder(block_index);
        }
        return this._rollbackIndexer(block_index);
    }

    // Indexer rollback (original behaviour)
    async _rollbackIndexer(block_index){
        let timer = this.util.startTimer();
        console.log('Starting indexer rollback to block ' + block_index + '...');

        // Get the first action_index at or after the given block
        let firstActionIndex = await this.db.getFirstActionIndex(block_index);

        await this.db.beginTransaction();
        try {
            // Delete from contract_emissions first (references contract_executions)
            if(firstActionIndex !== null){
                try {
                    await this.db.doQuery(
                        `DELETE FROM contract_emissions WHERE execution_index IN
                            (SELECT action_index FROM contract_executions WHERE action_index >= ?)`,
                        [firstActionIndex]
                    );
                } catch(e){
                    // Table may not exist — skip
                }
            }

            // ── In-place column resets (mirror xchain-indexer/src/rollback.js) ──
            // The source indexer's rollback runs in-place UPDATEs on SURVIVING rows
            // (action_index < firstActionIndex) to undo stamps that orphaned actions
            // wrote on them. The DELETE loops below only drop orphaned-RANGE rows, so
            // without replaying these resets the replica keeps the stale stamp and
            // diverges from the source (and from a from-genesis replay) after a reorg —
            // a consensus-affecting split. These three resets key purely on
            // firstActionIndex / block_index, so they port exactly; run them BEFORE the
            // deletes, matching the source order.
            //
            // NOTE: the source ALSO re-NULLs deactivation_block on
            // stakes/delegations/contract_stakes/contract_delegations, but those resets
            // key on the global ACTIVATION_DELAY_BLOCKS, which the thin replica does not
            // hold (it is a per-chain indexer default, only hub-overlaid when explicitly
            // set). That subset is tracked separately as the deactivation-block
            // sync-mirror gap and is intentionally NOT replayed here.
            if(firstActionIndex !== null){
                // tokens.escrow_action_index — an orphaned ORDER/SWAP/DISPENSER carrying
                // GIVE_OWNERSHIP stamped a surviving token (created by a much earlier
                // ISSUE) with the offer's action_index. Re-NULL stamps pointing into the
                // orphaned range so isOwnershipEscrowed() stops permanently rejecting
                // every owner-only action on the replica while the source accepts them.
                try {
                    await this.db.doQuery('UPDATE tokens SET escrow_action_index = NULL WHERE escrow_action_index >= ?', [firstActionIndex]);
                } catch(e){
                    // Column/table may not exist on older replica schemas — skip
                }

                // attests (ATTEST v0 request) — an orphaned v1 response / v2 expiry
                // flipped a surviving request out of 'pending'. Reset it (keyed on
                // resolved_block so BOTH flip paths reset) so a re-applied response is not
                // rejected as already-resolved and the pending-only deadline sweep can
                // re-synthesize the v2 row.
                try {
                    await this.db.doQuery(
                        "UPDATE attests SET request_status = 'pending', resolved_block = NULL " +
                        "WHERE version = 0 AND request_status IN ('fulfilled', 'errored', 'expired') AND resolved_block >= ?",
                        [block_index]
                    );
                } catch(e){
                    // Column/table may not exist on older replica schemas — skip
                }

                // xcalls (XCALL v0 request) — an orphaned result callback / deadline
                // expiry flipped a surviving request terminal. Reset so a re-applied
                // result is not silently lost to the already-resolved interlock (and an
                // expiry re-arms).
                try {
                    await this.db.doQuery(
                        "UPDATE xcalls SET request_status = 'pending', result_status = NULL, " +
                        "result_payload = NULL, resolved_block = NULL, callback_action_index = NULL " +
                        "WHERE version = 0 AND request_status IN ('completed', 'expired') AND resolved_block >= ?",
                        [block_index]
                    );
                } catch(e){
                    // Column/table may not exist on older replica schemas — skip
                }
            }

            // Delete from action-scoped data tables
            if(firstActionIndex !== null){
                for(let table of this.dataTables){
                    try {
                        await this.db.doQuery("DELETE FROM `" + table + "` WHERE action_index >= ?", [firstActionIndex]);
                    } catch(e){
                        // Table may not exist in older schemas — skip
                    }
                }
            }

            // Delete from block-scoped tables
            for(let table of this.blockTables){
                try {
                    await this.db.doQuery("DELETE FROM `" + table + "` WHERE block_index >= ?", [block_index]);
                } catch(e){
                    // Table may not exist — skip
                }
            }

            // price_snapshots anchors each round to a block via reference_block
            // (its equivalent of block_index) rather than block_index itself, so
            // it falls outside the generic blockTables loop above and needs its
            // own delete — mirroring the source indexer's rollback. Live
            // convergence on this table is hub-driven (the hub DB sync mirror
            // delivers row:deleted events), but that propagation lags the local
            // reorg, leaving the replica serving finalized rows for orphaned
            // rounds until the hub catches up. Deleting them here closes that
            // staleness window; any later hub-driven delete of an already-gone
            // row is a harmless SQL no-op.
            try {
                await this.db.doQuery("DELETE FROM price_snapshots WHERE reference_block >= ?", [block_index]);
            } catch(e){
                // Table may not exist on older replica schemas — skip
            }

            // Delete from sync_meta transparency log
            try {
                await this.db.doQuery("DELETE FROM sync_meta WHERE block_index >= ?", [block_index]);
            } catch(e){
                // Skip if table doesn't exist
            }

            // attest_validator_stats: running per-validator aggregate counters
            // (fulfilled/missed/slashed). This table is NOT block-streamed to
            // replicas — it only arrives via full-snapshot ride-along — and the thin
            // replica DB has none of the capability/governance machinery the source
            // uses to derive missed_count (the responsible-set capability snapshot),
            // so it cannot recompute these rows; reproducing that math here would
            // re-introduce the indexer-mirror drift the rollback guard exists to
            // catch. On reorg we therefore drop the rows whose most-recent touch is
            // in the orphaned range — so the replica never serves overcounted values
            // — and let the next full-snapshot ride-along restore correct counts
            // from the (now reorg-safe) source. Same recovery model as markets.
            try {
                await this.db.doQuery("DELETE FROM `attest_validator_stats` WHERE last_updated_block >= ?", [block_index]);
            } catch(e){
                // Table may not exist on older replica schemas — skip
            }

            // Recalculate balances from the surviving credits/debits.
            // SQL is shared with ClientApplier via balance-helpers to prevent divergence.
            try {
                await balanceHelpers.rebuildBalances(this.db);
            } catch(e){
                if(e.errno !== 1146) throw e;
                console.error('Error rebuilding balances after rollback:', e);
            }

            // Recalculate contract custody balances from the surviving valid
            // deposits/withdrawals.  contract_balances is a derived aggregate with
            // no action_index column, so it can't be deleted by the action-scoped
            // loop above and isn't streamed per-block.  A wholesale rebuild from the
            // remaining rows reproduces the source indexer's updateContractBalances()
            // exactly.  SQL shared with ClientApplier via balance-helpers.
            try {
                await balanceHelpers.rebuildContractBalances(this.db);
            } catch(e){
                if(e.errno !== 1146) throw e;
                console.error('Error rebuilding contract_balances after rollback:', e);
            }

            await this.db.commitTransaction();
            console.log('Indexer rollback to block ' + block_index + ' completed (' + this.util.getTimer(timer) + ')');

        } catch(e){
            await this.db.rollbackTransaction();
            console.error('Indexer rollback failed:', e);
            throw e;
        }
    }

    // Decoder rollback. Decoder schema has no actions / contract_emissions /
    // balances / sync_meta. Tx-scoped tables (transaction_outputs, dispensers)
    // must be deleted BEFORE the parent transactions rows that gave them their
    // tx_index scope. events is left untouched: it has no block_index and no
    // monotonic cursor, so per-block rollback isn't possible without a schema
    // change (decoder review Finding D).
    async _rollbackDecoder(block_index){
        let timer = this.util.startTimer();
        console.log('Starting decoder rollback to block ' + block_index + '...');

        await this.db.beginTransaction();
        try {
            // Collect tx_indexes for the blocks being rolled back so we can
            // clean tx-scoped tables before the transactions row goes away.
            let txRows = await this.db.doQuery(
                "SELECT tx_index FROM transactions WHERE block_index >= ?",
                [block_index]
            );
            let txIndexes = txRows.map(r => Number(r.tx_index));

            if(txIndexes.length > 0){
                let placeholders = txIndexes.map(() => '?').join(',');
                for(let t of this.decoderTxScopedTables){
                    try {
                        await this.db.doQuery("DELETE FROM `" + t + "` WHERE tx_index IN (" + placeholders + ")", txIndexes);
                    } catch(e){
                        // Table may not exist in the target schema — skip
                    }
                }
            }

            // Block-scoped: transactions before blocks (no declared FK in
            // current schema, but kept in dependency order for clarity).
            for(let t of this.decoderBlockTables){
                await this.db.doQuery("DELETE FROM `" + t + "` WHERE block_index >= ?", [block_index]);
            }

            // index_addresses, index_transactions, pubkeys: append-only;
            // orphan rows are harmless (the sync stream uses INSERT IGNORE to
            // re-introduce them when new blocks arrive). Skip.
            //
            // "Harmless" holds because these surrogate ids are local-only and feed no
            // consensus value — block hashes are computed from resolved strings, not lookup
            // ids (BLOCK_HASH_VERSION 2). Never let a lookup id back into a hashed projection.

            await this.db.commitTransaction();
            console.log('Decoder rollback to block ' + block_index + ' completed (' + this.util.getTimer(timer) + ')');

        } catch(e){
            await this.db.rollbackTransaction();
            console.error('Decoder rollback failed:', e);
            throw e;
        }
    }
}

module.exports = ClientRollback;
