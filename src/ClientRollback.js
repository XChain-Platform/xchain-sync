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
            'sweeps',
            'tokens',
            'stakes',
            'unstakes',
            'delegations',
            'reward_claims',
            'contracts',
            'contract_stakes',
            'contract_unstakes',
            'contract_delegations',
            'contract_executions',
            'deposits',
            'withdrawals',
            'attests',
            'prices'
        ];

        // ── Decoder-DB rollback (used by _rollbackDecoder) ──
        // Decoder schema has no actions / balances / sync_meta. Tx-scoped tables
        // are deleted before the block-scoped transactions row that gave them their
        // tx_index scope. index_*/pubkeys/events are append-only and left untouched
        // (the sync stream re-introduces them with INSERT IGNORE).

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
