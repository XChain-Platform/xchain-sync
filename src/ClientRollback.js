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

class ClientRollback {

    constructor(db, util) {
        this.db   = db;
        this.util = util;

        // IMPORTANT: These lists are copied from xchain-indexer/src/Rollback.js (lines 40-107).
        // They MUST be kept in sync when new tables are added to the indexer.

        // Tables that store data using block_index
        this.blockTables = [
            'blocks',
            'transactions',
            'validator_rewards',
            'contract_state',
            'attestation_validator_signatures',
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
            'contract_executions',
            'deposits',
            'withdrawals',
            'attestation_requests',
            'attestation_responses'
        ];
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
                    await this.db.doQuery("DELETE FROM contract_emissions WHERE execution_index >= ?", [firstActionIndex]);
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

            // Recalculate balances from credits/debits
            // After rollback, the new blocks will arrive via the sync stream and re-apply correct balances
            // For now, we rebuild balances from the remaining credit/debit data
            try {
                await this.db.doQuery("DELETE FROM balances");
                await this.db.doQuery(`INSERT INTO balances (address_id, tick_id, amount)
                    SELECT address_id, tick_id,
                        CAST(COALESCE(SUM(CASE WHEN t.type = 'credit' THEN t.amount ELSE -t.amount END), 0) AS CHAR)
                    FROM (
                        SELECT address_id, tick_id, amount, 'credit' as type FROM credits
                        UNION ALL
                        SELECT address_id, tick_id, amount, 'debit' as type FROM debits
                    ) t
                    GROUP BY address_id, tick_id
                    HAVING SUM(CASE WHEN t.type = 'credit' THEN t.amount ELSE -t.amount END) != 0`);
            } catch(e){
                console.error('Error rebuilding balances after rollback:', e.message);
            }

            await this.db.commitTransaction();
            console.log('Indexer rollback to block ' + block_index + ' completed (' + this.util.getTimer(timer) + ')');

        } catch(e){
            await this.db.rollbackTransaction();
            console.error('Indexer rollback failed:', e.message);
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
                for(let t of ['transaction_outputs', 'dispensers']){
                    try {
                        await this.db.doQuery("DELETE FROM `" + t + "` WHERE tx_index IN (" + placeholders + ")", txIndexes);
                    } catch(e){
                        // Table may not exist in the target schema — skip
                    }
                }
            }

            // Block-scoped: transactions before blocks (no declared FK in
            // current schema, but kept in dependency order for clarity).
            await this.db.doQuery("DELETE FROM transactions WHERE block_index >= ?", [block_index]);
            await this.db.doQuery("DELETE FROM blocks       WHERE block_index >= ?", [block_index]);

            // index_addresses, index_transactions, pubkeys: append-only;
            // orphan rows are harmless (the sync stream uses INSERT IGNORE to
            // re-introduce them when new blocks arrive). Skip.

            await this.db.commitTransaction();
            console.log('Decoder rollback to block ' + block_index + ' completed (' + this.util.getTimer(timer) + ')');

        } catch(e){
            await this.db.rollbackTransaction();
            console.error('Decoder rollback failed:', e.message);
            throw e;
        }
    }
}

module.exports = ClientRollback;
