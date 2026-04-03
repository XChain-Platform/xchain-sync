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
            'contract_state'
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
            'contract_executions',
            'deposits',
            'withdrawals'
        ];
    }

    // Roll back all data at or after the given block_index
    async rollback(block_index){
        let timer = this.util.startTimer();
        console.log('Starting rollback to block ' + block_index + '...');

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
            console.log('Rollback to block ' + block_index + ' completed (' + this.util.getTimer(timer) + ')');

        } catch(e){
            await this.db.rollbackTransaction();
            console.error('Rollback failed:', e.message);
            throw e;
        }
    }
}

module.exports = ClientRollback;
