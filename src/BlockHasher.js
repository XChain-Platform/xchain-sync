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
 * XChain Indexer Sync — Block Hasher (independent recomputation)
 *
 * Recomputes a block's chained consensus hashes (ledger / actions /
 * contract) from the RAW ROWS this validator replicated, rather than
 * trusting the committed hash a source published. Comparing the recomputed
 * hash to the committed hash detects a replica whose data does NOT match
 * the hash that was committed for it — replication corruption, a partial /
 * truncated apply, or a source serving rows inconsistent with its own
 * committed hash. (It composes with HashVerifier's cross-source check, which
 * catches two internally-consistent-but-divergent honest sources; neither
 * subsumes the other.)
 *
 * !!! CONSENSUS CONFORMANCE PAIR !!!
 * This is a byte-for-byte port of xchain-indexer/src/db.js getBlockHashes()
 * (+ utility.js getDataHash, reused here via the same conformance copy in
 * xchain-sync/src/utility.js). The two MUST stay identical: same SELECT
 * column sets, same ORDER BY, same object key-insertion order, the same
 * array-with-props quirk for `actions`, and the same previous-block chaining.
 * ANY change to the indexer's hash inputs MUST be mirrored here and the
 * test/fixtures/block-hash-vectors.json regenerated. The xchain-e2e-test
 * recompute-conformance scenario (indexer + sync on the regtest stack) is the
 * live drift guard.
 *
 ********************************************************************/

class BlockHasher {

    // db:   a DB handle exposing async doQuery(sql, params) against the REPLICA
    //       (schema-identical to the indexer, with surrogate ids preserved).
    // util: xchain-sync Utility — its getDataHash() is the conformance copy of
    //       the indexer's (JSON.stringify(Object.assign({}, data), bigint->string),
    //       SHA-256 hex).
    constructor(db, util){
        this.db   = db;
        this.util = util;
    }

    // Recompute { ledger_hash, actions_hash, contract_hash } for a block from the
    // replicated raw rows. Mirrors xchain-indexer/src/db.js getBlockHashes().
    async computeBlockHashes(block_index){
        let query   = null;
        let actions = [];
        let ledger  = {
            credits:  [],
            debits:   [],
            escrows:  []
        };
        let info    = [];
        let hashes  = [];
        // credits
        query = `SELECT
                    c.action_index,
                    c.address_id,
                    c.tick_id,
                    c.amount
                FROM
                    credits c
                    INNER JOIN actions      a ON (a.action_index=c.action_index)
                    INNER JOIN transactions t ON (t.tx_index=a.tx_index)
                WHERE
                    t.block_index=?
                ORDER BY
                    c.action_index ASC`;
        ledger.credits = await this.db.doQuery(query, [block_index]);
        // debits
        query = `SELECT
                    d.action_index,
                    d.address_id,
                    d.tick_id,
                    d.amount
                FROM
                    debits d
                    INNER JOIN actions      a ON (a.action_index=d.action_index)
                    INNER JOIN transactions t ON (t.tx_index=a.tx_index)
                WHERE
                    t.block_index=?
                ORDER BY
                    d.action_index ASC`;
        ledger.debits = await this.db.doQuery(query, [block_index]);
        // escrows
        query = `SELECT
                    e.action_index,
                    e.address_id,
                    e.tick_id,
                    e.amount
                FROM
                    escrows e
                    INNER JOIN actions      a ON (a.action_index=e.action_index)
                    INNER JOIN transactions t ON (t.tx_index=a.tx_index)
                WHERE
                    t.block_index=?
                ORDER BY
                    e.action_index ASC`;
        ledger.escrows = await this.db.doQuery(query, [block_index]);
        // actions
        query = `SELECT
                    a.action_index,
                    a.tx_index,
                    a.action_id
                FROM
                    actions a
                    INNER JOIN transactions t ON (t.tx_index=a.tx_index)
                WHERE
                    t.block_index=?
                ORDER BY
                    a.action_index ASC`;
        actions = await this.db.doQuery(query, [block_index]);
        // contract hash data
        let contracts_data = {
            contracts:   [],
            state:       [],
            executions:  [],
            emissions:   [],
            deposits:    [],
            withdrawals: []
        };
        // new deployments
        query = `SELECT c.action_index, c.source_id, c.code_hash, c.status_id
                 FROM contracts c
                 INNER JOIN actions a ON (a.action_index=c.action_index)
                 INNER JOIN transactions t ON (t.tx_index=a.tx_index)
                 WHERE t.block_index=?
                 ORDER BY c.action_index ASC`;
        contracts_data.contracts = await this.db.doQuery(query, [block_index]);
        // contract state (latest value per key written in this block)
        query = `SELECT cs.contract_index, cs.state_key, cs.state_value
                 FROM contract_state cs
                 INNER JOIN (
                     SELECT MAX(id) as max_id
                     FROM contract_state
                     WHERE block_index=?
                     GROUP BY contract_index, state_key
                 ) latest ON cs.id = latest.max_id
                 ORDER BY cs.contract_index ASC, cs.state_key ASC`;
        contracts_data.state = await this.db.doQuery(query, [block_index]);
        // executions
        query = `SELECT ce.action_index, ce.contract_index, ce.caller_id, ce.gas_used, ce.status_id, ce.emitted_count
                 FROM contract_executions ce
                 INNER JOIN actions a ON (a.action_index=ce.action_index)
                 INNER JOIN transactions t ON (t.tx_index=a.tx_index)
                 WHERE t.block_index=?
                 ORDER BY ce.action_index ASC`;
        contracts_data.executions = await this.db.doQuery(query, [block_index]);
        // emissions (join through executions to get block scope)
        query = `SELECT em.execution_index, em.emitted_action, em.action_index, em.position
                 FROM contract_emissions em
                 INNER JOIN contract_executions ce ON (ce.action_index=em.execution_index)
                 INNER JOIN actions a ON (a.action_index=ce.action_index)
                 INNER JOIN transactions t ON (t.tx_index=a.tx_index)
                 WHERE t.block_index=?
                 ORDER BY em.execution_index ASC, em.position ASC`;
        contracts_data.emissions = await this.db.doQuery(query, [block_index]);
        // deposits
        query = `SELECT d.action_index, d.contract_index, d.source_id, d.tick_id, d.amount, d.status_id
                 FROM deposits d
                 INNER JOIN actions a ON (a.action_index=d.action_index)
                 INNER JOIN transactions t ON (t.tx_index=a.tx_index)
                 WHERE t.block_index=?
                 ORDER BY d.action_index ASC`;
        contracts_data.deposits = await this.db.doQuery(query, [block_index]);
        // withdrawals
        query = `SELECT w.action_index, w.contract_index, w.source_id, w.tick_id, w.amount, w.status_id
                 FROM withdrawals w
                 INNER JOIN actions a ON (a.action_index=w.action_index)
                 INNER JOIN transactions t ON (t.tx_index=a.tx_index)
                 WHERE t.block_index=?
                 ORDER BY w.action_index ASC`;
        contracts_data.withdrawals = await this.db.doQuery(query, [block_index]);
        // previous block's committed hashes (for the chain)
        let prev_block_index = block_index - 1;
        query = `SELECT
                t1.hash as ledger,
                t2.hash as actions,
                t3.hash as contracts
            FROM
                blocks b
                LEFT JOIN index_transactions t1 ON (t1.id=b.ledger_hash_id)
                LEFT JOIN index_transactions t2 ON (t2.id=b.actions_hash_id)
                LEFT JOIN index_transactions t3 ON (t3.id=b.contract_hash_id)
            WHERE
                b.block_index=?`;
        let results = await this.db.doQuery(query, [prev_block_index]);
        if(results.length > 0){
            hashes['ledger']    = results[0].ledger;
            hashes['actions']   = results[0].actions;
            hashes['contracts'] = results[0].contracts;
        }
        // hash each of ledger / actions / contracts with block_index + previous hash
        let tables = ['ledger','actions','contracts'];
        tables.forEach(table => {
            var data = null;
            if(table=='ledger')    data = ledger;
            if(table=='actions')   data = actions;
            if(table=='contracts') data = contracts_data;
            data['block_index']   = block_index;
            data['previous_hash'] = hashes[table];
            info[table] = [];
            info[table]['hash'] = this.util.getDataHash(data);
        });
        // Normalise to the field names used across xchain-sync (ledger_hash, ...).
        return {
            ledger_hash:   info['ledger']['hash'],
            actions_hash:  info['actions']['hash'],
            contract_hash: info['contracts']['hash']
        };
    }
}

module.exports = BlockHasher;
