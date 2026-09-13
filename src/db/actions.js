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
 * The replicated action tables: the per-block action rows and the leaf rows
 * the state hash is built from.
 *
 * A Database mixin: every method below is installed on Database.prototype by
 * db/index.js, so `this` is the Database instance and call sites are unchanged.
 *
 ********************************************************************/

const path       = require('path');
const { canonicalizeHashAddress } = require('../util/protocol_address_roles');
const { isStateKeyBinCollationActive } = require('../state_key_collation_activation');
const lifecycle = require('../tableLifecycle');
const { assertValidIdentifier } = require('./shared.js');

module.exports = {

    // Gather a block's content rows for the block_merkle_root (SPV spec sec.5), in
    // the frozen cross-kind order stateCommitment.computeBlockMerkleRoot expects:
    // { ledger:{credits,debits,escrows}, actions, contracts:{contracts,state,
    //   executions,emissions,deposits,withdrawals} }.
    //
    // !!! CONFORMANCE: the SELECT column sets + ORDER BY below MUST stay
    // byte-identical to BlockHasher.computeBlockHashes (and the indexer's
    // getBlockHashes), since block_merkle_root covers the same rows the consensus
    // ledger/actions/contract hashes do. The xchain-e2e consensusHashConformance
    // test is the drift guard. !!!
    //
    // `network`/`coin` drive the state_key collation flag-day
    // (state_key_collation_activation.js), mirroring BlockHasher; omitted ->
    // legacy folding collation (pre-activation behavior).
    //
    // Every SELECT below is doQueryStrict, never doQuery (M-17). These rows ARE the
    // block_merkle_root leaf set, so a swallowed non-transactional error returning []
    // is not an error signal, it is a wrong answer: the caller hashes a truncated
    // leaf set into a valid-looking root and persists it. Whether a transaction
    // happens to be open is the CALLER's business and must not decide it; strict here
    // is identical to doQuery inside a transaction and inside an explicit conn, and
    // differs only on the path where the difference matters.
    async getBlockLeafRows(block_index, conn, network, coin){
        let q;
        let ledger = { credits: [], debits: [], escrows: [] };
        q = `SELECT c.action_index, a1.address AS address, t1.tick AS tick, c.amount
             FROM credits c
                INNER JOIN actions        a  ON (a.action_index=c.action_index)
                LEFT  JOIN index_addresses a1 ON (a1.id=c.address_id)
                LEFT  JOIN index_tickers   t1 ON (t1.id=c.tick_id)
             WHERE a.block_index=?
             ORDER BY c.action_index ASC, a1.address COLLATE utf8_bin ASC, t1.tick COLLATE utf8mb4_bin ASC, c.amount ASC`;
        ledger.credits = await this.doQueryStrict(q, [block_index], conn);
        q = `SELECT d.action_index, a1.address AS address, t1.tick AS tick, d.amount
             FROM debits d
                INNER JOIN actions        a  ON (a.action_index=d.action_index)
                LEFT  JOIN index_addresses a1 ON (a1.id=d.address_id)
                LEFT  JOIN index_tickers   t1 ON (t1.id=d.tick_id)
             WHERE a.block_index=?
             ORDER BY d.action_index ASC, a1.address COLLATE utf8_bin ASC, t1.tick COLLATE utf8mb4_bin ASC, d.amount ASC`;
        ledger.debits = await this.doQueryStrict(q, [block_index], conn);
        q = `SELECT e.action_index, a1.address AS address, t1.tick AS tick, e.amount
             FROM escrows e
                INNER JOIN actions        a  ON (a.action_index=e.action_index)
                LEFT  JOIN index_addresses a1 ON (a1.id=e.address_id)
                LEFT  JOIN index_tickers   t1 ON (t1.id=e.tick_id)
             WHERE a.block_index=?
             ORDER BY e.action_index ASC, a1.address COLLATE utf8_bin ASC, t1.tick COLLATE utf8mb4_bin ASC, e.amount ASC`;
        ledger.escrows = await this.doQueryStrict(q, [block_index], conn);
        // CONSENSUS: canonicalize protocol special addresses (BURN/GAS/DONATE/REWARD)
        // to their chain-independent role token, byte-for-byte mirror of BlockHasher
        // and the indexer's getBlockHashes. block_merkle_root covers the same ledger
        // rows the flat ledger_hash does, so without this the follower recomputes a
        // raw-address merkle root that diverges from the source's canonicalized root on
        // any special-address block and halts. Ordered AFTER the SQL sort (which keys
        // on the raw stored address) so the leaf sequence matches the source exactly.
        for (const row of ledger.credits) row.address = canonicalizeHashAddress(row.address);
        for (const row of ledger.debits)  row.address = canonicalizeHashAddress(row.address);
        for (const row of ledger.escrows) row.address = canonicalizeHashAddress(row.address);
        q = `SELECT a.action_index, a.tx_index, ia.action AS action
             FROM actions a
                LEFT JOIN index_actions ia ON (ia.id=a.action_id)
             WHERE a.block_index=?
             ORDER BY a.action_index ASC`;
        let actions = await this.doQueryStrict(q, [block_index], conn);
        let contracts = { contracts: [], state: [], executions: [], emissions: [], deposits: [], withdrawals: [] };
        q = `SELECT c.action_index, a1.address AS source_address, c.code_hash, s1.status AS status
             FROM contracts c
                INNER JOIN actions a ON (a.action_index=c.action_index)
                LEFT  JOIN index_addresses a1 ON (a1.id=c.source_id)
                LEFT  JOIN index_statuses  s1 ON (s1.id=c.status_id)
             WHERE a.block_index=?
             ORDER BY c.action_index ASC`;
        contracts.contracts = await this.doQueryStrict(q, [block_index], conn);
        // contract state (latest value per key written in this block).
        // state_key collation is flag-day gated, mirroring BlockHasher and the
        // indexer's getBlockHashes: legacy folding (utf8_general_ci) below the
        // activation height, COLLATE utf8_bin pinned at/after it
        // (see state_key_collation_activation.js).
        let stateKeyCollate = isStateKeyBinCollationActive(block_index, network, coin) ? ' COLLATE utf8_bin' : '';
        q = `SELECT cs.contract_index, cs.state_key, cs.state_value
             FROM contract_state cs
                INNER JOIN (
                    SELECT MAX(id) as max_id FROM contract_state
                    WHERE block_index=? GROUP BY contract_index, state_key` + stateKeyCollate + `
                ) latest ON cs.id = latest.max_id
             ORDER BY cs.contract_index ASC, cs.state_key` + stateKeyCollate + ` ASC`;
        contracts.state = await this.doQueryStrict(q, [block_index], conn);
        q = `SELECT ce.action_index, ce.contract_index, a1.address AS caller_address, ce.gas_used, s1.status AS status, ce.emitted_count
             FROM contract_executions ce
                INNER JOIN actions a ON (a.action_index=ce.action_index)
                LEFT  JOIN index_addresses a1 ON (a1.id=ce.caller_id)
                LEFT  JOIN index_statuses  s1 ON (s1.id=ce.status_id)
             WHERE a.block_index=?
             ORDER BY ce.action_index ASC`;
        contracts.executions = await this.doQueryStrict(q, [block_index], conn);
        // emissions (join through executions to get block scope)
        q = `SELECT em.execution_index, em.emitted_action, em.action_index, em.position
             FROM contract_emissions em
                INNER JOIN contract_executions ce ON (ce.action_index=em.execution_index)
                INNER JOIN actions a ON (a.action_index=ce.action_index)
             WHERE a.block_index=?
             ORDER BY em.execution_index ASC, em.position ASC`;
        contracts.emissions = await this.doQueryStrict(q, [block_index], conn);
        q = `SELECT d.action_index, d.contract_index, a1.address AS source_address, t1.tick AS tick, d.amount, s1.status AS status
             FROM deposits d
                INNER JOIN actions a ON (a.action_index=d.action_index)
                LEFT  JOIN index_addresses a1 ON (a1.id=d.source_id)
                LEFT  JOIN index_tickers   t1 ON (t1.id=d.tick_id)
                LEFT  JOIN index_statuses  s1 ON (s1.id=d.status_id)
             WHERE a.block_index=?
             ORDER BY d.action_index ASC, d.contract_index ASC, a1.address COLLATE utf8_bin ASC, t1.tick COLLATE utf8mb4_bin ASC, d.amount ASC, s1.status COLLATE utf8_bin ASC`;
        contracts.deposits = await this.doQueryStrict(q, [block_index], conn);
        q = `SELECT w.action_index, w.contract_index, a1.address AS source_address, t1.tick AS tick, w.amount, s1.status AS status
             FROM withdrawals w
                INNER JOIN actions a ON (a.action_index=w.action_index)
                LEFT  JOIN index_addresses a1 ON (a1.id=w.source_id)
                LEFT  JOIN index_tickers   t1 ON (t1.id=w.tick_id)
                LEFT  JOIN index_statuses  s1 ON (s1.id=w.status_id)
             WHERE a.block_index=?
             ORDER BY w.action_index ASC, w.contract_index ASC, a1.address COLLATE utf8_bin ASC, t1.tick COLLATE utf8mb4_bin ASC, w.amount ASC, s1.status COLLATE utf8_bin ASC`;
        contracts.withdrawals = await this.doQueryStrict(q, [block_index], conn);
        return { ledger, actions, contracts };
    },

    // `opts` is forwarded to doQuery, so a caller whose NULL answer gates a destructive
    // branch can pass { rethrow: true }. It matters because outside a transaction doQuery
    // collapses a query error into [], which reads here as "no actions in range" (M-17).
    // The default stays fail-soft for SnapshotBuilder, which only sizes a window with it.
    async getFirstActionIndex(block_index, conn, opts){
        let query = `SELECT action_index FROM actions a WHERE a.block_index >= ? ORDER BY a.action_index ASC LIMIT 1`;
        let rows  = await this.doQuery(query, [block_index], conn, opts);
        if(rows.length > 0)
            return Number(rows[0].action_index);
        return null;
    },

    // Resolve a status name to its local index_statuses id. index_statuses is replicated, so the
    // id resolves consistently against the replica's own *.status_id values. Used by
    // ClientRollback's cooldown-maturity reversal mirror. Returns null if the status is absent
    // (e.g. 'completed' never created because no cooldown has matured); the caller then skips.
    // `opts` is forwarded to doQuery, so a consensus-input caller can pass
    // { rethrow: true }. It matters because a swallowed non-transactional error here
    // returns null, and the stake readers below turn a null status id into an EMPTY
    // stake set over a populated stakes table: an M-17 wrong answer, not an error
    // signal. Operational callers (rollback, cooldown credits, stateHash) keep the
    // fail-soft default deliberately.
    async getStatusId(status, opts){
        let rows = await this.doQuery("SELECT id FROM index_statuses WHERE status = ? LIMIT 1", [status], undefined, opts);
        return rows.length > 0 ? Number(rows[0].id) : null;
    },

    // Get all rows from a table for actions in a given block (action_index-scoped tables).
    // Indexer-only: decoder DB has no actions table.
    // Scope by the ACTION's own block_index, NOT a transactions join: protocol-generated
    // actions (ORDER_MATCH / SWAP_MATCH / *_EXPIRE) carry tx_index = NULL with no transactions
    // row, so the old tx-join dropped their ledger rows (match settlements, expiry refunds) from
    // the payload while the consensus hash now includes them. A follower would then recompute a
    // divergent hash and halt. a.block_index is set for every action, so this streams them and
    // matches BlockHasher.
    async getActionScopedRows(table, block_index, conn){
        let query = `SELECT t.* FROM \`${table}\` t
            INNER JOIN actions a ON (a.action_index = t.action_index)
            WHERE a.block_index = ?
            ORDER BY t.action_index ASC`;
        return await this.doQuery(query, [block_index], conn);
    },

    // Discover in ONE round-trip which action-scoped tables actually carry rows in a
    // block, so the payload builder can fetch only those. Without it _buildBlockPayload
    // issues getActionScopedRows once per table in the lifecycle registry (86 today),
    // empty ones included, and that count rises with every replicated table added
    // over time.
    //
    // The existence predicate is getActionScopedRows' predicate verbatim (same INNER
    // JOIN on action_index, same a.block_index = ?), so "absent from this Set" means
    // exactly "getActionScopedRows would have returned zero rows". That equivalence is
    // the whole safety argument: the payload feeds a consensus hash followers recompute,
    // so a skip that is not provably empty would halt them.
    //
    // Candidates are filtered through listExistingTables first. A source legitimately
    // predates a table family, and where the per-table loop absorbs that as a skippable
    // schema gap (errno 1146), one missing table would fail the whole UNION. Callers
    // still fall back to the full loop when this throws, so a probe fault costs
    // round-trips, never rows.
    //
    // doQueryStrict, not doQuery: outside a transaction doQuery is fail-soft and returns
    // [] on error, which here would read as "every table is empty" and silently empty the
    // block.
    async getNonEmptyActionScopedTables(tables, block_index, conn){
        let present = await this.listExistingTables(conn);
        let candidates = [];
        for(let table of tables){
            assertValidIdentifier(table);
            if(present.has(table)) candidates.push(table);
        }
        if(candidates.length === 0) return new Set();
        // Table name is safe to inline as a literal: assertValidIdentifier above admits
        // only [A-Za-z0-9_].
        let branches = candidates.map(table =>
            "(SELECT '" + table + "' AS tbl FROM `" + table + "` t" +
            " INNER JOIN actions a ON (a.action_index = t.action_index)" +
            " WHERE a.block_index = ? LIMIT 1)");
        let rows = await this.doQueryStrict(branches.join(' UNION ALL '),
                                            candidates.map(() => block_index), conn);
        return new Set(rows.map(r => r.tbl));
    },

    // Get all contract_emissions rows for a block, including INTERNAL emissions whose
    // action_index IS NULL (e.g. a SLASH). The generic getActionScopedRows() above joins
    // on t.action_index, so its INNER JOIN drops NULL-action_index rows. The consensus
    // contract_hash (BlockHasher) includes them via the execution_index -> contract_executions
    // chain. Streaming via that same chain keeps the server payload and the hash in agreement,
    // so a follower's recompute can't diverge. Query is kept byte-aligned with BlockHasher's
    // emissions query (same joins, columns, and ORDER BY). Select the four protocol columns
    // explicitly (not em.*, which would carry the AUTO_INCREMENT `id` and break idempotent
    // re-apply after a reorg).
    async getEmissionRowsForBlock(block_index, conn){
        let query = `SELECT em.execution_index, em.emitted_action, em.action_index, em.position
            FROM contract_emissions em
            INNER JOIN contract_executions ce ON (ce.action_index = em.execution_index)
            INNER JOIN actions a ON (a.action_index = ce.action_index)
            WHERE a.block_index = ?
            ORDER BY em.execution_index ASC, em.position ASC`;
        return await this.doQuery(query, [block_index], conn);
    },

    // Get actions for a given block. Scope by the action's own block_index (not a transactions
    // join): protocol-generated actions (ORDER_MATCH / SWAP_MATCH / *_EXPIRE) have tx_index = NULL
    // and must still stream to followers (and are now in the consensus hash).
    async getActions(block_index, conn){
        let query = `SELECT a.* FROM actions a
            WHERE a.block_index = ?
            ORDER BY a.action_index ASC`;
        return await this.doQuery(query, [block_index], conn);
    },

};
